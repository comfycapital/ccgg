import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from py_clob_client_v2 import ApiCreds, BookParams, ClobClient, MarketOrderArgsV2, OrderType
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.exceptions import PolyException
from py_clob_client_v2.order_utils import SignatureTypeV2


SCRIPT_NAME = "buy_yes_rank_1_at_03_utc_paris_temperature"
SNAPSHOT_HOUR_UTC = 3
TARGET_MARKET_RANK = 1

ENV_FILE = Path(".env")
STATE_FILE = Path(f"{SCRIPT_NAME}_state.json")
BUY_ORDER_LOG_FILE = Path(f"{SCRIPT_NAME}.log")
LOGGER_NAME = SCRIPT_NAME
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = POLYGON
DEFAULT_GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DEFAULT_GAMMA_TAG_ID = 84
DEFAULT_GAMMA_TAG_SLUG = "paris"
DEFAULT_GAMMA_LIMIT = 100
DEFAULT_GAMMA_START_DATE_LOOKBACK_DAYS = 7
DEFAULT_MAX_BUY_PRICE = 0.99
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_SNAPSHOT_GRACE_SECONDS = 300

REQUEST_TIMEOUT_SECONDS = 30
MIN_BUY_AMOUNT_USDC = 5.0
PRICE_UNIT_MAX = 1.0
PRICE_PERCENT_MAX = 100.0

MARKET_TIMEZONE_NAME = "Europe/Paris"
TARGET_EVENT_TEXT = "highest temperature in paris"
TARGET_SERIES_SLUG = "paris-daily-highest-temperature"
LOWEST_SERIES_TEXT = "lowest temperature"

BUY_SIDE = "BUY"
YES_OUTCOME = "yes"
MARKET_ORDER_TYPE = OrderType.FAK
CLOB_NO_MATCH_MESSAGE = "no match"
NO_ASK_LIQUIDITY_REASON = "no_ask_liquidity"
NO_PARTIAL_FILL_LIQUIDITY_REASON = "no_partial_fill_liquidity"

PRIVATE_KEY_ENV = "POLY_PRIVATE_KEY"
API_KEY_ENV = "POLY_API_KEY"
API_SECRET_ENV = "POLY_API_SECRET"
API_PASSPHRASE_ENV = "POLY_API_PASSPHRASE"
SIGNATURE_TYPE_ENV = "POLY_SIGNATURE_TYPE"
FUNDER_ENV = "POLY_FUNDER"
CLOB_HOST_ENV = "CLOB_HOST"
CHAIN_ID_ENV = "POLY_CHAIN_ID"
BUY_AMOUNT_ENV = "BUY_AMOUNT_USDC"
MAX_BUY_PRICE_ENV = "MAX_BUY_PRICE"
POLL_INTERVAL_SECONDS_ENV = "POLL_INTERVAL_SECONDS"
SNAPSHOT_GRACE_SECONDS_ENV = "SNAPSHOT_GRACE_SECONDS"
CONFIRM_BUY_ENV = "CONFIRM_BUY"
TARGET_MARKET_DATE_ENV = "TARGET_MARKET_DATE"
GAMMA_EVENTS_URL_ENV = "GAMMA_EVENTS_URL"
GAMMA_TAG_ID_ENV = "GAMMA_TAG_ID"
GAMMA_TAG_SLUG_ENV = "GAMMA_TAG_SLUG"
GAMMA_LIMIT_ENV = "GAMMA_LIMIT"
GAMMA_START_DATE_LOOKBACK_DAYS_ENV = "GAMMA_START_DATE_LOOKBACK_DAYS"

CONFIRM_BUY_VALUE = "yes"
EXPORT_PREFIX = "export "
ENV_SEPARATOR = "="
COMMENT_PREFIX = "#"
SINGLE_QUOTE = "'"
DOUBLE_QUOTE = '"'
TEMP_VALUE_PATTERN = re.compile(r"(-?\d+)\D*C", re.IGNORECASE)

PRICE_RESPONSE_LIST_KEYS = ("data", "prices", "results")
PRICE_RESPONSE_TOKEN_KEYS = ("token_id", "tokenId", "asset_id", "assetId")
PRICE_RESPONSE_SIDE_KEYS = ("side", "orderSide")
PRICE_RESPONSE_VALUE_KEYS = ("price", BUY_SIDE, BUY_SIDE.lower(), "ask", "bestAsk")


@dataclass(frozen=True)
class TemperatureMarket:
    market_id: str
    condition_id: str
    question: str
    slug: str
    group_item_title: str
    temperature_celsius: int
    yes_token_id: str


@dataclass(frozen=True)
class MarketSnapshot:
    target_date: str
    event: dict[str, Any]
    markets: list[TemperatureMarket]


@dataclass(frozen=True)
class RankedMarket:
    market: TemperatureMarket
    yes_price: float
    rank: int


def load_env_file() -> None:
    if not ENV_FILE.exists():
        return

    for env_line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = env_line.strip()

        if not line or line.startswith(COMMENT_PREFIX):
            continue

        if line.startswith(EXPORT_PREFIX):
            line = line[len(EXPORT_PREFIX):].strip()

        if ENV_SEPARATOR not in line:
            continue

        key, value = line.split(ENV_SEPARATOR, 1)
        key = key.strip()
        value = strip_wrapping_quotes(value.strip())

        if key:
            os.environ.setdefault(key, value)


def strip_wrapping_quotes(value: str) -> str:
    if len(value) < 2:
        return value

    if value[0] == value[-1] and value[0] in (SINGLE_QUOTE, DOUBLE_QUOTE):
        return value[1:-1]

    return value


def build_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler(BUY_ORDER_LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def log_json(logger: logging.Logger, message: str, payload: dict[str, Any]) -> None:
    logger.info("%s %s", message, json.dumps(payload, ensure_ascii=True, sort_keys=True))


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_optional_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value if value else None


def get_string_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def get_float_env(name: str, default: Optional[float] = None) -> float:
    value = os.getenv(name)

    if value is None or value == "":
        if default is None:
            raise ValueError(f"Missing required environment variable: {name}")
        return default

    return float(value)


def get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None or value == "":
        return default

    return int(value)


def normalize_probability_value(value: float) -> float:
    if PRICE_UNIT_MAX < value <= PRICE_PERCENT_MAX:
        return value / PRICE_PERCENT_MAX

    return value


def get_probability_env(name: str, default: float) -> float:
    value = normalize_probability_value(get_float_env(name, default))

    if value <= 0 or value > PRICE_UNIT_MAX:
        raise ValueError(f"{name} must be between 0 and 1, or between 0 and 100 percent.")

    return value


def validate_confirmation() -> None:
    confirmation = os.getenv(CONFIRM_BUY_ENV, "").strip().lower()

    if confirmation != CONFIRM_BUY_VALUE:
        raise ValueError(f"Set {CONFIRM_BUY_ENV}={CONFIRM_BUY_VALUE} to place orders.")


def validate_buy_amount(buy_amount_usdc: float) -> None:
    if buy_amount_usdc < MIN_BUY_AMOUNT_USDC:
        raise ValueError(f"{BUY_AMOUNT_ENV} must be at least {MIN_BUY_AMOUNT_USDC}.")


def validate_positive_seconds(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0.")


def build_api_creds() -> Optional[ApiCreds]:
    api_key = get_optional_env(API_KEY_ENV)
    api_secret = get_optional_env(API_SECRET_ENV)
    api_passphrase = get_optional_env(API_PASSPHRASE_ENV)
    values = [api_key, api_secret, api_passphrase]

    if all(values):
        return ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )

    if any(values):
        raise ValueError(
            f"Set all of {API_KEY_ENV}, {API_SECRET_ENV}, and {API_PASSPHRASE_ENV}, or set none."
        )

    return None


def get_or_create_api_creds(client: ClobClient) -> ApiCreds:
    try:
        return client.derive_api_key()
    except Exception:
        return client.create_api_key()


def build_client() -> ClobClient:
    private_key = get_required_env(PRIVATE_KEY_ENV)
    api_creds = build_api_creds()
    host = get_string_env(CLOB_HOST_ENV, DEFAULT_CLOB_HOST)
    chain_id = get_int_env(CHAIN_ID_ENV, DEFAULT_CHAIN_ID)
    signature_type = SignatureTypeV2(get_int_env(SIGNATURE_TYPE_ENV, int(SignatureTypeV2.EOA)))
    funder = get_optional_env(FUNDER_ENV)

    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        creds=api_creds,
        signature_type=signature_type,
        funder=funder,
        use_server_time=True,
    )

    if api_creds is None:
        client.set_api_creds(get_or_create_api_creds(client))

    return client


def get_market_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(MARKET_TIMEZONE_NAME)
    except ZoneInfoNotFoundError as error:
        raise RuntimeError(
            f"Cannot load {MARKET_TIMEZONE_NAME}. Set {TARGET_MARKET_DATE_ENV}=YYYY-MM-DD "
            "or install Python timezone data."
        ) from error


def resolve_target_market_date() -> str:
    target_market_date = os.getenv(TARGET_MARKET_DATE_ENV)

    if target_market_date:
        return target_market_date

    return datetime.now(get_market_timezone()).date().isoformat()


def build_snapshot_time_utc(target_date: str) -> datetime:
    parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    return datetime.combine(
        parsed_date,
        datetime_time(hour=SNAPSHOT_HOUR_UTC),
        tzinfo=timezone.utc,
    )


def build_gamma_start_date_min(target_date: str) -> str:
    parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    lookback_days = get_int_env(
        GAMMA_START_DATE_LOOKBACK_DAYS_ENV,
        DEFAULT_GAMMA_START_DATE_LOOKBACK_DAYS,
    )
    lookback_date = parsed_date - timedelta(days=lookback_days)
    return f"{lookback_date.isoformat()}T00:00:00Z"


def parse_gamma_events(response_json: Any) -> list[dict[str, Any]]:
    if isinstance(response_json, list):
        events = response_json
    elif isinstance(response_json, dict):
        events = response_json.get("data")
    else:
        raise ValueError("Gamma events response is not a list or dictionary.")

    if not isinstance(events, list):
        raise ValueError("Gamma events response does not contain an events list.")

    return [event for event in events if isinstance(event, dict)]


def get_gamma_events(target_date: str) -> list[dict[str, Any]]:
    response = requests.get(
        get_string_env(GAMMA_EVENTS_URL_ENV, DEFAULT_GAMMA_EVENTS_URL),
        params={
            "tag_id": get_int_env(GAMMA_TAG_ID_ENV, DEFAULT_GAMMA_TAG_ID),
            "tag_slug": get_string_env(GAMMA_TAG_SLUG_ENV, DEFAULT_GAMMA_TAG_SLUG),
            "start_date_min": build_gamma_start_date_min(target_date),
            "limit": get_int_env(GAMMA_LIMIT_ENV, DEFAULT_GAMMA_LIMIT),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return parse_gamma_events(response.json())


def is_active_open_event(event: dict[str, Any]) -> bool:
    return event.get("closed") is False and event.get("active") is True


def is_target_temperature_event(event: dict[str, Any], target_date: str) -> bool:
    event_text = " ".join(
        str(event.get(key) or "")
        for key in ("title", "slug", "seriesSlug")
    ).lower()

    if event.get("eventDate") != target_date:
        return False

    if not is_active_open_event(event):
        return False

    if LOWEST_SERIES_TEXT in event_text:
        return False

    return TARGET_EVENT_TEXT in event_text or TARGET_SERIES_SLUG in event_text


def select_target_event(events: list[dict[str, Any]], target_date: str) -> dict[str, Any]:
    matching_events = [
        event
        for event in events
        if is_target_temperature_event(event, target_date)
    ]

    if not matching_events:
        raise ValueError(f"No active open Paris highest-temperature event found for {target_date}.")

    if len(matching_events) > 1:
        event_ids = [event.get("id") for event in matching_events]
        raise ValueError(f"Multiple target events found for {target_date}: {event_ids}")

    return matching_events[0]


def parse_json_list(value: Any, field_name: str) -> list[str]:
    if isinstance(value, str):
        parsed_value = json.loads(value)
    elif isinstance(value, list):
        parsed_value = value
    else:
        raise ValueError(f"{field_name} is not a JSON string or list.")

    if not isinstance(parsed_value, list):
        raise ValueError(f"{field_name} does not contain a list.")

    return [str(item) for item in parsed_value]


def get_outcome_token_id(market: dict[str, Any], outcome_name: str) -> str:
    outcomes = parse_json_list(market.get("outcomes"), "outcomes")
    token_ids = parse_json_list(market.get("clobTokenIds"), "clobTokenIds")

    if len(outcomes) != len(token_ids):
        raise ValueError("outcomes and clobTokenIds have different lengths.")

    for outcome, token_id in zip(outcomes, token_ids):
        if outcome.strip().lower() == outcome_name:
            return token_id

    raise ValueError(f"{outcome_name} outcome token was not found.")


def parse_market_temperature(market: dict[str, Any]) -> int:
    for field_name in ("groupItemTitle", "question", "slug"):
        text = str(market.get(field_name) or "")
        match = TEMP_VALUE_PATTERN.search(text)

        if match:
            return int(match.group(1))

    raise ValueError("Could not parse market temperature.")


def is_tradeable_market(market: dict[str, Any]) -> bool:
    return (
        market.get("closed") is False
        and market.get("active") is True
        and market.get("acceptingOrders") is True
    )


def build_temperature_market(market: dict[str, Any]) -> TemperatureMarket:
    return TemperatureMarket(
        market_id=str(market.get("id") or ""),
        condition_id=str(market.get("conditionId") or ""),
        question=str(market.get("question") or ""),
        slug=str(market.get("slug") or ""),
        group_item_title=str(market.get("groupItemTitle") or ""),
        temperature_celsius=parse_market_temperature(market),
        yes_token_id=get_outcome_token_id(market, YES_OUTCOME),
    )


def build_temperature_markets(
    event: dict[str, Any],
    logger: logging.Logger,
) -> list[TemperatureMarket]:
    markets = event.get("markets")

    if not isinstance(markets, list):
        raise ValueError("Target event does not contain a markets list.")

    temperature_markets = []

    for market in markets:
        if not isinstance(market, dict) or not is_tradeable_market(market):
            continue

        try:
            temperature_markets.append(build_temperature_market(market))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            log_json(
                logger,
                "market_skipped",
                {
                    "error": str(error),
                    "market_id": market.get("id"),
                    "question": market.get("question"),
                },
            )

    return sorted(temperature_markets, key=lambda item: item.temperature_celsius)


def load_market_snapshot(target_date: str, logger: logging.Logger) -> MarketSnapshot:
    events = get_gamma_events(target_date)
    event = select_target_event(events, target_date)
    markets = build_temperature_markets(event, logger)

    if not markets:
        raise ValueError(f"No tradeable temperature markets found for event {event.get('id')}.")

    log_json(
        logger,
        "markets_loaded",
        {
            "event_id": event.get("id"),
            "market_count": len(markets),
            "market_ids": [market.market_id for market in markets],
            "target_date": target_date,
            "title": event.get("title"),
        },
    )
    return MarketSnapshot(target_date=target_date, event=event, markets=markets)


def parse_numeric_price(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return normalize_probability_value(float(value))

    if isinstance(value, str):
        text = value.strip()

        if not text:
            return None

        return normalize_probability_value(float(text))

    return None


def parse_price_payload(value: Any) -> Optional[float]:
    direct_price = parse_numeric_price(value)

    if direct_price is not None:
        return direct_price

    if not isinstance(value, dict):
        return None

    for price_key in PRICE_RESPONSE_VALUE_KEYS:
        if price_key not in value:
            continue

        price = parse_numeric_price(value.get(price_key))

        if price is not None:
            return price

    return None


def get_first_mapping_value(source: dict[str, Any], keys: tuple[str, ...]) -> Optional[Any]:
    for key in keys:
        if key in source:
            return source.get(key)

    return None


def extract_prices_from_entries(
    entries: list[Any],
    requested_token_ids: set[str],
) -> dict[str, float]:
    prices: dict[str, float] = {}

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        token_id = get_first_mapping_value(entry, PRICE_RESPONSE_TOKEN_KEYS)

        if token_id is None:
            continue

        token_id_text = str(token_id)

        if token_id_text not in requested_token_ids:
            continue

        side = get_first_mapping_value(entry, PRICE_RESPONSE_SIDE_KEYS)

        if side is not None and str(side).upper() != BUY_SIDE:
            continue

        price = parse_price_payload(entry)

        if price is not None:
            prices[token_id_text] = price

    return prices


def extract_prices_from_keyed_dict(
    response_json: dict[str, Any],
    requested_token_ids: set[str],
) -> dict[str, float]:
    prices: dict[str, float] = {}

    for token_id in requested_token_ids:
        if token_id not in response_json:
            continue

        price = parse_price_payload(response_json.get(token_id))

        if price is not None:
            prices[token_id] = price

    return prices


def extract_yes_prices(
    response_json: Any,
    markets: list[TemperatureMarket],
) -> dict[str, float]:
    requested_token_ids = {market.yes_token_id for market in markets}

    if isinstance(response_json, list):
        return extract_prices_from_entries(response_json, requested_token_ids)

    if not isinstance(response_json, dict):
        return {}

    prices = extract_prices_from_keyed_dict(response_json, requested_token_ids)

    for list_key in PRICE_RESPONSE_LIST_KEYS:
        list_value = response_json.get(list_key)

        if isinstance(list_value, list):
            prices.update(extract_prices_from_entries(list_value, requested_token_ids))

        if isinstance(list_value, dict):
            prices.update(extract_prices_from_keyed_dict(list_value, requested_token_ids))

    return prices


def get_yes_prices(
    client: ClobClient,
    markets: list[TemperatureMarket],
) -> dict[str, float]:
    price_requests = [
        BookParams(token_id=market.yes_token_id, side=BUY_SIDE)
        for market in markets
    ]
    response = client.get_prices(price_requests)
    return extract_yes_prices(response, markets)


def rank_markets_by_yes_price(
    markets: list[TemperatureMarket],
    prices_by_token_id: dict[str, float],
) -> list[RankedMarket]:
    priced_markets = [
        (market, prices_by_token_id[market.yes_token_id])
        for market in markets
        if market.yes_token_id in prices_by_token_id
    ]
    priced_markets.sort(key=lambda item: (-item[1], item[0].temperature_celsius, item[0].market_id))

    return [
        RankedMarket(market=market, yes_price=yes_price, rank=index + 1)
        for index, (market, yes_price) in enumerate(priced_markets)
    ]


def build_price_snapshot(ranked_markets: list[RankedMarket]) -> list[dict[str, Any]]:
    return [
        {
            "market_id": ranked_market.market.market_id,
            "question": ranked_market.market.question,
            "rank": ranked_market.rank,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "yes_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        }
        for ranked_market in ranked_markets
    ]

