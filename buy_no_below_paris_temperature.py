import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from py_clob_client_v2 import ApiCreds, ClobClient, MarketOrderArgsV2, OrderType
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.exceptions import PolyException
from py_clob_client_v2.order_utils import SignatureTypeV2

from print_temperature import (
    TEMPERATURE_KEYS,
    VALIDITY_TIME_KEYS,
    fetch_observation,
    get_first_value,
    iter_values,
    normalize_temperature,
)


ENV_FILE = Path(".env")

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = POLYGON
DEFAULT_MAX_PRICE = 0.35
MIN_BUY_AMOUNT_USDC = 5.0

GAMMA_EVENTS_URL = os.getenv(
    "GAMMA_EVENTS_URL",
    "https://gamma-api.polymarket.com/events",
)
GAMMA_TAG_ID = int(os.getenv("GAMMA_TAG_ID", "84"))
GAMMA_TAG_SLUG = os.getenv("GAMMA_TAG_SLUG", "paris")
GAMMA_LIMIT = int(os.getenv("GAMMA_LIMIT", "100"))
GAMMA_START_DATE_LOOKBACK_DAYS = int(os.getenv("GAMMA_START_DATE_LOOKBACK_DAYS", "7"))
REQUEST_TIMEOUT_SECONDS = 30

MARKET_TIMEZONE_NAME = "Europe/Paris"
TARGET_MARKET_DATE_ENV = "TARGET_MARKET_DATE"
TARGET_EVENT_TEXT = "highest temperature in paris"
TARGET_SERIES_SLUG = "paris-daily-highest-temperature"
LOWEST_SERIES_TEXT = "lowest temperature"

POLL_WINDOW_MINUTES = (6, 36)
VALIDITY_MINUTE_BY_POLL_MINUTE = {
    6: 0,
    36: 30,
}
OBSERVATION_POLL_INTERVAL_SECONDS = 2
MAX_OBSERVATION_WAIT_SECONDS = 20 * 60

BUY_SIDE = "BUY"
MARKET_ORDER_TYPE = OrderType.FAK
NO_OUTCOME = "no"
YES_OUTCOME = "yes"
RANGE_EXACT = "exact"
RANGE_BELOW = "below"
RANGE_ABOVE = "above"
CLOB_NO_MATCH_MESSAGE = "no match"
NO_ASK_LIQUIDITY_REASON = "no_ask_liquidity"
NO_PARTIAL_FILL_LIQUIDITY_REASON = "no_partial_fill_liquidity"

BUY_ORDER_LOG_FILE = Path("buy_no_below_paris_temperature.log")
LOGGER_NAME = "buy_no_below_paris_temperature"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

PRIVATE_KEY_ENV = "POLY_PRIVATE_KEY"
API_KEY_ENV = "POLY_API_KEY"
API_SECRET_ENV = "POLY_API_SECRET"
API_PASSPHRASE_ENV = "POLY_API_PASSPHRASE"
SIGNATURE_TYPE_ENV = "POLY_SIGNATURE_TYPE"
FUNDER_ENV = "POLY_FUNDER"
CLOB_HOST_ENV = "CLOB_HOST"
CHAIN_ID_ENV = "POLY_CHAIN_ID"
BUY_AMOUNT_ENV = "BUY_AMOUNT_USDC"
MAX_PRICE_ENV = "MAX_PRICE"
CONFIRM_BUY_ENV = "CONFIRM_BUY"

CONFIRM_BUY_VALUE = "yes"
EXPORT_PREFIX = "export "
ENV_SEPARATOR = "="
COMMENT_PREFIX = "#"
SINGLE_QUOTE = "'"
DOUBLE_QUOTE = '"'
ISO_Z_SUFFIX = "Z"
ISO_UTC_OFFSET = "+00:00"
TEMP_VALUE_PATTERN = re.compile(r"(-?\d+)\D*C", re.IGNORECASE)


@dataclass(frozen=True)
class TemperatureMarket:
    market_id: str
    condition_id: str
    question: str
    slug: str
    temperature_celsius: int
    range_type: str
    no_token_id: str


@dataclass(frozen=True)
class Observation:
    temperature_celsius: float
    validity_time: datetime
    raw_validity_time: Any


def load_env_file() -> None:
    if not ENV_FILE.exists():
        return

    env_lines = ENV_FILE.read_text(encoding="utf-8").splitlines()

    for env_line in env_lines:
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


def validate_confirmation() -> None:
    confirmation = os.getenv(CONFIRM_BUY_ENV, "").strip().lower()

    if confirmation != CONFIRM_BUY_VALUE:
        raise ValueError(f"Set {CONFIRM_BUY_ENV}={CONFIRM_BUY_VALUE} to place orders.")


def validate_buy_amount(buy_amount_usdc: float) -> None:
    if buy_amount_usdc < MIN_BUY_AMOUNT_USDC:
        raise ValueError(
            f"{BUY_AMOUNT_ENV} must be at least {MIN_BUY_AMOUNT_USDC} for this market."
        )


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
    host = os.getenv(CLOB_HOST_ENV, DEFAULT_CLOB_HOST)
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


def get_target_market_date(logger: logging.Logger) -> str:
    target_market_date = os.getenv(TARGET_MARKET_DATE_ENV)

    if target_market_date:
        return target_market_date

    try:
        market_timezone = ZoneInfo(MARKET_TIMEZONE_NAME)
    except ZoneInfoNotFoundError as error:
        raise RuntimeError(
            f"Cannot load {MARKET_TIMEZONE_NAME}. Set {TARGET_MARKET_DATE_ENV}=YYYY-MM-DD "
            "or install Python timezone data."
        ) from error

    today = datetime.now(market_timezone).date()
    target_date = today.isoformat()
    log_json(logger, "target_market_date", {"date": target_date, "timezone": MARKET_TIMEZONE_NAME})
    return target_date


def build_gamma_start_date_min(target_date: str) -> str:
    parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    lookback_date = parsed_date - timedelta(days=GAMMA_START_DATE_LOOKBACK_DAYS)
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
        GAMMA_EVENTS_URL,
        params={
            "tag_id": GAMMA_TAG_ID,
            "tag_slug": GAMMA_TAG_SLUG,
            "start_date_min": build_gamma_start_date_min(target_date),
            "limit": GAMMA_LIMIT,
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


def get_no_token_id(market: dict[str, Any]) -> str:
    outcomes = parse_json_list(market.get("outcomes"), "outcomes")
    token_ids = parse_json_list(market.get("clobTokenIds"), "clobTokenIds")

    if len(outcomes) != len(token_ids):
        raise ValueError("outcomes and clobTokenIds have different lengths.")

    for outcome, token_id in zip(outcomes, token_ids):
        if outcome.strip().lower() == NO_OUTCOME:
            return token_id

    raise ValueError("No outcome token was not found.")


def parse_market_temperature(market: dict[str, Any]) -> int:
    for field_name in ("groupItemTitle", "question", "slug"):
        text = str(market.get(field_name) or "")
        match = TEMP_VALUE_PATTERN.search(text)

        if match:
            return int(match.group(1))

    raise ValueError("Could not parse market temperature.")


def parse_market_range_type(market: dict[str, Any]) -> str:
    text = " ".join(
        str(market.get(field_name) or "")
        for field_name in ("groupItemTitle", "question", "slug")
    ).lower()

    if "or below" in text or "orbelow" in text or "or lower" in text:
        return RANGE_BELOW

    if "or higher" in text or "orhigher" in text or "or above" in text:
        return RANGE_ABOVE

    return RANGE_EXACT


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
        temperature_celsius=parse_market_temperature(market),
        range_type=parse_market_range_type(market),
        no_token_id=get_no_token_id(market),
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

