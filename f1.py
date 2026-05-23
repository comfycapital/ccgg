import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from py_clob_client_v2 import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    BookParams,
    ClobClient,
    MarketOrderArgsV2,
    OpenOrderParams,
    OrderArgsV2,
    OrderMarketCancelParams,
    OrderType,
)
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.order_utils import SignatureTypeV2


ENV_FILE = Path(".env")

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = POLYGON
DEFAULT_GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DEFAULT_GAMMA_TAG_ID = 84
DEFAULT_GAMMA_TAG_SLUG = "paris"
DEFAULT_GAMMA_LIMIT = 100
DEFAULT_GAMMA_START_DATE_LOOKBACK_DAYS = 14

DEFAULT_POLL_INTERVAL_SECONDS = 10
DEFAULT_MARKET_REFRESH_INTERVAL_SECONDS = 300
DEFAULT_ENTRY_PRICE_THRESHOLD = 0.40
DEFAULT_TAKE_PROFIT_PRICE = 0.25
DEFAULT_LOW_EXIT_PRICE = 0.15
DEFAULT_LOW_EXIT_SECONDS = 180
DEFAULT_FORCE_EXIT_MIN_SELL_PRICE = "0.001"
DEFAULT_ORDER_SIZE_SHARES = 5.0
DEFAULT_MAX_ENTRY_MARKETS_PER_EVENT = 2
DEFAULT_LIMIT_ORDER_PRICES = "0.5c,1c,2c,5c"
DEFAULT_MARKET_ORDER_MIN_SIZE = 5.0

REQUEST_TIMEOUT_SECONDS = 30
PRICE_UNIT_MAX = 1.0
PRICE_PERCENT_MAX = 100.0
MIN_UTC_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
CONDITIONAL_TOKEN_DECIMAL_FACTOR = Decimal("1000000")
MIN_POSITION_TO_SELL = 0.000001

MARKET_TIMEZONE_NAME = "Europe/Paris"
TARGET_EVENT_TEXT = "highest temperature in paris"
TARGET_EVENT_SLUG_TEXT = "highest-temperature-in-paris"
LOWEST_SERIES_TEXT = "lowest temperature"

BUY_SIDE = "BUY"
SELL_SIDE = "SELL"
YES_OUTCOME = "yes"
LIMIT_ORDER_TYPE = OrderType.GTC
MARKET_SELL_ORDER_TYPE = OrderType.FAK

BUY_ORDER_LOG_FILE = Path("fat_fingers_paris_temperature.log")
LOGGER_NAME = "fat_fingers_paris_temperature"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

PRIVATE_KEY_ENV = "POLY_PRIVATE_KEY"
API_KEY_ENV = "POLY_API_KEY"
API_SECRET_ENV = "POLY_API_SECRET"
API_PASSPHRASE_ENV = "POLY_API_PASSPHRASE"
SIGNATURE_TYPE_ENV = "POLY_SIGNATURE_TYPE"
FUNDER_ENV = "POLY_FUNDER"
CLOB_HOST_ENV = "CLOB_HOST"
CHAIN_ID_ENV = "POLY_CHAIN_ID"
CONFIRM_BUY_ENV = "CONFIRM_BUY"

GAMMA_EVENTS_URL_ENV = "GAMMA_EVENTS_URL"
GAMMA_TAG_ID_ENV = "GAMMA_TAG_ID"
GAMMA_TAG_SLUG_ENV = "GAMMA_TAG_SLUG"
GAMMA_LIMIT_ENV = "GAMMA_LIMIT"
GAMMA_START_DATE_MIN_ENV = "GAMMA_START_DATE_MIN"
GAMMA_START_DATE_LOOKBACK_DAYS_ENV = "GAMMA_START_DATE_LOOKBACK_DAYS"

POLL_INTERVAL_SECONDS_ENV = "POLL_INTERVAL_SECONDS"
MARKET_REFRESH_INTERVAL_SECONDS_ENV = "MARKET_REFRESH_INTERVAL_SECONDS"
ENTRY_PRICE_THRESHOLD_ENV = "ENTRY_PRICE_THRESHOLD"
TAKE_PROFIT_PRICE_ENV = "TAKE_PROFIT_PRICE"
LOW_EXIT_PRICE_ENV = "LOW_EXIT_PRICE"
LOW_EXIT_SECONDS_ENV = "LOW_EXIT_SECONDS"
FORCE_EXIT_MIN_SELL_PRICE_ENV = "FORCE_EXIT_MIN_SELL_PRICE"
ORDER_SIZE_SHARES_ENV = "ORDER_SIZE_SHARES"
MAX_ENTRY_MARKETS_PER_EVENT_ENV = "MAX_ENTRY_MARKETS_PER_EVENT"
LIMIT_ORDER_PRICES_ENV = "LIMIT_ORDER_PRICES"

CONFIRM_BUY_VALUE = "yes"
EXPORT_PREFIX = "export "
ENV_SEPARATOR = "="
COMMENT_PREFIX = "#"
SINGLE_QUOTE = "'"
DOUBLE_QUOTE = '"'
ISO_Z_SUFFIX = "Z"
ISO_UTC_OFFSET = "+00:00"
CENT_SUFFIX = "c"
TEMP_VALUE_PATTERN = re.compile(r"(-?\d+)\D*C", re.IGNORECASE)

PRICE_RESPONSE_LIST_KEYS = ("data", "prices", "results")
PRICE_RESPONSE_TOKEN_KEYS = ("token_id", "tokenId", "asset_id", "assetId")
PRICE_RESPONSE_SIDE_KEYS = ("side", "orderSide")
PRICE_RESPONSE_VALUE_KEYS_BY_SIDE = {
    BUY_SIDE: ("price", BUY_SIDE, BUY_SIDE.lower(), "ask", "bestAsk"),
    SELL_SIDE: ("price", SELL_SIDE, SELL_SIDE.lower(), "bid", "bestBid"),
}

BALANCE_RESPONSE_KEYS = (
    "balance",
    "available_balance",
    "availableBalance",
    "asset_balance",
    "assetBalance",
    "amount",
)
OPEN_ORDER_PRICE_KEYS = ("price", "orderPrice", "original_price", "originalPrice")
OPEN_ORDER_SIDE_KEYS = ("side", "orderSide")
OPEN_ORDER_ASSET_KEYS = ("asset_id", "assetId", "token_id", "tokenId")


@dataclass(frozen=True)
class StrategyConfig:
    poll_interval_seconds: int
    market_refresh_interval_seconds: int
    entry_price_threshold: float
    take_profit_price: float
    low_exit_price: float
    low_exit_seconds: int
    force_exit_min_sell_price: Decimal
    order_size_shares: float
    max_entry_markets_per_event: int
    limit_order_prices: tuple[Decimal, ...]


@dataclass(frozen=True)
class TemperatureMarket:
    event_id: str
    event_title: str
    event_date: str
    event_cutoff_time: datetime
    market_id: str
    condition_id: str
    question: str
    slug: str
    group_item_title: str
    temperature_celsius: int
    yes_token_id: str
    tick_size: Optional[Decimal]
    order_min_size: float
    active: bool
    closed: bool
    accepting_orders: bool


@dataclass(frozen=True)
class TemperatureEvent:
    event_id: str
    title: str
    slug: str
    event_date: str
    cutoff_time: datetime
    markets: list[TemperatureMarket]


@dataclass
class RuntimeState:
    events: list[TemperatureEvent]
    markets_by_id: dict[str, TemperatureMarket]
    next_market_refresh_time: datetime
    posted_order_prices_by_market: dict[str, set[Decimal]]
    low_price_started_at_by_market: dict[str, datetime]
    deactivated_market_ids: set[str]
    cutoff_event_ids: set[str]


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


def get_string_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)

    if value is None or value == "":
        return default

    return int(value)


def get_float_env(name: str, default: Optional[float] = None) -> float:
    value = os.getenv(name)

    if value is None or value == "":
        if default is None:
            raise ValueError(f"Missing required environment variable: {name}")
        return default

    return float(value)


def normalize_probability_value(value: float) -> float:
    if PRICE_UNIT_MAX < value <= PRICE_PERCENT_MAX:
        return value / PRICE_PERCENT_MAX

    return value


def get_probability_env(name: str, default: float) -> float:
    value = normalize_probability_value(get_float_env(name, default))

    if value <= 0 or value > PRICE_UNIT_MAX:
        raise ValueError(f"{name} must be between 0 and 1, or between 0 and 100 percent.")

    return value


def parse_decimal_text(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"Invalid decimal value: {value}") from error


def parse_limit_order_price(value: str) -> Decimal:
    text = value.strip().lower()

    if not text:
        raise ValueError("Limit order price cannot be empty.")

    if text.endswith(CENT_SUFFIX):
        return parse_decimal_text(text[:-1].strip()) / Decimal("100")

    price = parse_decimal_text(text)

    if price <= 0 or price > Decimal("1"):
        raise ValueError("Limit order prices must be between 0 and 1, or use a c suffix.")

    return price


def get_limit_order_prices() -> tuple[Decimal, ...]:
    raw_value = os.getenv(LIMIT_ORDER_PRICES_ENV, DEFAULT_LIMIT_ORDER_PRICES)
    prices = tuple(parse_limit_order_price(item) for item in raw_value.split(",") if item.strip())

    if not prices:
        raise ValueError(f"{LIMIT_ORDER_PRICES_ENV} must contain at least one price.")

    return prices


def get_decimal_probability_env(name: str, default: str) -> Decimal:
    value = parse_decimal_text(os.getenv(name, default))

    if value <= 0 or value > Decimal("1"):
        raise ValueError(f"{name} must be between 0 and 1.")

    return value


def validate_confirmation() -> None:
    confirmation = os.getenv(CONFIRM_BUY_ENV, "").strip().lower()

    if confirmation != CONFIRM_BUY_VALUE:
        raise ValueError(f"Set {CONFIRM_BUY_ENV}={CONFIRM_BUY_VALUE} to place orders.")


def validate_positive_seconds(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0.")


def validate_positive_float(name: str, value: float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0.")


def validate_positive_int(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0.")


def build_strategy_config() -> StrategyConfig:
    config = StrategyConfig(
        poll_interval_seconds=get_int_env(
            POLL_INTERVAL_SECONDS_ENV,
            DEFAULT_POLL_INTERVAL_SECONDS,
        ),
        market_refresh_interval_seconds=get_int_env(
            MARKET_REFRESH_INTERVAL_SECONDS_ENV,
            DEFAULT_MARKET_REFRESH_INTERVAL_SECONDS,
        ),
        entry_price_threshold=get_probability_env(
            ENTRY_PRICE_THRESHOLD_ENV,
            DEFAULT_ENTRY_PRICE_THRESHOLD,
        ),
        take_profit_price=get_probability_env(
            TAKE_PROFIT_PRICE_ENV,
            DEFAULT_TAKE_PROFIT_PRICE,
        ),
        low_exit_price=get_probability_env(
            LOW_EXIT_PRICE_ENV,
            DEFAULT_LOW_EXIT_PRICE,
        ),
        low_exit_seconds=get_int_env(LOW_EXIT_SECONDS_ENV, DEFAULT_LOW_EXIT_SECONDS),
        force_exit_min_sell_price=get_decimal_probability_env(
            FORCE_EXIT_MIN_SELL_PRICE_ENV,
            DEFAULT_FORCE_EXIT_MIN_SELL_PRICE,
        ),
        order_size_shares=get_float_env(ORDER_SIZE_SHARES_ENV, DEFAULT_ORDER_SIZE_SHARES),
        max_entry_markets_per_event=get_int_env(
            MAX_ENTRY_MARKETS_PER_EVENT_ENV,
            DEFAULT_MAX_ENTRY_MARKETS_PER_EVENT,
        ),
        limit_order_prices=get_limit_order_prices(),
    )

    validate_positive_seconds(POLL_INTERVAL_SECONDS_ENV, config.poll_interval_seconds)
    validate_positive_seconds(
        MARKET_REFRESH_INTERVAL_SECONDS_ENV,
        config.market_refresh_interval_seconds,
    )
    validate_positive_seconds(LOW_EXIT_SECONDS_ENV, config.low_exit_seconds)
    validate_positive_float(ORDER_SIZE_SHARES_ENV, config.order_size_shares)
    validate_positive_int(
        MAX_ENTRY_MARKETS_PER_EVENT_ENV,
        config.max_entry_markets_per_event,
    )

    return config


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
        raise RuntimeError(f"Cannot load {MARKET_TIMEZONE_NAME}. Install Python timezone data.") from error


def build_gamma_start_date_min() -> str:
    explicit_start_date_min = os.getenv(GAMMA_START_DATE_MIN_ENV)

    if explicit_start_date_min:
        return explicit_start_date_min

    market_timezone = get_market_timezone()
    lookback_days = get_int_env(
        GAMMA_START_DATE_LOOKBACK_DAYS_ENV,
        DEFAULT_GAMMA_START_DATE_LOOKBACK_DAYS,
    )
    lookback_date = datetime.now(market_timezone).date() - timedelta(days=lookback_days)
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


def get_gamma_events() -> list[dict[str, Any]]:
    response = requests.get(
        get_string_env(GAMMA_EVENTS_URL_ENV, DEFAULT_GAMMA_EVENTS_URL),
        params={
            "tag_id": get_int_env(GAMMA_TAG_ID_ENV, DEFAULT_GAMMA_TAG_ID),
            "tag_slug": get_string_env(GAMMA_TAG_SLUG_ENV, DEFAULT_GAMMA_TAG_SLUG),
            "start_date_min": build_gamma_start_date_min(),
            "limit": get_int_env(GAMMA_LIMIT_ENV, DEFAULT_GAMMA_LIMIT),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return parse_gamma_events(response.json())


def is_active_open_event(event: dict[str, Any]) -> bool:
    return event.get("closed") is False and event.get("active") is True


def is_target_temperature_event(event: dict[str, Any]) -> bool:
    event_text = " ".join(
        str(event.get(key) or "")
        for key in ("title", "slug", "seriesSlug")
    ).lower()

    if not is_active_open_event(event):
        return False

    if LOWEST_SERIES_TEXT in event_text:
        return False

    return TARGET_EVENT_TEXT in event_text or TARGET_EVENT_SLUG_TEXT in event_text


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


def parse_optional_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None

    return parse_decimal_text(str(value))


def parse_order_min_size(value: Any) -> float:
    if value is None or value == "":
        return DEFAULT_MARKET_ORDER_MIN_SIZE

    return float(value)


def parse_datetime_utc(value: Any) -> datetime:
    text = str(value).strip()

    if text.endswith(ISO_Z_SUFFIX):
        text = f"{text[:-1]}{ISO_UTC_OFFSET}"

    parsed_time = datetime.fromisoformat(text)

    if parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(tzinfo=timezone.utc)

    return parsed_time.astimezone(timezone.utc).replace(microsecond=0)


def parse_event_cutoff_time(event: dict[str, Any]) -> datetime:
    end_date = event.get("endDate")

    if end_date:
        return parse_datetime_utc(end_date)

    event_date = str(event.get("eventDate") or "")

    if not event_date:
        raise ValueError("Event does not contain endDate or eventDate.")

    fallback_date = datetime.strptime(event_date, "%Y-%m-%d").date()
    return datetime(
        fallback_date.year,
        fallback_date.month,
        fallback_date.day,
        12,
        0,
        tzinfo=timezone.utc,
    )
