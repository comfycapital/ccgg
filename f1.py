import hashlib
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
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
    OrderType,
)
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.exceptions import PolyException
from py_clob_client_v2.order_utils import SignatureTypeV2


SCRIPT_NAME = "buy_sell_ranked_temperature"
DEFAULT_BUY_DAY_OFFSET = 0
DEFAULT_BUY_HOUR_UTC = 3
DEFAULT_SELL_DAY_OFFSET = 0
DEFAULT_SELL_HOUR_UTC = 17
DEFAULT_TRADE_TIME_RANDOM_WINDOW_SECONDS = 300
TARGET_MARKET_RANK = 1

ENV_FILE = Path(".env")
STATE_FILE = Path(f"{SCRIPT_NAME}_state.json")
SCRIPT_LOG_FILE = Path(f"{SCRIPT_NAME}.log")
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
DEFAULT_MIN_SELL_PRICE = 0.01
DEFAULT_BUY_TWAP_SLICES = 1
DEFAULT_BUY_TWAP_INTERVAL_SECONDS = 0
DEFAULT_SELL_TWAP_SLICES = 1
DEFAULT_SELL_TWAP_INTERVAL_SECONDS = 0
DEFAULT_POLL_INTERVAL_SECONDS = 30
DEFAULT_SNAPSHOT_GRACE_SECONDS = 300

REQUEST_TIMEOUT_SECONDS = 30
MIN_BUY_AMOUNT_USDC = 5.0
MIN_SELL_SHARES = 0.01
CONDITIONAL_TOKEN_DECIMAL_SCALE = 1_000_000.0
PRICE_UNIT_MAX = 1.0
PRICE_PERCENT_MAX = 100.0
HOURS_PER_DAY = 24
SECONDS_PER_HOUR = 3600

MARKET_TIMEZONE_NAME = "Europe/Paris"
TARGET_EVENT_TEXT = "highest temperature in paris"
TARGET_SERIES_SLUG = "paris-daily-highest-temperature"
LOWEST_SERIES_TEXT = "lowest temperature"

BUY_SIDE = "BUY"
SELL_SIDE = "SELL"
YES_OUTCOME = "YES"
NO_OUTCOME = "NO"
DEFAULT_TARGET_OUTCOME = YES_OUTCOME
VALID_TARGET_OUTCOMES = (YES_OUTCOME, NO_OUTCOME)
MARKET_ORDER_TYPE = OrderType.FAK
CLOB_NO_MATCH_MESSAGE = "no match"
NO_ASK_LIQUIDITY_REASON = "no_ask_liquidity"
NO_PARTIAL_FILL_LIQUIDITY_REASON = "no_partial_fill_liquidity"
NO_BID_LIQUIDITY_REASON = "no_bid_liquidity"

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
MIN_SELL_PRICE_ENV = "MIN_SELL_PRICE"
TARGET_OUTCOME_ENV = "TARGET_OUTCOME"
BUY_DAY_OFFSET_ENV = "BUY_DAY_OFFSET"
BUY_HOUR_UTC_ENV = "BUY_HOUR_UTC"
SELL_DAY_OFFSET_ENV = "SELL_DAY_OFFSET"
SELL_HOUR_UTC_ENV = "SELL_HOUR_UTC"
BUY_TWAP_SLICES_ENV = "BUY_TWAP_SLICES"
BUY_TWAP_INTERVAL_SECONDS_ENV = "BUY_TWAP_INTERVAL_SECONDS"
SELL_TWAP_SLICES_ENV = "SELL_TWAP_SLICES"
SELL_TWAP_INTERVAL_SECONDS_ENV = "SELL_TWAP_INTERVAL_SECONDS"
POLL_INTERVAL_SECONDS_ENV = "POLL_INTERVAL_SECONDS"
SNAPSHOT_GRACE_SECONDS_ENV = "SNAPSHOT_GRACE_SECONDS"
TRADE_TIME_RANDOM_WINDOW_SECONDS_ENV = "TRADE_TIME_RANDOM_WINDOW_SECONDS"
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
BALANCE_RESPONSE_VALUE_KEYS = ("balance", "available", "amount")
RANDOM_OFFSET_DIGEST_BYTES = 8


@dataclass(frozen=True)
class TemperatureMarket:
    market_id: str
    condition_id: str
    question: str
    slug: str
    group_item_title: str
    temperature_celsius: int
    yes_token_id: str
    no_token_id: str


@dataclass(frozen=True)
class MarketSnapshot:
    target_date: str
    event: dict[str, Any]
    markets: list[TemperatureMarket]


@dataclass(frozen=True)
class RankedMarket:
    market: TemperatureMarket
    outcome_price: float
    rank: int


@dataclass(frozen=True)
class TradeSchedule:
    buy_day_offset: int
    buy_hour_utc: int
    sell_day_offset: int
    sell_hour_utc: int
    trade_time_random_window_seconds: int


@dataclass(frozen=True)
class TwapConfig:
    buy_slices: int
    buy_interval_seconds: int
    sell_slices: int
    sell_interval_seconds: int


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

    file_handler = logging.FileHandler(SCRIPT_LOG_FILE, encoding="utf-8")
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


def normalize_target_outcome(value: str) -> str:
    normalized_outcome = value.strip().upper()

    if normalized_outcome not in VALID_TARGET_OUTCOMES:
        valid_values = ", ".join(VALID_TARGET_OUTCOMES)
        raise ValueError(f"{TARGET_OUTCOME_ENV} must be one of: {valid_values}.")

    return normalized_outcome


def get_target_outcome_env() -> str:
    return normalize_target_outcome(
        get_string_env(TARGET_OUTCOME_ENV, DEFAULT_TARGET_OUTCOME)
    )


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


def validate_hour_utc(name: str, value: int) -> None:
    if value < 0 or value > 23:
        raise ValueError(f"{name} must be between 0 and 23.")


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


def validate_non_negative_seconds(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be at least 0.")


def validate_positive_int(name: str, value: int) -> None:
    if value < 1:
        raise ValueError(f"{name} must be at least 1.")


def build_trade_schedule() -> TradeSchedule:
    schedule = TradeSchedule(
        buy_day_offset=get_int_env(BUY_DAY_OFFSET_ENV, DEFAULT_BUY_DAY_OFFSET),
        buy_hour_utc=get_int_env(BUY_HOUR_UTC_ENV, DEFAULT_BUY_HOUR_UTC),
        sell_day_offset=get_int_env(SELL_DAY_OFFSET_ENV, DEFAULT_SELL_DAY_OFFSET),
        sell_hour_utc=get_int_env(SELL_HOUR_UTC_ENV, DEFAULT_SELL_HOUR_UTC),
        trade_time_random_window_seconds=get_int_env(
            TRADE_TIME_RANDOM_WINDOW_SECONDS_ENV,
            DEFAULT_TRADE_TIME_RANDOM_WINDOW_SECONDS,
        ),
    )
    validate_trade_schedule(schedule)
    return schedule


def build_twap_config() -> TwapConfig:
    twap_config = TwapConfig(
        buy_slices=get_int_env(BUY_TWAP_SLICES_ENV, DEFAULT_BUY_TWAP_SLICES),
        buy_interval_seconds=get_int_env(BUY_TWAP_INTERVAL_SECONDS_ENV, DEFAULT_BUY_TWAP_INTERVAL_SECONDS),
        sell_slices=get_int_env(SELL_TWAP_SLICES_ENV, DEFAULT_SELL_TWAP_SLICES),
        sell_interval_seconds=get_int_env(SELL_TWAP_INTERVAL_SECONDS_ENV, DEFAULT_SELL_TWAP_INTERVAL_SECONDS),
    )
    validate_twap_config(twap_config)
    return twap_config


def validate_twap_config(twap_config: TwapConfig) -> None:
    validate_positive_int(BUY_TWAP_SLICES_ENV, twap_config.buy_slices)
    validate_positive_int(SELL_TWAP_SLICES_ENV, twap_config.sell_slices)
    validate_non_negative_seconds(BUY_TWAP_INTERVAL_SECONDS_ENV, twap_config.buy_interval_seconds)
    validate_non_negative_seconds(SELL_TWAP_INTERVAL_SECONDS_ENV, twap_config.sell_interval_seconds)


def validate_twap_buy_amount(buy_amount_usdc: float, twap_config: TwapConfig) -> None:
    buy_slice_amount_usdc = build_buy_slice_amount_usdc(buy_amount_usdc, twap_config)

    if buy_slice_amount_usdc < MIN_BUY_AMOUNT_USDC:
        raise ValueError(
            f"{BUY_AMOUNT_ENV} / {BUY_TWAP_SLICES_ENV} must be at least {MIN_BUY_AMOUNT_USDC}."
        )


def build_buy_slice_amount_usdc(buy_amount_usdc: float, twap_config: TwapConfig) -> float:
    return buy_amount_usdc / twap_config.buy_slices


def validate_trade_schedule(schedule: TradeSchedule) -> None:
    validate_hour_utc(BUY_HOUR_UTC_ENV, schedule.buy_hour_utc)
    validate_hour_utc(SELL_HOUR_UTC_ENV, schedule.sell_hour_utc)
    validate_non_negative_seconds(
        TRADE_TIME_RANDOM_WINDOW_SECONDS_ENV,
        schedule.trade_time_random_window_seconds,
    )

    buy_hour_index = build_relative_hour_index(schedule.buy_day_offset, schedule.buy_hour_utc)
    sell_hour_index = build_relative_hour_index(schedule.sell_day_offset, schedule.sell_hour_utc)

    if sell_hour_index <= buy_hour_index:
        raise ValueError("The configured sell time must be after the configured buy time.")

    validate_trade_time_random_window(schedule, buy_hour_index, sell_hour_index)


def validate_trade_time_random_window(
    schedule: TradeSchedule,
    buy_hour_index: int,
    sell_hour_index: int,
) -> None:
    trade_gap_seconds = (sell_hour_index - buy_hour_index) * SECONDS_PER_HOUR

    if schedule.trade_time_random_window_seconds >= trade_gap_seconds:
        raise ValueError(
            f"{TRADE_TIME_RANDOM_WINDOW_SECONDS_ENV} must be less than the configured "
            f"buy-to-sell gap of {trade_gap_seconds} seconds."
        )


def build_relative_hour_index(day_offset: int, hour_utc: int) -> int:
    return (day_offset * HOURS_PER_DAY) + hour_utc


def format_day_offset(day_offset: int) -> str:
    if day_offset > 0:
        return f"D+{day_offset}"
    return f"D{day_offset}"


def format_trade_time(day_offset: int, hour_utc: int) -> str:
    return f"{format_day_offset(day_offset)} {hour_utc:02d}:00 UTC"


def build_schedule_record(schedule: TradeSchedule) -> dict[str, Any]:
    return {
        "buy_day_offset": schedule.buy_day_offset,
        "buy_hour_utc": schedule.buy_hour_utc,
        "buy_time": format_trade_time(schedule.buy_day_offset, schedule.buy_hour_utc),
        "sell_day_offset": schedule.sell_day_offset,
        "sell_hour_utc": schedule.sell_hour_utc,
        "sell_time": format_trade_time(schedule.sell_day_offset, schedule.sell_hour_utc),
        "trade_time_random_window_seconds": schedule.trade_time_random_window_seconds,
    }


def build_twap_config_record(twap_config: TwapConfig) -> dict[str, int]:
    return {
        "buy_interval_seconds": twap_config.buy_interval_seconds,
        "buy_slices": twap_config.buy_slices,
        "sell_interval_seconds": twap_config.sell_interval_seconds,
        "sell_slices": twap_config.sell_slices,
    }


def build_twap_slice_record(side: str, slice_number: int, total_slices: int) -> dict[str, Any]:
    return {
        "side": side,
        "slice_number": slice_number,
        "total_slices": total_slices,
    }


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


def resolve_target_market_date(buy_day_offset: int) -> str:
    target_market_date = os.getenv(TARGET_MARKET_DATE_ENV)

    if target_market_date:
        return target_market_date

    buy_date = datetime.now(get_market_timezone()).date()
    target_date = buy_date - timedelta(days=buy_day_offset)
    return target_date.isoformat()


def build_trade_time_utc(target_date: str, day_offset: int, hour_utc: int) -> datetime:
    parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    trade_date = parsed_date + timedelta(days=day_offset)
    return datetime.combine(
        trade_date,
        datetime_time(hour=hour_utc),
        tzinfo=timezone.utc,
    )


def build_trade_time_random_delay_seconds(
    target_date: str,
    side: str,
    random_window_seconds: int,
) -> int:
    if random_window_seconds <= 0:
        return 0

    seed = f"{SCRIPT_NAME}:{target_date}:{side}"
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    random_value = int.from_bytes(digest[:RANDOM_OFFSET_DIGEST_BYTES], "big")
    return random_value % (random_window_seconds + 1)


def build_randomized_trade_time_utc(
    target_date: str,
    day_offset: int,
    hour_utc: int,
    side: str,
    random_window_seconds: int,
) -> datetime:
    trade_time = build_trade_time_utc(target_date, day_offset, hour_utc)
    random_delay_seconds = build_trade_time_random_delay_seconds(
        target_date,
        side,
        random_window_seconds,
    )
    return trade_time + timedelta(seconds=random_delay_seconds)


def build_trade_time_record(
    target_date: str,
    day_offset: int,
    hour_utc: int,
    side: str,
    random_window_seconds: int,
) -> dict[str, Any]:
    base_time = build_trade_time_utc(target_date, day_offset, hour_utc)
    random_delay_seconds = build_trade_time_random_delay_seconds(
        target_date,
        side,
        random_window_seconds,
    )
    return {
        "base_time_utc": base_time.isoformat(),
        "random_delay_seconds": random_delay_seconds,
        "time_utc": (base_time + timedelta(seconds=random_delay_seconds)).isoformat(),
    }


def build_trade_times_record(target_date: str, schedule: TradeSchedule) -> dict[str, Any]:
    return {
        "buy": build_trade_time_record(
            target_date=target_date,
            day_offset=schedule.buy_day_offset,
            hour_utc=schedule.buy_hour_utc,
            side=BUY_SIDE,
            random_window_seconds=schedule.trade_time_random_window_seconds,
        ),
        "sell": build_trade_time_record(
            target_date=target_date,
            day_offset=schedule.sell_day_offset,
            hour_utc=schedule.sell_hour_utc,
            side=SELL_SIDE,
            random_window_seconds=schedule.trade_time_random_window_seconds,
        ),
    }


def parse_utc_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or value == "":
        return None

    parsed_datetime = datetime.fromisoformat(value)

    if parsed_datetime.tzinfo is None:
        return parsed_datetime.replace(tzinfo=timezone.utc)

    return parsed_datetime.astimezone(timezone.utc)


def get_position_sell_time_utc(
    position: dict[str, Any],
    target_date: str,
    schedule: TradeSchedule,
) -> datetime:
    trade_times = position.get("trade_times")

    if isinstance(trade_times, dict):
        sell_record = trade_times.get("sell")

        if isinstance(sell_record, dict):
            sell_time = parse_utc_datetime(sell_record.get("time_utc"))

            if sell_time is not None:
                return sell_time

    return build_randomized_trade_time_utc(
        target_date=target_date,
        day_offset=schedule.sell_day_offset,
        hour_utc=schedule.sell_hour_utc,
        side=SELL_SIDE,
        random_window_seconds=schedule.trade_time_random_window_seconds,
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

    normalized_outcome_name = outcome_name.lower()

    for outcome, token_id in zip(outcomes, token_ids):
        if outcome.strip().lower() == normalized_outcome_name:
            return token_id

    raise ValueError(f"{outcome_name} outcome token was not found.")


def get_market_outcome_token_id(market: TemperatureMarket, target_outcome: str) -> str:
    if target_outcome == YES_OUTCOME:
        return market.yes_token_id

    if target_outcome == NO_OUTCOME:
        return market.no_token_id

    raise ValueError(f"Unsupported target outcome: {target_outcome}")


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
        no_token_id=get_outcome_token_id(market, NO_OUTCOME),
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


def extract_outcome_prices(
    response_json: Any,
    markets: list[TemperatureMarket],
    target_outcome: str,
) -> dict[str, float]:
    requested_token_ids = {
        get_market_outcome_token_id(market, target_outcome)
        for market in markets
    }

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


def get_outcome_prices(
    client: ClobClient,
    markets: list[TemperatureMarket],
    target_outcome: str,
) -> dict[str, float]:
    price_requests = [
        BookParams(
            token_id=get_market_outcome_token_id(market, target_outcome),
            side=BUY_SIDE,
        )
        for market in markets
    ]
    response = client.get_prices(price_requests)
    return extract_outcome_prices(response, markets, target_outcome)


def rank_markets_by_outcome_price(
    markets: list[TemperatureMarket],
    prices_by_token_id: dict[str, float],
    target_outcome: str,
) -> list[RankedMarket]:
    priced_markets = [
        (market, prices_by_token_id[get_market_outcome_token_id(market, target_outcome)])
        for market in markets
        if get_market_outcome_token_id(market, target_outcome) in prices_by_token_id
    ]
    priced_markets.sort(key=lambda item: (-item[1], item[0].temperature_celsius, item[0].market_id))

    return [
        RankedMarket(market=market, outcome_price=outcome_price, rank=index + 1)
        for index, (market, outcome_price) in enumerate(priced_markets)
    ]


def build_price_snapshot(
    ranked_markets: list[RankedMarket],
    target_outcome: str,
) -> list[dict[str, Any]]:
    return [
        {
            "market_id": ranked_market.market.market_id,
            "outcome_price": ranked_market.outcome_price,
            "outcome_token_id": get_market_outcome_token_id(ranked_market.market, target_outcome),
            "question": ranked_market.market.question,
            "rank": ranked_market.rank,
            "target_outcome": target_outcome,
            "temperature_celsius": ranked_market.market.temperature_celsius,
        }
        for ranked_market in ranked_markets
    ]


def get_error_message(error: Exception) -> str:
    exception_message = getattr(error, "msg", "")

    if exception_message:
        return str(exception_message)

    return str(error)


def get_clob_no_match_reason(error: Exception, side: str) -> Optional[str]:
    error_message = get_error_message(error)

    if side == SELL_SIDE and error_message == CLOB_NO_MATCH_MESSAGE:
        return NO_BID_LIQUIDITY_REASON

    if isinstance(error, PolyException) and error_message == CLOB_NO_MATCH_MESSAGE:
        return NO_ASK_LIQUIDITY_REASON

    if type(error) is Exception and error_message == CLOB_NO_MATCH_MESSAGE:
        return NO_PARTIAL_FILL_LIQUIDITY_REASON

    return None


def log_order_skipped_no_liquidity(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    target_outcome: str,
    buy_amount_usdc: float,
    max_buy_price: float,
    reason: str,
    error: Exception,
) -> None:
    outcome_token_id = get_market_outcome_token_id(ranked_market.market, target_outcome)
    log_json(
        logger,
        "order_skipped_no_liquidity",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": ranked_market.market.condition_id,
            "error": get_error_message(error),
            "error_type": type(error).__name__,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": ranked_market.market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "market_rank": ranked_market.rank,
            "max_buy_price": max_buy_price,
            "question": ranked_market.market.question,
            "reason": reason,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "target_outcome": target_outcome,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.outcome_price,
            "outcome_token_id": outcome_token_id,
        },
    )
