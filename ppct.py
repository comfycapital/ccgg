"""
Simple Polymarket CLOB copy trader.

The script polls the public Polymarket data API activity feed for TARGET_ADDRESS
trades and places matching market orders from your configured account.
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Optional

import requests
from py_clob_client_v2 import (
    ApiCreds,
    ClobClient,
    MarketOrderArgs,
    OrderType,
    SignatureTypeV2,
)


DOTENV_PATH = ".env"
HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
DATA_API_ACTIVITY_URL = "https://data-api.polymarket.com/activity"

TARGET_ADDRESS = "0x09aedca3605e725655e314a29b28910e0ec555bc"

MARKET_FILTER = ""
ASSET_ID_FILTER = ""
COPY_PERCENT = Decimal("20")

DATA_API_LIMIT = 10
DATA_API_ACTIVITY_TYPE = "TRADE"
REQUEST_TIMEOUT_SECONDS = 10
POLL_INTERVAL_SECONDS = 1
POLL_STATUS_INTERVAL_SECONDS = 5
STARTUP_LOOKBACK_SECONDS = 3
COPY_ORDER_TYPE = OrderType.FOK
LOG_TRADE_API_RESPONSES = True
TRADE_API_RESPONSE_LOG_PATH = "copy_trader_responses.jsonl"
LOG_FILE_PATH = "copy_trader.log"
AUTO_DERIVE_API_CREDENTIALS = True

DRY_RUN = False
ALLOW_SELF_COPY = False

SIGNATURE_TYPE = SignatureTypeV2.EOA
USE_SERVER_TIME = False

AMOUNT_QUANTUM = Decimal("0.000001")
MIN_COPY_AMOUNT = Decimal("0.000001")
SEEN_TRADE_LIMIT = 1000
LOG_LEVEL = logging.INFO

BUY_SIDE = "BUY"
SELL_SIDE = "SELL"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

TRADE_ID_FIELDS = ("id", "trade_id")
TRANSACTION_ID_FIELDS = ("transaction_hash", "transactionHash", "hash")
TOKEN_ID_FIELDS = ("asset_id", "token_id", "tokenId", "asset")
MARKET_FIELDS = ("market", "condition_id", "conditionId")
SLUG_FIELDS = ("slug", "eventSlug")
OUTCOME_FIELDS = ("outcome", "outcomeIndex")
TITLE_FIELDS = ("title",)
SIDE_FIELDS = ("side", "trade_side", "maker_side")
SIZE_FIELDS = ("size", "filled_size", "matched_amount", "quantity", "shares")
PRICE_FIELDS = ("price", "avg_price", "average_price")
AMOUNT_FIELDS = ("usdcSize", "usdc_size", "usdc_amount", "collateral_amount", "amount")
TIMESTAMP_FIELDS = ("match_time", "created_at", "timestamp", "time", "last_update")


def load_dotenv_file(path: str) -> None:
    if not os.path.exists(path):
        return

    with open(path, encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key and key not in os.environ:
                os.environ[key] = value


load_dotenv_file(DOTENV_PATH)

FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
API_KEY = os.getenv("POLYMARKET_API_KEY", "")
API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
API_CREDENTIAL_NAMES = (
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
)


@dataclass(frozen=True)
class ParsedTrade:
    trade_id: str
    side: str
    token_id: str
    market: str
    market_slug: str
    outcome: str
    title: str
    source_size: Decimal
    source_price: Optional[Decimal]
    copy_amount: Decimal
    timestamp: int


@dataclass(frozen=True)
class TradePollResult:
    recent_trades: list[dict[str, Any]]
    fetched_trade_count: int
    target_trade_count: int
    old_trade_count: int
    newest_target_timestamp: int


@dataclass(frozen=True)
class TradeApiResult:
    trades: list[dict[str, Any]]


def configure_logging() -> None:
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),
        ],
    )


def normalize_address(address: str) -> str:
    return address.strip().lower()


def validate_constants() -> None:
    if not TARGET_ADDRESS or normalize_address(TARGET_ADDRESS) == ZERO_ADDRESS:
        raise ValueError("Set TARGET_ADDRESS before running the copy trader.")
    if COPY_PERCENT <= 0:
        raise ValueError("COPY_PERCENT must be greater than 0.")
    if not PRIVATE_KEY and not DRY_RUN:
        raise ValueError("Set POLYMARKET_PRIVATE_KEY in the environment.")
    if not DRY_RUN and has_partial_api_credentials():
        raise ValueError(
            "Set all L2 API credentials or leave all of them empty so the script "
            "can derive existing credentials. Required variables: "
            + ", ".join(API_CREDENTIAL_NAMES)
        )


def format_configured_status(value: str) -> str:
    if value:
        return "set"
    return "missing"


def has_any_api_credentials() -> bool:
    return bool(API_KEY or API_SECRET or API_PASSPHRASE)


def has_complete_api_credentials() -> bool:
    return bool(API_KEY and API_SECRET and API_PASSPHRASE)


def has_partial_api_credentials() -> bool:
    return has_any_api_credentials() and not has_complete_api_credentials()


def format_api_credentials_status() -> str:
    if has_complete_api_credentials():
        return "set"
    if has_partial_api_credentials():
        return "incomplete"
    return "missing"


def log_startup_configuration() -> None:
    logging.info(
        (
            "Configuration private_key=%s api_credentials=%s auto_derive_api_credentials=%s "
            "funder_address=%s host=%s chain_id=%s "
            "poll_interval=%ss order_type=%s activity_type=%s activity_limit=%s "
            "response_log=%s response_log_path=%s log_file=%s"
        ),
        format_configured_status(PRIVATE_KEY),
        format_api_credentials_status(),
        AUTO_DERIVE_API_CREDENTIALS,
        FUNDER_ADDRESS or "default signer address",
        HOST,
        CHAIN_ID,
        POLL_INTERVAL_SECONDS,
        COPY_ORDER_TYPE,
        DATA_API_ACTIVITY_TYPE,
        DATA_API_LIMIT,
        LOG_TRADE_API_RESPONSES,
        TRADE_API_RESPONSE_LOG_PATH,
        LOG_FILE_PATH,
    )


def build_api_creds_from_environment() -> Optional[ApiCreds]:
    if not has_complete_api_credentials():
        return None

    return ApiCreds(
        api_key=API_KEY,
        api_secret=API_SECRET,
        api_passphrase=API_PASSPHRASE,
    )


def configure_level_2_credentials(client: ClobClient) -> None:
    api_creds = build_api_creds_from_environment()
    if api_creds is not None:
        client.set_api_creds(api_creds)
        logging.info("Using L2 API credentials from environment.")
        return

    if not AUTO_DERIVE_API_CREDENTIALS:
        raise ValueError(
            "Live order posting requires L2 API credentials. Set "
            + ", ".join(API_CREDENTIAL_NAMES)
            + "."
        )

    logging.info(
        "No L2 API credentials found. Deriving existing credentials with private key authentication."
    )
    try:
        derived_creds = client.derive_api_key()
    except Exception as error:
        logging.exception("Could not derive existing L2 API credentials.")
        raise ValueError(
            "Live order posting requires L2 API credentials. Create or set API "
            "credentials, then set "
            + ", ".join(API_CREDENTIAL_NAMES)
            + "."
        ) from error

    client.set_api_creds(derived_creds)
    logging.info("Derived existing L2 API credentials from private key authentication.")


def build_client() -> ClobClient:
    logging.info("Initializing CLOB client with private key authentication.")
    client = ClobClient(
        HOST,
        CHAIN_ID,
        key=PRIVATE_KEY,
        creds=None,
        signature_type=SIGNATURE_TYPE,
        funder=FUNDER_ADDRESS or None,
        use_server_time=USE_SERVER_TIME,
        retry_on_error=False,
    )
    configure_level_2_credentials(client)
    return client


def stop_if_self_copy(client: ClobClient) -> None:
    own_address = normalize_address(client.get_address())
    target_address = normalize_address(TARGET_ADDRESS)
    logging.info("CLOB client signer address=%s", own_address)

    if own_address == target_address and not ALLOW_SELF_COPY:
        raise ValueError("TARGET_ADDRESS matches your signer address. Refusing self-copy.")


def read_first(trade: dict[str, Any], fields: tuple[str, ...]) -> Any:
    for field in fields:
        value = trade.get(field)
        if value not in (None, ""):
            return value
    return None


def decimal_or_none(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def decimal_or_zero(value: Any) -> Decimal:
    return decimal_or_none(value) or Decimal("0")


def parse_timestamp(value: Any) -> int:
    if value in (None, ""):
        return 0

    if isinstance(value, (int, float)):
        return normalize_unix_timestamp(int(value))

    text = str(value).strip()
    if text.isdigit():
        return normalize_unix_timestamp(int(text))

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return 0

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def normalize_unix_timestamp(timestamp: int) -> int:
    if timestamp > 1_000_000_000_000:
        return timestamp // 1000
    return timestamp


def format_timestamp_for_log(timestamp: int) -> str:
    if timestamp <= 0:
        return "none"
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def log_recent_trade_candidate(trade: dict[str, Any]) -> None:
    trade_timestamp = parse_timestamp(read_first(trade, TIMESTAMP_FIELDS))
    logging.info(
        (
            "Recent activity candidate tx=%s type=%s timestamp=%s time=%s side=%s "
            "token=%s size=%s usdc_size=%s price=%s market=%s slug=%s outcome=%s title=%s"
        ),
        read_first(trade, TRANSACTION_ID_FIELDS),
        trade.get("type"),
        trade_timestamp,
        format_timestamp_for_log(trade_timestamp),
        read_first(trade, SIDE_FIELDS),
        read_first(trade, TOKEN_ID_FIELDS),
        read_first(trade, SIZE_FIELDS),
        read_first(trade, AMOUNT_FIELDS),
        read_first(trade, PRICE_FIELDS),
        read_first(trade, MARKET_FIELDS),
        read_first(trade, SLUG_FIELDS),
        read_first(trade, OUTCOME_FIELDS),
        read_first(trade, TITLE_FIELDS),
    )


def parse_side(value: Any) -> str:
    if isinstance(value, int):
        return BUY_SIDE if value == 0 else SELL_SIDE

    text = str(value).strip().upper()
    if text in ("BUY", "BID", "0"):
        return BUY_SIDE
    if text in ("SELL", "ASK", "1"):
        return SELL_SIDE

    raise ValueError(f"Unknown trade side: {value}")


def build_trade_id(trade: dict[str, Any]) -> str:
    trade_id = read_first(trade, TRADE_ID_FIELDS)
    if trade_id:
        return str(trade_id)

    transaction_id = read_first(trade, TRANSACTION_ID_FIELDS) or ""
    market = read_first(trade, MARKET_FIELDS) or ""
    market_slug = read_first(trade, SLUG_FIELDS) or ""
    token_id = read_first(trade, TOKEN_ID_FIELDS) or ""
    outcome = read_first(trade, OUTCOME_FIELDS) or ""
    side = read_first(trade, SIDE_FIELDS) or ""
    size = read_first(trade, SIZE_FIELDS) or ""
    price = read_first(trade, PRICE_FIELDS) or ""
    timestamp = read_first(trade, TIMESTAMP_FIELDS) or ""
    return f"{transaction_id}:{market}:{market_slug}:{token_id}:{outcome}:{side}:{size}:{price}:{timestamp}"


def parse_trade(trade: dict[str, Any]) -> ParsedTrade:
    side = parse_side(read_first(trade, SIDE_FIELDS))
    token_id = str(read_first(trade, TOKEN_ID_FIELDS) or "")
    market = str(read_first(trade, MARKET_FIELDS) or "")
    market_slug = str(read_first(trade, SLUG_FIELDS) or "")
    outcome = str(read_first(trade, OUTCOME_FIELDS) or "")
    title = str(read_first(trade, TITLE_FIELDS) or "")
    timestamp = parse_timestamp(read_first(trade, TIMESTAMP_FIELDS))

    if not token_id:
        raise ValueError(f"Trade is missing token id: {trade}")
    if not market:
        raise ValueError(f"Trade is missing condition id or market id: {trade}")

    source_size = decimal_or_zero(read_first(trade, SIZE_FIELDS))
    source_price = decimal_or_none(read_first(trade, PRICE_FIELDS))
    copy_amount = calculate_copy_amount(side, source_size, source_price, trade)

    return ParsedTrade(
        trade_id=build_trade_id(trade),
        side=side,
        token_id=token_id,
        market=market,
        market_slug=market_slug,
        outcome=outcome,
        title=title,
        source_size=source_size,
        source_price=source_price,
        copy_amount=copy_amount,
        timestamp=timestamp,
    )


def calculate_copy_amount(
    side: str,
    source_size: Decimal,
    source_price: Optional[Decimal],
    trade: dict[str, Any],
) -> Decimal:
    source_amount = source_size

    if side == BUY_SIDE:
        activity_amount = decimal_or_zero(read_first(trade, AMOUNT_FIELDS))
        if activity_amount > 0:
            source_amount = activity_amount
        elif source_size > 0 and source_price and source_price > 0:
            source_amount = source_size * source_price

    copy_amount = source_amount * COPY_PERCENT / Decimal("100")
    return copy_amount.quantize(AMOUNT_QUANTUM, rounding=ROUND_DOWN)


def build_trade_request_params() -> dict[str, Any]:
    return {
        "user": TARGET_ADDRESS,
        "limit": DATA_API_LIMIT,
        "type": DATA_API_ACTIVITY_TYPE,
    }


def extract_trade_list(response_data: Any) -> list[dict[str, Any]]:
    if isinstance(response_data, list):
        return [trade for trade in response_data if isinstance(trade, dict)]

    if isinstance(response_data, dict):
        for field_name in ("data", "trades", "results"):
            trades = response_data.get(field_name)
            if isinstance(trades, list):
                return [trade for trade in trades if isinstance(trade, dict)]

    raise ValueError(f"Unexpected data API activity response format: {response_data}")


def is_trade_activity(activity: dict[str, Any]) -> bool:
    return str(activity.get("type", "")).upper() == DATA_API_ACTIVITY_TYPE


def fetch_target_trades() -> TradeApiResult:
    request_params = build_trade_request_params()
    response = requests.get(
        DATA_API_ACTIVITY_URL,
        params=request_params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    raw_response = response.json()
    return TradeApiResult(
        trades=[
            trade
            for trade in extract_trade_list(raw_response)
            if is_trade_activity(trade)
        ],
    )


def write_trade_api_response_log(
    after_timestamp: int,
    poll_result: TradePollResult,
) -> None:
    if not LOG_TRADE_API_RESPONSES:
        return

    log_entry = {
        "logged_at": datetime.now(timezone.utc).isoformat(),
        "after_timestamp": after_timestamp,
        "after_time": format_timestamp_for_log(after_timestamp),
        "newest_target_timestamp": poll_result.newest_target_timestamp,
        "newest_target_time": format_timestamp_for_log(
            poll_result.newest_target_timestamp,
        ),
        "fetched_trade_count": poll_result.fetched_trade_count,
        "target_trade_count": poll_result.target_trade_count,
        "old_target_trade_count": poll_result.old_trade_count,
        "recent_target_trade_count": len(poll_result.recent_trades),
    }

    try:
        with open(TRADE_API_RESPONSE_LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(log_entry, default=str) + "\n")
    except OSError:
        logging.exception("Failed to write trade API response log.")


def poll_target_trades(after_timestamp: int) -> TradePollResult:
    recent_trades: list[dict[str, Any]] = []
    api_result = fetch_target_trades()
    target_trade_count = len(api_result.trades)
    newest_target_timestamp = 0

    for trade in api_result.trades:
        # The API request already filters by TARGET_ADDRESS using the user param.
        trade_timestamp = parse_timestamp(read_first(trade, TIMESTAMP_FIELDS))
        newest_target_timestamp = max(newest_target_timestamp, trade_timestamp)
        if trade_timestamp >= after_timestamp:
            recent_trades.append(trade)

    sorted_recent_trades = sorted(
        recent_trades,
        key=lambda trade: parse_timestamp(read_first(trade, TIMESTAMP_FIELDS)),
    )
    if sorted_recent_trades:
        logging.info(
            "Found %s recent target trades after_timestamp=%s after_time=%s.",
            len(sorted_recent_trades),
            after_timestamp,
            format_timestamp_for_log(after_timestamp),
        )

    poll_result = TradePollResult(
        recent_trades=sorted_recent_trades,
        fetched_trade_count=len(api_result.trades),
        target_trade_count=target_trade_count,
        old_trade_count=target_trade_count - len(sorted_recent_trades),
        newest_target_timestamp=newest_target_timestamp,
    )
    write_trade_api_response_log(after_timestamp, poll_result)
    return poll_result


def get_skip_reason(parsed_trade: ParsedTrade) -> Optional[str]:
    if MARKET_FILTER and MARKET_FILTER not in (parsed_trade.market, parsed_trade.market_slug):
        return "market filter mismatch"
    if ASSET_ID_FILTER and parsed_trade.token_id != ASSET_ID_FILTER:
        return "asset id filter mismatch"
    if parsed_trade.copy_amount < MIN_COPY_AMOUNT:
        return "copy amount below minimum"
    return None


def place_copy_order(client: Optional[ClobClient], parsed_trade: ParsedTrade) -> None:
    if DRY_RUN:
        logging.info(
            "DRY_RUN copy %s token=%s amount=%s market=%s slug=%s outcome=%s source_size=%s source_price=%s title=%s",
            parsed_trade.side,
            parsed_trade.token_id,
            parsed_trade.copy_amount,
            parsed_trade.market,
            parsed_trade.market_slug,
            parsed_trade.outcome,
            parsed_trade.source_size,
            parsed_trade.source_price,
            parsed_trade.title,
        )
        return

    if client is None:
        raise ValueError("Live copy trading requires an initialized CLOB client.")

    order_amount = float(parsed_trade.copy_amount)
    logging.info(
        (
            "Preparing copy order trade_id=%s side=%s token=%s amount=%s "
            "order_type=%s market=%s slug=%s outcome=%s source_size=%s source_price=%s"
        ),
        parsed_trade.trade_id,
        parsed_trade.side,
        parsed_trade.token_id,
        order_amount,
        COPY_ORDER_TYPE,
        parsed_trade.market,
        parsed_trade.market_slug,
        parsed_trade.outcome,
        parsed_trade.source_size,
        parsed_trade.source_price,
    )

    order_args = MarketOrderArgs(
        token_id=parsed_trade.token_id,
        amount=order_amount,
        side=parsed_trade.side,
        order_type=COPY_ORDER_TYPE,
    )

    try:
        logging.info(
            "Posting copy order to CLOB trade_id=%s token=%s side=%s amount=%s order_type=%s.",
            parsed_trade.trade_id,
            parsed_trade.token_id,
            parsed_trade.side,
            order_amount,
            COPY_ORDER_TYPE,
        )
        response = client.create_and_post_market_order(
            order_args,
            order_type=COPY_ORDER_TYPE,
        )
    except Exception:
        logging.exception(
            (
                "Copy order failed trade_id=%s side=%s token=%s amount=%s "
                "order_type=%s market=%s slug=%s outcome=%s"
            ),
            parsed_trade.trade_id,
            parsed_trade.side,
            parsed_trade.token_id,
            order_amount,
            COPY_ORDER_TYPE,
            parsed_trade.market,
            parsed_trade.market_slug,
            parsed_trade.outcome,
        )
        raise

    logging.info(
        "CLOB order response trade_id=%s side=%s token=%s amount=%s response_type=%s response=%s",
        parsed_trade.trade_id,
        parsed_trade.side,
        parsed_trade.token_id,
        order_amount,
        type(response).__name__,
        response,
    )


def remember_trade(
    seen_trade_ids: set[str],
    seen_trade_order: list[str],
    trade_id: str,
) -> None:
    seen_trade_ids.add(trade_id)
    seen_trade_order.append(trade_id)

    if len(seen_trade_order) <= SEEN_TRADE_LIMIT:
        return

    stale_trade_ids = seen_trade_order[: len(seen_trade_order) - SEEN_TRADE_LIMIT]
    del seen_trade_order[: len(stale_trade_ids)]

    for stale_trade_id in stale_trade_ids:
        seen_trade_ids.discard(stale_trade_id)


def maybe_log_poll_status(
    last_status_log_time: float,
    after_timestamp: int,
    processed_trade_count: int,
    poll_result: TradePollResult,
) -> float:
    current_time = time.time()
    if current_time - last_status_log_time < POLL_STATUS_INTERVAL_SECONDS:
        return last_status_log_time

    logging.info(
        (
            "Poll status fetched=%s target_trades=%s old_target_trades=%s "
            "recent_target_trades=%s processed_trade_count=%s after_timestamp=%s "
            "after_time=%s newest_target_timestamp=%s newest_target_time=%s."
        ),
        poll_result.fetched_trade_count,
        poll_result.target_trade_count,
        poll_result.old_trade_count,
        len(poll_result.recent_trades),
        processed_trade_count,
        after_timestamp,
        format_timestamp_for_log(after_timestamp),
        poll_result.newest_target_timestamp,
        format_timestamp_for_log(poll_result.newest_target_timestamp),
    )
    return current_time


def run_copy_trader() -> None:
    configure_logging()
    validate_constants()
    log_startup_configuration()

    client: Optional[ClobClient] = None
    if not DRY_RUN:
        client = build_client()
        stop_if_self_copy(client)
        logging.info("Live order mode is enabled.")
    else:
        logging.info("Dry-run mode is enabled. Orders will not be posted.")

    seen_trade_ids: set[str] = set()
    seen_trade_order: list[str] = []
    after_timestamp = int(time.time()) - STARTUP_LOOKBACK_SECONDS
    last_status_log_time = 0.0

    logging.info(
        "Copy trader will only copy trades at or after after_timestamp=%s after_time=%s.",
        after_timestamp,
        format_timestamp_for_log(after_timestamp),
    )

    logging.info(
        "Copy trader started target=%s market_filter=%s asset_filter=%s copy_percent=%s dry_run=%s",
        TARGET_ADDRESS,
        MARKET_FILTER or "ALL",
        ASSET_ID_FILTER or "ALL",
        COPY_PERCENT,
        DRY_RUN,
    )

    while True:
        try:
            poll_result = poll_target_trades(after_timestamp)
            last_status_log_time = maybe_log_poll_status(
                last_status_log_time,
                after_timestamp,
                len(seen_trade_ids),
                poll_result,
            )
            newest_handled_timestamp = after_timestamp
            oldest_failed_timestamp: Optional[int] = None

            for trade in poll_result.recent_trades:
                log_recent_trade_candidate(trade)
                try:
                    parsed_trade = parse_trade(trade)
                except Exception:
                    logging.exception("Skipping trade that could not be parsed: %s", trade)
                    continue

                if parsed_trade.trade_id in seen_trade_ids:
                    logging.info("Skipping duplicate trade_id=%s.", parsed_trade.trade_id)
                    newest_handled_timestamp = max(
                        newest_handled_timestamp,
                        parsed_trade.timestamp,
                    )
                    continue

                logging.info(
                    "Parsed trade_id=%s side=%s token=%s copy_amount=%s market=%s slug=%s outcome=%s timestamp=%s.",
                    parsed_trade.trade_id,
                    parsed_trade.side,
                    parsed_trade.token_id,
                    parsed_trade.copy_amount,
                    parsed_trade.market,
                    parsed_trade.market_slug,
                    parsed_trade.outcome,
                    parsed_trade.timestamp,
                )

                skip_reason = get_skip_reason(parsed_trade)
                if skip_reason:
                    logging.info(
                        "Skipping trade_id=%s reason=%s.",
                        parsed_trade.trade_id,
                        skip_reason,
                    )
                    remember_trade(seen_trade_ids, seen_trade_order, parsed_trade.trade_id)
                    newest_handled_timestamp = max(
                        newest_handled_timestamp,
                        parsed_trade.timestamp,
                    )
                    continue

                logging.info("Trade accepted for copy trade_id=%s.", parsed_trade.trade_id)
                try:
                    place_copy_order(client, parsed_trade)
                except Exception:
                    logging.info(
                        "Trade not marked as seen after failed copy trade_id=%s.",
                        parsed_trade.trade_id,
                    )
                    if parsed_trade.timestamp > 0:
                        if oldest_failed_timestamp is None:
                            oldest_failed_timestamp = parsed_trade.timestamp
                        else:
                            oldest_failed_timestamp = min(
                                oldest_failed_timestamp,
                                parsed_trade.timestamp,
                            )
                    continue

                remember_trade(seen_trade_ids, seen_trade_order, parsed_trade.trade_id)
                newest_handled_timestamp = max(
                    newest_handled_timestamp,
                    parsed_trade.timestamp,
                )

            next_after_timestamp = newest_handled_timestamp - 1
            if oldest_failed_timestamp is not None:
                next_after_timestamp = min(
                    next_after_timestamp,
                    oldest_failed_timestamp - 1,
                )

            if next_after_timestamp > after_timestamp:
                after_timestamp = next_after_timestamp
                logging.info("Advanced after_timestamp to %s.", after_timestamp)

        except Exception:
            logging.exception("Copy trader loop failed. Continuing after poll interval.")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_copy_trader()
