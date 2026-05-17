import json
import logging
import os
from pathlib import Path
from typing import Optional

from py_clob_client_v2 import ApiCreds, ClobClient, MarketOrderArgsV2, OrderType
from py_clob_client_v2.constants import POLYGON
from py_clob_client_v2.order_utils import SignatureTypeV2


ENV_FILE = Path(".env")

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = POLYGON

TARGET_EVENT_TITLE = "Highest temperature in Paris on May 18?"
TARGET_MARKET_QUESTION = "Will the highest temperature in Paris be 15C on May 18?"
TARGET_MARKET_ID = "2274135"
TARGET_CONDITION_ID = "0x70deace4d51c2a802b652c607bad07a035e4393dbe283f1b7de38619dd7d3426"
YES_TOKEN_ID = "1773916751087153081178152303666316231610011542094001251137015718602616336664"
YES_SIDE = "BUY"

DEFAULT_MAX_PRICE = 0.35
MIN_BUY_AMOUNT_USDC = 5.0

BUY_ORDER_LOG_FILE = Path("buy_yes_15c_paris_may18.log")
LOGGER_NAME = "buy_yes_15c_paris_may18"
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
        value = value.strip()
        value = strip_wrapping_quotes(value)

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

    file_handler = logging.FileHandler(BUY_ORDER_LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)

    return logger


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
        raise ValueError(f"Set {CONFIRM_BUY_ENV}={CONFIRM_BUY_VALUE} to place this order.")


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
        client.set_api_creds(client.create_or_derive_api_key())

    return client


def validate_market_price(client: ClobClient, buy_amount_usdc: float, max_price: float) -> float:
    required_price = client.calculate_market_price(
        YES_TOKEN_ID,
        YES_SIDE,
        buy_amount_usdc,
        OrderType.FOK,
    )

    if required_price > max_price:
        raise ValueError(
            f"Required market price {required_price} is greater than {MAX_PRICE_ENV} {max_price}."
        )

    return required_price


def build_order_args(buy_amount_usdc: float, max_price: float) -> MarketOrderArgsV2:
    return MarketOrderArgsV2(
        token_id=YES_TOKEN_ID,
        amount=buy_amount_usdc,
        side=YES_SIDE,
        price=max_price,
        order_type=OrderType.FOK,
    )


def log_json(logger: logging.Logger, message: str, payload: dict) -> None:
    logger.info("%s %s", message, json.dumps(payload, ensure_ascii=False, sort_keys=True))


def buy_yes_15c_paris_may18() -> dict:
    load_env_file()
    logger = build_logger()
    validate_confirmation()

    buy_amount_usdc = get_float_env(BUY_AMOUNT_ENV)
    max_price = get_float_env(MAX_PRICE_ENV, DEFAULT_MAX_PRICE)
    validate_buy_amount(buy_amount_usdc)

    client = build_client()
    required_price = validate_market_price(client, buy_amount_usdc, max_price)
    order_args = build_order_args(buy_amount_usdc, max_price)

    log_json(
        logger,
        "order_attempt",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": TARGET_CONDITION_ID,
            "event_title": TARGET_EVENT_TITLE,
            "market_id": TARGET_MARKET_ID,
            "max_price": max_price,
            "question": TARGET_MARKET_QUESTION,
            "required_price": required_price,
            "side": YES_SIDE,
            "token_id": YES_TOKEN_ID,
        },
    )

    response = client.create_and_post_market_order(
        order_args,
        order_type=OrderType.FOK,
    )

    log_json(logger, "order_response", response if isinstance(response, dict) else {"response": response})
    return response


if __name__ == "__main__":
    buy_yes_15c_paris_may18()
