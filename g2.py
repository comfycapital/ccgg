def get_error_message(error: Exception) -> str:
    exception_message = getattr(error, "msg", "")

    if exception_message:
        return str(exception_message)

    return str(error)


def get_clob_no_match_reason(error: Exception) -> Optional[str]:
    error_message = get_error_message(error)

    if isinstance(error, PolyException) and error_message == CLOB_NO_MATCH_MESSAGE:
        return NO_ASK_LIQUIDITY_REASON

    if type(error) is Exception and error_message == CLOB_NO_MATCH_MESSAGE:
        return NO_PARTIAL_FILL_LIQUIDITY_REASON

    return None


def log_order_skipped_no_liquidity(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    buy_amount_usdc: float,
    max_buy_price: float,
    reason: str,
    error: Exception,
) -> None:
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
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        },
    )


def log_order_skipped_price_above_max(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    required_price: float,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> None:
    log_json(
        logger,
        "order_skipped_price_above_max",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": ranked_market.market.condition_id,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": ranked_market.market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "market_rank": ranked_market.rank,
            "max_buy_price": max_buy_price,
            "question": ranked_market.market.question,
            "required_price": required_price,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        },
    )


def log_order_skipped_trigger_above_max(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> None:
    log_json(
        logger,
        "order_skipped_trigger_above_max",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": ranked_market.market.condition_id,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": ranked_market.market.market_id,
            "market_rank": ranked_market.rank,
            "max_buy_price": max_buy_price,
            "question": ranked_market.market.question,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        },
    )


def calculate_required_market_price(
    client: ClobClient,
    market: TemperatureMarket,
    buy_amount_usdc: float,
) -> float:
    return client.calculate_market_price(
        market.yes_token_id,
        BUY_SIDE,
        buy_amount_usdc,
        MARKET_ORDER_TYPE,
    )


def build_order_args(
    market: TemperatureMarket,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> MarketOrderArgsV2:
    return MarketOrderArgsV2(
        token_id=market.yes_token_id,
        amount=buy_amount_usdc,
        side=BUY_SIDE,
        price=max_buy_price,
        order_type=MARKET_ORDER_TYPE,
    )


def buy_ranked_yes_market(
    client: ClobClient,
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> Optional[dict[str, Any]]:
    if ranked_market.yes_price > max_buy_price:
        log_order_skipped_trigger_above_max(
            logger=logger,
            snapshot=snapshot,
            ranked_market=ranked_market,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        return None

    try:
        required_price = calculate_required_market_price(
            client=client,
            market=ranked_market.market,
            buy_amount_usdc=buy_amount_usdc,
        )
    except Exception as error:
        no_match_reason = get_clob_no_match_reason(error)

        if no_match_reason is None:
            raise

        log_order_skipped_no_liquidity(
            logger=logger,
            snapshot=snapshot,
            ranked_market=ranked_market,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
            reason=no_match_reason,
            error=error,
        )
        return None

    if required_price > max_buy_price:
        log_order_skipped_price_above_max(
            logger=logger,
            snapshot=snapshot,
            ranked_market=ranked_market,
            required_price=required_price,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        return None

    order_args = build_order_args(
        market=ranked_market.market,
        buy_amount_usdc=buy_amount_usdc,
        max_buy_price=max_buy_price,
    )

    log_json(
        logger,
        "order_attempt",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": ranked_market.market.condition_id,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": ranked_market.market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "market_rank": ranked_market.rank,
            "max_buy_price": max_buy_price,
            "question": ranked_market.market.question,
            "required_price": required_price,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        },
    )

    response = client.create_and_post_market_order(
        order_args,
        order_type=MARKET_ORDER_TYPE,
    )
    payload = response if isinstance(response, dict) else {"response": response}
    log_json(logger, "order_response", payload)
    return payload


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"processed_order_keys": [], "processed_records": []}

    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"processed_order_keys": [], "processed_records": []}

    if not isinstance(state, dict):
        return {"processed_order_keys": [], "processed_records": []}

    state.setdefault("processed_order_keys", [])
    state.setdefault("processed_records", [])
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def build_order_key(target_date: str) -> str:
    return f"{target_date}:hour={SNAPSHOT_HOUR_UTC}:rank={TARGET_MARKET_RANK}:side=YES"


def is_order_key_processed(state: dict[str, Any], order_key: str) -> bool:
    processed_order_keys = state.get("processed_order_keys", [])
    return isinstance(processed_order_keys, list) and order_key in processed_order_keys


def mark_order_key_processed(
    state: dict[str, Any],
    order_key: str,
    status: str,
    payload: dict[str, Any],
) -> None:
    processed_order_keys = state.setdefault("processed_order_keys", [])
    processed_records = state.setdefault("processed_records", [])

    if order_key not in processed_order_keys:
        processed_order_keys.append(order_key)

    processed_records.append(
        {
            "order_key": order_key,
            "payload": payload,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
        }
    )
    save_state(state)


def wait_for_snapshot_window(
    snapshot_time: datetime,
    snapshot_grace_seconds: int,
    logger: logging.Logger,
) -> bool:
    snapshot_deadline = snapshot_time + timedelta(seconds=snapshot_grace_seconds)

    while True:
        now = datetime.now(timezone.utc)

        if now < snapshot_time:
            seconds_until_snapshot = (snapshot_time - now).total_seconds()
            log_json(
                logger,
                "snapshot_wait",
                {
                    "seconds_until_snapshot": round(seconds_until_snapshot, 3),
                    "snapshot_time": snapshot_time.isoformat(),
                },
            )
            time.sleep(min(seconds_until_snapshot, 60))
            continue

        if now > snapshot_deadline:
            log_json(
                logger,
                "snapshot_window_missed",
                {
                    "now": now.isoformat(),
                    "snapshot_deadline": snapshot_deadline.isoformat(),
                    "snapshot_grace_seconds": snapshot_grace_seconds,
                    "snapshot_time": snapshot_time.isoformat(),
                },
            )
            return False

        return True


def sleep_after_processed_date(
    target_date: str,
    logger: logging.Logger,
    poll_interval_seconds: int,
) -> None:
    if os.getenv(TARGET_MARKET_DATE_ENV):
        time.sleep(poll_interval_seconds)
        return

    market_timezone = get_market_timezone()
    now = datetime.now(market_timezone)
    tomorrow = now.date() + timedelta(days=1)
    next_check = datetime.combine(
        tomorrow,
        datetime_time(hour=0, minute=1),
        tzinfo=market_timezone,
    )
    seconds_until_next_check = max(
        poll_interval_seconds,
        int((next_check - now).total_seconds()),
    )
    log_json(
        logger,
        "next_market_date_wait",
        {
            "seconds_until_next_check": seconds_until_next_check,
            "target_date": target_date,
        },
    )
    time.sleep(seconds_until_next_check)


def execute_snapshot_order(
    client: ClobClient,
    logger: logging.Logger,
    state: dict[str, Any],
    target_date: str,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> bool:
    snapshot = load_market_snapshot(target_date, logger)
    prices_by_token_id = get_yes_prices(client, snapshot.markets)
    ranked_markets = rank_markets_by_yes_price(snapshot.markets, prices_by_token_id)
    price_snapshot = build_price_snapshot(ranked_markets)

    log_json(
        logger,
        "ranked_prices_polled",
        {
            "event_id": snapshot.event.get("id"),
            "market_count": len(snapshot.markets),
            "priced_market_count": len(ranked_markets),
            "prices": price_snapshot,
            "target_date": target_date,
            "target_market_rank": TARGET_MARKET_RANK,
        },
    )

    if len(ranked_markets) < TARGET_MARKET_RANK:
        log_json(
            logger,
            "order_skipped_not_enough_priced_markets",
            {
                "priced_market_count": len(ranked_markets),
                "target_market_rank": TARGET_MARKET_RANK,
            },
        )
        return False

    ranked_market = ranked_markets[TARGET_MARKET_RANK - 1]
    order_key = build_order_key(target_date)

    log_json(
        logger,
        "ranked_market_selected",
        {
            "event_id": snapshot.event.get("id"),
            "market_id": ranked_market.market.market_id,
            "market_rank": ranked_market.rank,
            "question": ranked_market.market.question,
            "target_date": target_date,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "yes_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        },
    )

    order_response = buy_ranked_yes_market(
        client=client,
        logger=logger,
        snapshot=snapshot,
        ranked_market=ranked_market,
        buy_amount_usdc=buy_amount_usdc,
        max_buy_price=max_buy_price,
    )

    status = "order_posted" if order_response is not None else "order_skipped"
    mark_order_key_processed(
        state=state,
        order_key=order_key,
        status=status,
        payload={
            "market_id": ranked_market.market.market_id,
            "market_rank": ranked_market.rank,
            "target_date": target_date,
            "yes_price": ranked_market.yes_price,
            "yes_token_id": ranked_market.market.yes_token_id,
        },
    )
    return True


def run_snapshot_cycle(
    client: ClobClient,
    logger: logging.Logger,
    state: dict[str, Any],
    buy_amount_usdc: float,
    max_buy_price: float,
    snapshot_grace_seconds: int,
    poll_interval_seconds: int,
) -> None:
    target_date = resolve_target_market_date()
    order_key = build_order_key(target_date)

    if is_order_key_processed(state, order_key):
        log_json(logger, "order_skipped_already_processed", {"order_key": order_key})
        sleep_after_processed_date(target_date, logger, poll_interval_seconds)
        return

    snapshot_time = build_snapshot_time_utc(target_date)

    if not wait_for_snapshot_window(snapshot_time, snapshot_grace_seconds, logger):
        sleep_after_processed_date(target_date, logger, poll_interval_seconds)
        return

    executed = execute_snapshot_order(
        client=client,
        logger=logger,
        state=state,
        target_date=target_date,
        buy_amount_usdc=buy_amount_usdc,
        max_buy_price=max_buy_price,
    )

    if executed:
        sleep_after_processed_date(target_date, logger, poll_interval_seconds)
    else:
        time.sleep(poll_interval_seconds)


def run_forever() -> None:
    load_env_file()
    logger = build_logger()
    validate_confirmation()

    buy_amount_usdc = get_float_env(BUY_AMOUNT_ENV)
    max_buy_price = get_probability_env(MAX_BUY_PRICE_ENV, DEFAULT_MAX_BUY_PRICE)
    poll_interval_seconds = get_int_env(
        POLL_INTERVAL_SECONDS_ENV,
        DEFAULT_POLL_INTERVAL_SECONDS,
    )
    snapshot_grace_seconds = get_int_env(
        SNAPSHOT_GRACE_SECONDS_ENV,
        DEFAULT_SNAPSHOT_GRACE_SECONDS,
    )

    validate_buy_amount(buy_amount_usdc)
    validate_positive_seconds(POLL_INTERVAL_SECONDS_ENV, poll_interval_seconds)
    validate_positive_seconds(SNAPSHOT_GRACE_SECONDS_ENV, snapshot_grace_seconds)

    log_json(
        logger,
        "script_started",
        {
            "buy_amount_usdc": buy_amount_usdc,
            "market_rank": TARGET_MARKET_RANK,
            "max_buy_price": max_buy_price,
            "poll_interval_seconds": poll_interval_seconds,
            "snapshot_grace_seconds": snapshot_grace_seconds,
            "snapshot_hour_utc": SNAPSHOT_HOUR_UTC,
        },
    )

    client = build_client()
    state = load_state()

    while True:
        try:
            run_snapshot_cycle(
                client=client,
                logger=logger,
                state=state,
                buy_amount_usdc=buy_amount_usdc,
                max_buy_price=max_buy_price,
                snapshot_grace_seconds=snapshot_grace_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )
        except KeyboardInterrupt:
            raise
        except Exception as error:
            logger.exception("cycle_error %s", error)
            time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    run_forever()
