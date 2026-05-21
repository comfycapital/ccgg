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


def build_price_snapshot(
    markets: list[TemperatureMarket],
    prices_by_token_id: dict[str, float],
) -> list[dict[str, Any]]:
    price_snapshot = []

    for market in markets:
        price_snapshot.append(
            {
                "market_id": market.market_id,
                "question": market.question,
                "temperature_celsius": market.temperature_celsius,
                "yes_price": prices_by_token_id.get(market.yes_token_id),
                "yes_token_id": market.yes_token_id,
            }
        )

    return price_snapshot


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
    market: TemperatureMarket,
    trigger_price: float,
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
            "condition_id": market.condition_id,
            "error": get_error_message(error),
            "error_type": type(error).__name__,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "max_buy_price": max_buy_price,
            "question": market.question,
            "reason": reason,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": market.temperature_celsius,
            "trigger_price": trigger_price,
            "yes_token_id": market.yes_token_id,
        },
    )


def log_order_skipped_price_above_max(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    market: TemperatureMarket,
    trigger_price: float,
    required_price: float,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> None:
    log_json(
        logger,
        "order_skipped_price_above_max",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": market.condition_id,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "max_buy_price": max_buy_price,
            "question": market.question,
            "required_price": required_price,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": market.temperature_celsius,
            "trigger_price": trigger_price,
            "yes_token_id": market.yes_token_id,
        },
    )


def log_order_skipped_trigger_above_max(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    market: TemperatureMarket,
    trigger_price: float,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> None:
    log_json(
        logger,
        "order_skipped_trigger_above_max",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": market.condition_id,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "max_buy_price": max_buy_price,
            "question": market.question,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": market.temperature_celsius,
            "trigger_price": trigger_price,
            "yes_token_id": market.yes_token_id,
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


def buy_yes_market(
    client: ClobClient,
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    market: TemperatureMarket,
    trigger_price: float,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> Optional[dict[str, Any]]:
    if trigger_price > max_buy_price:
        log_order_skipped_trigger_above_max(
            logger=logger,
            snapshot=snapshot,
            market=market,
            trigger_price=trigger_price,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        return None

    try:
        required_price = calculate_required_market_price(client, market, buy_amount_usdc)
    except Exception as error:
        no_match_reason = get_clob_no_match_reason(error)

        if no_match_reason is None:
            raise

        log_order_skipped_no_liquidity(
            logger=logger,
            snapshot=snapshot,
            market=market,
            trigger_price=trigger_price,
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
            market=market,
            trigger_price=trigger_price,
            required_price=required_price,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        return None

    order_args = build_order_args(market, buy_amount_usdc, max_buy_price)

    log_json(
        logger,
        "order_attempt",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": market.condition_id,
            "event_id": snapshot.event.get("id"),
            "event_title": snapshot.event.get("title"),
            "market_id": market.market_id,
            "market_order_type": MARKET_ORDER_TYPE,
            "max_buy_price": max_buy_price,
            "question": market.question,
            "required_price": required_price,
            "side": BUY_SIDE,
            "target_date": snapshot.target_date,
            "temperature_celsius": market.temperature_celsius,
            "trigger_price": trigger_price,
            "yes_token_id": market.yes_token_id,
        },
    )

    response = client.create_and_post_market_order(
        order_args,
        order_type=MARKET_ORDER_TYPE,
    )
    payload = response if isinstance(response, dict) else {"response": response}
    log_json(logger, "order_response", payload)
    return payload


def run_price_cycle(
    client: ClobClient,
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    bought_market_ids: set[str],
    buy_amount_usdc: float,
    price_threshold: float,
    max_buy_price: float,
) -> None:
    unbought_markets = [
        market
        for market in snapshot.markets
        if market.market_id not in bought_market_ids
    ]

    if not unbought_markets:
        log_json(
            logger,
            "all_markets_already_bought",
            {
                "event_id": snapshot.event.get("id"),
                "target_date": snapshot.target_date,
            },
        )
        return

    prices_by_token_id = get_yes_prices(client, unbought_markets)
    price_snapshot = build_price_snapshot(unbought_markets, prices_by_token_id)

    log_json(
        logger,
        "prices_polled",
        {
            "event_id": snapshot.event.get("id"),
            "price_count": len(prices_by_token_id),
            "price_threshold": price_threshold,
            "prices": price_snapshot,
            "target_date": snapshot.target_date,
        },
    )

    for market in unbought_markets:
        trigger_price = prices_by_token_id.get(market.yes_token_id)

        if trigger_price is None or trigger_price <= price_threshold:
            continue

        log_json(
            logger,
            "price_triggered",
            {
                "event_id": snapshot.event.get("id"),
                "market_id": market.market_id,
                "price_threshold": price_threshold,
                "question": market.question,
                "target_date": snapshot.target_date,
                "temperature_celsius": market.temperature_celsius,
                "trigger_price": trigger_price,
                "yes_token_id": market.yes_token_id,
            },
        )

        order_response = buy_yes_market(
            client=client,
            logger=logger,
            snapshot=snapshot,
            market=market,
            trigger_price=trigger_price,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )

        if order_response is not None:
            bought_market_ids.add(market.market_id)


def maybe_refresh_market_snapshot(
    logger: logging.Logger,
    snapshot: Optional[MarketSnapshot],
    bought_market_ids: set[str],
    next_market_refresh_time: datetime,
    market_refresh_interval_seconds: int,
) -> tuple[MarketSnapshot, datetime]:
    target_date = resolve_target_market_date()
    now = datetime.now(timezone.utc)
    needs_first_load = snapshot is None
    needs_date_reload = snapshot is not None and snapshot.target_date != target_date
    needs_scheduled_reload = now >= next_market_refresh_time

    if not (needs_first_load or needs_date_reload or needs_scheduled_reload):
        return snapshot, next_market_refresh_time

    if needs_date_reload:
        bought_market_ids.clear()
        log_json(
            logger,
            "target_date_changed",
            {
                "new_target_date": target_date,
                "old_target_date": snapshot.target_date if snapshot else None,
            },
        )

    refreshed_snapshot = load_market_snapshot(target_date, logger)
    refreshed_next_market_refresh_time = now + timedelta(seconds=market_refresh_interval_seconds)
    return refreshed_snapshot, refreshed_next_market_refresh_time


def run_forever() -> None:
    load_env_file()
    logger = build_logger()
    validate_confirmation()

    buy_amount_usdc = get_float_env(BUY_AMOUNT_ENV)
    price_threshold = get_probability_env(PRICE_THRESHOLD_ENV, DEFAULT_PRICE_THRESHOLD)
    max_buy_price = get_probability_env(MAX_BUY_PRICE_ENV, DEFAULT_MAX_BUY_PRICE)
    poll_interval_seconds = get_int_env(
        POLL_INTERVAL_SECONDS_ENV,
        DEFAULT_POLL_INTERVAL_SECONDS,
    )
    market_refresh_interval_seconds = get_int_env(
        MARKET_REFRESH_INTERVAL_SECONDS_ENV,
        DEFAULT_MARKET_REFRESH_INTERVAL_SECONDS,
    )

    validate_buy_amount(buy_amount_usdc)
    validate_positive_seconds(POLL_INTERVAL_SECONDS_ENV, poll_interval_seconds)
    validate_positive_seconds(MARKET_REFRESH_INTERVAL_SECONDS_ENV, market_refresh_interval_seconds)

    client = build_client()
    snapshot: Optional[MarketSnapshot] = None
    bought_market_ids: set[str] = set()
    next_market_refresh_time = MIN_UTC_TIME

    while True:
        try:
            snapshot, next_market_refresh_time = maybe_refresh_market_snapshot(
                logger=logger,
                snapshot=snapshot,
                bought_market_ids=bought_market_ids,
                next_market_refresh_time=next_market_refresh_time,
                market_refresh_interval_seconds=market_refresh_interval_seconds,
            )
            run_price_cycle(
                client=client,
                logger=logger,
                snapshot=snapshot,
                bought_market_ids=bought_market_ids,
                buy_amount_usdc=buy_amount_usdc,
                price_threshold=price_threshold,
                max_buy_price=max_buy_price,
            )
        except KeyboardInterrupt:
            raise
        except Exception as error:
            logger.exception("cycle_error %s", error)

        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    run_forever()
