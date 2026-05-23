def build_temperature_market(
    event: dict[str, Any],
    cutoff_time: datetime,
    market: dict[str, Any],
) -> TemperatureMarket:
    return TemperatureMarket(
        event_id=str(event.get("id") or ""),
        event_title=str(event.get("title") or ""),
        event_date=str(event.get("eventDate") or ""),
        event_cutoff_time=cutoff_time,
        market_id=str(market.get("id") or ""),
        condition_id=str(market.get("conditionId") or ""),
        question=str(market.get("question") or ""),
        slug=str(market.get("slug") or ""),
        group_item_title=str(market.get("groupItemTitle") or ""),
        temperature_celsius=parse_market_temperature(market),
        yes_token_id=get_outcome_token_id(market, YES_OUTCOME),
        tick_size=parse_optional_decimal(market.get("orderPriceMinTickSize")),
        order_min_size=parse_order_min_size(market.get("orderMinSize")),
        active=market.get("active") is True,
        closed=market.get("closed") is True,
        accepting_orders=market.get("acceptingOrders") is True,
    )


def build_temperature_event(
    event: dict[str, Any],
    logger: logging.Logger,
) -> TemperatureEvent:
    markets = event.get("markets")

    if not isinstance(markets, list):
        raise ValueError("Target event does not contain a markets list.")

    cutoff_time = parse_event_cutoff_time(event)
    temperature_markets = []

    for market in markets:
        if not isinstance(market, dict):
            continue

        try:
            temperature_markets.append(build_temperature_market(event, cutoff_time, market))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            log_json(
                logger,
                "market_skipped",
                {
                    "error": str(error),
                    "event_id": event.get("id"),
                    "market_id": market.get("id"),
                    "question": market.get("question"),
                },
            )

    return TemperatureEvent(
        event_id=str(event.get("id") or ""),
        title=str(event.get("title") or ""),
        slug=str(event.get("slug") or ""),
        event_date=str(event.get("eventDate") or ""),
        cutoff_time=cutoff_time,
        markets=sorted(temperature_markets, key=lambda item: item.temperature_celsius),
    )


def is_tradeable_market(market: TemperatureMarket) -> bool:
    return market.closed is False and market.active is True and market.accepting_orders is True


def get_tradeable_markets(event: TemperatureEvent) -> list[TemperatureMarket]:
    return [market for market in event.markets if is_tradeable_market(market)]


def load_temperature_events(logger: logging.Logger) -> list[TemperatureEvent]:
    raw_events = get_gamma_events()
    temperature_events = []

    for event in raw_events:
        if not is_target_temperature_event(event):
            continue

        temperature_event = build_temperature_event(event, logger)

        if not temperature_event.markets:
            log_json(
                logger,
                "event_skipped_no_markets",
                {"event_id": temperature_event.event_id, "title": temperature_event.title},
            )
            continue

        temperature_events.append(temperature_event)

    temperature_events.sort(key=lambda item: (item.cutoff_time, item.event_id))

    log_json(
        logger,
        "events_loaded",
        {
            "event_count": len(temperature_events),
            "events": [
                {
                    "cutoff_time": event.cutoff_time.isoformat(),
                    "event_date": event.event_date,
                    "event_id": event.event_id,
                    "market_count": len(event.markets),
                    "title": event.title,
                    "tradeable_market_count": len(get_tradeable_markets(event)),
                }
                for event in temperature_events
            ],
        },
    )

    return temperature_events


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


def parse_price_payload(value: Any, side: str) -> Optional[float]:
    direct_price = parse_numeric_price(value)

    if direct_price is not None:
        return direct_price

    if not isinstance(value, dict):
        return None

    for price_key in PRICE_RESPONSE_VALUE_KEYS_BY_SIDE[side]:
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
    side: str,
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

        response_side = get_first_mapping_value(entry, PRICE_RESPONSE_SIDE_KEYS)

        if response_side is not None and str(response_side).upper() != side:
            continue

        price = parse_price_payload(entry, side)

        if price is not None:
            prices[token_id_text] = price

    return prices


def extract_prices_from_keyed_dict(
    response_json: dict[str, Any],
    requested_token_ids: set[str],
    side: str,
) -> dict[str, float]:
    prices: dict[str, float] = {}

    for token_id in requested_token_ids:
        if token_id not in response_json:
            continue

        price = parse_price_payload(response_json.get(token_id), side)

        if price is not None:
            prices[token_id] = price

    return prices


def extract_side_prices(
    response_json: Any,
    markets: list[TemperatureMarket],
    side: str,
) -> dict[str, float]:
    requested_token_ids = {market.yes_token_id for market in markets}

    if isinstance(response_json, list):
        return extract_prices_from_entries(response_json, requested_token_ids, side)

    if not isinstance(response_json, dict):
        return {}

    prices = extract_prices_from_keyed_dict(response_json, requested_token_ids, side)

    for list_key in PRICE_RESPONSE_LIST_KEYS:
        list_value = response_json.get(list_key)

        if isinstance(list_value, list):
            prices.update(extract_prices_from_entries(list_value, requested_token_ids, side))

        if isinstance(list_value, dict):
            prices.update(extract_prices_from_keyed_dict(list_value, requested_token_ids, side))

    return prices


def get_yes_side_prices(
    client: ClobClient,
    markets: list[TemperatureMarket],
    side: str,
) -> dict[str, float]:
    if not markets:
        return {}

    price_requests = [
        BookParams(token_id=market.yes_token_id, side=side)
        for market in markets
    ]
    response = client.get_prices(price_requests)
    return extract_side_prices(response, markets, side)


def build_price_snapshot(
    markets: list[TemperatureMarket],
    buy_prices_by_token_id: dict[str, float],
    sell_prices_by_token_id: dict[str, float],
) -> list[dict[str, Any]]:
    return [
        {
            "event_id": market.event_id,
            "market_id": market.market_id,
            "question": market.question,
            "temperature_celsius": market.temperature_celsius,
            "yes_buy_price": buy_prices_by_token_id.get(market.yes_token_id),
            "yes_sell_price": sell_prices_by_token_id.get(market.yes_token_id),
            "yes_token_id": market.yes_token_id,
        }
        for market in markets
    ]


def decimal_price_to_float(value: Decimal) -> float:
    return float(value)


def decimal_price_to_log(value: Decimal) -> str:
    return format(value.normalize(), "f")


def get_market_tick_size(client: ClobClient, market: TemperatureMarket) -> Decimal:
    if market.tick_size is not None:
        return market.tick_size

    return parse_decimal_text(str(client.get_tick_size(market.yes_token_id)))


def is_valid_limit_price(price: Decimal, tick_size: Decimal) -> bool:
    if price < tick_size:
        return False

    if price > Decimal("1") - tick_size:
        return False

    units = price / tick_size
    return units == units.to_integral_value()


def get_open_order_side(order: dict[str, Any]) -> Optional[str]:
    side = get_first_mapping_value(order, OPEN_ORDER_SIDE_KEYS)

    if side is None:
        return None

    return str(side).upper()


def get_open_order_asset_id(order: dict[str, Any]) -> Optional[str]:
    asset_id = get_first_mapping_value(order, OPEN_ORDER_ASSET_KEYS)

    if asset_id is None:
        return None

    return str(asset_id)


def parse_open_order_price(order: dict[str, Any]) -> Optional[Decimal]:
    for key in OPEN_ORDER_PRICE_KEYS:
        value = order.get(key)

        if value is None or value == "":
            continue

        return parse_decimal_text(str(value))

    return None


def get_open_buy_order_prices(
    client: ClobClient,
    market: TemperatureMarket,
) -> set[Decimal]:
    orders = client.get_open_orders(OpenOrderParams(asset_id=market.yes_token_id))
    open_prices: set[Decimal] = set()

    for order in orders:
        if not isinstance(order, dict):
            continue

        asset_id = get_open_order_asset_id(order)

        if asset_id is not None and asset_id != market.yes_token_id:
            continue

        side = get_open_order_side(order)

        if side is not None and side != BUY_SIDE:
            continue

        price = parse_open_order_price(order)

        if price is not None:
            open_prices.add(price)

    return open_prices


def ensure_limit_orders_for_market(
    client: ClobClient,
    logger: logging.Logger,
    state: RuntimeState,
    config: StrategyConfig,
    market: TemperatureMarket,
    trigger_price: float,
) -> None:
    if config.order_size_shares < market.order_min_size:
        log_json(
            logger,
            "limit_orders_skipped_order_size_below_market_min",
            {
                "configured_order_size_shares": config.order_size_shares,
                "market_id": market.market_id,
                "market_order_min_size": market.order_min_size,
                "question": market.question,
            },
        )
        return

    tick_size = get_market_tick_size(client, market)
    posted_prices = set(state.posted_order_prices_by_market.get(market.market_id, set()))
    open_prices = get_open_buy_order_prices(client, market)
    tracked_limit_prices = set(config.limit_order_prices)
    posted_prices.update(open_prices.intersection(tracked_limit_prices))

    if posted_prices:
        state.posted_order_prices_by_market[market.market_id] = posted_prices

    for limit_price in config.limit_order_prices:
        if limit_price in posted_prices:
            continue

        if not is_valid_limit_price(limit_price, tick_size):
            log_json(
                logger,
                "limit_order_price_skipped_invalid_tick",
                {
                    "limit_price": decimal_price_to_log(limit_price),
                    "market_id": market.market_id,
                    "question": market.question,
                    "tick_size": decimal_price_to_log(tick_size),
                },
            )
            continue

        order_args = OrderArgsV2(
            token_id=market.yes_token_id,
            price=decimal_price_to_float(limit_price),
            size=config.order_size_shares,
            side=BUY_SIDE,
        )

        log_json(
            logger,
            "limit_order_attempt",
            {
                "event_id": market.event_id,
                "event_title": market.event_title,
                "limit_price": decimal_price_to_log(limit_price),
                "market_id": market.market_id,
                "order_size_shares": config.order_size_shares,
                "order_type": LIMIT_ORDER_TYPE,
                "post_only": True,
                "question": market.question,
                "tick_size": decimal_price_to_log(tick_size),
                "trigger_yes_buy_price": trigger_price,
                "yes_token_id": market.yes_token_id,
            },
        )

        response = client.create_and_post_order(
            order_args,
            order_type=LIMIT_ORDER_TYPE,
            post_only=True,
        )
        payload = response if isinstance(response, dict) else {"response": response}
        log_json(logger, "limit_order_response", payload)
        posted_prices.add(limit_price)
        state.posted_order_prices_by_market[market.market_id] = posted_prices


def select_entry_markets_for_event(
    event: TemperatureEvent,
    buy_prices_by_token_id: dict[str, float],
    config: StrategyConfig,
    deactivated_market_ids: set[str],
) -> list[tuple[TemperatureMarket, float]]:
    candidates = []

    for market in get_tradeable_markets(event):
        if market.market_id in deactivated_market_ids:
            continue

        trigger_price = buy_prices_by_token_id.get(market.yes_token_id)

        if trigger_price is None or trigger_price <= config.entry_price_threshold:
            continue

        candidates.append((market, trigger_price))

    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates[:config.max_entry_markets_per_event]


def parse_raw_balance_amount(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return float(Decimal(value) / CONDITIONAL_TOKEN_DECIMAL_FACTOR)

    if isinstance(value, float):
        return value

    if isinstance(value, str):
        text = value.strip()

        if not text:
            return None

        if "." in text:
            return float(text)

        return float(parse_decimal_text(text) / CONDITIONAL_TOKEN_DECIMAL_FACTOR)

    return None


def extract_balance_from_response(response: Any, token_id: str) -> float:
    if isinstance(response, dict):
        if token_id in response:
            direct_token_balance = parse_raw_balance_amount(response.get(token_id))

            if direct_token_balance is not None:
                return direct_token_balance

        for key in BALANCE_RESPONSE_KEYS:
            if key not in response:
                continue

            balance = parse_raw_balance_amount(response.get(key))

            if balance is not None:
                return balance

        for value in response.values():
            if isinstance(value, (dict, list)):
                nested_balance = extract_balance_from_response(value, token_id)

                if nested_balance > 0:
                    return nested_balance

    if isinstance(response, list):
        for item in response:
            nested_balance = extract_balance_from_response(item, token_id)

            if nested_balance > 0:
                return nested_balance

    return 0.0


def get_yes_share_balance(client: ClobClient, market: TemperatureMarket) -> tuple[float, Any]:
    response = client.get_balance_allowance(
        BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=market.yes_token_id,
        )
    )
    return extract_balance_from_response(response, market.yes_token_id), response


def get_exit_price_for_market(
    client: ClobClient,
    market: TemperatureMarket,
    requested_price: Decimal,
) -> Decimal:
    tick_size = get_market_tick_size(client, market)

    if requested_price < tick_size:
        return tick_size

    if requested_price > Decimal("1") - tick_size:
        return Decimal("1") - tick_size

    return requested_price


def sell_yes_shares(
    client: ClobClient,
    logger: logging.Logger,
    market: TemperatureMarket,
    requested_min_sell_price: Decimal,
    reason: str,
) -> Optional[dict[str, Any]]:
    balance, raw_balance_response = get_yes_share_balance(client, market)

    if balance <= MIN_POSITION_TO_SELL:
        log_json(
            logger,
            "sell_skipped_no_position",
            {
                "market_id": market.market_id,
                "question": market.question,
                "raw_balance_response": raw_balance_response,
                "reason": reason,
                "yes_balance": balance,
                "yes_token_id": market.yes_token_id,
            },
        )
        return None

    min_sell_price = get_exit_price_for_market(client, market, requested_min_sell_price)
    order_args = MarketOrderArgsV2(
        token_id=market.yes_token_id,
        amount=balance,
        side=SELL_SIDE,
        price=decimal_price_to_float(min_sell_price),
        order_type=MARKET_SELL_ORDER_TYPE,
    )

    log_json(
        logger,
        "sell_order_attempt",
        {
            "event_id": market.event_id,
            "market_id": market.market_id,
            "min_sell_price": decimal_price_to_log(min_sell_price),
            "order_type": MARKET_SELL_ORDER_TYPE,
            "question": market.question,
            "reason": reason,
            "side": SELL_SIDE,
            "yes_balance": balance,
            "yes_token_id": market.yes_token_id,
        },
    )

    response = client.create_and_post_market_order(
        order_args,
        order_type=MARKET_SELL_ORDER_TYPE,
    )
    payload = response if isinstance(response, dict) else {"response": response}
    log_json(logger, "sell_order_response", payload)
    return payload


def cancel_market_orders(
    client: ClobClient,
    logger: logging.Logger,
    market: TemperatureMarket,
    reason: str,
) -> Optional[dict[str, Any]]:
    log_json(
        logger,
        "cancel_market_orders_attempt",
        {
            "condition_id": market.condition_id,
            "market_id": market.market_id,
            "question": market.question,
            "reason": reason,
            "yes_token_id": market.yes_token_id,
        },
    )
    response = client.cancel_market_orders(
        OrderMarketCancelParams(
            market=market.condition_id,
            asset_id=market.yes_token_id,
        )
    )
    payload = response if isinstance(response, dict) else {"response": response}
    log_json(logger, "cancel_market_orders_response", payload)
    return payload


def safe_cancel_market_orders(
    client: ClobClient,
    logger: logging.Logger,
    market: TemperatureMarket,
    reason: str,
) -> None:
    try:
        cancel_market_orders(client, logger, market, reason)
    except Exception as error:
        logger.exception("cancel_market_orders_error %s", error)


def safe_sell_yes_shares(
    client: ClobClient,
    logger: logging.Logger,
    market: TemperatureMarket,
    requested_min_sell_price: Decimal,
    reason: str,
) -> None:
    try:
        sell_yes_shares(client, logger, market, requested_min_sell_price, reason)
    except Exception as error:
        logger.exception("sell_order_error %s", error)


def handle_event_cutoff(
    client: ClobClient,
    logger: logging.Logger,
    state: RuntimeState,
    config: StrategyConfig,
    event: TemperatureEvent,
    now: datetime,
) -> bool:
    if event.event_id in state.cutoff_event_ids:
        return True

    if now < event.cutoff_time:
        return False

    log_json(
        logger,
        "event_cutoff_triggered",
        {
            "cutoff_time": event.cutoff_time.isoformat(),
            "event_id": event.event_id,
            "now": now.isoformat(),
            "title": event.title,
        },
    )

    for market in event.markets:
        safe_cancel_market_orders(client, logger, market, "event_cutoff")
        safe_sell_yes_shares(
            client,
            logger,
            market,
            config.force_exit_min_sell_price,
            "event_cutoff",
        )
        state.deactivated_market_ids.add(market.market_id)
        state.posted_order_prices_by_market.pop(market.market_id, None)
        state.low_price_started_at_by_market.pop(market.market_id, None)

    state.cutoff_event_ids.add(event.event_id)
    return True


def handle_take_profit(
    client: ClobClient,
    logger: logging.Logger,
    config: StrategyConfig,
    market: TemperatureMarket,
    sell_price: Optional[float],
) -> None:
    if sell_price is None or sell_price <= config.take_profit_price:
        return

    sell_yes_shares(
        client,
        logger,
        market,
        Decimal(str(config.take_profit_price)),
        "take_profit",
    )


def handle_sustained_low_exit(
    client: ClobClient,
    logger: logging.Logger,
    state: RuntimeState,
    config: StrategyConfig,
    market: TemperatureMarket,
    sell_price: Optional[float],
    now: datetime,
) -> None:
    if sell_price is None:
        return

    if sell_price >= config.low_exit_price:
        started_at = state.low_price_started_at_by_market.pop(market.market_id, None)

        if started_at is not None:
            log_json(
                logger,
                "low_price_cleared",
                {
                    "elapsed_seconds": (now - started_at).total_seconds(),
                    "market_id": market.market_id,
                    "question": market.question,
                    "yes_sell_price": sell_price,
                },
            )

        return

    started_at = state.low_price_started_at_by_market.get(market.market_id)

    if started_at is None:
        state.low_price_started_at_by_market[market.market_id] = now
        log_json(
            logger,
            "low_price_started",
            {
                "low_exit_price": config.low_exit_price,
                "low_exit_seconds": config.low_exit_seconds,
                "market_id": market.market_id,
                "question": market.question,
                "started_at": now.isoformat(),
                "yes_sell_price": sell_price,
            },
        )
        return

    elapsed_seconds = (now - started_at).total_seconds()

    log_json(
        logger,
        "low_price_continued",
        {
            "elapsed_seconds": elapsed_seconds,
            "low_exit_price": config.low_exit_price,
            "low_exit_seconds": config.low_exit_seconds,
            "market_id": market.market_id,
            "question": market.question,
            "started_at": started_at.isoformat(),
            "yes_sell_price": sell_price,
        },
    )

    if elapsed_seconds < config.low_exit_seconds:
        return

    log_json(
        logger,
        "low_price_exit_triggered",
        {
            "elapsed_seconds": elapsed_seconds,
            "low_exit_price": config.low_exit_price,
            "market_id": market.market_id,
            "question": market.question,
            "yes_sell_price": sell_price,
        },
    )

    safe_cancel_market_orders(client, logger, market, "sustained_low_price")
    safe_sell_yes_shares(
        client,
        logger,
        market,
        config.force_exit_min_sell_price,
        "sustained_low_price",
    )
    state.deactivated_market_ids.add(market.market_id)
    state.posted_order_prices_by_market.pop(market.market_id, None)
    state.low_price_started_at_by_market.pop(market.market_id, None)


def merge_unique_markets(markets: list[TemperatureMarket]) -> list[TemperatureMarket]:
    unique_markets: dict[str, TemperatureMarket] = {}

    for market in markets:
        unique_markets[market.market_id] = market

    return list(unique_markets.values())


def refresh_events_if_needed(
    logger: logging.Logger,
    state: RuntimeState,
    config: StrategyConfig,
    now: datetime,
) -> None:
    if state.events and now < state.next_market_refresh_time:
        return

    events = load_temperature_events(logger)
    state.events = events
    state.markets_by_id.update(
        {
            market.market_id: market
            for event in events
            for market in event.markets
        }
    )
    state.next_market_refresh_time = now + timedelta(
        seconds=config.market_refresh_interval_seconds
    )

    log_json(
        logger,
        "next_market_refresh_scheduled",
        {"next_market_refresh_time": state.next_market_refresh_time.isoformat()},
    )


def get_monitor_markets(state: RuntimeState) -> list[TemperatureMarket]:
    markets = []

    for market_id in state.posted_order_prices_by_market:
        market = state.markets_by_id.get(market_id)

        if market is not None:
            markets.append(market)

    return markets


def run_price_cycle(
    client: ClobClient,
    logger: logging.Logger,
    state: RuntimeState,
    config: StrategyConfig,
) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    refresh_events_if_needed(logger, state, config, now)

    for event in state.events:
        handle_event_cutoff(client, logger, state, config, event, now)

    active_events = [
        event
        for event in state.events
        if event.event_id not in state.cutoff_event_ids and now < event.cutoff_time
    ]
    tradeable_markets = [
        market
        for event in active_events
        for market in get_tradeable_markets(event)
    ]
    monitor_markets = get_monitor_markets(state)
    price_markets = merge_unique_markets(tradeable_markets + monitor_markets)
    buy_prices_by_token_id = get_yes_side_prices(client, price_markets, BUY_SIDE)
    sell_prices_by_token_id = get_yes_side_prices(client, price_markets, SELL_SIDE)

    log_json(
        logger,
        "prices_polled",
        {
            "market_count": len(price_markets),
            "prices": build_price_snapshot(
                price_markets,
                buy_prices_by_token_id,
                sell_prices_by_token_id,
            ),
        },
    )

    for event in active_events:
        selected_markets = select_entry_markets_for_event(
            event,
            buy_prices_by_token_id,
            config,
            state.deactivated_market_ids,
        )

        log_json(
            logger,
            "entry_markets_selected",
            {
                "entry_price_threshold": config.entry_price_threshold,
                "event_id": event.event_id,
                "selected": [
                    {
                        "market_id": market.market_id,
                        "question": market.question,
                        "yes_buy_price": trigger_price,
                    }
                    for market, trigger_price in selected_markets
                ],
                "title": event.title,
            },
        )

        for market, trigger_price in selected_markets:
            ensure_limit_orders_for_market(
                client,
                logger,
                state,
                config,
                market,
                trigger_price,
            )

    for market in get_monitor_markets(state):
        if market.market_id in state.deactivated_market_ids:
            continue

        sell_price = sell_prices_by_token_id.get(market.yes_token_id)
        handle_take_profit(client, logger, config, market, sell_price)
        handle_sustained_low_exit(client, logger, state, config, market, sell_price, now)


def build_initial_state() -> RuntimeState:
    return RuntimeState(
        events=[],
        markets_by_id={},
        next_market_refresh_time=MIN_UTC_TIME,
        posted_order_prices_by_market={},
        low_price_started_at_by_market={},
        deactivated_market_ids=set(),
        cutoff_event_ids=set(),
    )


def sleep_until_next_cycle(seconds: int) -> None:
    time.sleep(seconds)


def run_forever() -> None:
    load_env_file()
    logger = build_logger()
    validate_confirmation()
    config = build_strategy_config()
    client = build_client()
    state = build_initial_state()

    log_json(
        logger,
        "strategy_started",
        {
            "entry_price_threshold": config.entry_price_threshold,
            "force_exit_min_sell_price": decimal_price_to_log(
                config.force_exit_min_sell_price
            ),
            "limit_order_prices": [
                decimal_price_to_log(price)
                for price in config.limit_order_prices
            ],
            "low_exit_price": config.low_exit_price,
            "low_exit_seconds": config.low_exit_seconds,
            "market_refresh_interval_seconds": config.market_refresh_interval_seconds,
            "max_entry_markets_per_event": config.max_entry_markets_per_event,
            "order_size_shares": config.order_size_shares,
            "poll_interval_seconds": config.poll_interval_seconds,
            "take_profit_price": config.take_profit_price,
        },
    )

    while True:
        try:
            run_price_cycle(client, logger, state, config)
        except KeyboardInterrupt:
            raise
        except Exception as error:
            logger.exception("cycle_error %s", error)

        sleep_until_next_cycle(config.poll_interval_seconds)


if __name__ == "__main__":
    run_forever()
