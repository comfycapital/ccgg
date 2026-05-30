def log_order_skipped_price_above_max(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    target_outcome: str,
    required_price: float,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> None:
    outcome_token_id = get_market_outcome_token_id(ranked_market.market, target_outcome)
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
            "target_outcome": target_outcome,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.outcome_price,
            "outcome_token_id": outcome_token_id,
        },
    )


def log_order_skipped_trigger_above_max(
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    target_outcome: str,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> None:
    outcome_token_id = get_market_outcome_token_id(ranked_market.market, target_outcome)
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
            "target_outcome": target_outcome,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.outcome_price,
            "outcome_token_id": outcome_token_id,
        },
    )


def calculate_required_market_price(
    client: ClobClient,
    market: TemperatureMarket,
    target_outcome: str,
    buy_amount_usdc: float,
) -> float:
    return client.calculate_market_price(
        get_market_outcome_token_id(market, target_outcome),
        BUY_SIDE,
        buy_amount_usdc,
        MARKET_ORDER_TYPE,
    )


def build_order_args(
    market: TemperatureMarket,
    target_outcome: str,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> MarketOrderArgsV2:
    return MarketOrderArgsV2(
        token_id=get_market_outcome_token_id(market, target_outcome),
        amount=buy_amount_usdc,
        side=BUY_SIDE,
        price=max_buy_price,
        order_type=MARKET_ORDER_TYPE,
    )


def buy_ranked_outcome_market(
    client: ClobClient,
    logger: logging.Logger,
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    target_outcome: str,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> Optional[dict[str, Any]]:
    outcome_token_id = get_market_outcome_token_id(ranked_market.market, target_outcome)

    if ranked_market.outcome_price > max_buy_price:
        log_order_skipped_trigger_above_max(
            logger=logger,
            snapshot=snapshot,
            ranked_market=ranked_market,
            target_outcome=target_outcome,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        return None

    try:
        required_price = calculate_required_market_price(
            client=client,
            market=ranked_market.market,
            target_outcome=target_outcome,
            buy_amount_usdc=buy_amount_usdc,
        )
    except Exception as error:
        no_match_reason = get_clob_no_match_reason(error, BUY_SIDE)

        if no_match_reason is None:
            raise

        log_order_skipped_no_liquidity(
            logger=logger,
            snapshot=snapshot,
            ranked_market=ranked_market,
            target_outcome=target_outcome,
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
            target_outcome=target_outcome,
            required_price=required_price,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        return None

    order_args = build_order_args(
        market=ranked_market.market,
        target_outcome=target_outcome,
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
            "target_outcome": target_outcome,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "trigger_price": ranked_market.outcome_price,
            "outcome_token_id": outcome_token_id,
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
        return build_empty_state()

    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return build_empty_state()

    if not isinstance(state, dict):
        return build_empty_state()

    state.setdefault("processed_order_keys", [])
    state.setdefault("processed_records", [])
    state.setdefault("active_position", None)
    return state


def build_empty_state() -> dict[str, Any]:
    return {
        "active_position": None,
        "processed_order_keys": [],
        "processed_records": [],
    }


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def build_buy_order_key(target_date: str, schedule: TradeSchedule, target_outcome: str) -> str:
    return (
        f"{target_date}:buy_day_offset={schedule.buy_day_offset}:buy_hour={schedule.buy_hour_utc}:"
        f"rank={TARGET_MARKET_RANK}:side={target_outcome}"
    )


def build_sell_order_key(target_date: str, schedule: TradeSchedule, target_outcome: str) -> str:
    return (
        f"{target_date}:sell_day_offset={schedule.sell_day_offset}:sell_hour={schedule.sell_hour_utc}:"
        f"rank={TARGET_MARKET_RANK}:side={target_outcome}"
    )


def build_buy_slice_order_key(
    target_date: str,
    schedule: TradeSchedule,
    target_outcome: str,
    slice_number: int,
) -> str:
    return f"{build_buy_order_key(target_date, schedule, target_outcome)}:slice={slice_number}"


def build_sell_slice_order_key(
    target_date: str,
    schedule: TradeSchedule,
    target_outcome: str,
    slice_number: int,
) -> str:
    return f"{build_sell_order_key(target_date, schedule, target_outcome)}:slice={slice_number}"


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


def get_active_position(state: dict[str, Any]) -> Optional[dict[str, Any]]:
    active_position = state.get("active_position")

    if isinstance(active_position, dict):
        return active_position

    return None


def set_active_position(state: dict[str, Any], position: dict[str, Any]) -> None:
    state["active_position"] = position
    save_state(state)


def save_active_position(state: dict[str, Any], position: dict[str, Any]) -> None:
    state["active_position"] = position
    save_state(state)


def clear_active_position(state: dict[str, Any]) -> None:
    state["active_position"] = None
    save_state(state)


def wait_for_buy_window(
    buy_time: datetime,
    snapshot_grace_seconds: int,
    logger: logging.Logger,
) -> bool:
    buy_deadline = buy_time + timedelta(seconds=snapshot_grace_seconds)

    while True:
        now = datetime.now(timezone.utc)

        if now < buy_time:
            seconds_until_buy = (buy_time - now).total_seconds()
            log_json(
                logger,
                "buy_wait",
                {
                    "buy_time": buy_time.isoformat(),
                    "seconds_until_buy": round(seconds_until_buy, 3),
                },
            )
            time.sleep(min(seconds_until_buy, 60))
            continue

        if now > buy_deadline:
            log_json(
                logger,
                "buy_window_missed",
                {
                    "buy_deadline": buy_deadline.isoformat(),
                    "buy_time": buy_time.isoformat(),
                    "now": now.isoformat(),
                    "snapshot_grace_seconds": snapshot_grace_seconds,
                },
            )
            return False

        return True


def wait_until_sell_time(sell_time: datetime, logger: logging.Logger) -> None:
    while True:
        now = datetime.now(timezone.utc)

        if now >= sell_time:
            return

        seconds_until_sell = (sell_time - now).total_seconds()
        log_json(
            logger,
            "sell_wait",
            {
                "seconds_until_sell": round(seconds_until_sell, 3),
                "sell_time": sell_time.isoformat(),
            },
        )
        time.sleep(min(seconds_until_sell, 60))


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


def build_position_record(
    snapshot: MarketSnapshot,
    ranked_market: RankedMarket,
    target_outcome: str,
    schedule: TradeSchedule,
    twap_config: TwapConfig,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> dict[str, Any]:
    outcome_token_id = get_market_outcome_token_id(ranked_market.market, target_outcome)

    return {
        "buy_amount_usdc": buy_amount_usdc,
        "buy_order_responses": [],
        "buy_twap_complete": False,
        "completed_buy_slices": [],
        "buy_price": ranked_market.outcome_price,
        "condition_id": ranked_market.market.condition_id,
        "event_id": snapshot.event.get("id"),
        "event_title": snapshot.event.get("title"),
        "market_id": ranked_market.market.market_id,
        "market_rank": ranked_market.rank,
        "max_buy_price": max_buy_price,
        "no_token_id": ranked_market.market.no_token_id,
        "outcome_token_id": outcome_token_id,
        "question": ranked_market.market.question,
        "schedule": build_schedule_record(schedule),
        "target_outcome": target_outcome,
        "target_date": snapshot.target_date,
        "temperature_celsius": ranked_market.market.temperature_celsius,
        "trade_times": build_trade_times_record(snapshot.target_date, schedule),
        "twap": build_twap_config_record(twap_config),
        "yes_token_id": ranked_market.market.yes_token_id,
    }


def build_market_snapshot_from_position(position: dict[str, Any]) -> MarketSnapshot:
    return MarketSnapshot(
        target_date=str(position.get("target_date") or ""),
        event={
            "id": position.get("event_id"),
            "title": position.get("event_title"),
        },
        markets=[build_temperature_market_from_position(position)],
    )


def build_ranked_market_from_position(position: dict[str, Any]) -> RankedMarket:
    return RankedMarket(
        market=build_temperature_market_from_position(position),
        outcome_price=float(position.get("buy_price") or 0.0),
        rank=int(position.get("market_rank") or TARGET_MARKET_RANK),
    )


def build_temperature_market_from_position(position: dict[str, Any]) -> TemperatureMarket:
    return TemperatureMarket(
        market_id=str(position.get("market_id") or ""),
        condition_id=str(position.get("condition_id") or ""),
        question=str(position.get("question") or ""),
        slug=str(position.get("market_slug") or ""),
        group_item_title=str(position.get("group_item_title") or ""),
        temperature_celsius=int(position.get("temperature_celsius") or 0),
        yes_token_id=str(position.get("yes_token_id") or ""),
        no_token_id=str(position.get("no_token_id") or ""),
    )


def get_position_target_outcome(position: dict[str, Any]) -> str:
    target_outcome = position.get("target_outcome")

    if isinstance(target_outcome, str) and target_outcome.strip():
        return normalize_target_outcome(target_outcome)

    return YES_OUTCOME


def get_position_outcome_token_id(position: dict[str, Any]) -> str:
    outcome_token_id = position.get("outcome_token_id")

    if isinstance(outcome_token_id, str) and outcome_token_id:
        return outcome_token_id

    target_outcome = get_position_target_outcome(position)

    if target_outcome == NO_OUTCOME:
        no_token_id = position.get("no_token_id")

        if isinstance(no_token_id, str) and no_token_id:
            return no_token_id

        raise ValueError("Active position is missing a NO outcome token id.")

    yes_token_id = position.get("yes_token_id")

    if isinstance(yes_token_id, str) and yes_token_id:
        return yes_token_id

    raise ValueError("Active position is missing a YES outcome token id.")


def append_completed_slice(position: dict[str, Any], field_name: str, slice_number: int) -> None:
    completed_slices = position.setdefault(field_name, [])

    if isinstance(completed_slices, list) and slice_number not in completed_slices:
        completed_slices.append(slice_number)


def append_buy_order_response(
    position: dict[str, Any],
    slice_number: int,
    order_response: dict[str, Any],
) -> None:
    order_responses = position.setdefault("buy_order_responses", [])

    if isinstance(order_responses, list):
        order_responses.append(
            {
                "order_response": order_response,
                "slice_number": slice_number,
            }
        )


def get_int_list(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []

    int_values: list[int] = []
    for item in value:
        try:
            int_values.append(int(item))
        except (TypeError, ValueError):
            continue

    return int_values


def count_unprocessed_sell_slices(
    state: dict[str, Any],
    target_date: str,
    schedule: TradeSchedule,
    target_outcome: str,
    twap_config: TwapConfig,
    current_slice_number: int,
) -> int:
    remaining_slices = 0

    for slice_number in range(current_slice_number, twap_config.sell_slices + 1):
        sell_slice_key = build_sell_slice_order_key(
            target_date,
            schedule,
            target_outcome,
            slice_number,
        )

        if not is_order_key_processed(state, sell_slice_key):
            remaining_slices += 1

    return max(remaining_slices, 1)


def sleep_between_twap_slices(interval_seconds: int, logger: logging.Logger, payload: dict[str, Any]) -> None:
    if interval_seconds <= 0:
        return

    log_json(
        logger,
        "twap_slice_wait",
        {
            **payload,
            "interval_seconds": interval_seconds,
        },
    )
    time.sleep(interval_seconds)


def execute_buy_twap(
    client: ClobClient,
    logger: logging.Logger,
    state: dict[str, Any],
    target_date: str,
    target_outcome: str,
    schedule: TradeSchedule,
    twap_config: TwapConfig,
    buy_amount_usdc: float,
    max_buy_price: float,
) -> bool:
    active_position = get_active_position(state)

    if active_position is not None:
        target_outcome = get_position_target_outcome(active_position)

    order_key = build_buy_order_key(target_date, schedule, target_outcome)

    if active_position is None:
        snapshot = load_market_snapshot(target_date, logger)
        prices_by_token_id = get_outcome_prices(client, snapshot.markets, target_outcome)
        ranked_markets = rank_markets_by_outcome_price(
            snapshot.markets,
            prices_by_token_id,
            target_outcome,
        )
        price_snapshot = build_price_snapshot(ranked_markets, target_outcome)

        log_json(
            logger,
            "ranked_prices_polled",
            {
                "event_id": snapshot.event.get("id"),
                "market_count": len(snapshot.markets),
                "priced_market_count": len(ranked_markets),
                "prices": price_snapshot,
                "schedule": build_schedule_record(schedule),
                "target_outcome": target_outcome,
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
                    "target_outcome": target_outcome,
                    "target_market_rank": TARGET_MARKET_RANK,
                },
            )
            return False

        ranked_market = ranked_markets[TARGET_MARKET_RANK - 1]
        active_position = build_position_record(
            snapshot=snapshot,
            ranked_market=ranked_market,
            target_outcome=target_outcome,
            schedule=schedule,
            twap_config=twap_config,
            buy_amount_usdc=buy_amount_usdc,
            max_buy_price=max_buy_price,
        )
        set_active_position(state, active_position)
        outcome_token_id = get_market_outcome_token_id(ranked_market.market, target_outcome)
    else:
        snapshot = build_market_snapshot_from_position(active_position)
        ranked_market = build_ranked_market_from_position(active_position)
        outcome_token_id = get_position_outcome_token_id(active_position)
        log_json(
            logger,
            "buy_twap_resumed",
            {
                "completed_buy_slices": get_int_list(active_position.get("completed_buy_slices")),
                "market_id": ranked_market.market.market_id,
                "market_rank": ranked_market.rank,
                "outcome_token_id": outcome_token_id,
                "schedule": build_schedule_record(schedule),
                "target_outcome": target_outcome,
                "target_date": target_date,
                "twap": build_twap_config_record(twap_config),
            },
        )

    log_json(
        logger,
        "ranked_market_selected",
        {
            "event_id": snapshot.event.get("id"),
            "market_id": ranked_market.market.market_id,
            "market_rank": ranked_market.rank,
            "outcome_price": ranked_market.outcome_price,
            "outcome_token_id": outcome_token_id,
            "question": ranked_market.market.question,
            "schedule": build_schedule_record(schedule),
            "target_outcome": target_outcome,
            "target_date": target_date,
            "temperature_celsius": ranked_market.market.temperature_celsius,
            "twap": build_twap_config_record(twap_config),
        },
    )

    buy_slice_amount_usdc = build_buy_slice_amount_usdc(buy_amount_usdc, twap_config)
    attempted_slice_in_this_call = False

    for slice_number in range(1, twap_config.buy_slices + 1):
        buy_slice_key = build_buy_slice_order_key(
            target_date,
            schedule,
            target_outcome,
            slice_number,
        )

        if is_order_key_processed(state, buy_slice_key):
            continue

        if attempted_slice_in_this_call:
            sleep_between_twap_slices(
                twap_config.buy_interval_seconds,
                logger,
                {
                    "side": BUY_SIDE,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "next_slice_number": slice_number,
                    "total_slices": twap_config.buy_slices,
                },
            )

        twap_slice = build_twap_slice_record(BUY_SIDE, slice_number, twap_config.buy_slices)
        log_json(
            logger,
            "buy_twap_slice_started",
            {
                "amount_usdc": buy_slice_amount_usdc,
                "market_id": ranked_market.market.market_id,
                "market_rank": ranked_market.rank,
                "outcome_token_id": outcome_token_id,
                "target_outcome": target_outcome,
                "target_date": target_date,
                "twap_slice": twap_slice,
            },
        )
        attempted_slice_in_this_call = True

        order_response = buy_ranked_outcome_market(
            client=client,
            logger=logger,
            snapshot=snapshot,
            ranked_market=ranked_market,
            target_outcome=target_outcome,
            buy_amount_usdc=buy_slice_amount_usdc,
            max_buy_price=max_buy_price,
        )

        status = "buy_slice_order_posted" if order_response is not None else "buy_slice_order_skipped"
        payload = {
            "amount_usdc": buy_slice_amount_usdc,
            "market_id": ranked_market.market.market_id,
            "market_rank": ranked_market.rank,
            "outcome_price": ranked_market.outcome_price,
            "outcome_token_id": outcome_token_id,
            "schedule": build_schedule_record(schedule),
            "target_outcome": target_outcome,
            "target_date": target_date,
            "twap_slice": twap_slice,
        }

        if order_response is not None:
            payload["order_response"] = order_response
            append_buy_order_response(active_position, slice_number, order_response)

        append_completed_slice(active_position, "completed_buy_slices", slice_number)
        mark_order_key_processed(
            state=state,
            order_key=buy_slice_key,
            status=status,
            payload=payload,
        )
        save_active_position(state, active_position)

    active_position["buy_twap_complete"] = True
    save_active_position(state, active_position)
    posted_buy_responses = active_position.get("buy_order_responses", [])
    status = "buy_twap_complete" if posted_buy_responses else "buy_twap_skipped"
    mark_order_key_processed(
        state=state,
        order_key=order_key,
        status=status,
        payload={
            "completed_buy_slices": get_int_list(active_position.get("completed_buy_slices")),
            "market_id": ranked_market.market.market_id,
            "market_rank": ranked_market.rank,
            "outcome_token_id": outcome_token_id,
            "posted_buy_slice_count": len(posted_buy_responses) if isinstance(posted_buy_responses, list) else 0,
            "schedule": build_schedule_record(schedule),
            "target_outcome": target_outcome,
            "target_date": target_date,
            "twap": build_twap_config_record(twap_config),
        },
    )

    if not posted_buy_responses:
        clear_active_position(state)

    return True


def parse_balance_amount(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None

    if isinstance(value, int):
        return float(value) / CONDITIONAL_TOKEN_DECIMAL_SCALE

    if isinstance(value, float):
        return float(value)

    if isinstance(value, str):
        clean_value = value.strip()

        if clean_value == "":
            return None

        if "." in clean_value:
            return float(clean_value)

        return float(int(clean_value)) / CONDITIONAL_TOKEN_DECIMAL_SCALE

    return None


def extract_balance_amount(response_json: Any) -> Optional[float]:
    direct_balance = parse_balance_amount(response_json)

    if direct_balance is not None:
        return direct_balance

    if not isinstance(response_json, dict):
        return None

    for field_name in BALANCE_RESPONSE_VALUE_KEYS:
        if field_name not in response_json:
            continue

        balance = parse_balance_amount(response_json.get(field_name))

        if balance is not None:
            return balance

    return None


def get_conditional_token_balance(client: ClobClient, token_id: str) -> tuple[float, Any]:
    response = client.get_balance_allowance(
        BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=token_id,
        )
    )
    balance = extract_balance_amount(response)

    if balance is None:
        raise ValueError(f"Could not parse conditional token balance for token {token_id}.")

    return balance, response


def round_sell_shares(shares: float) -> float:
    return math.floor(shares * 100.0) / 100.0


def build_sell_order_args(token_id: str, shares: float, min_sell_price: float) -> MarketOrderArgsV2:
    return MarketOrderArgsV2(
        token_id=token_id,
        amount=shares,
        side=SELL_SIDE,
        price=min_sell_price,
        order_type=MARKET_ORDER_TYPE,
    )


def sell_active_position(
    client: ClobClient,
    logger: logging.Logger,
    state: dict[str, Any],
    position: dict[str, Any],
    schedule: TradeSchedule,
    twap_config: TwapConfig,
    min_sell_price: float,
) -> bool:
    target_outcome = get_position_target_outcome(position)
    token_id = get_position_outcome_token_id(position)
    target_date = str(position["target_date"])
    sell_order_key = build_sell_order_key(target_date, schedule, target_outcome)

    if is_order_key_processed(state, sell_order_key):
        log_json(
            logger,
            "sell_skipped_already_processed",
            {
                "order_key": sell_order_key,
                "target_outcome": target_outcome,
            },
        )
        clear_active_position(state)
        return True

    attempted_slice_in_this_call = False

    for slice_number in range(1, twap_config.sell_slices + 1):
        sell_slice_key = build_sell_slice_order_key(
            target_date,
            schedule,
            target_outcome,
            slice_number,
        )

        if is_order_key_processed(state, sell_slice_key):
            continue

        if attempted_slice_in_this_call:
            sleep_between_twap_slices(
                twap_config.sell_interval_seconds,
                logger,
                {
                    "side": SELL_SIDE,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "next_slice_number": slice_number,
                    "total_slices": twap_config.sell_slices,
                },
            )

        remaining_slices = count_unprocessed_sell_slices(
            state=state,
            target_date=target_date,
            schedule=schedule,
            target_outcome=target_outcome,
            twap_config=twap_config,
            current_slice_number=slice_number,
        )
        token_balance, balance_response = get_conditional_token_balance(client, token_id)
        sell_shares = round_sell_shares(token_balance / remaining_slices)
        twap_slice = build_twap_slice_record(SELL_SIDE, slice_number, twap_config.sell_slices)

        log_json(
            logger,
            "sell_balance_checked",
            {
                "balance_response": balance_response,
                "raw_balance": token_balance,
                "remaining_slices": remaining_slices,
                "sell_shares": sell_shares,
                "target_outcome": target_outcome,
                "target_date": target_date,
                "twap_slice": twap_slice,
                "outcome_token_id": token_id,
            },
        )

        if sell_shares < MIN_SELL_SHARES:
            log_json(
                logger,
                "sell_skipped_no_balance",
                {
                    "min_sell_shares": MIN_SELL_SHARES,
                    "sell_shares": sell_shares,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "twap_slice": twap_slice,
                    "outcome_token_id": token_id,
                },
            )
            mark_order_key_processed(
                state=state,
                order_key=sell_slice_key,
                status="sell_slice_skipped_no_balance",
                payload={
                    "min_sell_shares": MIN_SELL_SHARES,
                    "sell_shares": sell_shares,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "twap_slice": twap_slice,
                    "outcome_token_id": token_id,
                },
            )
            mark_order_key_processed(
                state=state,
                order_key=sell_order_key,
                status="sell_skipped_no_balance",
                payload={
                    "min_sell_shares": MIN_SELL_SHARES,
                    "sell_shares": sell_shares,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "twap_slice": twap_slice,
                    "outcome_token_id": token_id,
                },
            )
            clear_active_position(state)
            return True

        try:
            required_price = client.calculate_market_price(
                token_id,
                SELL_SIDE,
                sell_shares,
                MARKET_ORDER_TYPE,
            )
        except Exception as error:
            no_match_reason = get_clob_no_match_reason(error, SELL_SIDE)

            if no_match_reason is None:
                raise

            log_json(
                logger,
                "sell_skipped_no_liquidity",
                {
                    "error": get_error_message(error),
                    "error_type": type(error).__name__,
                    "reason": no_match_reason,
                    "sell_shares": sell_shares,
                    "side": SELL_SIDE,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "twap_slice": twap_slice,
                    "outcome_token_id": token_id,
                },
            )
            return False

        if required_price < min_sell_price:
            log_json(
                logger,
                "sell_skipped_price_below_min",
                {
                    "min_sell_price": min_sell_price,
                    "required_price": required_price,
                    "sell_shares": sell_shares,
                    "side": SELL_SIDE,
                    "target_outcome": target_outcome,
                    "target_date": target_date,
                    "twap_slice": twap_slice,
                    "outcome_token_id": token_id,
                },
            )
            return False

        log_json(
            logger,
            "sell_order_attempt",
            {
                "market_id": position.get("market_id"),
                "market_order_type": MARKET_ORDER_TYPE,
                "market_rank": position.get("market_rank"),
                "min_sell_price": min_sell_price,
                "question": position.get("question"),
                "required_price": required_price,
                "sell_shares": sell_shares,
                "side": SELL_SIDE,
                "target_outcome": target_outcome,
                "target_date": target_date,
                "twap_slice": twap_slice,
                "outcome_token_id": token_id,
            },
        )

        response = client.create_and_post_market_order(
            build_sell_order_args(token_id, sell_shares, min_sell_price),
            order_type=MARKET_ORDER_TYPE,
        )
        payload = response if isinstance(response, dict) else {"response": response}
        log_json(logger, "sell_order_response", payload)
        append_completed_slice(position, "completed_sell_slices", slice_number)
        mark_order_key_processed(
            state=state,
            order_key=sell_slice_key,
            status="sell_slice_order_posted",
            payload={
                "min_sell_price": min_sell_price,
                "order_response": payload,
                "required_price": required_price,
                "sell_shares": sell_shares,
                "target_outcome": target_outcome,
                "target_date": target_date,
                "twap_slice": twap_slice,
                "outcome_token_id": token_id,
            },
        )
        save_active_position(state, position)
        attempted_slice_in_this_call = True

    completed_sell_slices = get_int_list(position.get("completed_sell_slices"))
    mark_order_key_processed(
        state=state,
        order_key=sell_order_key,
        status="sell_twap_complete",
        payload={
            "completed_sell_slices": completed_sell_slices,
            "outcome_token_id": token_id,
            "sell_slice_count": twap_config.sell_slices,
            "target_outcome": target_outcome,
            "target_date": target_date,
            "twap": build_twap_config_record(twap_config),
        },
    )
    clear_active_position(state)
    return True

def run_trade_cycle(
    client: ClobClient,
    logger: logging.Logger,
    state: dict[str, Any],
    target_outcome: str,
    schedule: TradeSchedule,
    twap_config: TwapConfig,
    buy_amount_usdc: float,
    max_buy_price: float,
    min_sell_price: float,
    snapshot_grace_seconds: int,
    poll_interval_seconds: int,
) -> None:
    active_position = get_active_position(state)

    if active_position is not None:
        target_date = str(active_position["target_date"])
        active_target_outcome = get_position_target_outcome(active_position)

        if active_position.get("buy_twap_complete") is not True:
            executed = execute_buy_twap(
                client=client,
                logger=logger,
                state=state,
                target_date=target_date,
                target_outcome=active_target_outcome,
                schedule=schedule,
                twap_config=twap_config,
                buy_amount_usdc=buy_amount_usdc,
                max_buy_price=max_buy_price,
            )

            if not executed:
                time.sleep(poll_interval_seconds)
            return

        sell_time = get_position_sell_time_utc(active_position, target_date, schedule)
        wait_until_sell_time(sell_time, logger)
        sold = sell_active_position(
            client=client,
            logger=logger,
            state=state,
            position=active_position,
            schedule=schedule,
            twap_config=twap_config,
            min_sell_price=min_sell_price,
        )

        if sold:
            sleep_after_processed_date(target_date, logger, poll_interval_seconds)
        else:
            time.sleep(poll_interval_seconds)
        return

    target_date = resolve_target_market_date(schedule.buy_day_offset)
    buy_order_key = build_buy_order_key(target_date, schedule, target_outcome)

    if is_order_key_processed(state, buy_order_key):
        log_json(
            logger,
            "buy_skipped_already_processed",
            {
                "order_key": buy_order_key,
                "target_outcome": target_outcome,
            },
        )
        sleep_after_processed_date(target_date, logger, poll_interval_seconds)
        return

    trade_times = build_trade_times_record(target_date, schedule)
    log_json(
        logger,
        "trade_times_scheduled",
        {
            "schedule": build_schedule_record(schedule),
            "target_outcome": target_outcome,
            "target_date": target_date,
            "trade_times": trade_times,
        },
    )

    buy_time = build_randomized_trade_time_utc(
        target_date=target_date,
        day_offset=schedule.buy_day_offset,
        hour_utc=schedule.buy_hour_utc,
        side=BUY_SIDE,
        random_window_seconds=schedule.trade_time_random_window_seconds,
    )

    if not wait_for_buy_window(buy_time, snapshot_grace_seconds, logger):
        mark_order_key_processed(
            state=state,
            order_key=buy_order_key,
            status="buy_window_missed",
            payload={
                "buy_time": buy_time.isoformat(),
                "schedule": build_schedule_record(schedule),
                "target_outcome": target_outcome,
                "target_date": target_date,
            },
        )
        sleep_after_processed_date(target_date, logger, poll_interval_seconds)
        return

    executed = execute_buy_twap(
        client=client,
        logger=logger,
        state=state,
        target_date=target_date,
        target_outcome=target_outcome,
        schedule=schedule,
        twap_config=twap_config,
        buy_amount_usdc=buy_amount_usdc,
        max_buy_price=max_buy_price,
    )

    if not executed:
        time.sleep(poll_interval_seconds)


def run_forever() -> None:
    load_env_file()
    logger = build_logger()
    validate_confirmation()

    target_outcome = get_target_outcome_env()
    schedule = build_trade_schedule()
    twap_config = build_twap_config()
    buy_amount_usdc = get_float_env(BUY_AMOUNT_ENV)
    max_buy_price = get_probability_env(MAX_BUY_PRICE_ENV, DEFAULT_MAX_BUY_PRICE)
    min_sell_price = get_probability_env(MIN_SELL_PRICE_ENV, DEFAULT_MIN_SELL_PRICE)
    poll_interval_seconds = get_int_env(
        POLL_INTERVAL_SECONDS_ENV,
        DEFAULT_POLL_INTERVAL_SECONDS,
    )
    snapshot_grace_seconds = get_int_env(
        SNAPSHOT_GRACE_SECONDS_ENV,
        DEFAULT_SNAPSHOT_GRACE_SECONDS,
    )

    validate_buy_amount(buy_amount_usdc)
    validate_twap_buy_amount(buy_amount_usdc, twap_config)
    validate_positive_seconds(POLL_INTERVAL_SECONDS_ENV, poll_interval_seconds)
    validate_positive_seconds(SNAPSHOT_GRACE_SECONDS_ENV, snapshot_grace_seconds)

    log_json(
        logger,
        "script_started",
        {
            "buy_amount_usdc": buy_amount_usdc,
            "market_rank": TARGET_MARKET_RANK,
            "max_buy_price": max_buy_price,
            "min_sell_price": min_sell_price,
            "poll_interval_seconds": poll_interval_seconds,
            "schedule": build_schedule_record(schedule),
            "snapshot_grace_seconds": snapshot_grace_seconds,
            "target_outcome": target_outcome,
            "twap": build_twap_config_record(twap_config),
        },
    )

    client = build_client()
    state = load_state()

    while True:
        try:
            run_trade_cycle(
                client=client,
                logger=logger,
                state=state,
                target_outcome=target_outcome,
                schedule=schedule,
                twap_config=twap_config,
                buy_amount_usdc=buy_amount_usdc,
                max_buy_price=max_buy_price,
                min_sell_price=min_sell_price,
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
