
def get_today_temperature_markets(
    logger: logging.Logger,
) -> tuple[str, dict[str, Any], list[TemperatureMarket]]:
    target_date = get_target_market_date(logger)
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
    return target_date, event, markets


def parse_validity_time(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc).replace(microsecond=0)

    text = str(value).strip()

    if text.endswith(ISO_Z_SUFFIX):
        text = f"{text[:-1]}{ISO_UTC_OFFSET}"

    parsed_time = datetime.fromisoformat(text)

    if parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(tzinfo=timezone.utc)

    return parsed_time.astimezone(timezone.utc).replace(microsecond=0)


def extract_observations(data: Any) -> list[Observation]:
    observations = []

    for item in iter_values(data):
        temperature_value = get_first_value(item, TEMPERATURE_KEYS)
        validity_time_value = get_first_value(item, VALIDITY_TIME_KEYS)

        if temperature_value is None or validity_time_value is None:
            continue

        try:
            observations.append(
                Observation(
                    temperature_celsius=normalize_temperature(temperature_value),
                    validity_time=parse_validity_time(validity_time_value),
                    raw_validity_time=validity_time_value,
                )
            )
        except (TypeError, ValueError):
            continue

    return observations


def get_latest_observation(observations: list[Observation]) -> Optional[Observation]:
    if not observations:
        return None

    return max(observations, key=lambda observation: observation.validity_time)


def poll_for_valid_observation(
    target_validity_time: datetime,
    logger: logging.Logger,
) -> Observation:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=MAX_OBSERVATION_WAIT_SECONDS)

    while datetime.now(timezone.utc) <= deadline:
        try:
            observations = extract_observations(fetch_observation())
            matching_observations = [
                observation
                for observation in observations
                if observation.validity_time == target_validity_time
            ]

            if matching_observations:
                observation = matching_observations[0]
                log_json(
                    logger,
                    "observation_matched",
                    {
                        "raw_validity_time": str(observation.raw_validity_time),
                        "target_validity_time": target_validity_time.isoformat(),
                        "temperature_celsius": observation.temperature_celsius,
                    },
                )
                return observation

            latest_observation = get_latest_observation(observations)
            log_json(
                logger,
                "observation_wait",
                {
                    "latest_validity_time": (
                        latest_observation.validity_time.isoformat()
                        if latest_observation
                        else None
                    ),
                    "target_validity_time": target_validity_time.isoformat(),
                },
            )
        except (
            HTTPError,
            URLError,
            TimeoutError,
            RuntimeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            log_json(
                logger,
                "observation_error",
                {
                    "error": str(error),
                    "target_validity_time": target_validity_time.isoformat(),
                },
            )

        time.sleep(OBSERVATION_POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Timed out waiting for observation {target_validity_time.isoformat()}.")


def round_temperature_celsius(temperature_celsius: float) -> int:
    rounded_temperature = Decimal(str(temperature_celsius)).quantize(
        Decimal("1"),
        rounding=ROUND_HALF_UP,
    )
    return int(rounded_temperature)


def select_no_market_below_temperature(
    markets: list[TemperatureMarket],
    rounded_temperature_celsius: int,
) -> TemperatureMarket:
    target_temperature = rounded_temperature_celsius - 1
    exact_matches = [
        market
        for market in markets
        if market.range_type == RANGE_EXACT and market.temperature_celsius == target_temperature
    ]

    if len(exact_matches) == 1:
        return exact_matches[0]

    if len(exact_matches) > 1:
        market_ids = [market.market_id for market in exact_matches]
        raise ValueError(f"Multiple exact markets found for {target_temperature}C: {market_ids}")

    below_range_matches = [
        market
        for market in markets
        if market.range_type == RANGE_BELOW and market.temperature_celsius <= target_temperature
    ]

    if below_range_matches:
        return max(below_range_matches, key=lambda market: market.temperature_celsius)

    raise ValueError(
        f"No market found for the temperature below {rounded_temperature_celsius}C "
        f"(target {target_temperature}C)."
    )


def validate_market_price(
    client: ClobClient,
    market: TemperatureMarket,
    buy_amount_usdc: float,
    max_price: float,
) -> float:
    required_price = client.calculate_market_price(
        market.no_token_id,
        BUY_SIDE,
        buy_amount_usdc,
        MARKET_ORDER_TYPE,
    )

    if required_price > max_price:
        raise ValueError(
            f"Required market price {required_price} is greater than {MAX_PRICE_ENV} {max_price}."
        )

    return required_price


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
    event: dict[str, Any],
    market: TemperatureMarket,
    observation: Observation,
    rounded_temperature_celsius: int,
    buy_amount_usdc: float,
    max_price: float,
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
            "event_id": event.get("id"),
            "event_title": event.get("title"),
            "market_id": market.market_id,
            "market_temperature_celsius": market.temperature_celsius,
            "market_order_type": MARKET_ORDER_TYPE,
            "max_price": max_price,
            "no_token_id": market.no_token_id,
            "observed_temperature_celsius": observation.temperature_celsius,
            "question": market.question,
            "reason": reason,
            "rounded_temperature_celsius": rounded_temperature_celsius,
            "side": BUY_SIDE,
            "validity_time": observation.validity_time.isoformat(),
        },
    )


def build_order_args(
    market: TemperatureMarket,
    buy_amount_usdc: float,
    max_price: float,
) -> MarketOrderArgsV2:
    return MarketOrderArgsV2(
        token_id=market.no_token_id,
        amount=buy_amount_usdc,
        side=BUY_SIDE,
        price=max_price,
        order_type=MARKET_ORDER_TYPE,
    )


def buy_no_market(
    client: ClobClient,
    logger: logging.Logger,
    event: dict[str, Any],
    market: TemperatureMarket,
    observation: Observation,
    rounded_temperature_celsius: int,
    buy_amount_usdc: float,
    max_price: float,
) -> Optional[dict[str, Any]]:
    try:
        required_price = validate_market_price(client, market, buy_amount_usdc, max_price)
    except Exception as error:
        no_match_reason = get_clob_no_match_reason(error)

        if no_match_reason is None:
            raise

        log_order_skipped_no_liquidity(
            logger=logger,
            event=event,
            market=market,
            observation=observation,
            rounded_temperature_celsius=rounded_temperature_celsius,
            buy_amount_usdc=buy_amount_usdc,
            max_price=max_price,
            reason=no_match_reason,
            error=error,
        )
        return None

    order_args = build_order_args(market, buy_amount_usdc, max_price)

    log_json(
        logger,
        "order_attempt",
        {
            "amount_usdc": buy_amount_usdc,
            "condition_id": market.condition_id,
            "event_id": event.get("id"),
            "event_title": event.get("title"),
            "market_id": market.market_id,
            "market_temperature_celsius": market.temperature_celsius,
            "market_order_type": MARKET_ORDER_TYPE,
            "max_price": max_price,
            "no_token_id": market.no_token_id,
            "observed_temperature_celsius": observation.temperature_celsius,
            "question": market.question,
            "required_price": required_price,
            "rounded_temperature_celsius": rounded_temperature_celsius,
            "side": BUY_SIDE,
            "validity_time": observation.validity_time.isoformat(),
        },
    )

    response = client.create_and_post_market_order(
        order_args,
        order_type=MARKET_ORDER_TYPE,
    )

    payload = response if isinstance(response, dict) else {"response": response}
    log_json(logger, "order_response", payload)
    return payload


def get_next_poll_window(now: datetime) -> datetime:
    current_hour = now.replace(second=0, microsecond=0)

    for minute in POLL_WINDOW_MINUTES:
        candidate = current_hour.replace(minute=minute)

        if now <= candidate:
            return candidate

    next_hour = current_hour + timedelta(hours=1)
    return next_hour.replace(minute=POLL_WINDOW_MINUTES[0])


def get_target_validity_time(poll_window: datetime) -> datetime:
    target_minute = VALIDITY_MINUTE_BY_POLL_MINUTE[poll_window.minute]
    return poll_window.replace(minute=target_minute, second=0, microsecond=0)


def sleep_until(target_time: datetime) -> None:
    while True:
        seconds_until_target = (target_time - datetime.now(timezone.utc)).total_seconds()

        if seconds_until_target <= 0:
            return

        time.sleep(min(seconds_until_target, 60))


def run_cycle(
    client: ClobClient,
    logger: logging.Logger,
    buy_amount_usdc: float,
    max_price: float,
    bought_market_ids: set[str],
) -> None:
    poll_window = get_next_poll_window(datetime.now(timezone.utc))
    target_validity_time = get_target_validity_time(poll_window)

    log_json(
        logger,
        "poll_window_wait",
        {
            "poll_window": poll_window.isoformat(),
            "target_validity_time": target_validity_time.isoformat(),
        },
    )
    sleep_until(poll_window)

    observation = poll_for_valid_observation(target_validity_time, logger)
    _, event, markets = get_today_temperature_markets(logger)
    rounded_temperature_celsius = round_temperature_celsius(observation.temperature_celsius)
    market = select_no_market_below_temperature(markets, rounded_temperature_celsius)

    if market.market_id in bought_market_ids:
        log_json(
            logger,
            "order_skipped_duplicate_market",
            {
                "market_id": market.market_id,
                "rounded_temperature_celsius": rounded_temperature_celsius,
                "validity_time": observation.validity_time.isoformat(),
            },
        )
        return

    order_response = buy_no_market(
        client=client,
        logger=logger,
        event=event,
        market=market,
        observation=observation,
        rounded_temperature_celsius=rounded_temperature_celsius,
        buy_amount_usdc=buy_amount_usdc,
        max_price=max_price,
    )

    if order_response is not None:
        bought_market_ids.add(market.market_id)


def run_forever() -> None:
    load_env_file()
    logger = build_logger()
    validate_confirmation()

    buy_amount_usdc = get_float_env(BUY_AMOUNT_ENV)
    max_price = get_float_env(MAX_PRICE_ENV, DEFAULT_MAX_PRICE)
    validate_buy_amount(buy_amount_usdc)

    get_today_temperature_markets(logger)
    client = build_client()
    bought_market_ids: set[str] = set()

    while True:
        try:
            run_cycle(
                client=client,
                logger=logger,
                buy_amount_usdc=buy_amount_usdc,
                max_price=max_price,
                bought_market_ids=bought_market_ids,
            )
        except KeyboardInterrupt:
            raise
        except Exception as error:
            logger.exception("cycle_error %s", error)


if __name__ == "__main__":
    run_forever()
