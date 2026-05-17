import json
import logging
import os
from pathlib import Path
from typing import Any

import requests


GAMMA_EVENTS_URL = os.getenv(
    "GAMMA_EVENTS_URL",
    "https://gamma-api.polymarket.com/events",
)
GAMMA_TAG_ID = int(os.getenv("GAMMA_TAG_ID", "84"))
GAMMA_TAG_SLUG = os.getenv("GAMMA_TAG_SLUG", "paris")
START_DATE_MIN = os.getenv("START_DATE_MIN", "2026-05-15T00:00:00Z")
GAMMA_LIMIT = int(os.getenv("GAMMA_LIMIT", "20"))
EVENTS_LOG_FILE = Path(os.getenv("EVENTS_LOG_FILE", "events.log"))
REQUEST_TIMEOUT_SECONDS = 30

LOGGER_NAME = "events_logger"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

TAG_ID_QUERY_PARAM = "tag_id"
TAG_SLUG_QUERY_PARAM = "tag_slug"
START_DATE_MIN_QUERY_PARAM = "start_date_min"
LIMIT_QUERY_PARAM = "limit"
CLOSED_KEY = "closed"
ACTIVE_KEY = "active"
EVENTS_DATA_KEY = "data"
EVENT_LOG_MESSAGE = "event"
COMPLETE_LOG_MESSAGE = "events_complete"


def build_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    file_handler = logging.FileHandler(EVENTS_LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)

    return logger


def get_gamma_events() -> list[dict[str, Any]]:
    response = requests.get(
        GAMMA_EVENTS_URL,
        params={
            TAG_ID_QUERY_PARAM: GAMMA_TAG_ID,
            TAG_SLUG_QUERY_PARAM: GAMMA_TAG_SLUG,
            START_DATE_MIN_QUERY_PARAM: START_DATE_MIN,
            LIMIT_QUERY_PARAM: GAMMA_LIMIT,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    response_json = response.json()
    return parse_gamma_events(response_json)


def parse_gamma_events(response_json: Any) -> list[dict[str, Any]]:
    if isinstance(response_json, list):
        events = response_json
    elif isinstance(response_json, dict):
        events = response_json.get(EVENTS_DATA_KEY)
    else:
        raise ValueError("Gamma events response is not a list or dictionary.")

    if not isinstance(events, list):
        raise ValueError("Gamma events response does not contain an events list.")

    return [event for event in events if isinstance(event, dict)]


def get_active_open_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if event.get(CLOSED_KEY) is False and event.get(ACTIVE_KEY) is True
    ]


def log_json(logger: logging.Logger, message: str, payload: dict[str, Any]) -> None:
    logger.info("%s %s", message, json.dumps(payload, ensure_ascii=False, sort_keys=True))


def log_all_events() -> int:
    logger = build_logger()
    events = get_gamma_events()
    active_open_events = get_active_open_events(events)

    for event in active_open_events:
        log_json(logger, EVENT_LOG_MESSAGE, event)

    log_json(
        logger,
        COMPLETE_LOG_MESSAGE,
        {
            "events": len(events),
            "logged_events": len(active_open_events),
            "log_file": str(EVENTS_LOG_FILE),
            "tag_id": GAMMA_TAG_ID,
            "tag_slug": GAMMA_TAG_SLUG,
            "start_date_min": START_DATE_MIN,
            "limit": GAMMA_LIMIT,
        },
    )

    return len(active_open_events)


if __name__ == "__main__":
    log_all_events()
