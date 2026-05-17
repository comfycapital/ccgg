import json
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://public-api.meteofrance.fr/public/DPObs/v1/station/infrahoraire-6m"
STATION_ID = "95088001"
RESPONSE_FORMAT = "json"
API_KEY = "eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ3ZWF0aGVyZGF0YWdnQGNhcmJvbi5zdXBlciIsImFwcGxpY2F0aW9uIjp7Im93bmVyIjoid2VhdGhlcmRhdGFnZyIsInRpZXJRdW90YVR5cGUiOm51bGwsInRpZXIiOiJVbmxpbWl0ZWQiLCJuYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaWQiOjQwOTE3LCJ1dWlkIjoiMTM5MDlhMmUtZWQ2MC00MGQzLTkyOTAtMWU0NDI5YjgyZmUwIn0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInN1YnNjcmliZWRBUElzIjpbeyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6IkRvbm5lZXNQdWJsaXF1ZXNPYnNlcnZhdGlvbiIsImNvbnRleHQiOiJcL3B1YmxpY1wvRFBPYnNcL3YxIiwicHVibGlzaGVyIjoiYmFzdGllbmciLCJ2ZXJzaW9uIjoidjEiLCJzdWJzY3JpcHRpb25UaWVyIjoiNTBQZXJNaW4ifV0sImV4cCI6MTg3MzcyMzkyMiwidG9rZW5fdHlwZSI6ImFwaUtleSIsImlhdCI6MTc3OTA1MTEyMiwianRpIjoiYTQ5ZDYyYWYtZjY4YS00YWM5LTgzMjUtNTk2YTc0MGM5Y2NmIn0=.GRfpLgJE1D4nbVe6UCa6tYtlQ2wWMwqu_fZiz4P_IYEOqM7iK_uBGn-guhYoAUhqLNwzlrpIz_Zlx3Tf59kdFwIJarBim5uAlL-azooVOm_bJocyOCeUnxCqRQYWaifQ-bIGkn0ffeyxCQS2YHzg_ZFqXajdN8ntC3lVc1oVprlQvPiNaq1qj5zAjDWDtBb-b2Uv7ChzIF2QvTVTpIpL9rYDQZxXJqAX9VlA6x5-RdYT_GjuquG40ZSI9tiCKTMjYdAgRWeDGMYiriUVdRYQEl--qCMJ9-YZY_AgxVzzc9a2vRMKiF-bPBf6S2S-iQh761ZndSe0waHMHgSKi4uriA=="

POLL_INTERVAL_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 20

TEMPERATURE_KEYS = ("temperature", "temperature_c", "t", "T")
VALIDITY_TIME_KEYS = (
    "validity_time",
    "validityTime",
    "date",
    "datetime",
    "timestamp",
    "time",
)
UNKNOWN_VALIDITY_TIME = "unknown"
KELVIN_CELSIUS_OFFSET = 273.15
KELVIN_MINIMUM_REASONABLE_VALUE = 170


def build_request_url():
    query_params = {
        "id_station": STATION_ID,
        "format": RESPONSE_FORMAT,
    }
    return f"{API_URL}?{urlencode(query_params)}"


def get_api_key():
    return API_KEY


def fetch_observation():
    request = Request(
        build_request_url(),
        headers={
            "accept": "*/*",
            "apikey": get_api_key(),
        },
        method="GET",
    )

    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        response_body = response.read().decode("utf-8")
        return json.loads(response_body)


def iter_values(data):
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from iter_values(value)

    if isinstance(data, list):
        for item in data:
            yield from iter_values(item)


def normalize_temperature(value):
    temperature = float(value)

    if temperature > KELVIN_MINIMUM_REASONABLE_VALUE:
        return temperature - KELVIN_CELSIUS_OFFSET

    return temperature


def get_first_value(data, keys):
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]

    return None


def extract_observation_values(data):
    for item in iter_values(data):
        temperature_value = get_first_value(item, TEMPERATURE_KEYS)
        if temperature_value is not None:
            validity_time = get_first_value(item, VALIDITY_TIME_KEYS)
            if validity_time is None:
                validity_time = UNKNOWN_VALIDITY_TIME

            return normalize_temperature(temperature_value), validity_time

    raise ValueError("No temperature field found in the response.")


def print_temperature():
    observation = fetch_observation()
    temperature_celsius, validity_time = extract_observation_values(observation)
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(
        f"{timestamp} validity_time={validity_time} temperature={temperature_celsius:.1f} C",
        flush=True,
    )


def main():
    while True:
        try:
            print_temperature()
        except (
            HTTPError,
            URLError,
            TimeoutError,
            RuntimeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            print(f"{timestamp} error={error}", flush=True)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
