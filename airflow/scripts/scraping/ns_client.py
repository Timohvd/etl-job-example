"""Client for the NS reisinformatie API (Dutch Railways travel information).

All HTTP access to NS goes through `NSClient`, so authentication, timeouts and
retry behaviour are defined once. The extract scripts only call the typed
methods (`get_stations`, `get_disruptions`) and never build URLs themselves.

The API is reached through the NS API portal gateway and requires a
subscription key, sent in the `Ocp-Apim-Subscription-Key` header. The key is
read from the NS_API_KEY environment variable and must never be hardcoded or
committed.
"""

import os
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://gateway.apiportal.ns.nl/reisinformatie-api/api"


class NSClient:
    """HTTP client for the NS API with a request timeout and automatic retries.

    A single `requests.Session` is reused for all calls so TCP/TLS connections
    are kept alive and the authentication header is set once.

    Retry behaviour (handled by urllib3, before Airflow's own task retries):
        * Up to 3 retries on HTTP 429, 500, 502, 503 and 504.
        * Exponential backoff between attempts (`backoff_factor=1`).
        * A `Retry-After` header from the server is respected.
        * Only GET requests are retried, because they are safe to repeat.

    Client errors such as 401 (bad key) or 404 are not retried: repeating them
    cannot succeed. They raise `requests.HTTPError` immediately.

    Two retry layers exist on purpose. This one absorbs short network or
    gateway hiccups within a single task attempt; Airflow's task retries (see
    the DAG) handle longer outages with larger delays.
    """

    def __init__(self, api_key: Optional[str] = None, timeout: float = 30.0):
        """Create a client.

        Args:
            api_key: NS subscription key. Defaults to the NS_API_KEY
                environment variable.
            timeout: Seconds to wait for a response before giving up. Without
                a timeout a stalled connection could block a task forever.

        Raises:
            ValueError: If no key is given and NS_API_KEY is not set.
        """
        api_key = api_key or os.getenv("NS_API_KEY")
        if not api_key:
            raise ValueError("Missing required environment variable: NS_API_KEY")

        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Ocp-Apim-Subscription-Key": api_key, "Cache-Control": "no-cache"})
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def get(self, path: str, params: Optional[dict] = None) -> dict:
        """Send a GET request and return the decoded JSON body.

        Args:
            path: Endpoint path relative to `BASE_URL`, e.g. "v2/stations".
            params: Query-string parameters, e.g. `{"countryCodes": "NL"}`.

        Returns:
            The parsed JSON. Depending on the endpoint this is a dict (stations
            wrap their data in a "payload" key) or a list (disruptions).

        Raises:
            requests.HTTPError: For a 4xx/5xx response after retries are used up.
            requests.RetryError: If the retries for a retryable status are exhausted.
            requests.Timeout: If the server does not respond within `timeout`.
        """
        response = self.session.get(f"{BASE_URL}/{path.lstrip('/')}", params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_stations(self, country_codes: str = "NL") -> list:
        """Fetch all railway stations.

        Args:
            country_codes: Comma-separated ISO country codes, default "NL".

        Returns:
            A list of station dicts (code, names, coordinates, UIC/EVA codes,
            station type, ...), taken from the response's "payload" key.
        """
        return self.get("v2/stations", {"countryCodes": country_codes})["payload"]

    def get_disruptions(self, is_active: bool = True) -> list:
        """Fetch disruptions and planned maintenance notices.

        Args:
            is_active: True (default) returns only currently active items;
                False returns all items the API exposes.

        Returns:
            A list of disruption dicts (id, type, title, priority, ...). The
            `v3/disruptions` endpoint returns a bare list, not a "payload"
            wrapper.
        """
        return self.get("v3/disruptions", {"isActive": str(is_active).lower()})
