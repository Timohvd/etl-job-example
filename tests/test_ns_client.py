"""Unit tests for NSClient; HTTP is mocked, so no network or API key is needed."""

import pytest
import requests
import responses

from scraping.ns_client import BASE_URL, NSClient


def test_missing_api_key_raises(monkeypatch):
    """No key given and NS_API_KEY unset: fail immediately instead of calling the API."""
    monkeypatch.delenv("NS_API_KEY", raising=False)
    with pytest.raises(ValueError, match="NS_API_KEY"):
        NSClient()


def test_api_key_is_sent_as_header():
    """The subscription key is sent in the Ocp-Apim-Subscription-Key header."""
    client = NSClient(api_key="secret")
    assert client.session.headers["Ocp-Apim-Subscription-Key"] == "secret"


def test_retry_policy_covers_transient_errors():
    """HTTP retries cover 429/5xx and only apply to GET. (The `responses` mock bypasses urllib3,
    so the policy is checked on the adapter instead of by simulating failures.)"""
    adapter = NSClient(api_key="k").session.get_adapter(BASE_URL)
    retry = adapter.max_retries
    assert retry.total == 3
    assert {429, 500, 502, 503, 504} <= set(retry.status_forcelist)
    assert "POST" not in retry.allowed_methods


@responses.activate
def test_get_stations_unwraps_payload():
    """Stations are wrapped in a "payload" key; get_stations returns the inner list."""
    responses.add(
        responses.GET,
        f"{BASE_URL}/v2/stations",
        json={"payload": [{"code": "UT"}, {"code": "AMS"}]},
    )
    stations = NSClient(api_key="k").get_stations()
    assert [s["code"] for s in stations] == ["UT", "AMS"]
    assert "countryCodes=NL" in responses.calls[0].request.url


@responses.activate
def test_get_disruptions_returns_bare_list():
    """The disruptions endpoint returns a bare list and defaults to active items only."""
    responses.add(responses.GET, f"{BASE_URL}/v3/disruptions", json=[{"id": "x"}])
    assert NSClient(api_key="k").get_disruptions() == [{"id": "x"}]
    assert "isActive=true" in responses.calls[0].request.url


@responses.activate
def test_http_error_raises():
    """A 401 (bad key) raises HTTPError and is not silently ignored or retried."""
    responses.add(responses.GET, f"{BASE_URL}/v2/stations", status=401, json={"message": "denied"})
    with pytest.raises(requests.HTTPError):
        NSClient(api_key="bad").get_stations()
