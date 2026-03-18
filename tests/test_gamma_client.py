from __future__ import annotations

import time
from typing import Any

from app.polymarket.gamma_client import GammaClient


class _FakeResponse:
    def __init__(self, payload: Any, *, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class _FakeSession:
    def __init__(self, responses: dict[tuple[str, tuple[tuple[str, str], ...]], _FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []

    def mount(self, *_args, **_kwargs) -> None:
        return

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int | None = None) -> _FakeResponse:  # noqa: ARG002
        normalized_params = tuple(sorted((str(key), str(value)) for key, value in (params or {}).items()))
        key = (url, normalized_params)
        self.calls.append(key)
        response = self.responses.get(key)
        if response is None:
            raise AssertionError(f"unexpected request: {key}")
        return response


class _SequencedSession:
    def __init__(self, responses: dict[tuple[str, tuple[tuple[str, str], ...]], list[_FakeResponse]]) -> None:
        self.responses = {key: list(value) for key, value in responses.items()}
        self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []

    def mount(self, *_args, **_kwargs) -> None:
        return

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int | None = None) -> _FakeResponse:  # noqa: ARG002
        normalized_params = tuple(sorted((str(key), str(value)) for key, value in (params or {}).items()))
        key = (url, normalized_params)
        self.calls.append(key)
        queue = self.responses.get(key)
        if not queue:
            raise AssertionError(f"unexpected request: {key}")
        return queue.pop(0)


def test_get_market_by_slug_uses_slug_endpoint_and_hydrates_event_metadata() -> None:
    slug = "btc-updown-5m-123"
    base_url = "https://gamma-api.polymarket.com"
    session = _FakeSession(
        {
            (f"{base_url}/markets/slug/{slug}", ()): _FakeResponse(
                {
                    "slug": slug,
                    "question": "Bitcoin Up or Down",
                    "events": [{"id": "269026", "eventMetadata": {}}],
                }
            ),
            (f"{base_url}/events/269026", ()): _FakeResponse(
                {
                    "id": "269026",
                    "eventMetadata": {"priceToBeat": 71840.27507838454},
                    "resolutionSource": "https://data.chain.link/streams/btc-usd",
                }
            ),
        }
    )
    client = GammaClient(base_url)
    client.session = session

    market = client.get_market_by_slug(slug)

    assert market is not None
    assert market["slug"] == slug
    assert market["events"][0]["eventMetadata"]["priceToBeat"] == 71840.27507838454
    assert market["events"][0]["resolutionSource"] == "https://data.chain.link/streams/btc-usd"
    assert session.calls[0][0].endswith(f"/markets/slug/{slug}")


def test_get_market_by_slug_falls_back_to_list_endpoint_on_missing_slug_route() -> None:
    slug = "btc-updown-5m-456"
    base_url = "https://gamma-api.polymarket.com"
    session = _FakeSession(
        {
            (f"{base_url}/markets/slug/{slug}", ()): _FakeResponse({}, status_code=404),
            (
                f"{base_url}/markets",
                (("limit", "1"), ("slug", slug)),
            ): _FakeResponse(
                [
                    {
                        "slug": slug,
                        "question": "Bitcoin Up or Down",
                        "events": [{"id": "1", "eventMetadata": {"priceToBeat": 71815.32502}}],
                    }
                ]
            ),
        }
    )
    client = GammaClient(base_url)
    client.session = session

    market = client.get_market_by_slug(slug)

    assert market is not None
    assert market["slug"] == slug
    assert market["events"][0]["eventMetadata"]["priceToBeat"] == 71815.32502


def test_get_market_by_slug_refreshes_event_when_first_event_payload_is_incomplete() -> None:
    slug = "btc-updown-5m-789"
    base_url = "https://gamma-api.polymarket.com"
    session = _SequencedSession(
        {
            (f"{base_url}/markets/slug/{slug}", ()): [
                _FakeResponse(
                    {
                        "slug": slug,
                        "question": "Bitcoin Up or Down",
                        "events": [{"id": "269999", "eventMetadata": {}}],
                    }
                ),
                _FakeResponse(
                    {
                        "slug": slug,
                        "question": "Bitcoin Up or Down",
                        "events": [{"id": "269999", "eventMetadata": {}}],
                    }
                ),
            ],
            (f"{base_url}/events/269999", ()): [
                _FakeResponse({"id": "269999", "eventMetadata": {}}),
                _FakeResponse({"id": "269999", "eventMetadata": {"priceToBeat": 71840.27507838454}}),
            ],
        }
    )
    client = GammaClient(base_url)
    client.session = session

    first_market = client.get_market_by_slug(slug)
    assert first_market is not None
    assert first_market["events"][0]["eventMetadata"] == {}

    time.sleep(2.1)

    second_market = client.get_market_by_slug(slug)
    assert second_market is not None
    assert second_market["events"][0]["eventMetadata"]["priceToBeat"] == 71840.27507838454
