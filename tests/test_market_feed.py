from __future__ import annotations

import json
import logging

from app.polymarket.clob_client import CLOBClient
from app.polymarket.market_feed import FeedStatus, MarketFeed, _apply_price_changes, _canonical_book
from app.settings import EnvSettings


class _FakeFeed:
    def __init__(self) -> None:
        self.book = {
            "bids": [{"price": "0.41", "size": "25"}],
            "asks": [{"price": "0.43", "size": "20"}],
        }
        self.tracked_assets: tuple[str, ...] = ()

    def ensure_assets(self, asset_ids):  # noqa: ANN001
        self.tracked_assets = tuple(asset_ids)
        return True

    def get_book(self, token_id: str):  # noqa: ARG002
        return self.book

    def get_midpoint(self, token_id: str):  # noqa: ARG002
        return 0.42

    def status(self) -> FeedStatus:
        return FeedStatus(mode="websocket", connected=True, tracked_assets=len(self.tracked_assets), age_ms=120)

    def close(self) -> None:
        return None


class _FakeResponse:
    def __init__(self, payload) -> None:  # noqa: ANN001
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):  # noqa: ANN001
        return self._payload


def test_canonical_book_accepts_buys_and_sells_shape() -> None:
    payload = {
        "asset_id": "asset-up",
        "buys": [{"price": "0.41", "size": "25"}],
        "sells": [{"price": "0.43", "size": "20"}],
    }

    assert _canonical_book(payload) == {
        "bids": [{"price": "0.41", "size": "25"}],
        "asks": [{"price": "0.43", "size": "20"}],
    }


def test_apply_price_changes_updates_and_removes_levels() -> None:
    current = {
        "bids": [{"price": "0.41", "size": "25"}],
        "asks": [{"price": "0.43", "size": "20"}],
    }
    payload = {
        "asset_id": "asset-up",
        "changes": [
            {"side": "BUY", "price": "0.42", "size": "10"},
            {"side": "SELL", "price": "0.43", "size": "0"},
            {"side": "SELL", "price": "0.44", "size": "12"},
        ],
    }

    updated = _apply_price_changes(current, payload)

    assert updated["bids"][0] == {"price": "0.42", "size": "10"}
    assert updated["asks"] == [{"price": "0.44", "size": "12"}]


def test_clob_client_prefers_market_feed_before_rest() -> None:
    feed = _FakeFeed()
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(), market_feed=feed)

    def _no_rest(*args, **kwargs):  # noqa: ANN001, ARG001
        raise AssertionError("REST should not be called when feed has fresh data")

    client.session.get = _no_rest  # type: ignore[assignment]

    client.track_assets(["asset-up", "asset-down"])

    assert client.get_book("asset-up") == feed.book
    assert client.get_midpoint("asset-up") == 0.42
    status = client.market_feed_status()
    assert status.mode == "websocket"
    assert status.connected is True
    assert status.tracked_assets == 2


def test_clob_client_falls_back_to_rest_when_market_feed_book_is_incomplete() -> None:
    feed = _FakeFeed()
    feed.book = {
        "bids": [{"price": "0.41", "size": "25"}],
        "asks": [],
    }
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(), market_feed=feed)

    def _rest_book(*args, **kwargs):  # noqa: ANN001, ARG001
        return _FakeResponse(
            {
                "bids": [{"price": "0.41", "size": "25"}],
                "asks": [{"price": "0.43", "size": "20"}],
            }
        )

    client.session.get = _rest_book  # type: ignore[assignment]

    assert client.get_book("asset-up") == {
        "bids": [{"price": "0.41", "size": "25"}],
        "asks": [{"price": "0.43", "size": "20"}],
    }


def test_market_feed_reports_idle_when_no_assets_tracked() -> None:
    feed = MarketFeed("wss://clob.example/ws", logging.getLogger("test-market-feed"), enabled=True)
    feed._ws_supported = lambda: True  # type: ignore[method-assign]

    status = feed.status()

    assert status.mode == "websocket-idle"
    assert status.connected is False
    assert status.tracked_assets == 0


def test_market_feed_emits_trade_listener_event() -> None:
    feed = MarketFeed("wss://clob.example/ws", logging.getLogger("test-market-feed"), enabled=True)
    seen = []
    feed.register_listener(seen.append)

    feed._handle_message(
        None,
        json.dumps(
            {
                "event_type": "last_trade_price",
                "asset_id": "asset-up",
                "price": "0.48",
                "size": "22",
                "side": "BUY",
                "timestamp": 1773341700123,
            }
        ),
    )

    assert len(seen) == 1
    assert seen[0].kind == "market_trade"
    assert seen[0].asset_id == "asset-up"
    assert seen[0].payload["side"] == "buy"
