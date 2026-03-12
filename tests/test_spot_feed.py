from __future__ import annotations

import logging
import time

from app.polymarket.spot_feed import SpotFeed, _iter_price_points


def test_iter_price_points_parses_binance_trade_payload() -> None:
    payload = {
        "e": "trade",
        "E": 1773341700123,
        "s": "BTCUSDT",
        "p": "70045.57",
    }

    assert _iter_price_points(payload) == [("btcusdt", 70045.57)]


def test_spot_feed_snapshot_prefers_binance_direct_when_available() -> None:
    feed = SpotFeed("wss://stream.binance.com:9443/ws/btcusdt@trade", logging.getLogger("test-spot-feed"))
    now = time.time()
    with feed._lock:
        feed._prices["btcusdt"] = (70045.57, now)

    snapshot = feed.get_snapshot()

    assert snapshot.reference_price == 70045.57
    assert snapshot.lead_price == 70045.57
    assert snapshot.binance_price == 70045.57
    assert snapshot.source == "binance-direct"
    assert snapshot.connected is False
