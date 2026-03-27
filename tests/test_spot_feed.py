from __future__ import annotations

import json
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


def test_iter_price_points_parses_chainlink_history_payload() -> None:
    payload = {
        "payload": {
            "symbol": "btc/usd",
            "data": [
                {"timestamp": 1773577194000, "value": 71744.847},
                {"timestamp": 1773577195000, "value": 71744.843},
                {"timestamp": 1773577196000, "value": 71744.36964104824},
            ],
        },
        "topic": "crypto_prices",
        "type": "subscribe",
    }

    assert _iter_price_points(payload) == [("btc/usd", 71744.36964104824)]


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


def test_spot_feed_snapshot_prefers_chainlink_reference_when_available() -> None:
    feed = SpotFeed("wss://ws-live-data.polymarket.com", logging.getLogger("test-spot-feed"))
    now = time.time()
    with feed._lock:
        feed._prices["btcusdt"] = (70045.57, now)
        feed._prices["btc/usd"] = (70062.12, now)

    snapshot = feed.get_snapshot()

    assert snapshot.reference_price == 70062.12
    assert snapshot.lead_price == 70045.57
    assert snapshot.binance_price == 70045.57
    assert snapshot.chainlink_price == 70062.12
    assert snapshot.source == "polymarket-rtds+binance"


def test_spot_feed_keeps_chainlink_reference_alive_longer_than_binance() -> None:
    feed = SpotFeed("wss://ws-live-data.polymarket.com", logging.getLogger("test-spot-feed"))
    now = time.time()
    with feed._lock:
        feed._prices["btcusdt"] = (70045.57, now - 2.0)
        feed._prices["btc/usd"] = (70062.12, now - 2.0)

    snapshot = feed.get_snapshot()

    assert snapshot.reference_price == 70062.12
    assert snapshot.chainlink_price == 70062.12
    assert snapshot.binance_price is None
    assert snapshot.source == "polymarket-rtds-chainlink"


def test_spot_feed_anchor_price_prefers_chainlink_sample_near_window_start() -> None:
    feed = SpotFeed("wss://ws-live-data.polymarket.com", logging.getLogger("test-spot-feed"))
    target = time.time()
    with feed._lock:
        feed._history["btc/usd"].append((71840.28, target - 0.4))
        feed._history["btc/usd"].append((71852.76, target + 0.1))
        feed._prices["btc/usd"] = (71852.76, target + 0.1)

    anchor = feed.get_anchor_price(symbol="btc/usd", target_ts=target)

    assert anchor == 71852.76


def test_spot_feed_anchor_price_uses_chainlink_event_timestamp_not_local_receive_time() -> None:
    feed = SpotFeed("wss://ws-live-data.polymarket.com", logging.getLogger("test-spot-feed"))
    target = 1773577195.0

    payload = {
        "payload": {
            "symbol": "btc/usd",
            "data": [
                {"timestamp": int((target - 1.0) * 1000), "value": 71744.11},
                {"timestamp": int(target * 1000), "value": 71744.84},
                {"timestamp": int((target + 1.0) * 1000), "value": 71745.33},
            ],
        },
        "topic": "crypto_prices_chainlink",
        "type": "subscribe",
    }

    feed._handle_message(json.dumps(payload))

    anchor = feed.get_anchor_price(symbol="btc/usd", target_ts=target)

    assert anchor == 71744.84


def test_spot_feed_emits_listener_events_for_binance_trade() -> None:
    feed = SpotFeed("wss://stream.binance.com:9443/ws/btcusdt@aggTrade", logging.getLogger("test-spot-feed"))
    seen = []
    feed.register_listener(seen.append)

    feed._handle_message(
        """
        {"e":"aggTrade","E":1773341700123,"s":"BTCUSDT","p":"70045.57","q":"0.17","m":false}
        """.strip()
    )

    assert len(seen) == 1
    assert seen[0].kind == "spot_price"
    assert seen[0].payload["symbol"] == "btcusdt"
    assert seen[0].payload["price"] == 70045.57
