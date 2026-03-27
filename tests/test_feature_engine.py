from __future__ import annotations

import json
import time

from app.core.event_bus import BaseEvent
from app.core.feature_engine import FeatureEngine
from app.core.state_store import StateStore
from app.polymarket.spot_feed import SpotSnapshot


def test_feature_engine_builds_microstructure_frame() -> None:
    store = StateStore()
    now_ms = int(time.time() * 1000)

    store.apply(
        BaseEvent(
            kind="book_snapshot",
            source="test",
            asset_id="up-asset",
            payload={
                "asset_id": "up-asset",
                "book": {
                    "bids": [{"price": "0.44", "size": "120"}],
                    "asks": [{"price": "0.46", "size": "80"}],
                },
            },
            ts_exchange_ms=now_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
        )
    )
    store.apply(
        BaseEvent(
            kind="book_snapshot",
            source="test",
            asset_id="down-asset",
            payload={
                "asset_id": "down-asset",
                "book": {
                    "bids": [{"price": "0.43", "size": "90"}],
                    "asks": [{"price": "0.47", "size": "110"}],
                },
            },
            ts_exchange_ms=now_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
        )
    )
    store.apply(
        BaseEvent(
            kind="market_trade",
            source="test",
            asset_id="up-asset",
            payload={"asset_id": "up-asset", "price": 0.46, "size": 40, "side": "buy"},
            ts_exchange_ms=now_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
        )
    )
    store.apply(
        BaseEvent(
            kind="market_trade",
            source="test",
            asset_id="down-asset",
            payload={"asset_id": "down-asset", "price": 0.47, "size": 15, "side": "sell"},
            ts_exchange_ms=now_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
        )
    )
    store.apply(
        BaseEvent(
            kind="spot_price",
            source="spot-feed:binance",
            asset_id="btcusdt",
            payload={"symbol": "btcusdt", "price": 70200.0, "quantity": 0.2, "side": "buy"},
            ts_exchange_ms=now_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
        )
    )
    store.apply(
        BaseEvent(
            kind="liquidation",
            source="liquidation:binance",
            payload={
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "side": "buy",
                "price": 70220.0,
                "quantity": 0.5,
                "notional": 35110.0,
            },
            ts_exchange_ms=now_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
        )
    )

    market = {
        "slug": "btc-updown-5m-test",
        "question": "Bitcoin Up or Down - Test",
        "outcomes": json.dumps(["Up", "Down"]),
        "clobTokenIds": json.dumps(["up-asset", "down-asset"]),
    }
    snapshot = SpotSnapshot(
        reference_price=70205.0,
        lead_price=70200.0,
        binance_price=70200.0,
        chainlink_price=70205.0,
        basis=5.0,
        source="polymarket-rtds+binance",
        age_ms=25,
        connected=True,
    )

    frame = FeatureEngine().build_for_market(
        market=market,
        state_store=store,
        official_price_to_beat=70150.0,
        spot_snapshot=snapshot,
        seconds_into_window=130,
        current_up_exposure=60.0,
        current_down_exposure=20.0,
    )

    assert frame is not None
    assert frame.market_slug == "btc-updown-5m-test"
    assert frame.best_ask_up == 0.46
    assert frame.best_ask_down == 0.47
    assert frame.locked_edge_bps > 0
    assert frame.internal_bullish_pressure_5s > 0
    assert frame.liq_buy_notional_30s == 35110.0
    assert frame.spot_anchor_delta_bps > 0
    assert frame.window_third == "mid"


def test_feature_engine_uses_book_age_not_last_generic_event_lag() -> None:
    store = StateStore()
    now_ns = time.time_ns()
    now_ms = now_ns // 1_000_000

    for asset_id, bid_price, ask_price in (("up-asset", "0.44", "0.46"), ("down-asset", "0.43", "0.47")):
        store.apply(
            BaseEvent(
                kind="book_snapshot",
                source="test",
                asset_id=asset_id,
                payload={
                    "asset_id": asset_id,
                    "book": {
                        "bids": [{"price": bid_price, "size": "120"}],
                        "asks": [{"price": ask_price, "size": "80"}],
                    },
                },
                ts_exchange_ms=now_ms,
                ts_recv_ns=now_ns,
                ts_process_ns=now_ns,
            )
        )

    # A stale generic event should not poison the market latency shown for the book.
    stale_ns = time.time_ns()
    store.apply(
        BaseEvent(
            kind="spot_price",
            source="spot-feed:binance",
            asset_id="btcusdt",
            payload={"symbol": "btcusdt", "price": 70200.0, "quantity": 0.2, "side": "buy"},
            ts_exchange_ms=int(time.time() * 1000) - 18_000,
            ts_recv_ns=stale_ns,
            ts_process_ns=stale_ns,
        )
    )

    market = {
        "slug": "btc-updown-5m-test",
        "question": "Bitcoin Up or Down - Test",
        "outcomes": json.dumps(["Up", "Down"]),
        "clobTokenIds": json.dumps(["up-asset", "down-asset"]),
    }
    snapshot = SpotSnapshot(
        reference_price=70205.0,
        lead_price=70200.0,
        binance_price=70200.0,
        chainlink_price=70205.0,
        basis=5.0,
        source="polymarket-rtds+binance",
        age_ms=25,
        connected=True,
    )

    frame = FeatureEngine().build_for_market(
        market=market,
        state_store=store,
        official_price_to_beat=70150.0,
        spot_snapshot=snapshot,
        seconds_into_window=130,
        current_up_exposure=0.0,
        current_down_exposure=0.0,
    )

    assert frame is not None
    assert frame.market_event_lag_ms < 1_000
