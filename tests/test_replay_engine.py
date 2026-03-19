from __future__ import annotations

import json
import time
from pathlib import Path

from app.core.event_bus import BaseEvent
from app.core.lab_artifacts import append_jsonl, events_log_path
from app.core.replay_engine import ReplayEngine


def _append_event(research_dir: Path, name: str, event: BaseEvent) -> None:
    append_jsonl(events_log_path(research_dir, name), event.to_dict())


def test_replay_engine_rebuilds_feature_and_decision_streams(tmp_path: Path) -> None:
    research_dir = tmp_path / "research"
    output_dir = tmp_path / "replay"
    ts_ms = int(time.time() * 1000)
    slug = f"btc-updown-5m-{int(ts_ms / 1000) - 60}"
    market = {
        "slug": slug,
        "question": "Bitcoin Up or Down",
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"eventMetadata": {"priceToBeat": 70000.0}}],
    }

    _append_event(
        research_dir,
        "market_events",
        BaseEvent(
            kind="book_snapshot",
            source="market_ws",
            market_id="cond-1",
            asset_id="asset-up",
            window_id=slug,
            ts_exchange_ms=ts_ms,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
            payload={"bids": [{"price": 0.52, "size": 100}], "asks": [{"price": 0.53, "size": 120}], "tick_size": 0.01},
        ),
    )
    _append_event(
        research_dir,
        "market_events",
        BaseEvent(
            kind="book_snapshot",
            source="market_ws",
            market_id="cond-1",
            asset_id="asset-down",
            window_id=slug,
            ts_exchange_ms=ts_ms + 1,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
            payload={"bids": [{"price": 0.46, "size": 90}], "asks": [{"price": 0.47, "size": 110}], "tick_size": 0.01},
        ),
    )
    _append_event(
        research_dir,
        "market_events",
        BaseEvent(
            kind="market_trade",
            source="market_ws",
            market_id="cond-1",
            asset_id="asset-up",
            window_id=slug,
            ts_exchange_ms=ts_ms + 2,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
            payload={"price": 0.53, "size": 25.0, "side": "buy"},
        ),
    )
    _append_event(
        research_dir,
        "spot_events",
        BaseEvent(
            kind="spot_price",
            source="binance",
            market_id="cond-1",
            window_id=slug,
            ts_exchange_ms=ts_ms + 3,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
            payload={"symbol": "btcusdt", "price": 70025.0, "quantity": 1.5, "side": "buy"},
        ),
    )
    _append_event(
        research_dir,
        "spot_events",
        BaseEvent(
            kind="spot_price",
            source="chainlink",
            market_id="cond-1",
            window_id=slug,
            ts_exchange_ms=ts_ms + 4,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
            payload={"symbol": "btc/usd", "price": 70022.0, "quantity": 0.0, "side": "buy"},
        ),
    )
    _append_event(
        research_dir,
        "liquidation_events",
        BaseEvent(
            kind="liquidation",
            source="binance_liq",
            market_id="cond-1",
            window_id=slug,
            ts_exchange_ms=ts_ms + 5,
            ts_recv_ns=time.time_ns(),
            ts_process_ns=time.time_ns(),
            payload={"exchange": "binance", "symbol": "BTCUSDT", "side": "buy", "price": 70030.0, "quantity": 0.4},
        ),
    )

    replay = ReplayEngine(
        market=market,
        research_dir=research_dir,
        output_dir=output_dir,
    )
    summary = replay.run()

    assert summary.market_slug == slug
    assert summary.events >= 6
    assert summary.feature_frames >= 1
    assert summary.decision_traces >= 1
    assert (output_dir / "replay_feature_frames.jsonl").exists()
    assert (output_dir / "replay_decision_traces.jsonl").exists()
    payload = json.loads((output_dir / "replay_summary.json").read_text(encoding="utf-8"))
    assert payload["market_slug"] == slug
