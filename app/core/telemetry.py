from __future__ import annotations

import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.decision_engine import DecisionTrace, ReadinessScorer, RegimeDetector, StrategyEngine
from app.core.event_bus import BaseEvent, EventBus
from app.core.feature_engine import FeatureEngine, FeatureFrame
from app.core.lab_artifacts import (
    append_jsonl,
    dump_json,
    events_log_path,
    latency_snapshot_path,
    liquidation_snapshot_path,
    microstructure_snapshot_path,
)
from app.core.state_store import StateStore
from app.db import Database
from app.polymarket.spot_feed import SpotSnapshot
from app.services.microstructure_projection import (
    build_latency_snapshot,
    build_liquidation_snapshot,
    build_microstructure_snapshot,
    microstructure_bot_state_entries,
)


class MicrostructureTelemetry:
    def __init__(
        self,
        *,
        db: Database,
        research_dir: Path,
        bus: EventBus,
        state_store: StateStore,
        feature_engine: FeatureEngine,
        readiness_scorer: ReadinessScorer,
        regime_detector: RegimeDetector,
        strategy_engine: StrategyEngine,
        log_events: bool = True,
    ) -> None:
        self.db = db
        self.research_dir = research_dir
        self.bus = bus
        self.state_store = state_store
        self.feature_engine = feature_engine
        self.readiness_scorer = readiness_scorer
        self.regime_detector = regime_detector
        self.strategy_engine = strategy_engine
        self.log_events = bool(log_events)
        self._latest_frame: FeatureFrame | None = None
        self._latest_decision: DecisionTrace | None = None
        self._latest_snapshot_note = ""
        self.bus.subscribe("*", self._handle_event)

    def _handle_event(self, event: BaseEvent) -> None:
        self.state_store.apply(event)
        if self.log_events:
            append_jsonl(self._event_log_path(event.kind), event.to_dict())

    def snapshot_market(
        self,
        *,
        market: dict[str, Any] | None,
        official_price_to_beat: float,
        spot_snapshot: SpotSnapshot | None,
        seconds_into_window: int,
        current_up_exposure: float = 0.0,
        current_down_exposure: float = 0.0,
        note: str = "",
    ) -> tuple[FeatureFrame | None, DecisionTrace | None]:
        if market is None:
            self._latest_frame = None
            self._latest_decision = None
            self._latest_snapshot_note = note
            self._persist_snapshots(note=note)
            return None, None

        start_ns = time.perf_counter_ns()
        frame = self.feature_engine.build_for_market(
            market=market,
            state_store=self.state_store,
            official_price_to_beat=official_price_to_beat,
            spot_snapshot=spot_snapshot,
            seconds_into_window=seconds_into_window,
            current_up_exposure=current_up_exposure,
            current_down_exposure=current_down_exposure,
        )
        feature_elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
        decision: DecisionTrace | None = None
        if frame is not None:
            readiness_score, blockers = self.readiness_scorer.score(frame)
            regime = self.regime_detector.classify(frame, readiness_score=readiness_score)
            frame = replace(frame, readiness_score=readiness_score, regime=regime)
            decision = self.strategy_engine.evaluate(frame, blockers=blockers)
            append_jsonl(events_log_path(self.research_dir, "feature_frames"), frame.to_dict())
            append_jsonl(events_log_path(self.research_dir, "decision_traces"), decision.to_dict())

        self._latest_frame = frame
        self._latest_decision = decision
        self._latest_snapshot_note = note
        self._persist_snapshots(
            note=note,
            feature_elapsed_ms=feature_elapsed_ms,
        )
        return frame, decision

    def latest_frame(self) -> FeatureFrame | None:
        return self._latest_frame

    def latest_decision(self) -> DecisionTrace | None:
        return self._latest_decision

    def _persist_snapshots(self, *, note: str, feature_elapsed_ms: float = 0.0) -> None:
        generated_at = datetime.now(timezone.utc).isoformat()
        frame = self._latest_frame
        decision = self._latest_decision
        dump_json(
            microstructure_snapshot_path(self.research_dir),
            build_microstructure_snapshot(frame=frame, decision=decision, note=note),
        )

        recent_liquidations = self.state_store.recent_liquidations(lookback_seconds=900.0, symbol="BTCUSDT")
        liquidation_totals = {
            "buy_30s": round(sum(row.notional for row in self.state_store.recent_liquidations(lookback_seconds=30.0, symbol="BTCUSDT") if row.side == "buy"), 4),
            "sell_30s": round(sum(row.notional for row in self.state_store.recent_liquidations(lookback_seconds=30.0, symbol="BTCUSDT") if row.side == "sell"), 4),
            "buy_5m": round(sum(row.notional for row in self.state_store.recent_liquidations(lookback_seconds=300.0, symbol="BTCUSDT") if row.side == "buy"), 4),
            "sell_5m": round(sum(row.notional for row in self.state_store.recent_liquidations(lookback_seconds=300.0, symbol="BTCUSDT") if row.side == "sell"), 4),
            "by_exchange_5m": _totals_by_exchange(self.state_store.recent_liquidations(lookback_seconds=300.0, symbol="BTCUSDT")),
        }
        dump_json(
            liquidation_snapshot_path(self.research_dir),
            build_liquidation_snapshot(generated_at=generated_at, totals=liquidation_totals, recent=recent_liquidations),
        )

        latency_payload = build_latency_snapshot(
            generated_at=generated_at,
            latencies={
                "market_event_lag_ms": round(frame.market_event_lag_ms, 4) if frame is not None else 0.0,
                "spot_age_ms": int(frame.spot_age_ms) if frame is not None else 0,
                "feature_compute_ms": round(feature_elapsed_ms, 4),
                "decision_blockers": len(decision.blocked_by) if decision is not None else 0,
            },
        )
        dump_json(latency_snapshot_path(self.research_dir), latency_payload)

        for key, value in microstructure_bot_state_entries(frame=frame, decision=decision).items():
            self.db.set_bot_state(key, value)
        if decision is not None:
            self.db.set_bot_state("strategy_last_decision_at_ns", str(time.time_ns()))
            self.db.set_bot_state("strategy_last_expected_edge_bps", f"{decision.expected_edge_bps:.6f}")

    def _event_log_path(self, kind: str) -> Path:
        if kind == "spot_price":
            return events_log_path(self.research_dir, "spot_events")
        if kind == "liquidation":
            return events_log_path(self.research_dir, "liquidation_events")
        if kind.startswith("user_"):
            return events_log_path(self.research_dir, "user_events")
        return events_log_path(self.research_dir, "market_events")


def _totals_by_exchange(liquidations: list[Any]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for row in liquidations:
        exchange = str(getattr(row, "exchange", "") or "").strip().lower()
        if not exchange:
            continue
        totals[exchange] = round(totals.get(exchange, 0.0) + float(getattr(row, "notional", 0.0) or 0.0), 4)
    return totals
