from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from app.core.decision_engine import DecisionTrace, ReadinessScorer, RegimeDetector, StrategyEngine
from app.core.event_bus import BaseEvent
from app.core.feature_engine import FeatureEngine, FeatureFrame
from app.core.lab_artifacts import append_jsonl, dump_json, events_log_path
from app.core.state_store import StateStore
from app.polymarket.spot_feed import SpotSnapshot


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    market_slug: str
    events: int
    feature_frames: int
    decision_traces: int
    latest_regime: str
    latest_readiness_score: float
    latest_expected_edge_bps: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ReplayEngine:
    def __init__(
        self,
        *,
        market: dict[str, Any],
        research_dir: Path,
        output_dir: Path,
        official_price_to_beat: float = 0.0,
        feature_engine: FeatureEngine | None = None,
        readiness_scorer: ReadinessScorer | None = None,
        regime_detector: RegimeDetector | None = None,
        strategy_engine: StrategyEngine | None = None,
    ) -> None:
        self.market = dict(market)
        self.research_dir = research_dir
        self.output_dir = output_dir
        self.official_price_to_beat = float(official_price_to_beat or _market_price_to_beat(market) or 0.0)
        self.state_store = StateStore()
        self.feature_engine = feature_engine or FeatureEngine()
        self.readiness_scorer = readiness_scorer or ReadinessScorer()
        self.regime_detector = regime_detector or RegimeDetector()
        self.strategy_engine = strategy_engine or StrategyEngine()

    def run(self) -> ReplaySummary:
        events = sorted(self._load_runtime_events(), key=_event_sort_key)
        feature_frames = 0
        decision_traces = 0
        latest_frame: FeatureFrame | None = None
        latest_decision: DecisionTrace | None = None

        self.output_dir.mkdir(parents=True, exist_ok=True)
        feature_path = self.output_dir / "replay_feature_frames.jsonl"
        decision_path = self.output_dir / "replay_decision_traces.jsonl"
        if feature_path.exists():
            feature_path.unlink()
        if decision_path.exists():
            decision_path.unlink()

        for event in events:
            self.state_store.apply(event)
            if event.kind not in {"book_snapshot", "book_delta", "best_bid_ask", "market_trade", "spot_price", "liquidation"}:
                continue
            spot_snapshot = _spot_snapshot_from_state(self.state_store)
            frame = self.feature_engine.build_for_market(
                market=self.market,
                state_store=self.state_store,
                official_price_to_beat=self.official_price_to_beat,
                spot_snapshot=spot_snapshot,
                seconds_into_window=_seconds_into_window(event=event, market_slug=str(self.market.get("slug") or "")),
            )
            if frame is None:
                continue
            readiness_score, blockers = self.readiness_scorer.score(frame)
            frame = FeatureFrame(**{**frame.to_dict(), "readiness_score": readiness_score, "regime": self.regime_detector.classify(frame, readiness_score=readiness_score)})
            decision = self.strategy_engine.evaluate(frame, blockers=blockers)
            append_jsonl(feature_path, frame.to_dict())
            append_jsonl(decision_path, decision.to_dict())
            feature_frames += 1
            decision_traces += 1
            latest_frame = frame
            latest_decision = decision

        summary = ReplaySummary(
            market_slug=str(self.market.get("slug") or ""),
            events=len(events),
            feature_frames=feature_frames,
            decision_traces=decision_traces,
            latest_regime=str(latest_frame.regime if latest_frame is not None else ""),
            latest_readiness_score=float(latest_frame.readiness_score if latest_frame is not None else 0.0),
            latest_expected_edge_bps=float(latest_decision.expected_edge_bps if latest_decision is not None else 0.0),
        )
        dump_json(self.output_dir / "replay_summary.json", summary.to_dict())
        return summary

    def _load_runtime_events(self) -> list[BaseEvent]:
        paths = [
            events_log_path(self.research_dir, "market_events"),
            events_log_path(self.research_dir, "spot_events"),
            events_log_path(self.research_dir, "liquidation_events"),
            events_log_path(self.research_dir, "user_events"),
        ]
        events: list[BaseEvent] = []
        for path in paths:
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    events.append(
                        BaseEvent(
                            kind=str(payload.get("kind") or "unknown"),
                            source=str(payload.get("source") or "runtime"),
                            payload=dict(payload.get("payload") or {}),
                            market_id=str(payload.get("market_id") or "") or None,
                            asset_id=str(payload.get("asset_id") or "") or None,
                            window_id=str(payload.get("window_id") or "") or None,
                            ts_exchange_ms=int(float(payload.get("ts_exchange_ms") or 0)),
                            ts_recv_ns=int(payload.get("ts_recv_ns") or 0),
                            ts_process_ns=int(payload.get("ts_process_ns") or 0),
                            seq=int(payload["seq"]) if payload.get("seq") not in (None, "") else None,
                            event_id=str(payload.get("event_id") or ""),
                        )
                    )
        return events


def _event_sort_key(event: BaseEvent) -> tuple[int, int]:
    primary = int(event.ts_exchange_ms or 0)
    secondary = int(event.ts_recv_ns or 0)
    return primary, secondary


def _spot_snapshot_from_state(state_store: StateStore) -> SpotSnapshot:
    binance_points = state_store.recent_spot_points("btcusdt", lookback_seconds=15.0)
    chainlink_points = state_store.recent_spot_points("btc/usd", lookback_seconds=15.0)
    latest_binance = binance_points[-1].price if binance_points else None
    latest_chainlink = chainlink_points[-1].price if chainlink_points else None
    reference_price = latest_chainlink if latest_chainlink is not None else latest_binance
    lead_price = latest_binance if latest_binance is not None else latest_chainlink
    basis = 0.0
    if latest_chainlink is not None and latest_binance is not None:
        basis = latest_chainlink - latest_binance
    return SpotSnapshot(
        reference_price=reference_price,
        lead_price=lead_price,
        binance_price=latest_binance,
        chainlink_price=latest_chainlink,
        basis=basis,
        source="replay",
        age_ms=0,
        connected=True,
    )


def _seconds_into_window(*, event: BaseEvent, market_slug: str) -> int:
    exchange_ms = int(event.ts_exchange_ms or 0)
    if exchange_ms <= 0:
        return 0
    market_start_s = _market_start_from_slug(market_slug)
    if market_start_s <= 0:
        return 0
    return max(int((exchange_ms // 1000) - market_start_s), 0)


def _market_start_from_slug(slug: str) -> int:
    safe_slug = str(slug or "").strip()
    if not safe_slug:
        return 0
    tail = safe_slug.rsplit("-", 1)[-1]
    if not tail.isdigit():
        return 0
    return int(tail)


def _market_price_to_beat(market: dict[str, Any]) -> float:
    events = market.get("events") or []
    for event in events:
        if not isinstance(event, dict):
            continue
        metadata = event.get("eventMetadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        if not isinstance(metadata, dict):
            continue
        try:
            value = float(metadata.get("priceToBeat") or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return 0.0
