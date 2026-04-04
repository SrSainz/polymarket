from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from app.core.feature_engine import FeatureFrame


@dataclass(frozen=True, slots=True)
class DecisionTrace:
    window_id: str
    market_slug: str
    market_title: str
    readiness_score: float
    regime: str
    signal_side: str
    expected_edge_bps: float
    maker_ev_bps: float
    taker_ev_bps: float
    maker_fill_prob: float
    selected_execution: str
    blocked_by: tuple[str, ...]
    latency_penalty_bps: float
    spread_penalty_bps: float
    adverse_selection_penalty_bps: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ReadinessScorer:
    def __init__(self, *, min_score: float = 70.0) -> None:
        self.min_score = float(min_score)

    def score(self, frame: FeatureFrame) -> tuple[float, tuple[str, ...]]:
        score = 100.0
        blockers: list[str] = []
        max_spread_bps = max(frame.spread_bps_up, frame.spread_bps_down)
        max_spread_ticks = max(frame.spread_ticks_up, frame.spread_ticks_down)
        if frame.market_event_lag_ms > 1_500:
            score -= 35.0
            blockers.append("market_lag")
        elif frame.market_event_lag_ms > 900:
            score -= 15.0
        if frame.spot_age_ms > 1_800:
            score -= 15.0
            blockers.append("spot_stale")
        elif frame.spot_age_ms > 900:
            score -= 8.0
        if max_spread_ticks > 4:
            score -= 30.0
            blockers.append("spread_ticks")
        elif max_spread_ticks > 2:
            score -= 10.0
        if max_spread_bps > 150:
            score -= 25.0
            blockers.append("spread_bps")
        elif max_spread_bps > 75:
            score -= 12.0
        if frame.sweep_cost_50_bps > 18:
            score -= 20.0
            blockers.append("sweep_cost")
        elif frame.sweep_cost_50_bps > 8:
            score -= 10.0
        if abs(frame.basis_drift_bps) > 10:
            score -= 10.0
        if frame.seconds_left <= 20:
            score -= 35.0
            blockers.append("close_window")
        elif frame.seconds_left <= 45:
            score -= 12.0
        if abs(frame.inventory_skew) > 0.65:
            score -= 10.0
        if abs(frame.near_liq_cluster_distance_bps) <= 12 and frame.liq_burst_zscore > 1.5:
            score -= 8.0
        return max(min(score, 100.0), 0.0), tuple(blockers)


class RegimeDetector:
    def classify(self, frame: FeatureFrame, *, readiness_score: float) -> str:
        if readiness_score < 45:
            return "degraded"
        if frame.seconds_left <= 20:
            return "close_hedge_only"
        if abs(frame.inventory_skew) > 0.40 and frame.seconds_left <= 90:
            return "inventory_repair_only"
        if frame.seconds_into_window < 25:
            return "opening_discovery"
        directional_force = abs(frame.paired_cvd) + abs(frame.external_spot_pressure_5s) + abs(frame.spot_anchor_delta_bps)
        if directional_force >= 20:
            return "directional_pressure"
        return "balanced_arb"


class StrategyEngine:
    def __init__(
        self,
        *,
        min_taker_edge_bps: float = 8.0,
        min_maker_edge_bps: float = 6.0,
    ) -> None:
        self.min_taker_edge_bps = float(min_taker_edge_bps)
        self.min_maker_edge_bps = float(min_maker_edge_bps)

    def evaluate(self, frame: FeatureFrame, *, blockers: tuple[str, ...]) -> DecisionTrace:
        max_spread_ticks = max(frame.spread_ticks_up, frame.spread_ticks_down)
        max_spread_bps = max(frame.spread_bps_up, frame.spread_bps_down)
        directional_score = (
            (frame.paired_ofi_z * 4.0)
            + (frame.paired_cvd / 50.0)
            + (frame.spot_anchor_delta_bps * 0.8)
            + (_liquidation_bias(frame) * 5.0)
            + (_external_pressure_bias(frame) * 2.0)
        )
        signal_side = "up" if directional_score > 0 else "down" if directional_score < 0 else "neutral"
        spread_penalty_bps = max(frame.spread_bps_up, frame.spread_bps_down) * 0.10
        latency_penalty_bps = max(frame.market_event_lag_ms, frame.spot_age_ms) / 200.0
        adverse_selection_penalty_bps = abs(frame.paired_ofi_z) * 1.1 + max(frame.spot_vol_5s, 0.0) * 0.25
        maker_fill_prob = min(max(0.20 + (frame.readiness_score / 120.0) - (spread_penalty_bps / 50.0), 0.05), 0.95)
        expected_edge_bps = frame.locked_edge_bps + directional_score - spread_penalty_bps - latency_penalty_bps
        maker_ev_bps = (frame.locked_edge_bps * maker_fill_prob) + (directional_score * 0.35) - adverse_selection_penalty_bps
        taker_fee_bps = max(float(frame.taker_fee_bps_estimate or 0.0), 0.0)
        taker_ev_bps = expected_edge_bps - max(frame.sweep_cost_50_bps, 0.0) - taker_fee_bps

        selected_execution = "no_trade"
        blocked = list(blockers)
        if frame.readiness_score < 45:
            blocked.append("readiness")
        maker_threshold = self.min_maker_edge_bps
        if frame.readiness_score >= 25 and frame.seconds_left > 45:
            maker_threshold = min(maker_threshold, 3.0)
        if max_spread_ticks <= 2:
            maker_threshold = min(maker_threshold, 2.5)
        if max_spread_ticks > 4 or max_spread_bps > 3500 or frame.seconds_left <= 30:
            maker_threshold = self.min_maker_edge_bps
        if maker_ev_bps >= maker_threshold and maker_ev_bps > taker_ev_bps and frame.seconds_left > 30:
            selected_execution = "maker_post_only_gtc"
        elif taker_ev_bps >= self.min_taker_edge_bps and frame.seconds_left > 15:
            selected_execution = "taker_fak"
        else:
            blocked.append("insufficient_ev")

        return DecisionTrace(
            window_id=frame.window_id,
            market_slug=frame.market_slug,
            market_title=frame.market_title,
            readiness_score=frame.readiness_score,
            regime=frame.regime,
            signal_side=signal_side,
            expected_edge_bps=round(expected_edge_bps, 4),
            maker_ev_bps=round(maker_ev_bps, 4),
            taker_ev_bps=round(taker_ev_bps, 4),
            maker_fill_prob=round(maker_fill_prob, 4),
            selected_execution=selected_execution,
            blocked_by=tuple(dict.fromkeys(blocked)),
            latency_penalty_bps=round(latency_penalty_bps, 4),
            spread_penalty_bps=round(spread_penalty_bps, 4),
            adverse_selection_penalty_bps=round(adverse_selection_penalty_bps, 4),
        )


def _liquidation_bias(frame: FeatureFrame) -> float:
    total = frame.liq_buy_notional_30s + frame.liq_sell_notional_30s
    if total <= 0:
        return 0.0
    return (frame.liq_buy_notional_30s - frame.liq_sell_notional_30s) / total


def _external_pressure_bias(frame: FeatureFrame) -> float:
    return frame.external_spot_pressure_5s / max(abs(frame.external_spot_pressure_30s), 1.0)
