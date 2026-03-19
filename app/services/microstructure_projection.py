from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from app.core.decision_engine import DecisionTrace
from app.core.feature_engine import FeatureFrame
from app.core.state_store import LiquidationPrint


def build_microstructure_snapshot(
    *,
    frame: FeatureFrame | None,
    decision: DecisionTrace | None,
    note: str = "",
) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "generated_at": now_iso,
        "market_slug": frame.market_slug if frame is not None else "",
        "market_title": frame.market_title if frame is not None else "",
        "note": note,
        "frame": frame.to_dict() if frame is not None else {},
        "decision": decision.to_dict() if decision is not None else {},
    }


def build_liquidation_snapshot(
    *,
    generated_at: str,
    totals: dict[str, Any],
    recent: list[LiquidationPrint],
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "totals": totals,
        "recent": [row.to_dict() for row in recent[:40]],
    }


def build_latency_snapshot(
    *,
    generated_at: str,
    latencies: dict[str, float | int],
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "latencies": dict(latencies),
    }


def microstructure_bot_state_entries(
    *,
    frame: FeatureFrame | None,
    decision: DecisionTrace | None,
) -> dict[str, str]:
    if frame is None:
        return {
            "strategy_readiness_score": "0.0000",
            "strategy_regime": "",
            "strategy_signal_side": "",
            "strategy_expected_edge_bps": "0.0000",
            "strategy_maker_ev_bps": "0.0000",
            "strategy_taker_ev_bps": "0.0000",
            "strategy_selected_execution": "",
            "strategy_spread_bps_up": "0.0000",
            "strategy_spread_bps_down": "0.0000",
            "strategy_best_bid_up": "0.000000",
            "strategy_best_ask_up": "0.000000",
            "strategy_best_bid_down": "0.000000",
            "strategy_best_ask_down": "0.000000",
            "strategy_internal_bullish_pressure_5s": "0.0000",
            "strategy_internal_bearish_pressure_5s": "0.0000",
            "strategy_external_spot_pressure_5s": "0.0000",
            "strategy_cvd_5s": "0.0000",
            "strategy_cvd_30s": "0.0000",
            "strategy_liq_buy_notional_30s": "0.0000",
            "strategy_liq_sell_notional_30s": "0.0000",
            "strategy_liq_burst_zscore": "0.0000",
            "strategy_near_liq_cluster_distance_bps": "0.0000",
            "strategy_window_third": "",
            "strategy_market_event_lag_ms": "0.0000",
            "strategy_decision_blocked_by": "",
        }
    entries = {
        "strategy_readiness_score": f"{frame.readiness_score:.4f}",
        "strategy_regime": frame.regime,
        "strategy_spread_bps_up": f"{frame.spread_bps_up:.4f}",
        "strategy_spread_bps_down": f"{frame.spread_bps_down:.4f}",
        "strategy_best_bid_up": f"{frame.best_bid_up:.6f}",
        "strategy_best_ask_up": f"{frame.best_ask_up:.6f}",
        "strategy_best_bid_down": f"{frame.best_bid_down:.6f}",
        "strategy_best_ask_down": f"{frame.best_ask_down:.6f}",
        "strategy_internal_bullish_pressure_5s": f"{frame.internal_bullish_pressure_5s:.4f}",
        "strategy_internal_bearish_pressure_5s": f"{frame.internal_bearish_pressure_5s:.4f}",
        "strategy_external_spot_pressure_5s": f"{frame.external_spot_pressure_5s:.4f}",
        "strategy_cvd_5s": f"{frame.cvd_5s:.4f}",
        "strategy_cvd_30s": f"{frame.cvd_30s:.4f}",
        "strategy_liq_buy_notional_30s": f"{frame.liq_buy_notional_30s:.4f}",
        "strategy_liq_sell_notional_30s": f"{frame.liq_sell_notional_30s:.4f}",
        "strategy_liq_burst_zscore": f"{frame.liq_burst_zscore:.4f}",
        "strategy_near_liq_cluster_distance_bps": f"{frame.near_liq_cluster_distance_bps:.4f}",
        "strategy_window_third": frame.window_third,
        "strategy_market_event_lag_ms": f"{frame.market_event_lag_ms:.4f}",
    }
    if decision is None:
        entries.update(
            {
                "strategy_signal_side": "",
                "strategy_expected_edge_bps": "0.0000",
                "strategy_maker_ev_bps": "0.0000",
                "strategy_taker_ev_bps": "0.0000",
                "strategy_selected_execution": "",
                "strategy_decision_blocked_by": "",
            }
        )
        return entries
    entries.update(
        {
            "strategy_signal_side": decision.signal_side,
            "strategy_expected_edge_bps": f"{decision.expected_edge_bps:.4f}",
            "strategy_maker_ev_bps": f"{decision.maker_ev_bps:.4f}",
            "strategy_taker_ev_bps": f"{decision.taker_ev_bps:.4f}",
            "strategy_selected_execution": decision.selected_execution,
            "strategy_decision_blocked_by": ",".join(decision.blocked_by),
        }
    )
    return entries
