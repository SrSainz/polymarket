from __future__ import annotations

from app.core.decision_engine import StrategyEngine
from app.core.feature_engine import FeatureFrame


def _frame(**overrides: float | int | str) -> FeatureFrame:
    payload: dict[str, float | int | str] = {
        "window_id": "w",
        "market_slug": "slug",
        "market_title": "title",
        "cadence": "tick",
        "generated_at_ns": 1,
        "up_asset": "asset-up",
        "down_asset": "asset-down",
        "up_label": "Up",
        "down_label": "Down",
        "best_bid_up": 0.51,
        "best_ask_up": 0.52,
        "best_bid_down": 0.48,
        "best_ask_down": 0.49,
        "spread_ticks_up": 1.0,
        "spread_ticks_down": 1.0,
        "spread_bps_up": 194.0,
        "spread_bps_down": 206.0,
        "top_imbalance_up": 0.12,
        "top_imbalance_down": -0.08,
        "level_imbalance_3_up": 0.10,
        "level_imbalance_3_down": -0.05,
        "microprice_up": 0.515,
        "microprice_down": 0.485,
        "pair_sum_bps": 100.0,
        "locked_edge_bps": 0.0,
        "paired_ofi_z": 0.5,
        "internal_bullish_pressure_5s": 20.0,
        "internal_bearish_pressure_5s": 10.0,
        "internal_bullish_pressure_30s": 200.0,
        "internal_bearish_pressure_30s": 50.0,
        "external_spot_pressure_5s": 0.0,
        "external_spot_pressure_30s": 0.0,
        "cvd_5s": 10.0,
        "cvd_30s": 300.0,
        "paired_cvd": 300.0,
        "trade_aggressor_imbalance": 0.2,
        "sweep_cost_25_bps": 1.0,
        "sweep_cost_50_bps": 4.0,
        "sweep_cost_100_bps": 10.0,
        "book_slope_up": 0.0,
        "book_slope_down": 0.0,
        "book_convexity_up": 0.0,
        "book_convexity_down": 0.0,
        "spot_anchor_delta_bps": 4.0,
        "basis_drift_bps": 0.0,
        "spot_vol_5s": 0.0,
        "liq_buy_notional_30s": 0.0,
        "liq_sell_notional_30s": 0.0,
        "liq_burst_zscore": 0.0,
        "near_liq_cluster_distance_bps": 25.0,
        "seconds_into_window": 60,
        "seconds_left": 240,
        "window_third": "opening",
        "inventory_skew": 0.0,
        "market_event_lag_ms": 50.0,
        "spot_age_ms": 1600,
        "readiness_score": 30.0,
        "regime": "degraded",
        "taker_fee_bps_estimate": 70.0,
    }
    payload.update(overrides)
    return FeatureFrame(**payload)


def test_strategy_engine_allows_maker_when_ev_is_positive_but_below_old_threshold() -> None:
    frame = _frame()
    decision = StrategyEngine(min_taker_edge_bps=8.0, min_maker_edge_bps=6.0).evaluate(
        frame,
        blockers=("spot_stale",),
    )

    assert 3.0 <= decision.maker_ev_bps < 6.0
    assert decision.selected_execution == "maker_post_only_gtc"
    assert "spot_stale" in decision.blocked_by


def test_strategy_engine_keeps_no_trade_near_window_close_even_with_positive_maker_ev() -> None:
    frame = _frame(seconds_left=25)
    decision = StrategyEngine(min_taker_edge_bps=8.0, min_maker_edge_bps=6.0).evaluate(
        frame,
        blockers=("spot_stale",),
    )

    assert decision.selected_execution == "no_trade"
    assert "insufficient_ev" in decision.blocked_by
