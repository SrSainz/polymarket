from __future__ import annotations

from app.services.variant_tournament import build_variant_tournament_payload


def test_variant_tournament_recommends_rotating_to_best_passing_variant() -> None:
    payload = build_variant_tournament_payload(
        active_variant="arb-micro-balanced-v1",
        experiment_payload={
            "variants": [
                {
                    "variant": "vidarx-tilted-v1",
                    "rank": 1,
                    "status": "pass",
                    "gate_passed": True,
                    "score": 12.0,
                    "net_realized_pnl_usdc": 9.5,
                    "max_drawdown_usdc": 3.0,
                    "fill_rate": 0.61,
                    "hit_rate": 0.57,
                    "expectancy_window_usdc": 1.2,
                    "real_edge_bps": 11.0,
                },
                {
                    "variant": "arb-micro-balanced-v1",
                    "rank": 2,
                    "status": "fail",
                    "gate_passed": False,
                    "score": 2.0,
                    "net_realized_pnl_usdc": -1.2,
                    "max_drawdown_usdc": 8.0,
                    "fill_rate": 0.33,
                    "hit_rate": 0.41,
                    "expectancy_window_usdc": -0.1,
                    "real_edge_bps": -1.5,
                },
            ]
        },
        dataset_payload={"windows": 42, "events": 800, "trades": 90},
        runtime_compare_payload={
            "status": "shared",
            "same_window": True,
            "shared_slug": "btc-updown-5m-1",
            "history": {
                "summary": {
                    "shared_window_count": 120,
                    "shadow_participation_pct": 96.0,
                    "shadow_two_sided_window_pct": 98.0,
                    "shadow_one_sided_window_count": 1,
                    "shadow_active_window_count": 40,
                    "shadow_settlement_window_pct": 97.0,
                    "paper_avg_open_cadence_seconds": 3.0,
                    "shadow_avg_open_cadence_seconds": 4.0,
                },
                "sample_summary": {},
            },
        },
        incubation_summary={
            "stage": "paper",
            "progress_pct": 100.0,
            "ready_to_scale": True,
            "max_drawdown": -8.0,
            "max_drawdown_limit": 40.0,
            "drawdown_breached": False,
        },
    )

    assert payload["best_passing_variant_row"]["variant"] == "vidarx-tilted-v1"
    assert payload["recommendation"]["code"] == "rotate-shadow-to-best-pass"
    assert payload["recommendation"]["candidate_variant"] == "vidarx-tilted-v1"
