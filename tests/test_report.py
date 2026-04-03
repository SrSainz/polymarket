from __future__ import annotations

import json
from pathlib import Path

from app.db import Database
from app.services.report import ReportService


def test_report_includes_variant_and_incubation_summary(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_variant", "arb-micro-v1")
    db.set_bot_state("strategy_notes", "baseline paper incubation")
    db.set_bot_state("strategy_incubation_stage", "paper")
    db.set_bot_state("strategy_incubation_min_days", "0")
    db.set_bot_state("strategy_incubation_min_resolutions", "1")
    db.set_bot_state("strategy_incubation_max_drawdown", "25")
    db.upsert_strategy_window(
        slug="btc-updown-5m-report",
        condition_id="cond-report",
        title="Bitcoin Up or Down - Report",
        price_mode="underround",
        timing_regime="mid-late",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.6,
        planned_budget=15.0,
        current_exposure=15.0,
        notes="report setup",
    )
    db.record_strategy_window_fills(
        slug="btc-updown-5m-report",
        fill_count=2,
        added_notional=14.25,
        replenishment_count=0,
        notes="report fills",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-report",
        realized_pnl=3.75,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="report resolved",
    )
    research_root = tmp_path / "research"
    (research_root / "experiments").mkdir(parents=True, exist_ok=True)
    (research_root / "hypotheses").mkdir(parents=True, exist_ok=True)
    (research_root / "datasets" / "btc5m").mkdir(parents=True, exist_ok=True)
    (research_root / "experiments" / "variant_leaderboard.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-18T10:00:00Z",
                "variants": [
                    {
                        "variant": "arb-micro-v1",
                        "status": "pass",
                        "gate_passed": True,
                        "windows": 4,
                        "net_realized_pnl_usdc": 8.0,
                        "max_drawdown_usdc": 2.5,
                        "fill_rate": 0.75,
                        "hit_rate": 0.5,
                        "real_edge_bps": 12.0,
                        "expectancy_window_usdc": 2.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (research_root / "experiments" / "tournament_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-18T12:00:00Z",
                "active_variant": "arb-micro-v1",
                "recommendation": {
                    "label": "Mantener live pausado",
                    "summary": "Todavia falta evidencia antes de live.",
                    "next_step": "Seguir en shadow y revisar las ventanas malas.",
                },
                "leaderboard": [
                    {
                        "variant": "arb-micro-v1",
                        "rank": 1,
                        "status": "pass",
                        "pnl": 8.0,
                        "expectancy_window_usdc": 2.0,
                        "real_edge_bps": 12.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (research_root / "hypotheses" / "top_wallet_patterns.json").write_text(
        json.dumps(
            {
                "hypotheses": [
                    {"title": "Priorizar variantes crypto-first", "detail": "Hay edge neto en crypto corto."}
                ]
            }
        ),
        encoding="utf-8",
    )
    (research_root / "datasets" / "btc5m" / "dataset_summary.json").write_text(
        json.dumps({"generated_at": "2026-03-18T09:00:00Z", "windows": 2, "events": 60, "trades": 4}),
        encoding="utf-8",
    )
    (research_root / "runtime").mkdir(parents=True, exist_ok=True)
    (research_root / "runtime" / "diagnostics_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-18T11:00:00Z",
                "status": "watch",
                "summary": "Estado watch: stale book moderado.",
                "findings": [
                    {
                        "severity": "medium",
                        "title": "Latencia del libro",
                        "detail": "Muchas decisiones bloqueadas por book_age_ms.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    content, path = ReportService(db, tmp_path / "reports").generate()

    assert path.exists()
    assert "Strategy variant: arb-micro-v1" in content
    assert "## Incubation" in content
    assert "baseline paper incubation" in content
    assert "btc-updown-5m-report" in content
    assert "Lista para escalar" in content
    assert "## Backtest / Experiments" in content
    assert "## Variant Tournament" in content
    assert "Mantener live pausado" in content
    assert "## Runtime Diagnostics" in content
    assert "Latencia del libro" in content
    assert "## Native Dataset" in content
    assert "## Wallet Hypotheses" in content
    assert "Priorizar variantes crypto-first" in content

    db.close()
