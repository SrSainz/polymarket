from __future__ import annotations

import json
from pathlib import Path

from app.db import Database
from app.models import ExecutionResult, SignalAction
from app.services.runtime_diagnostics import RuntimeDiagnosticsService, evaluate_runtime_guard
from app.settings import AppPaths, AppSettings, BotConfig, EnvSettings


def _settings(root: Path) -> AppSettings:
    paths = AppPaths(
        root=root,
        db_path=root / "bot.db",
        logs_dir=root / "logs",
        reports_dir=root / "reports",
    )
    paths.ensure()
    return AppSettings(
        config=BotConfig(
            watched_wallets=["0xabc"],
            strategy_mode="btc5m_orderbook",
            strategy_entry_mode="arb_micro",
            strategy_variant="arb-micro-balanced-v1",
        ),
        env=EnvSettings(live_trading=True),
        paths=paths,
    )


def test_runtime_diagnostics_detects_recent_failure_cluster(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    db = Database(settings.paths.db_path)
    db.init_schema()

    now_ts = 1_773_700_000
    for index, pnl_delta in enumerate([-18.0, -22.0, -9.5], start=1):
        db.record_execution(
            result=ExecutionResult(
                mode="paper",
                status="filled",
                action=SignalAction.CLOSE,
                asset=f"asset-{index}",
                size=10.0,
                price=0.4,
                notional=4.0,
                pnl_delta=pnl_delta,
                message="close",
            ),
            side="sell",
            condition_id=f"cond-{index}",
            source_wallet="strategy:arb_micro",
            source_signal_id=index,
            notes="autonomous stop_loss -10.00%",
        )
        db.conn.execute("UPDATE executions SET ts = ? WHERE id = (SELECT MAX(id) FROM executions)", (now_ts - index * 60,))
    db.conn.commit()

    decisions_path = settings.paths.research_dir / "paper_decisions.jsonl"
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    decision_rows = []
    for offset in range(24):
        decision_rows.append(
            {
                "decision": {
                    "strategy_name": "underround_arb",
                    "should_trade": False,
                    "reason": f"book_age_ms {300 + offset} > 250",
                }
            }
        )
    decisions_path.write_text("\n".join(json.dumps(item) for item in decision_rows) + "\n", encoding="utf-8")

    payload = RuntimeDiagnosticsService(db, settings.paths.research_dir, settings).generate(now_ts=now_ts)

    assert payload["status"] == "critical"
    assert "sangrado" in payload["findings"][0]["title"].lower()
    assert (settings.paths.research_dir / "runtime" / "diagnostics_latest.json").exists()
    assert db.get_bot_state("runtime_diagnostics_status") == "critical"
    assert db.get_bot_state("runtime_diagnostics_dominant_strategy") == "underround_arb"
    db.close()


def test_runtime_guard_blocks_after_loss_streak() -> None:
    executions = [
        {"ts": 1_000_000, "action": "close", "pnl_delta": -12.0},
        {"ts": 999_940, "action": "reduce", "pnl_delta": -8.0},
        {"ts": 999_880, "action": "close", "pnl_delta": -15.0},
        {"ts": 999_820, "action": "close", "pnl_delta": 4.0},
    ]

    decision = evaluate_runtime_guard(
        executions,
        now_ts=1_000_000,
        lookback_minutes=10,
        loss_streak_limit=3,
        max_recent_close_pnl=-20.0,
        cooldown_minutes=45,
    )

    assert decision["blocked"] is True
    assert decision["consecutive_losses"] == 3
    assert decision["recent_close_pnl"] == -31.0
    assert decision["cooldown_until"] == 1_002_700


def test_runtime_guard_can_disable_loss_streak_and_only_use_pnl_limit() -> None:
    executions = [
        {"ts": 1_000_000, "action": "close", "pnl_delta": -8.0},
        {"ts": 999_940, "action": "reduce", "pnl_delta": -7.0},
        {"ts": 999_880, "action": "close", "pnl_delta": -6.0},
    ]

    decision = evaluate_runtime_guard(
        executions,
        now_ts=1_000_000,
        lookback_minutes=10,
        loss_streak_limit=0,
        max_recent_close_pnl=-25.0,
        cooldown_minutes=45,
    )

    assert decision["blocked"] is False
    assert decision["consecutive_losses"] == 3
    assert decision["recent_close_pnl"] == -21.0
    assert decision["reason"] == ""
