from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide
from app.services.dashboard_server import (
    _allowed_cors_origin,
    _apply_live_control_action,
    _claimable_positions_snapshot,
    _destructive_request_allowed,
    _executions_payload,
    _latency_payload,
    _liquidations_payload,
    _metrics_payload,
    _microstructure_payload,
    _reset_compare_state,
    _restart_runtime_state,
    _reset_runtime_state,
    _summary_payload,
    _window_audit_payload,
)


def test_summary_payload_exposes_live_state(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()

    instruction = CopyInstruction(
        action=SignalAction.OPEN,
        side=TradeSide.BUY,
        asset="asset-1",
        condition_id="cond-1",
        size=10.0,
        price=0.5,
        notional=5.0,
        source_wallet="0xsrc",
        source_signal_id=1,
        title="Market",
        slug="market",
        outcome="Yes",
        category="crypto",
        reason="paper fill",
    )
    PaperBroker(db).execute(instruction)
    db.set_bot_state("live_cash_balance", "12.34")
    db.set_bot_state("live_cash_allowance", "10.01")
    db.set_bot_state("live_total_capital", "18.56")
    db.set_bot_state("strategy_mode", "btc5m_orderbook")
    db.set_bot_state("strategy_entry_mode", "buy_opposite")
    db.set_bot_state("strategy_target_outcome", "Down")
    db.set_bot_state("strategy_target_price", "0.02")
    db.set_bot_state("strategy_last_note", "buy_opposite trigger Up ask=0.99 -> buy Down ask=0.02")
    db.set_bot_state("strategy_operability_state", "executing")
    db.set_bot_state("strategy_operability_label", "Comprando")
    db.set_bot_state("strategy_operability_reason", "Hay plan activo y el motor esta ejecutando o acompanando el bracket actual.")
    db.set_bot_state("strategy_operability_blocking", "0")
    db.set_bot_state("strategy_operating_bankroll", "125.000000")
    db.set_bot_state("strategy_reserved_profit", "25.000000")
    db.set_bot_state("strategy_exposure_cap_mode", "percent-after-compounding")
    db.set_bot_state("strategy_market_exposure_cap", "26.355434")
    db.set_bot_state("strategy_total_exposure_cap", "105.421737")
    db.set_bot_state("strategy_market_exposure_remaining", "6.054321")
    db.set_bot_state("strategy_total_exposure_remaining", "84.991237")
    db.set_bot_state("strategy_cash_available_for_cycle", "12.340000")
    db.set_bot_state("strategy_budget_effective_ceiling", "6.054321")
    db.set_bot_state("strategy_effective_min_notional", "3.200000")
    db.set_bot_state("strategy_cycle_budget_floor_applied", "1")
    db.close()
    runtime_dir = tmp_path / "research" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "diagnostics_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-19T10:00:00Z",
                "status": "degraded",
                "summary": "Estado degraded: 2 hallazgos.",
                "findings": [{"severity": "medium", "title": "Libro viejo", "detail": "stale book alto"}],
            }
        ),
        encoding="utf-8",
    )

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["live_mode_active"] is True
    assert summary["configured_execution_mode"] == "live"
    assert summary["live_trading_enabled"] is True
    assert "live_executions_today" in summary
    assert "live_realized_pnl_today" in summary
    assert summary["live_cash_balance"] == 12.34
    assert summary["live_available_to_trade"] == 10.01
    assert summary["live_total_capital"] == 18.56
    assert summary["live_equity_estimate"] == 17.34
    assert summary["strategy_mode"] == "btc5m_orderbook"
    assert summary["strategy_entry_mode"] == "buy_opposite"
    assert summary["strategy_target_outcome"] == "Down"
    assert summary["strategy_operability_state"] == "executing"
    assert summary["strategy_operability_label"] == "Comprando"
    assert summary["strategy_operability_reason"].startswith("Hay plan activo")
    assert summary["strategy_operability_blocking"] is False
    assert summary["strategy_operating_bankroll"] == 125.0
    assert summary["strategy_reserved_profit"] == 25.0
    assert summary["strategy_exposure_cap_mode"] == "percent-after-compounding"
    assert summary["strategy_market_exposure_cap"] == 26.3554
    assert summary["strategy_total_exposure_cap"] == 105.4217
    assert summary["strategy_market_exposure_remaining"] == 6.0543
    assert summary["strategy_total_exposure_remaining"] == 84.9912
    assert summary["strategy_cash_available_for_cycle"] == 12.34
    assert summary["strategy_budget_effective_ceiling"] == 6.0543
    assert summary["strategy_effective_min_notional"] == 3.2
    assert summary["strategy_cycle_budget_floor_applied"] is True
    assert summary["runtime_diagnostics_status"] == "degraded"
    assert summary["runtime_diagnostics_findings"][0]["title"] == "Libro viejo"


def test_summary_payload_exposes_claimable_redeem_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("live_cash_balance", "40.00")
    db.set_bot_state("live_cash_allowance", "40.00")
    db.set_bot_state("live_total_capital", "40.00")
    db.close()

    with patch(
        "app.services.dashboard_server._claimable_positions_snapshot",
        return_value={
            "available": True,
            "wallet": "0xabc",
            "positions_count": 2,
            "shares_total": 17.5,
            "usdc_estimate": 17.5,
            "positions": [
                {
                    "slug": "btc-updown-5m-1",
                    "title": "Bitcoin Up or Down",
                    "outcome": "Up",
                    "size": 10.0,
                    "estimated_usdc": 10.0,
                    "end_date": "2026-03-30T10:00:00Z",
                }
            ],
            "error": "",
            "detected_at": 1774828800,
        },
    ):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="live",
            live_trading_enabled=True,
        )

    assert summary["claimable_available"] is True
    assert summary["claimable_wallet"] == "0xabc"
    assert summary["claimable_positions_count"] == 2
    assert summary["claimable_shares_total"] == 17.5
    assert summary["claimable_usdc_estimate"] == 17.5
    assert summary["claimable_positions"][0]["slug"] == "btc-updown-5m-1"
    assert summary["claimable_detected_at"] == 1774828800
    assert "currentValue" in summary["dashboard_metric_sources"]["claimable_usdc_estimate"]


def test_summary_payload_exposes_pending_live_orders(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("live_cash_balance", "97.72")
    db.set_bot_state("live_cash_allowance", "97.72")
    db.set_bot_state("live_total_capital", "97.72")
    db.set_bot_state(
        "live_pending_order:abc123",
        json.dumps(
            {
                "order_id": "abc123",
                "action": "open",
                "side": "buy",
                "asset": "asset-live-1",
                "condition_id": "cond-live-1",
                "size": 4.0,
                "price": 0.41,
                "notional": 1.64,
                "source_wallet": "strategy-live",
                "source_signal_id": 17,
                "title": "BTC Up or Down",
                "slug": "btc-updown-5m-1774910400",
                "outcome": "Up",
                "reason": "fase abrir",
                "execution_profile": "taker_fak",
                "response_status": "live",
                "submitted_at": 1774910401,
            },
            separators=(",", ":"),
        ),
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["live_pending_orders_count"] == 1
    assert summary["live_pending_orders_total_notional"] == 1.64
    assert summary["last_live_activity_ts"] == 1774910401
    assert summary["live_pending_orders"][0]["order_id"] == "abc123"
    assert summary["live_pending_orders"][0]["status"] == "submitted"


def test_summary_payload_uses_outstanding_pending_notional_and_current_market_pending_exposure(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("live_cash_balance", "97.72")
    db.set_bot_state("live_cash_allowance", "97.72")
    db.set_bot_state("live_total_capital", "97.72")
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-1774910400")
    db.set_bot_state("strategy_market_title", "BTC Up or Down")
    db.set_bot_state(
        "live_pending_order:partial-1",
        json.dumps(
            {
                "order_id": "partial-1",
                "action": "open",
                "side": "buy",
                "asset": "asset-live-2",
                "condition_id": "cond-live-2",
                "size": 10.0,
                "reconciled_size": 4.0,
                "price": 0.41,
                "notional": 4.10,
                "source_wallet": "strategy-live",
                "source_signal_id": 21,
                "title": "BTC Up or Down",
                "slug": "btc-updown-5m-1774910400",
                "outcome": "Down",
                "reason": "fase abrir",
                "execution_profile": "taker_fak",
                "response_status": "matched",
                "submitted_at": 1774910402,
            },
            separators=(",", ":"),
        ),
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["live_pending_orders_count"] == 1
    assert summary["live_pending_orders_total_notional"] == 2.46
    assert summary["live_pending_orders"][0]["size"] == 6.0
    assert summary["live_pending_orders"][0]["status"] == "partial"
    assert summary["strategy_pending_total_exposure"] == 2.46
    assert summary["strategy_pending_market_exposure"] == 2.46
    assert summary["strategy_current_market_total_exposure"] == 2.46


def test_executions_payload_includes_pending_live_orders_before_fills(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.record_execution(
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-filled",
            size=5.0,
            price=0.4,
            notional=2.0,
            pnl_delta=0.0,
            message="filled",
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-filled",
        source_wallet="0xsrc",
        source_signal_id=11,
        notes="fill real",
    )
    db.set_bot_state(
        "live_pending_order:pending-1",
        json.dumps(
            {
                "order_id": "pending-1",
                "action": "open",
                "side": "buy",
                "asset": "asset-pending",
                "condition_id": "cond-pending",
                "size": 3.0,
                "price": 0.42,
                "notional": 1.26,
                "source_wallet": "strategy-live",
                "source_signal_id": 19,
                "title": "BTC Up or Down",
                "slug": "btc-updown-5m-1774910700",
                "outcome": "Down",
                "reason": "fase abrir",
                "execution_profile": "taker_fak",
                "response_status": "matched",
                "submitted_at": int(time.time()) + 60,
            },
            separators=(",", ":"),
        ),
    )
    db.close()

    payload = _executions_payload(db_path, limit=10)

    assert len(payload["items"]) >= 2
    assert payload["items"][0]["pending_live_order"] is True
    assert payload["items"][0]["status"] == "submitted"
    assert payload["items"][0]["order_id"] == "pending-1"
    assert payload["items"][0]["slug"] == "btc-updown-5m-1774910700"
    assert payload["items"][1]["pending_live_order"] is False


def test_summary_payload_exposes_observed_live_trades(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("live_cash_balance", "97.72")
    db.set_bot_state("live_cash_allowance", "97.72")
    db.set_bot_state("live_total_capital", "97.72")
    db.set_bot_state(
        "live_observed_activity:manual-order:trade-7",
        json.dumps(
            {
                "order_id": "manual-order",
                "trade_id": "trade-7",
                "action": "close",
                "side": "sell",
                "asset": "asset-live-7",
                "condition_id": "cond-live-7",
                "size": 5.0,
                "price": 0.35,
                "notional": 1.75,
                "source_wallet": "live-user-feed",
                "title": "BTC Up or Down",
                "slug": "btc-updown-5m-1774911000",
                "outcome": "Down",
                "status": "confirmed",
                "observed_at": 1774911002,
                "notes": "movimiento live observado fuera del bot",
                "observed_live_activity": True,
            },
            separators=(",", ":"),
        ),
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["live_observed_trades_count"] == 1
    assert summary["live_observed_trades_total_notional"] == 1.75
    assert summary["last_live_activity_ts"] == 1774911002
    assert summary["live_observed_trades"][0]["trade_id"] == "trade-7"
    assert summary["live_observed_trades"][0]["observed_live_activity"] is True


def test_summary_payload_live_recent_resolutions_require_real_live_executions(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_runtime_mode", "live")
    db.upsert_strategy_window(
        slug="btc-updown-5m-live-empty",
        condition_id="cond-live-empty",
        title="Bitcoin Up or Down - Empty",
        price_mode="repair-bracket",
        timing_regime="mid-late",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.8,
        planned_budget=5.0,
        current_exposure=0.0,
        notes="window open",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-live-empty",
        realized_pnl=0.0,
        winning_outcome="Down",
        current_exposure=0.0,
        notes="window closed",
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["strategy_recent_resolutions"] == []
    assert summary["strategy_resolution_pnl_curve"]["window_count"] == 0


def test_summary_payload_live_recent_resolutions_use_execution_rollup_pnl(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_runtime_mode", "live")
    db.upsert_strategy_window(
        slug="btc-updown-5m-live-rollup",
        condition_id="cond-live-rollup",
        title="Bitcoin Up or Down - Rollup",
        price_mode="repair-bracket",
        timing_regime="mid-late",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.8,
        planned_budget=5.0,
        current_exposure=0.0,
        notes="window open",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-live-rollup",
        realized_pnl=0.0,
        winning_outcome="Down",
        current_exposure=0.0,
        notes="window closed",
    )
    db.record_execution(
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-live-rollup",
            size=10.0,
            price=0.4,
            notional=4.0,
            pnl_delta=0.0,
            message="buy",
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-live-rollup",
        source_wallet="wallet-sync:test",
        source_signal_id=0,
        notes="wallet_activity_import tx=buy",
        title="Bitcoin Up or Down - Rollup",
        slug="btc-updown-5m-live-rollup",
        outcome="Down",
        category="crypto",
        ts=1775000000,
    )
    db.record_execution(
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-live-rollup",
            size=10.0,
            price=0.55,
            notional=5.5,
            pnl_delta=1.5,
            message="sell",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-live-rollup",
        source_wallet="wallet-sync:test",
        source_signal_id=0,
        notes="wallet_activity_import tx=sell",
        title="Bitcoin Up or Down - Rollup",
        slug="btc-updown-5m-live-rollup",
        outcome="Down",
        category="crypto",
        ts=1775000050,
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["strategy_recent_resolutions"][0]["slug"] == "btc-updown-5m-live-rollup"
    assert summary["strategy_recent_resolutions"][0]["pnl"] == 1.5
    assert summary["strategy_recent_resolutions"][0]["deployed_notional"] == 9.5
    assert summary["strategy_resolution_pnl_curve"]["window_count"] == 1
    assert summary["strategy_resolution_pnl_curve"]["total_realized_pnl"] == 1.5


def test_executions_payload_includes_observed_live_trades(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.record_execution(
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-filled",
            size=5.0,
            price=0.4,
            notional=2.0,
            pnl_delta=0.0,
            message="filled",
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-filled",
        source_wallet="0xsrc",
        source_signal_id=11,
        notes="fill real",
    )
    db.set_bot_state(
        "live_observed_activity:manual-order:trade-8",
        json.dumps(
            {
                "order_id": "manual-order",
                "trade_id": "trade-8",
                "action": "close",
                "side": "sell",
                "asset": "asset-observed",
                "condition_id": "cond-observed",
                "size": 4.0,
                "price": 0.36,
                "notional": 1.44,
                "source_wallet": "live-user-feed",
                "title": "BTC Up or Down",
                "slug": "btc-updown-5m-1774911300",
                "outcome": "Down",
                "status": "confirmed",
                "observed_at": int(time.time()) + 90,
                "notes": "movimiento live observado fuera del bot",
                "observed_live_activity": True,
            },
            separators=(",", ":"),
        ),
    )
    db.close()

    payload = _executions_payload(db_path, limit=10)

    assert len(payload["items"]) >= 2
    assert payload["items"][0]["observed_live_activity"] is True
    assert payload["items"][0]["pending_live_order"] is False
    assert payload["items"][0]["trade_id"] == "trade-8"
    assert payload["items"][0]["slug"] == "btc-updown-5m-1774911300"
    assert payload["items"][1]["observed_live_activity"] is False


def test_executions_payload_keeps_execution_market_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    instruction = CopyInstruction(
        action=SignalAction.OPEN,
        side=TradeSide.BUY,
        asset="asset-meta",
        condition_id="cond-meta",
        size=6.0,
        price=0.37,
        notional=2.22,
        source_wallet="strategy:arb_micro",
        source_signal_id=77,
        title="Bitcoin Up or Down - March 31",
        slug="btc-updown-5m-1774920300",
        outcome="Up",
        category="crypto",
        reason="metadata fill",
    )
    PaperBroker(db).execute(instruction)
    db.close()

    payload = _executions_payload(db_path, limit=10)

    assert payload["items"][0]["title"] == "Bitcoin Up or Down - March 31"
    assert payload["items"][0]["slug"] == "btc-updown-5m-1774920300"
    assert payload["items"][0]["outcome"] == "Up"
    assert payload["items"][0]["category"] == "crypto"


def test_window_audit_payload_exposes_recent_strategy_snapshots(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_live.db"
    db = Database(db_path)
    db.init_schema()
    db.record_strategy_window_audit(
        ts=1775050505,
        slug="btc-updown-5m-1775050500",
        condition_id="cond-audit",
        title="Bitcoin Up or Down - March 31, 10:35AM-10:40AM ET",
        runtime_mode="live",
        note="arb_micro no locked edge: pair sum 1.010",
        operability_state="waiting_bracket",
        operability_reason="Hace falta un bracket ejecutable antes de abrir.",
        bracket_phase="abrir",
        price_mode="cheap-side",
        target_outcome="Down",
        signal_side="down",
        selected_execution="taker_fak",
        pair_sum=1.01,
        expected_edge_bps=27.4,
        terminal_ev_pct=0.0412,
        spot_delta_bps=-8.5,
        cycle_budget=25.0,
        budget_effective_ceiling=25.0,
        current_exposure=0.0,
        current_market_total_exposure=0.0,
        live_cash_balance=97.72,
        live_available_to_trade=97.72,
        payload_json=json.dumps({"strategy_reference_quality": "captured-chainlink"}),
    )
    db.close()

    payload = _window_audit_payload(db_path, limit=10)

    assert len(payload["items"]) == 1
    assert payload["items"][0]["slug"] == "btc-updown-5m-1775050500"
    assert payload["items"][0]["operability_state"] == "waiting_bracket"
    assert payload["items"][0]["terminal_ev_pct"] == 0.0412
    assert payload["items"][0]["snapshot"]["strategy_reference_quality"] == "captured-chainlink"


def test_claimable_positions_snapshot_uses_env_wallet_and_data_api(tmp_path: Path) -> None:
    fake_settings = SimpleNamespace(
        env=SimpleNamespace(
            polymarket_funder="0xFunder",
            bot_wallet_address="0xBot",
            data_api_host="https://data-api.polymarket.com",
        )
    )
    fake_positions = [
        {
            "redeemable": True,
            "size": "4",
            "currentValue": "4",
            "curPrice": "1",
            "slug": "btc-updown-5m-1",
            "title": "Bitcoin Up or Down",
            "outcome": "Up",
            "endDate": "2026-03-30T10:00:00Z",
        },
        {
            "redeemable": False,
            "size": "3",
            "currentValue": "3",
            "curPrice": "1",
            "slug": "btc-updown-5m-2",
            "title": "Bitcoin Up or Down",
            "outcome": "Down",
            "endDate": "2026-03-30T10:05:00Z",
        },
    ]

    with patch("app.services.dashboard_server.load_settings", return_value=fake_settings), patch(
        "app.services.dashboard_server.ActivityClient.get_positions",
        return_value=fake_positions,
    ):
        snapshot = _claimable_positions_snapshot(tmp_path)

    assert snapshot["available"] is True
    assert snapshot["wallet"] == "0xfunder"
    assert snapshot["positions_count"] == 1
    assert snapshot["usdc_estimate"] == 4.0
    assert snapshot["positions"][0]["slug"] == "btc-updown-5m-1"


def test_summary_payload_exposes_vidarx_lab_state(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()

    db.set_bot_state("live_cash_balance", "28.00")
    db.set_bot_state("live_cash_allowance", "28.00")
    db.set_bot_state("live_total_capital", "30.75")
    db.set_bot_state("strategy_mode", "btc5m_orderbook")
    db.set_bot_state("strategy_entry_mode", "vidarx_micro")
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-1773233700")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Current")
    db.set_bot_state("strategy_market_bias", "Down primary / Up hedge")
    db.set_bot_state("strategy_plan_legs", "2")
    db.set_bot_state("strategy_window_seconds", "176")
    db.set_bot_state("strategy_cycle_budget", "3.25")
    db.set_bot_state("strategy_current_market_exposure", "5.54")
    db.set_bot_state("strategy_resolution_mode", "paper-settle-at-close")
    db.set_bot_state("strategy_timing_regime", "mid-late")
    db.set_bot_state("strategy_price_mode", "extreme")
    db.set_bot_state("strategy_spot_price", "71787.28")
    db.set_bot_state("strategy_spot_anchor", "71775.07")
    db.set_bot_state("strategy_spot_local_anchor", "71761.47")
    db.set_bot_state("strategy_official_price_to_beat", "71775.07")
    db.set_bot_state("strategy_anchor_source", "polymarket-official")
    db.set_bot_state("strategy_reference_quality", "official")
    db.set_bot_state("strategy_reference_comparable", "1")
    db.set_bot_state("strategy_reference_note", "referencia oficial Polymarket + Chainlink RTDS")
    db.set_bot_state("strategy_operability_state", "ready")
    db.set_bot_state("strategy_operability_label", "Listo para ejecutar")
    db.set_bot_state("strategy_operability_reason", "Hay un plan valido preparado para este ciclo.")
    db.set_bot_state("strategy_operability_blocking", "0")
    db.set_bot_state("strategy_spot_delta_bps", "3.60")
    db.set_bot_state("strategy_spot_fair_up", "0.559")
    db.set_bot_state("strategy_spot_fair_down", "0.441")
    db.set_bot_state("strategy_spot_price_mode", "lead-basis")
    db.set_bot_state("strategy_primary_ratio", "0.80")
    db.set_bot_state("strategy_primary_outcome", "Up")
    db.set_bot_state("strategy_hedge_outcome", "Down")
    db.set_bot_state("strategy_primary_exposure", "4.40")
    db.set_bot_state("strategy_hedge_exposure", "1.14")
    db.set_bot_state("strategy_replenishment_count", "2")
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-current",
        size=10.0,
        avg_price=0.8,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Current",
        slug="btc-updown-5m-1773233700",
        outcome="Up",
        category="crypto",
    )
    db.upsert_copy_position(
        asset="asset-down",
        condition_id="cond-current",
        size=5.0,
        avg_price=0.2,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Current",
        slug="btc-updown-5m-1773233700",
        outcome="Down",
        category="crypto",
    )
    db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-1",
            size=10.0,
            price=1.0,
            notional=10.0,
            pnl_delta=6.0,
            message="resolved",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-1",
        source_wallet="strategy:vidarx_micro",
        source_signal_id=0,
        notes="vidarx_resolution:btc-updown-5m-1773233400:Up",
    )
    db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(71775.07, "public-gamma")):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    assert summary["strategy_is_lab"] is True
    assert summary["strategy_market_bias"] == "Down primary / Up hedge"
    assert summary["strategy_plan_legs"] == 2
    assert summary["strategy_window_seconds"] == 176
    assert summary["strategy_cycle_budget"] == 3.25
    assert summary["strategy_current_market_exposure"] == 5.54
    assert summary["strategy_resolution_mode"] == "paper-settle-at-close"
    assert summary["strategy_timing_regime"] == "mid-late"
    assert summary["strategy_price_mode"] == "extreme"
    assert summary["strategy_spot_anchor"] == 71775.07
    assert summary["strategy_spot_local_anchor"] == 71761.47
    assert summary["strategy_official_price_to_beat"] == 71775.07
    assert summary["strategy_anchor_source"] == "polymarket-official"
    assert summary["strategy_reference_quality"] == "official"
    assert summary["strategy_reference_comparable"] is True
    assert summary["strategy_reference_note"] == "referencia oficial Polymarket + Chainlink RTDS"
    assert summary["strategy_spot_price_mode"] == "lead-basis"
    assert summary["strategy_operability_state"] == "ready"
    assert summary["strategy_operability_label"] == "Listo para ejecutar"
    assert summary["strategy_operability_reason"] == "Hay un plan valido preparado para este ciclo."
    assert summary["strategy_operability_blocking"] is False
    assert summary["strategy_primary_ratio"] == 0.8
    assert summary["strategy_primary_outcome"] == "Up"
    assert summary["strategy_hedge_outcome"] == "Down"
    assert summary["strategy_replenishment_count"] == 2
    assert summary["strategy_current_market_total_exposure"] == 9.0
    assert summary["strategy_current_market_primary_exposure"] == 8.0
    assert summary["strategy_current_market_hedge_exposure"] == 1.0
    assert len(summary["strategy_current_market_breakdown"]) == 2
    assert summary["strategy_current_market_breakdown"][0]["outcome"] == "Up"
    assert summary["strategy_recent_resolutions"][0]["slug"] == "btc-updown-5m-1773233400"
    assert summary["strategy_resolution_count_today"] == 1
    assert summary["strategy_resolution_pnl_today"] == 6.0


def test_summary_payload_prefers_public_polymarket_price_to_beat_over_zero_bot_state(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-1774260600")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Current")
    db.set_bot_state("strategy_official_price_to_beat", "0.000000")
    db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(68606.91914280105, "public-gamma")):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    assert summary["strategy_official_price_to_beat"] == 68606.9191
    assert summary["strategy_official_price_source"] == "public-gamma"
    assert summary["strategy_official_price_available"] is True


def test_summary_payload_exposes_public_web_price_to_beat_source(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-1774260600")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Current")
    db.set_bot_state("strategy_official_price_to_beat", "0.000000")
    db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(66010.0, "public-web")):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    assert summary["strategy_official_price_to_beat"] == 66010.0
    assert summary["strategy_official_price_source"] == "public-web"
    assert summary["strategy_effective_price_to_beat"] == 66010.0
    assert summary["strategy_effective_price_source"] == "public-web"


def test_summary_payload_runtime_compare_prefers_public_polymarket_price_to_beat(tmp_path: Path) -> None:
    paper_db_path = tmp_path / "bot.db"
    shadow_db_path = tmp_path / "bot_shadow.db"

    paper_db = Database(paper_db_path)
    paper_db.init_schema()
    paper_db.set_bot_state("strategy_runtime_mode", "paper")
    paper_db.set_bot_state("strategy_market_slug", "btc-updown-5m-1774271700")
    paper_db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Shared")
    paper_db.set_bot_state("strategy_official_price_to_beat", "0.000000")
    paper_db.close()

    shadow_db = Database(shadow_db_path)
    shadow_db.init_schema()
    shadow_db.set_bot_state("strategy_runtime_mode", "shadow")
    shadow_db.set_bot_state("strategy_market_slug", "btc-updown-5m-1774271700")
    shadow_db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Shared")
    shadow_db.set_bot_state("strategy_official_price_to_beat", "0.000000")
    shadow_db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(70888.12, "public-gamma")):
        summary = _summary_payload(
            shadow_db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    compare = summary["strategy_runtime_window_compare"]
    assert compare["paper"]["official_price_to_beat"] == 70888.12
    assert compare["paper"]["official_price_source"] == "public-gamma"
    assert compare["paper"]["official_price_available"] is True
    assert compare["shadow"]["official_price_to_beat"] == 70888.12
    assert compare["shadow"]["official_price_source"] == "public-gamma"
    assert compare["shadow"]["official_price_available"] is True


def test_summary_payload_ignores_bot_state_official_when_slug_mismatch_and_public_gamma_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-current")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Current")
    db.set_bot_state("strategy_official_price_to_beat", "70222.11")
    db.set_bot_state("strategy_official_price_slug", "btc-updown-5m-old")
    db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(0.0, "public-gamma-missing")):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    assert summary["strategy_official_price_to_beat"] == 0.0
    assert summary["strategy_official_price_source"] == "public-gamma-missing"
    assert summary["strategy_official_price_available"] is False


def test_summary_payload_exposes_captured_chainlink_as_effective_beat_when_public_gamma_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-current")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Current")
    db.set_bot_state("strategy_official_price_to_beat", "0.000000")
    db.set_bot_state("strategy_captured_price_to_beat", "70123.45")
    db.set_bot_state("strategy_captured_price_slug", "btc-updown-5m-current")
    db.set_bot_state("strategy_captured_price_source", "captured-chainlink")
    db.set_bot_state("strategy_effective_price_to_beat", "70123.45")
    db.set_bot_state("strategy_effective_price_slug", "btc-updown-5m-current")
    db.set_bot_state("strategy_effective_price_source", "captured-chainlink")
    db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(0.0, "public-gamma-missing")):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    assert summary["strategy_official_price_to_beat"] == 0.0
    assert summary["strategy_official_price_source"] == "public-gamma-missing"
    assert summary["strategy_captured_price_to_beat"] == 70123.45
    assert summary["strategy_captured_price_source"] == "captured-chainlink"
    assert summary["strategy_effective_price_to_beat"] == 70123.45
    assert summary["strategy_effective_price_source"] == "captured-chainlink"
    assert summary["strategy_effective_price_available"] is True


def test_summary_payload_current_window_exposure_ignores_stale_bot_state_without_positions(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()

    db.set_bot_state("strategy_mode", "btc5m_orderbook")
    db.set_bot_state("strategy_entry_mode", "vidarx_micro")
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-stale")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Stale")
    db.set_bot_state("strategy_current_market_exposure", "47.33")
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    assert summary["strategy_current_market_exposure"] == 47.33
    assert summary["strategy_current_market_total_exposure"] == 0.0
    assert summary["strategy_current_market_live_pnl"] == 0.0
    assert summary["strategy_current_market_breakdown"] == []


def test_summary_payload_metric_sources_match_computed_balance_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    sources = summary["dashboard_metric_sources"]
    assert sources["live_cash_balance"] == "bot_state.live_cash_balance"
    assert "min(bot_state.live_cash_balance, bot_state.live_cash_allowance)" in sources["live_available_to_trade"]
    assert "live_cash_balance + exposure_mark" in sources["live_equity_estimate"]
    assert "Gamma publica de Polymarket" in sources["strategy_official_price_to_beat"]
    assert "captura propia Chainlink RTDS" in sources["strategy_captured_price_to_beat"]
    assert "captura propia Chainlink RTDS" in sources["strategy_effective_price_to_beat"]
    assert "expected_edge_bps + maker/taker EV" in sources["strategy_user_intel"]


def test_summary_payload_exposes_setup_performance(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.upsert_strategy_window(
        slug="btc-updown-5m-1",
        condition_id="cond-1",
        title="Bitcoin Up or Down - 1",
        price_mode="extreme",
        timing_regime="second-wave",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.8,
        planned_budget=120.0,
        current_exposure=120.0,
        notes="setup 1",
    )
    db.record_strategy_window_fills(
        slug="btc-updown-5m-1",
        fill_count=6,
        added_notional=118.0,
        replenishment_count=2,
        notes="fills 1",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-1",
        realized_pnl=32.5,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="resolved 1",
    )
    db.upsert_strategy_window(
        slug="btc-updown-5m-2",
        condition_id="cond-2",
        title="Bitcoin Up or Down - 2",
        price_mode="balanced",
        timing_regime="early-mid",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.55,
        planned_budget=80.0,
        current_exposure=80.0,
        notes="setup 2",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-2",
        realized_pnl=-12.0,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="resolved 2",
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    assert len(summary["strategy_setup_performance"]) == 2
    assert summary["strategy_setup_performance"][0]["price_mode"] == "extreme"
    assert summary["strategy_setup_performance"][0]["timing_regime"] == "second-wave"
    assert summary["strategy_setup_performance"][0]["pnl_total"] == 32.5


def test_summary_payload_exposes_paper_vs_shadow_window_compare(tmp_path: Path) -> None:
    paper_db_path = tmp_path / "bot.db"
    shadow_db_path = tmp_path / "bot_shadow.db"

    paper_db = Database(paper_db_path)
    paper_db.init_schema()
    paper_db.set_bot_state("strategy_runtime_mode", "paper")
    paper_db.set_bot_state("strategy_market_slug", "btc-updown-5m-shared")
    paper_db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Shared")
    paper_db.set_bot_state("strategy_price_mode", "underround")
    paper_db.set_bot_state("strategy_operability_state", "ready")
    paper_db.set_bot_state("strategy_last_note", "paper comparativa")
    paper_db.set_bot_state("strategy_cycle_budget", "342.68")
    paper_db.set_bot_state("strategy_effective_min_notional", "5.00")
    paper_db.set_bot_state("strategy_spot_price", "68837.73")
    paper_db.set_bot_state("strategy_official_price_to_beat", "68856.67")
    paper_db.set_bot_state("strategy_spot_fair_up", "0.215")
    paper_db.set_bot_state("strategy_spot_fair_down", "0.785")
    paper_db.set_bot_state("strategy_reference_quality", "official")
    paper_db.set_bot_state("strategy_desired_up_ratio", "0.42")
    paper_db.set_bot_state("strategy_current_up_ratio", "0.39")
    paper_db.upsert_copy_position(
        asset="paper-up",
        condition_id="cond-shared",
        size=20.0,
        avg_price=0.41,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Shared",
        slug="btc-updown-5m-shared",
        outcome="Up",
        category="crypto",
    )
    paper_db.upsert_copy_position(
        asset="paper-down",
        condition_id="cond-shared",
        size=25.0,
        avg_price=0.54,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Shared",
        slug="btc-updown-5m-shared",
        outcome="Down",
        category="crypto",
    )
    paper_db.close()

    shadow_db = Database(shadow_db_path)
    shadow_db.init_schema()
    shadow_db.set_bot_state("strategy_runtime_mode", "shadow")
    shadow_db.set_bot_state("strategy_market_slug", "btc-updown-5m-shared")
    shadow_db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Shared")
    shadow_db.set_bot_state("strategy_price_mode", "underround")
    shadow_db.set_bot_state("strategy_operability_state", "ready")
    shadow_db.set_bot_state("strategy_last_note", "shadow comparativa")
    shadow_db.set_bot_state("strategy_cycle_budget", "25.00")
    shadow_db.set_bot_state("strategy_effective_min_notional", "3.45")
    shadow_db.set_bot_state("strategy_spot_price", "68837.73")
    shadow_db.set_bot_state("strategy_official_price_to_beat", "68856.67")
    shadow_db.set_bot_state("strategy_spot_fair_up", "0.215")
    shadow_db.set_bot_state("strategy_spot_fair_down", "0.785")
    shadow_db.set_bot_state("strategy_reference_quality", "soft-stale-rtds")
    shadow_db.set_bot_state("strategy_desired_up_ratio", "0.42")
    shadow_db.set_bot_state("strategy_current_up_ratio", "0.43")
    shadow_db.upsert_copy_position(
        asset="shadow-up",
        condition_id="cond-shared",
        size=7.0,
        avg_price=0.41,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Shared",
        slug="btc-updown-5m-shared",
        outcome="Up",
        category="crypto",
    )
    shadow_db.upsert_copy_position(
        asset="shadow-down",
        condition_id="cond-shared",
        size=8.0,
        avg_price=0.54,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Shared",
        slug="btc-updown-5m-shared",
        outcome="Down",
        category="crypto",
    )
    shadow_db.record_execution(
        result=ExecutionResult(
            mode="shadow",
            status="filled",
            action=SignalAction.OPEN,
            asset="shadow-down",
            size=6.0,
            price=0.25,
            notional=1.5,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-shared",
        source_wallet="strategy:shadow",
        source_signal_id=0,
        notes="shadow-open-1",
    )
    shadow_db.record_execution(
        result=ExecutionResult(
            mode="shadow",
            status="filled",
            action=SignalAction.OPEN,
            asset="shadow-up",
            size=5.0,
            price=0.33,
            notional=1.65,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-shared",
        source_wallet="strategy:shadow",
        source_signal_id=0,
        notes="shadow-open-2",
    )
    shadow_db.record_execution(
        result=ExecutionResult(
            mode="shadow",
            status="filled",
            action=SignalAction.OPEN,
            asset="shadow-other",
            size=9.0,
            price=0.44,
            notional=3.96,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-other",
        source_wallet="strategy:shadow",
        source_signal_id=0,
        notes="shadow-open-other-window",
    )
    shadow_db.upsert_strategy_window(
        slug="btc-updown-5m-history-1",
        condition_id="cond-history-1",
        title="Bitcoin Up or Down - History 1",
        price_mode="underround",
        timing_regime="mid-late",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.52,
        planned_budget=26.0,
        current_exposure=0.0,
        notes="shadow history 1",
    )
    shadow_db.record_strategy_window_fills(
        slug="btc-updown-5m-history-1",
        fill_count=2,
        added_notional=3.15,
        replenishment_count=1,
        notes="shadow history fills 1",
    )
    shadow_db.close_strategy_window(
        slug="btc-updown-5m-history-1",
        realized_pnl=1.10,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="shadow history close 1",
    )
    shadow_db.upsert_strategy_window(
        slug="btc-updown-5m-shadow-only",
        condition_id="cond-shadow-only",
        title="Bitcoin Up or Down - Shadow Only",
        price_mode="underround",
        timing_regime="mid-late",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.48,
        planned_budget=18.0,
        current_exposure=0.0,
        notes="shadow history only",
    )
    shadow_db.record_strategy_window_fills(
        slug="btc-updown-5m-shadow-only",
        fill_count=1,
        added_notional=1.65,
        replenishment_count=0,
        notes="shadow history fills only",
    )
    shadow_db.close_strategy_window(
        slug="btc-updown-5m-shadow-only",
        realized_pnl=-0.25,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="shadow history close only",
    )
    shadow_db.close()

    paper_db = Database(paper_db_path)
    paper_db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.OPEN,
            asset="paper-up",
            size=10.0,
            price=0.41,
            notional=4.1,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-shared",
        source_wallet="strategy:paper",
        source_signal_id=0,
        notes="paper-open-1",
    )
    paper_db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.OPEN,
            asset="paper-other",
            size=4.0,
            price=0.62,
            notional=2.48,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-other",
        source_wallet="strategy:paper",
        source_signal_id=0,
        notes="paper-open-other-window",
    )
    paper_db.upsert_strategy_window(
        slug="btc-updown-5m-history-1",
        condition_id="cond-history-1",
        title="Bitcoin Up or Down - History 1",
        price_mode="underround",
        timing_regime="mid-late",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.52,
        planned_budget=84.0,
        current_exposure=0.0,
        notes="paper history 1",
    )
    paper_db.record_strategy_window_fills(
        slug="btc-updown-5m-history-1",
        fill_count=6,
        added_notional=14.25,
        replenishment_count=2,
        notes="paper history fills 1",
    )
    paper_db.close_strategy_window(
        slug="btc-updown-5m-history-1",
        realized_pnl=4.20,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="paper history close 1",
    )
    paper_db.upsert_strategy_window(
        slug="btc-updown-5m-paper-only",
        condition_id="cond-paper-only",
        title="Bitcoin Up or Down - Paper Only",
        price_mode="underround",
        timing_regime="mid-late",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.49,
        planned_budget=72.0,
        current_exposure=0.0,
        notes="paper history only",
    )
    paper_db.record_strategy_window_fills(
        slug="btc-updown-5m-paper-only",
        fill_count=4,
        added_notional=9.6,
        replenishment_count=1,
        notes="paper history fills only",
    )
    paper_db.close_strategy_window(
        slug="btc-updown-5m-paper-only",
        realized_pnl=2.50,
        winning_outcome="Down",
        current_exposure=0.0,
        notes="paper history close only",
    )
    paper_db.close()

    summary = _summary_payload(
        shadow_db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    compare = summary["strategy_runtime_window_compare"]
    assert compare["available"] is True
    assert compare["same_window"] is True
    assert compare["status"] == "shared"
    assert summary["dashboard_build"].startswith("2026-")
    assert "pnl_total" in summary["dashboard_metric_sources"]
    assert "compare_samples" in summary["dashboard_metric_sources"]
    assert compare["paper"]["runtime_mode"] == "paper"
    assert compare["paper"]["cycle_budget"] == 342.68
    assert compare["paper"]["effective_min_notional"] == 5.0
    assert compare["paper"]["open_legs"] == 2
    assert compare["paper"]["open_execution_count"] == 1
    assert compare["paper"]["closed_window_count"] == 2
    assert compare["paper"]["total_realized_pnl"] == 6.7
    assert compare["shadow"]["runtime_mode"] == "shadow"
    assert compare["shadow"]["cycle_budget"] == 25.0
    assert compare["shadow"]["remaining_cycle_budget"] == 17.81
    assert compare["shadow"]["effective_min_notional"] == 3.45
    assert compare["shadow"]["spot_price"] == 68837.73
    assert compare["shadow"]["official_price_to_beat"] == 68856.67
    assert compare["shadow"]["open_legs"] == 2
    assert compare["shadow"]["open_execution_count"] == 2
    assert compare["shadow"]["open_avg_notional"] == 1.575
    assert compare["shadow"]["closed_window_count"] == 2
    assert compare["shadow"]["total_realized_pnl"] == 0.85
    assert compare["shadow"]["recent_executions"][0]["notes"].startswith("shadow-open")
    history = compare["history"]
    assert history["available"] is True
    assert history["sample_available"] is True
    assert history["sample_summary"]["paper_latest_notional"] == 4.1
    assert history["sample_summary"]["shadow_latest_notional"] == 3.15
    assert len(history["sample_series"]["paper"]) == 1
    assert len(history["sample_series"]["shadow"]) == 1
    assert history["summary"]["paper_window_count"] == 2
    assert history["summary"]["shadow_window_count"] == 2
    assert history["summary"]["shared_window_count"] == 1
    assert history["summary"]["point_count"] == 3
    assert history["summary"]["paper_comparable_realized_pnl"] == 4.2
    assert history["summary"]["shadow_comparable_realized_pnl"] == 1.1
    assert history["summary"]["comparable_pnl_gap"] == 3.1
    assert history["summary"]["paper_comparable_filled_orders"] == 6
    assert history["summary"]["shadow_comparable_filled_orders"] == 2
    assert history["summary"]["comparable_filled_orders_gap"] == 4
    assert history["summary"]["paper_total_realized_pnl"] == 6.7
    assert history["summary"]["shadow_total_realized_pnl"] == 0.85
    assert history["summary"]["total_pnl_gap"] == 5.85
    assert history["summary"]["cumulative_pnl_gap"] == 5.85
    assert history["summary"]["paper_total_filled_orders"] == 10
    assert history["summary"]["shadow_total_filled_orders"] == 3
    assert history["sample_summary"]["shadow_dominant_operability_state"] == "ready"
    assert history["sample_summary"]["shadow_dominant_operability_pct"] == 100.0
    assert any(
        item["slug"] == "btc-updown-5m-paper-only" and item["shadow_status"] == "missing"
        for item in history["points"]
    )
    assert any(
        item["slug"] == "btc-updown-5m-shadow-only" and item["paper_status"] == "missing"
        for item in history["points"]
    )
    assert history["series"]["paper"][-1]["cumulative_realized_pnl"] == 6.7
    assert history["series"]["shadow"][-1]["cumulative_realized_pnl"] == 0.85
    assert Path(compare["db_path"]).exists()
    assert summary["strategy_runtime_compare_db_path"].endswith("runtime_compare.db")
    assert summary["strategy_cycle_budget_remaining"] == 17.81
    assert summary["strategy_effective_min_notional"] == 3.45


def test_summary_payload_exposes_compare_snapshot_series_without_closed_history(tmp_path: Path) -> None:
    paper_db_path = tmp_path / "bot.db"
    shadow_db_path = tmp_path / "bot_shadow.db"

    paper_db = Database(paper_db_path)
    paper_db.init_schema()
    paper_db.set_bot_state("strategy_runtime_mode", "paper")
    paper_db.set_bot_state("strategy_market_slug", "btc-updown-5m-live")
    paper_db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Live")
    paper_db.set_bot_state("strategy_price_mode", "underround")
    paper_db.set_bot_state("strategy_operability_state", "waiting_edge")
    paper_db.set_bot_state("strategy_last_note", "paper open")
    paper_db.set_bot_state("strategy_cycle_budget", "68.00")
    paper_db.set_bot_state("strategy_effective_min_notional", "1.00")
    paper_db.set_bot_state("strategy_spot_price", "68177.62")
    paper_db.set_bot_state("strategy_official_price_to_beat", "68180.00")
    paper_db.set_bot_state("strategy_spot_fair_up", "0.577")
    paper_db.set_bot_state("strategy_spot_fair_down", "0.423")
    paper_db.set_bot_state("strategy_reference_quality", "soft-stale-rtds")
    paper_db.set_bot_state("strategy_desired_up_ratio", "0.51")
    paper_db.set_bot_state("strategy_current_up_ratio", "0.51")
    paper_db.upsert_copy_position(
        asset="paper-live-up",
        condition_id="cond-live",
        size=10.0,
        avg_price=0.41,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Live",
        slug="btc-updown-5m-live",
        outcome="Up",
        category="crypto",
    )
    paper_db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.OPEN,
            asset="paper-live-up",
            size=10.0,
            price=0.41,
            notional=4.1,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-live",
        source_wallet="strategy:paper",
        source_signal_id=0,
        notes="paper-live-open",
    )
    paper_db.close()

    shadow_db = Database(shadow_db_path)
    shadow_db.init_schema()
    shadow_db.set_bot_state("strategy_runtime_mode", "shadow")
    shadow_db.set_bot_state("strategy_market_slug", "btc-updown-5m-live")
    shadow_db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Live")
    shadow_db.set_bot_state("strategy_price_mode", "underround")
    shadow_db.set_bot_state("strategy_operability_state", "waiting_book")
    shadow_db.set_bot_state("strategy_last_note", "shadow open")
    shadow_db.set_bot_state("strategy_cycle_budget", "31.55")
    shadow_db.set_bot_state("strategy_effective_min_notional", "1.00")
    shadow_db.set_bot_state("strategy_spot_price", "68237.25")
    shadow_db.set_bot_state("strategy_official_price_to_beat", "68230.00")
    shadow_db.set_bot_state("strategy_spot_fair_up", "0.974")
    shadow_db.set_bot_state("strategy_spot_fair_down", "0.026")
    shadow_db.set_bot_state("strategy_reference_quality", "rtds-derived")
    shadow_db.set_bot_state("strategy_desired_up_ratio", "0.70")
    shadow_db.set_bot_state("strategy_current_up_ratio", "0.0")
    shadow_db.upsert_copy_position(
        asset="shadow-live-down",
        condition_id="cond-live",
        size=5.0,
        avg_price=0.29,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Live",
        slug="btc-updown-5m-live",
        outcome="Down",
        category="crypto",
    )
    shadow_db.record_execution(
        result=ExecutionResult(
            mode="shadow",
            status="filled",
            action=SignalAction.OPEN,
            asset="shadow-live-down",
            size=5.0,
            price=0.29,
            notional=1.45,
        ),
        side=TradeSide.BUY.value,
        condition_id="cond-live",
        source_wallet="strategy:shadow",
        source_signal_id=0,
        notes="shadow-live-open",
    )
    shadow_db.close()

    summary = _summary_payload(
        shadow_db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    compare = summary["strategy_runtime_window_compare"]
    history = compare["history"]
    assert compare["same_window"] is True
    assert history["available"] is False
    assert history["sample_available"] is True
    assert history["sample_summary"]["paper_latest_notional"] == 4.1
    assert history["sample_summary"]["shadow_latest_notional"] == 1.45
    assert history["sample_summary"]["shadow_dominant_operability_state"] == "waiting_book"
    assert history["sample_summary"]["shadow_dominant_operability_pct"] == 100.0
    assert len(history["sample_series"]["paper"]) == 1
    assert len(history["sample_series"]["shadow"]) == 1
    assert compare["paper"]["closed_window_count"] == 0
    assert compare["shadow"]["closed_window_count"] == 0
    assert compare["paper"]["total_realized_pnl"] == 0.0
    assert compare["shadow"]["total_realized_pnl"] == 0.0


def test_summary_payload_runtime_compare_exposes_lifecycle_metrics(tmp_path: Path) -> None:
    slug = "btc-updown-5m-shared"
    title = "Bitcoin Up or Down - Shared"
    base_ts = 1774260600
    paper_db_path = tmp_path / "bot.db"
    shadow_db_path = tmp_path / "bot_shadow.db"

    paper_db = Database(paper_db_path)
    paper_db.init_schema()
    with paper_db.conn:
        paper_db.conn.execute(
            """
            INSERT INTO strategy_windows(
                slug, condition_id, title, status, opened_at, first_trade_at, last_trade_at, closed_at,
                price_mode, timing_regime, primary_outcome, hedge_outcome, primary_ratio,
                planned_budget, deployed_notional, current_exposure, filled_orders, replenishment_count,
                realized_pnl, winning_outcome, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slug,
                "cond-shared",
                title,
                "closed",
                base_ts,
                base_ts + 1,
                base_ts + 3,
                base_ts + 295,
                "underround",
                "mid-window",
                "Up",
                "Down",
                0.5,
                12.0,
                8.0,
                0.0,
                2,
                0,
                1.25,
                "Up",
                "resolved up",
            ),
        )
        paper_db.conn.executemany(
            """
            INSERT INTO executions(
                ts, mode, status, action, side, asset, condition_id, size, price, notional,
                source_wallet, source_signal_id, strategy_variant, notes, pnl_delta
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    base_ts + 1,
                    "paper",
                    "filled",
                    "open",
                    "buy",
                    "paper-up",
                    "cond-shared",
                    10.0,
                    0.41,
                    4.1,
                    "strategy:paper",
                    0,
                    "",
                    "paper-open-up",
                    0.0,
                ),
                (
                    base_ts + 3,
                    "paper",
                    "filled",
                    "open",
                    "buy",
                    "paper-down",
                    "cond-shared",
                    10.0,
                    0.39,
                    3.9,
                    "strategy:paper",
                    0,
                    "",
                    "paper-open-down",
                    0.0,
                ),
                (
                    base_ts + 295,
                    "paper",
                    "filled",
                    "close",
                    "sell",
                    "paper-up",
                    "cond-shared",
                    10.0,
                    1.0,
                    10.0,
                    "strategy:settlement",
                    0,
                    "",
                    f"strategy_resolution:{slug}:Up",
                    1.25,
                ),
            ],
        )
    paper_db.close()

    shadow_db = Database(shadow_db_path)
    shadow_db.init_schema()
    shadow_db.set_bot_state("strategy_runtime_mode", "shadow")
    shadow_db.set_bot_state("strategy_market_slug", slug)
    shadow_db.set_bot_state("strategy_market_title", title)
    with shadow_db.conn:
        shadow_db.conn.execute(
            """
            INSERT INTO strategy_windows(
                slug, condition_id, title, status, opened_at, first_trade_at, last_trade_at, closed_at,
                price_mode, timing_regime, primary_outcome, hedge_outcome, primary_ratio,
                planned_budget, deployed_notional, current_exposure, filled_orders, replenishment_count,
                realized_pnl, winning_outcome, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                slug,
                "cond-shared",
                title,
                "closed",
                base_ts,
                base_ts + 2,
                base_ts + 2,
                base_ts + 295,
                "underround",
                "mid-window",
                "Up",
                "Down",
                0.5,
                12.0,
                1.8,
                0.0,
                1,
                0,
                0.0,
                "",
                "shadow one sided",
            ),
        )
        shadow_db.conn.execute(
            """
            INSERT INTO executions(
                ts, mode, status, action, side, asset, condition_id, size, price, notional,
                source_wallet, source_signal_id, strategy_variant, notes, pnl_delta
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                base_ts + 2,
                "shadow",
                "filled",
                "open",
                "buy",
                "shadow-down",
                "cond-shared",
                5.0,
                0.36,
                1.8,
                "strategy:shadow",
                0,
                "",
                "shadow-open-down",
                0.0,
            ),
        )
    shadow_db.close()

    with patch("app.services.dashboard_server._public_market_official_price_to_beat", return_value=(0.0, "public-gamma-missing")):
        summary = _summary_payload(
            shadow_db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    compare = summary["strategy_runtime_window_compare"]
    history = compare["history"]
    metrics = history["summary"]

    assert compare["same_window"] is True
    assert history["available"] is True
    assert metrics["paper_active_window_count"] == 1
    assert metrics["shadow_active_window_count"] == 1
    assert metrics["paper_two_sided_window_count"] == 1
    assert metrics["shadow_two_sided_window_count"] == 0
    assert metrics["paper_one_sided_window_count"] == 0
    assert metrics["shadow_one_sided_window_count"] == 1
    assert metrics["paper_two_sided_window_pct"] == 100.0
    assert metrics["shadow_two_sided_window_pct"] == 0.0
    assert metrics["paper_settlement_window_pct"] == 100.0
    assert metrics["shadow_settlement_window_pct"] == 0.0
    assert metrics["paper_avg_open_cadence_seconds"] == 2.0
    assert metrics["shadow_avg_open_cadence_seconds"] == 0.0
    assert metrics["paper_avg_open_span_seconds"] == 2.0
    assert metrics["shadow_avg_open_span_seconds"] == 0.0
    assert history["points"][0]["paper_two_sided"] is True
    assert history["points"][0]["shadow_two_sided"] is False
    assert history["points"][0]["paper_settlement_visible"] is True
    assert history["points"][0]["shadow_settlement_visible"] is False


def test_summary_payload_exposes_live_readiness_gate_blocked(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.close()

    fake_compare = {
        "available": True,
        "history": {
            "available": True,
            "summary": {
                "shared_window_count": 24,
                "shadow_participation_pct": 87.5,
                "shadow_two_sided_window_pct": 76.0,
                "shadow_active_window_count": 21,
                "shadow_one_sided_window_count": 5,
                "shadow_settlement_window_pct": 90.0,
                "paper_avg_open_cadence_seconds": 2.2,
                "shadow_avg_open_cadence_seconds": 20.5,
            },
            "sample_summary": {
                "shadow_dominant_operability_state": "budget_limited",
                "shadow_dominant_operability_pct": 62.0,
            },
        },
    }
    fake_incubation = {
        "stage": "incubating",
        "stage_label": "Incubando",
        "min_days": 3,
        "min_resolutions": 100,
        "max_drawdown_limit": 40.0,
        "days_observed": 3.37,
        "resolutions": 815,
        "wins": 525,
        "losses": 290,
        "win_rate_pct": 64.42,
        "pnl_total": 5828.3206,
        "avg_pnl": 7.1513,
        "deployed_total": 125000.0,
        "avg_deployed": 153.37,
        "max_drawdown": -43.4089,
        "best_resolution": 70.653,
        "worst_resolution": -55.0,
        "progress_pct": 100.0,
        "ready_to_scale": False,
        "drawdown_breached": True,
        "recommendation": "review",
        "recommendation_label": "Pausar y revisar",
        "first_closed_at": 1774300000,
        "last_closed_at": 1774600000,
    }
    fake_transition = {
        "next_stage": "incubating",
        "transition_ready": False,
        "label": "Pausar y revisar",
        "reason": "drawdown en rojo",
        "auto_apply_ready": False,
    }

    with (
        patch("app.services.dashboard_server._runtime_compare_payload", return_value=fake_compare),
        patch("app.services.dashboard_server.build_incubation_summary", return_value=fake_incubation),
        patch("app.services.dashboard_server.evaluate_incubation_progress", return_value=fake_transition),
    ):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    readiness = summary["strategy_live_readiness"]
    assert readiness["status"] == "blocked"
    assert readiness["ready"] is False
    assert readiness["score"] < 90
    assert any("Drawdown max" in item for item in readiness["blockers"])
    assert any("Participacion shadow" in item for item in readiness["blockers"])
    assert any("Bloqueo dominante" in item for item in readiness["blockers"])
    assert "runtime_compare + incubacion" in summary["dashboard_metric_sources"]["strategy_live_readiness"]


def test_summary_payload_exposes_live_readiness_gate_ready(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.close()

    fake_compare = {
        "available": True,
        "history": {
            "available": True,
            "summary": {
                "shared_window_count": 120,
                "shadow_participation_pct": 94.0,
                "shadow_two_sided_window_pct": 98.0,
                "shadow_active_window_count": 110,
                "shadow_one_sided_window_count": 2,
                "shadow_settlement_window_pct": 95.0,
                "paper_avg_open_cadence_seconds": 3.0,
                "shadow_avg_open_cadence_seconds": 6.0,
            },
            "sample_summary": {
                "shadow_dominant_operability_state": "waiting_edge",
                "shadow_dominant_operability_pct": 18.0,
            },
        },
    }
    fake_incubation = {
        "stage": "ready",
        "stage_label": "Listo",
        "min_days": 3,
        "min_resolutions": 100,
        "max_drawdown_limit": 40.0,
        "days_observed": 6.0,
        "resolutions": 220,
        "wins": 150,
        "losses": 70,
        "win_rate_pct": 68.18,
        "pnl_total": 9200.0,
        "avg_pnl": 41.82,
        "deployed_total": 55000.0,
        "avg_deployed": 250.0,
        "max_drawdown": -18.25,
        "best_resolution": 88.2,
        "worst_resolution": -22.0,
        "progress_pct": 100.0,
        "ready_to_scale": True,
        "drawdown_breached": False,
        "recommendation": "scale",
        "recommendation_label": "Escalar",
        "first_closed_at": 1774300000,
        "last_closed_at": 1774700000,
    }
    fake_transition = {
        "next_stage": "live",
        "transition_ready": True,
        "label": "Escalar",
        "reason": "todo en verde",
        "auto_apply_ready": False,
    }

    with (
        patch("app.services.dashboard_server._runtime_compare_payload", return_value=fake_compare),
        patch("app.services.dashboard_server.build_incubation_summary", return_value=fake_incubation),
        patch("app.services.dashboard_server.evaluate_incubation_progress", return_value=fake_transition),
    ):
        summary = _summary_payload(
            db_path,
            clob_host="https://clob.polymarket.com",
            execution_mode="paper",
            live_trading_enabled=False,
        )

    readiness = summary["strategy_live_readiness"]
    assert readiness["status"] == "ready"
    assert readiness["ready"] is True
    assert readiness["label"] == "GO"
    assert readiness["blockers"] == []
    assert readiness["score"] >= 90
    assert readiness["metrics"]["cadence_ratio"] == 2.0


def test_summary_payload_exposes_user_intel_latency_and_break_even(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_expected_edge_bps", "12.5")
    db.set_bot_state("strategy_maker_ev_bps", "9.2")
    db.set_bot_state("strategy_taker_ev_bps", "5.4")
    db.set_bot_state("strategy_taker_fee_bps", "1.7")
    db.set_bot_state("strategy_selected_execution", "taker_fak")
    db.set_bot_state("strategy_market_event_lag_ms", "88.5")
    db.set_bot_state("strategy_spot_age_ms", "143")
    db.set_bot_state("strategy_feed_age_ms", "21")
    db.set_bot_state("strategy_last_updated_at", str(int(time.time()) - 1))
    db.set_bot_state("strategy_effective_price_source", "captured-chainlink")
    db.set_bot_state("strategy_reference_quality", "captured-chainlink")
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    intel = summary["strategy_user_intel"]
    assert summary["strategy_taker_fee_bps"] == 1.7
    assert intel["edge"]["gross_edge_bps"] == 12.5
    assert intel["edge"]["selected_execution"] == "taker_fak"
    assert intel["edge"]["selected_ev_bps"] == 5.4
    assert intel["edge"]["estimated_cost_bps"] == 7.1
    assert intel["edge"]["taker_fee_bps"] == 1.7
    assert intel["edge"]["break_even_gap_bps"] == 0.0
    assert intel["edge"]["edge_status"] == "neto positivo"
    assert intel["latency"]["market_event_lag_ms"] == 88.5
    assert intel["latency"]["spot_age_ms"] == 143
    assert intel["latency"]["feed_age_ms"] == 21
    assert intel["latency"]["decision_age_ms"] >= 1000
    assert intel["reference"]["effective_price_source"] == "captured-chainlink"
    assert intel["reference"]["reference_quality"] == "captured-chainlink"


def test_summary_payload_exposes_break_even_gap_when_selected_edge_is_negative(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_expected_edge_bps", "-149.2")
    db.set_bot_state("strategy_maker_ev_bps", "-10.7")
    db.set_bot_state("strategy_taker_ev_bps", "-203.0")
    db.set_bot_state("strategy_taker_fee_bps", "144.0")
    db.set_bot_state("strategy_selected_execution", "maker_post_only_gtc")
    db.set_bot_state("strategy_market_event_lag_ms", "125.0")
    db.set_bot_state("strategy_spot_age_ms", "80")
    db.set_bot_state("strategy_feed_age_ms", "15")
    db.set_bot_state("strategy_last_updated_at", str(int(time.time()) - 1))
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    intel = summary["strategy_user_intel"]
    assert intel["edge"]["selected_ev_bps"] == -10.7
    assert intel["edge"]["estimated_cost_bps"] == 0.0
    assert intel["edge"]["break_even_gap_bps"] == 10.7
    assert intel["edge"]["edge_status"] == "sin edge neto"


def test_summary_payload_exposes_live_control_state(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("live_control_state", "paused")
    db.set_bot_state("live_control_reason", "seguimos en rojo")
    db.set_bot_state("live_control_updated_at", "1710755400")
    db.set_bot_state("telegram_status_summary_enabled", "1")
    db.set_bot_state("telegram_status_summary_interval_minutes", "30")
    db.set_bot_state("telegram_status_summary_recent_limit", "5")
    db.set_bot_state("telegram_status_summary_last_sent_ts", "1710753600")
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="live",
        live_trading_enabled=True,
    )

    assert summary["live_mode_active"] is False
    assert summary["live_control_state"] == "paused"
    assert summary["live_control_label"] == "Live pausado"
    assert summary["live_control_reason"] == "seguimos en rojo"
    assert summary["live_control_can_execute"] is False
    assert summary["live_control_is_live_session"] is True
    assert summary["telegram_status_summary_enabled"] is True
    assert summary["telegram_status_summary_interval_minutes"] == 30
    assert summary["telegram_status_summary_last_sent_at"] == 1710753600


def test_summary_payload_uses_runtime_mode_for_live_control_session(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("live_control_state", "armed")
    db.set_bot_state("live_control_reason", "armado para live_small")
    db.set_bot_state("live_control_updated_at", "1710755400")
    db.set_bot_state("strategy_runtime_mode", "live")
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=True,
    )

    assert summary["strategy_runtime_mode"] == "live"
    assert summary["live_control_state"] == "armed"
    assert summary["live_control_label"] == "Live armado"
    assert summary["live_control_can_execute"] is True
    assert summary["live_control_is_live_session"] is True


def test_apply_live_control_action_updates_runtime_state(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.close()

    arm_result = _apply_live_control_action(db_path, action="arm")
    pause_result = _apply_live_control_action(db_path, action="pause")
    summary_result = _apply_live_control_action(db_path, action="summary_now")

    db = Database(db_path)
    assert arm_result["ok"] is True
    assert pause_result["ok"] is True
    assert summary_result["ok"] is True
    assert db.get_bot_state("live_control_state") == "paused"
    assert db.get_bot_state("telegram_status_summary_force_send") == "1"
    db.close()


def test_summary_payload_exposes_experiments_hypotheses_and_dataset(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_variant", "arb-micro-v1")
    db.set_bot_state("strategy_notes", "variant under test")
    db.set_bot_state("strategy_incubation_stage", "idea")
    db.set_bot_state("strategy_incubation_auto_promote", "1")
    db.set_bot_state("strategy_incubation_min_days", "0")
    db.set_bot_state("strategy_incubation_min_resolutions", "1")
    db.set_bot_state("strategy_incubation_max_drawdown", "25")
    db.set_bot_state("strategy_incubation_min_backtest_pnl", "0")
    db.set_bot_state("strategy_incubation_min_backtest_fill_rate", "0.2")
    db.set_bot_state("strategy_incubation_min_backtest_hit_rate", "0.2")
    db.set_bot_state("strategy_incubation_min_backtest_edge_bps", "0")
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
                        "windows": 5,
                        "net_realized_pnl_usdc": 12.5,
                        "max_drawdown_usdc": 3.0,
                        "fill_rate": 0.8,
                        "hit_rate": 0.6,
                        "real_edge_bps": 14.2,
                        "expectancy_window_usdc": 2.5,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (research_root / "hypotheses" / "top_wallet_patterns.json").write_text(
        json.dumps(
            {
                "patterns": [{"label": "Categoria dominante", "value": "crypto 90%"}],
                "hypotheses": [{"title": "Priorizar variantes crypto-first", "detail": "Hay sesgo crypto claro."}],
            }
        ),
        encoding="utf-8",
    )
    (research_root / "datasets" / "btc5m" / "dataset_summary.json").write_text(
        json.dumps({"generated_at": "2026-03-18T09:00:00Z", "windows": 3, "events": 120, "trades": 8}),
        encoding="utf-8",
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    assert summary["strategy_variant_backtest_status"] == "pass"
    assert summary["strategy_variant_backtest_gate_passed"] is True
    assert summary["strategy_variant_backtest_real_edge_bps"] == 14.2
    assert summary["strategy_incubation_transition_ready"] is True
    assert summary["strategy_incubation_next_stage"] == "backtest_pass"
    assert summary["strategy_incubation_auto_apply_ready"] is True
    assert summary["strategy_wallet_hypotheses"][0]["title"] == "Priorizar variantes crypto-first"
    assert summary["strategy_dataset_windows"] == 3


def test_summary_payload_recent_windows_use_deployed_notional(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.upsert_strategy_window(
        slug="btc-updown-5m-100",
        condition_id="cond-100",
        title="Bitcoin Up or Down - 100",
        price_mode="underround",
        timing_regime="early-mid",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.52,
        planned_budget=3.5,
        current_exposure=0.0,
        notes="window open",
    )
    db.record_strategy_window_fills(
        slug="btc-updown-5m-100",
        fill_count=4,
        added_notional=28.75,
        replenishment_count=1,
        notes="window fills",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-100",
        realized_pnl=7.25,
        winning_outcome="Down",
        current_exposure=0.0,
        notes="window closed",
    )
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    assert summary["strategy_recent_resolutions"][0]["slug"] == "btc-updown-5m-100"
    assert summary["strategy_recent_resolutions"][0]["notional"] == 28.75
    assert summary["strategy_recent_resolutions"][0]["deployed_notional"] == 28.75
    assert summary["strategy_recent_resolutions"][0]["planned_budget"] == 3.5
    assert summary["strategy_resolution_pnl_curve"]["window_count"] == 1
    assert summary["strategy_resolution_pnl_curve"]["baseline_pnl"] == 0.0
    assert summary["strategy_resolution_pnl_curve"]["total_realized_pnl"] == 7.25
    assert summary["strategy_resolution_pnl_curve"]["items"][0]["slug"] == "btc-updown-5m-100"
    assert summary["strategy_resolution_pnl_curve"]["items"][0]["cumulative_pnl"] == 7.25


def test_summary_payload_filters_strategy_variant_and_exposes_incubation(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()

    db.set_bot_state("strategy_variant", "arb-micro-v1")
    db.set_bot_state("strategy_notes", "baseline")
    db.set_bot_state("strategy_incubation_stage", "paper")
    db.set_bot_state("strategy_incubation_min_days", "14")
    db.set_bot_state("strategy_incubation_min_resolutions", "1")
    db.set_bot_state("strategy_incubation_max_drawdown", "20")
    db.upsert_strategy_window(
        slug="btc-updown-5m-v1",
        condition_id="cond-v1",
        title="Bitcoin Up or Down - V1",
        price_mode="underround",
        timing_regime="mid-late",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.62,
        planned_budget=12.0,
        current_exposure=12.0,
        notes="variant v1",
    )
    db.record_strategy_window_fills(
        slug="btc-updown-5m-v1",
        fill_count=3,
        added_notional=11.25,
        replenishment_count=1,
        notes="fills v1",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-v1",
        realized_pnl=4.5,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="resolved v1",
    )

    db.set_bot_state("strategy_variant", "arb-micro-v2")
    db.upsert_strategy_window(
        slug="btc-updown-5m-v2",
        condition_id="cond-v2",
        title="Bitcoin Up or Down - V2",
        price_mode="balanced",
        timing_regime="early-mid",
        primary_outcome="Down",
        hedge_outcome="Up",
        primary_ratio=0.55,
        planned_budget=9.0,
        current_exposure=9.0,
        notes="variant v2",
    )
    db.close_strategy_window(
        slug="btc-updown-5m-v2",
        realized_pnl=-3.0,
        winning_outcome="Up",
        current_exposure=0.0,
        notes="resolved v2",
    )

    db.set_bot_state("strategy_variant", "arb-micro-v1")
    db.close()

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    assert summary["strategy_variant"] == "arb-micro-v1"
    assert summary["strategy_notes"] == "baseline"
    assert summary["strategy_incubation_stage"] == "paper"
    assert summary["strategy_incubation_resolutions"] == 1
    assert summary["strategy_incubation_pnl_total"] == 4.5
    assert summary["strategy_incubation_ready_to_scale"] is False
    assert summary["strategy_recent_resolutions"][0]["slug"] == "btc-updown-5m-v1"
    assert len(summary["strategy_setup_performance"]) == 1
    assert summary["strategy_setup_performance"][0]["price_mode"] == "underround"


def test_restart_runtime_state_preserves_history_and_balance_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_shadow.db"
    db = Database(db_path)
    db.init_schema()
    now_ts = int(time.time())
    db.set_bot_state("strategy_runtime_mode", "shadow")
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-stale")
    db.set_bot_state("strategy_official_price_to_beat", "68123.45")
    db.set_bot_state("strategy_last_note", "nota vieja")
    db.set_bot_state("live_cash_balance", "150.12")
    db.set_bot_state("live_cash_allowance", "150.12")
    db.set_bot_state("live_total_capital", "203.45")
    db.set_bot_state("live_balance_updated_at", str(now_ts - 25))
    db.set_bot_state("strategy_capital_target", "111.26")
    db.set_bot_state("strategy_operating_bankroll", "132.75")
    db.set_bot_state("strategy_reserved_profit", "70.70")
    db.set_bot_state("strategy_total_exposure", "53.33")
    db.set_bot_state("live_marked_exposure", "52.10")
    db.set_bot_state("live_unrealized_pnl", "1.23")
    db.set_bot_state("runtime_guard_state", "ready")
    db.set_bot_state("position_ledger_mode", "shadow")
    db.set_bot_state("live_control_state", "paused")
    db.set_bot_state("live_control_reason", "pausado para test")
    db.set_bot_state("live_control_updated_at", str(now_ts - 50))
    db.upsert_copy_position(
        asset="asset-1",
        condition_id="cond-1",
        size=10.0,
        avg_price=0.5,
        realized_pnl=0.0,
        title="Market",
        slug="btc-updown-5m-1",
        outcome="Up",
        category="crypto",
    )
    with db.conn:
        db.conn.execute(
            """
            INSERT INTO executions(
                ts, mode, status, action, side, asset, condition_id, size, price, notional, pnl_delta
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now_ts, "shadow", "filled", "close", "sell", "asset-1", "cond-1", 10.0, 1.0, 10.0, 4.5),
        )
        db.conn.execute("INSERT INTO daily_pnl(day, pnl) VALUES (?, ?)", ("2026-03-30", 4.5))
        db.conn.execute(
            """
            INSERT INTO strategy_windows(
                slug, condition_id, title, status, opened_at, price_mode, realized_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("btc-updown-5m-test", "cond-1", "Bitcoin Up or Down - Test", "closed", now_ts, "underround", 4.25),
        )
    db.close()

    result = _restart_runtime_state(db_path)

    assert result["ok"] is True
    db = Database(db_path)
    assert db.get_bot_state("strategy_market_slug") == ""
    assert db.get_bot_state("strategy_official_price_to_beat") == "0"
    assert db.get_bot_state("strategy_runtime_mode") == "shadow"
    assert db.get_bot_state("strategy_last_note").startswith("runtime shadow reiniciado")
    assert db.get_bot_state("strategy_operability_label") == "Reiniciando"
    assert db.get_bot_state("live_cash_balance") == "150.12000000"
    assert db.get_bot_state("live_total_capital") == "203.45000000"
    assert db.get_bot_state("strategy_operating_bankroll") == "132.75000000"
    assert db.get_bot_state("strategy_reserved_profit") == "70.70000000"
    assert db.get_bot_state("runtime_guard_state") == "ready"
    assert db.get_bot_state("position_ledger_mode") == "shadow"
    assert db.get_bot_state("live_control_state") == "paused"
    execution_count = db.conn.execute("SELECT COUNT(*) AS value FROM executions").fetchone()["value"]
    strategy_window_count = db.conn.execute("SELECT COUNT(*) AS value FROM strategy_windows").fetchone()["value"]
    daily_pnl_count = db.conn.execute("SELECT COUNT(*) AS value FROM daily_pnl").fetchone()["value"]
    copy_position_count = db.conn.execute("SELECT COUNT(*) AS value FROM copy_positions").fetchone()["value"]
    assert execution_count == 1
    assert strategy_window_count == 1
    assert daily_pnl_count == 1
    assert copy_position_count == 1
    db.close()


def test_reset_runtime_state_clears_visible_snapshot_and_seeds_clean_runtime_state(tmp_path: Path) -> None:
    db_path = tmp_path / "bot_shadow.db"
    db = Database(db_path)
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-stale")
    db.set_bot_state("strategy_official_price_to_beat", "68123.45")
    db.set_bot_state("strategy_runtime_mode", "shadow")
    db.set_bot_state("runtime_guard_state", "active")
    db.set_bot_state("live_control_state", "paused")
    db.set_bot_state("live_cash_balance", "101.34")
    db.set_bot_state("live_total_capital", "101.34")
    db.set_bot_state("position_ledger_mode", "shadow")
    now_ts = int(time.time())
    with db.conn:
        db.conn.execute(
            """
            INSERT INTO strategy_windows(
                slug, condition_id, title, status, opened_at, price_mode, realized_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("btc-updown-5m-test", "cond-1", "Bitcoin Up or Down - Test", "closed", now_ts, "underround", 4.25),
        )
        db.conn.execute("INSERT INTO daily_pnl(day, pnl) VALUES (?, ?)", ("2026-03-22", 7.5))
    db.close()

    result = _reset_runtime_state(db_path)

    assert result["deleted"]["bot_state_runtime_reset"] >= 2
    assert result["deleted"]["strategy_windows"] == 1
    assert result["deleted"]["daily_pnl"] == 1
    db = Database(db_path)
    assert db.get_bot_state("strategy_market_slug") == ""
    assert db.get_bot_state("strategy_official_price_to_beat") == "0"
    assert db.get_bot_state("strategy_runtime_mode") == "shadow"
    assert db.get_bot_state("strategy_last_note").startswith("runtime shadow limpiado")
    assert db.get_bot_state("strategy_operability_label") == "Reiniciando"
    assert int(float(db.get_bot_state("live_balance_updated_at") or 0)) >= now_ts
    assert db.get_bot_state("runtime_guard_state") is None
    assert db.get_bot_state("position_ledger_mode") is None
    assert db.get_bot_state("live_control_state") == "paused"
    strategy_window_count = db.conn.execute("SELECT COUNT(*) AS value FROM strategy_windows").fetchone()["value"]
    daily_pnl_count = db.conn.execute("SELECT COUNT(*) AS value FROM daily_pnl").fetchone()["value"]
    assert strategy_window_count == 0
    assert daily_pnl_count == 0
    db.close()


def test_reset_compare_state_clears_paper_shadow_and_compare_db(tmp_path: Path) -> None:
    paper_db_path = tmp_path / "bot.db"
    shadow_db_path = tmp_path / "bot_shadow.db"

    for db_path, slug in ((paper_db_path, "paper-slug"), (shadow_db_path, "shadow-slug")):
        db = Database(db_path)
        db.init_schema()
        db.set_bot_state("strategy_market_slug", slug)
        db.set_bot_state("runtime_guard_state", "active")
        now_ts = int(time.time())
        with db.conn:
            db.conn.execute(
                """
                INSERT INTO executions(
                    ts, mode, status, action, side, asset, condition_id, size, price, notional
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (now_ts, "paper", "filled", "open", "buy", "asset-1", "cond-1", 5.0, 0.5, 2.5),
            )
            db.conn.execute(
                """
                INSERT INTO strategy_windows(
                    slug, condition_id, title, status, opened_at, price_mode, realized_pnl
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (slug, "cond-1", "Bitcoin Up or Down - Test", "closed", now_ts, "underround", 3.0),
            )
        db.close()

    compare_db_path = tmp_path / "runtime_compare.db"
    compare_db_path.write_text("placeholder", encoding="utf-8")

    result = _reset_compare_state(shadow_db_path)

    assert result["runtimes"]["paper"]["deleted"]["executions"] == 1
    assert result["runtimes"]["paper"]["deleted"]["strategy_windows"] == 1
    assert result["runtimes"]["shadow"]["deleted"]["executions"] == 1
    assert result["runtimes"]["shadow"]["deleted"]["strategy_windows"] == 1
    assert result["compare_files_removed"]["runtime_compare.db"] is True
    assert compare_db_path.exists() is False

    for db_path in (paper_db_path, shadow_db_path):
        db = Database(db_path)
        execution_count = db.conn.execute("SELECT COUNT(*) AS value FROM executions").fetchone()["value"]
        strategy_window_count = db.conn.execute("SELECT COUNT(*) AS value FROM strategy_windows").fetchone()["value"]
        assert execution_count == 0
        assert strategy_window_count == 0
        assert db.get_bot_state("strategy_market_slug") == ""
        assert db.get_bot_state("strategy_last_note").startswith("runtime")
        assert db.get_bot_state("runtime_guard_state") is None
        db.close()


def test_allowed_cors_origin_accepts_same_site_and_blocks_foreign() -> None:
    assert _allowed_cors_origin("https://polysainz.com", "nas.polysainz.com:8765") == "https://polysainz.com"
    assert _allowed_cors_origin("https://nas.polysainz.com", "nas.polysainz.com:8765") == "https://nas.polysainz.com"
    assert _allowed_cors_origin("https://evil.example", "nas.polysainz.com:8765") == ""


def test_destructive_request_allowed_requires_private_client_or_same_site_origin() -> None:
    assert _destructive_request_allowed(
        client_host="127.0.0.1",
        origin="",
        host_header="nas.polysainz.com:8765",
    ) is True
    assert _destructive_request_allowed(
        client_host="203.0.113.10",
        origin="https://polysainz.com",
        host_header="nas.polysainz.com:8765",
    ) is True
    assert _destructive_request_allowed(
        client_host="203.0.113.10",
        origin="https://evil.example",
        host_header="nas.polysainz.com:8765",
    ) is False


def test_dashboard_payloads_expose_microstructure_runtime_files(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.init_schema()
    db.close()

    runtime_root = tmp_path / "research" / "runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)
    (runtime_root / "microstructure_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-19T11:00:00Z",
                "market_slug": "btc-updown-5m-test",
                "market_title": "Bitcoin Up or Down - Test",
                "note": "telemetry snapshot",
                "frame": {"readiness_score": 81.5, "regime": "directional_pressure", "pair_sum_bps": -14.0},
                "decision": {"selected_execution": "taker_fak", "expected_edge_bps": 12.4},
            }
        ),
        encoding="utf-8",
    )
    (runtime_root / "liquidations_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-19T11:00:00Z",
                "totals": {"buy_30s": 12000.0, "sell_30s": 4000.0},
                "recent": [{"exchange": "binance", "side": "buy", "notional": 12000.0, "price": 70200.0}],
            }
        ),
        encoding="utf-8",
    )
    (runtime_root / "latency_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-19T11:00:00Z",
                "latencies": {"market_event_lag_ms": 42.0, "spot_age_ms": 18, "feature_compute_ms": 1.7},
            }
        ),
        encoding="utf-8",
    )

    summary = _summary_payload(
        db_path,
        clob_host="https://clob.polymarket.com",
        execution_mode="paper",
        live_trading_enabled=False,
    )

    assert summary["microstructure_snapshot"]["frame"]["readiness_score"] == 81.5
    assert summary["liquidations_snapshot"]["totals"]["buy_30s"] == 12000.0
    assert summary["latency_snapshot"]["latencies"]["market_event_lag_ms"] == 42.0
    assert _microstructure_payload(db_path)["market_slug"] == "btc-updown-5m-test"
    assert _liquidations_payload(db_path)["recent"][0]["exchange"] == "binance"
    assert _latency_payload(db_path)["latencies"]["feature_compute_ms"] == 1.7
    metrics = _metrics_payload(db_path)
    assert "pm_readiness_score 81.500000" in metrics
    assert "pm_liq_buy_notional_30s 12000.000000" in metrics
