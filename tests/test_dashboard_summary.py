from __future__ import annotations

from pathlib import Path

from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide
from app.services.dashboard_server import _summary_payload


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
    db.close()

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
