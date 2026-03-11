from __future__ import annotations

from pathlib import Path

from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
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
