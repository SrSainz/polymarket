from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.services.manual_approval import ManualApprovalService
from app.settings import BotConfig, EnvSettings


def _service(db: Database) -> ManualApprovalService:
    config = BotConfig(
        watched_wallets=["0xabc"],
        manual_confirmation_enabled=True,
        confirmation_start_hour=8,
        confirmation_end_hour=20,
        confirmation_timeout_minutes=30,
    )
    env = EnvSettings(
        telegram_bot_token="",
        telegram_chat_id="",
    )
    return ManualApprovalService(db, config, env, logger=_noop_logger())


def _noop_logger():
    import logging

    logger = logging.getLogger("test_manual_approval")
    logger.setLevel(logging.CRITICAL)
    return logger


def test_collect_ready_approvals_sets_timeout_decision(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()

    approval_id = db.create_trade_approval(
        source_signal_id=1,
        asset="asset-1",
        condition_id="cond-1",
        action="open",
        side_proposed="buy",
        size=10.0,
        price=0.5,
        notional=5.0,
        source_wallet="0xsrc",
        title="Market",
        slug="market",
        outcome="Yes",
        category="sports",
        reason="copy",
        timeout_minutes=30,
    )
    with db.conn:
        db.conn.execute("UPDATE trade_approvals SET expires_at = created_at - 1 WHERE id = ?", (approval_id,))

    service = _service(db)
    ready = service.collect_ready_approvals()
    assert len(ready) == 1
    assert int(ready[0]["id"]) == approval_id
    assert str(ready[0]["decision_source"]) == "timeout_auto"
    assert str(ready[0]["side_decided"]) == "buy"
    db.close()


def test_instruction_from_approval_uses_decided_side(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()

    approval_id = db.create_trade_approval(
        source_signal_id=2,
        asset="asset-2",
        condition_id="cond-2",
        action="open",
        side_proposed="buy",
        size=20.0,
        price=0.4,
        notional=8.0,
        source_wallet="0xsrc",
        title="Market2",
        slug="market2",
        outcome="No",
        category="sports",
        reason="copy",
        timeout_minutes=30,
    )
    db.set_trade_approval_decision(
        approval_id=approval_id,
        side_decided="sell",
        decision_source="user_telegram",
        decision_note="manual override",
    )
    row = db.get_trade_approval(approval_id)
    assert row is not None

    service = _service(db)
    instruction = service.instruction_from_approval(dict(row))
    assert instruction is not None
    assert instruction.side == TradeSide.SELL
    assert instruction.action == SignalAction.OPEN
    db.close()


def test_dynamic_market_skips_manual_confirmation(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    config = BotConfig(
        watched_wallets=["0xabc"],
        manual_confirmation_enabled=True,
        confirmation_start_hour=0,
        confirmation_end_hour=24,
        confirmation_timezone="UTC",
        dynamic_keywords=["bitcoin", "5m"],
        dynamic_skip_manual_confirmation=True,
    )
    env = EnvSettings(
        telegram_bot_token="token",
        telegram_chat_id="123",
    )
    service = ManualApprovalService(db, config, env, logger=_noop_logger())
    service.is_within_confirmation_window = lambda: True  # type: ignore[method-assign]

    def _fail_if_called(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("telegram should not be called for dynamic markets")

    service._send_approval_message = _fail_if_called  # type: ignore[method-assign]
    instruction = CopyInstruction(
        action=SignalAction.OPEN,
        side=TradeSide.BUY,
        asset="asset-btc-5m",
        condition_id="cond",
        size=10.0,
        price=0.5,
        notional=5.0,
        source_wallet="0xabc",
        source_signal_id=1,
        title="Bitcoin up or down in 5m?",
        slug="bitcoin-up-or-down-5m",
        outcome="Yes",
        category="crypto",
        reason="",
    )
    should_wait = service.request_confirmation(instruction, source_signal_id=1)
    assert not should_wait
    assert db.list_pending_trade_approvals(limit=10) == []
    db.close()
