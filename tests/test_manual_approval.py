from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.models import SignalAction, TradeSide
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
