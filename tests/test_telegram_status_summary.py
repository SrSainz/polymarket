from __future__ import annotations

import logging
import json
from datetime import datetime, timezone
from pathlib import Path

from app.db import Database
from app.models import ExecutionResult, SignalAction, TradeSide
from app.services.telegram_daily_summary import TelegramDailySummaryService
from app.settings import BotConfig, EnvSettings


def _service(db: Database) -> TelegramDailySummaryService:
    config = BotConfig(
        watched_wallets=["0xabc"],
        telegram_daily_summary_enabled=False,
        telegram_status_summary_enabled=True,
        telegram_status_summary_interval_minutes=30,
        telegram_status_summary_recent_limit=2,
        live_control_default_state="paused",
    )
    env = EnvSettings(
        telegram_bot_token="token",
        telegram_chat_id="123",
    )
    logger = logging.getLogger("test_telegram_status_summary")
    logger.setLevel(logging.CRITICAL)
    return TelegramDailySummaryService(db, config, env, logger=logger)


def test_status_summary_respects_interval_and_force_send(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_runtime_mode", "paper")
    db.set_bot_state("strategy_variant", "arb-micro-v1")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Current")
    db.set_bot_state("strategy_operability_label", "Rebalanceando")
    db.set_bot_state("strategy_operability_reason", "Falta liquidez visible")
    db.set_bot_state("live_control_state", "paused")
    db.set_bot_state("live_control_reason", "seguimos en perdidas")
    db.set_bot_state("live_control_updated_at", "1710755400")
    research_root = tmp_path / "research"
    (research_root / "experiments").mkdir(parents=True, exist_ok=True)
    (research_root / "experiments" / "tournament_summary.json").write_text(
        json.dumps(
            {
                "active_variant": "arb-micro-v1",
                "recommendation": {
                    "label": "Mantener live pausado",
                    "candidate_variant": "vidarx-tilted-v1",
                },
            }
        ),
        encoding="utf-8",
    )

    base_now = datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)
    today = base_now.date().isoformat()

    db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-1",
            size=10.0,
            price=0.2,
            notional=2.0,
            pnl_delta=-8.95,
            message="resolved",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-1",
        source_wallet="strategy",
        source_signal_id=0,
        notes="btc5m loss",
    )
    db.add_daily_pnl(today, -8.95)
    with db.conn:
        db.conn.execute(
            "UPDATE executions SET ts = ? WHERE id = (SELECT MAX(id) FROM executions)",
            (int(base_now.timestamp()) - 300,),
        )

    service = _service(db)
    sent_messages: list[str] = []

    def _capture(text: str) -> bool:
        sent_messages.append(text)
        return True

    service._send_message = _capture  # type: ignore[method-assign]

    assert service.send_if_due(now_utc=base_now) is True
    assert len(sent_messages) == 1
    assert "Resumen 30m paper" in sent_messages[0]
    assert "seguimos en perdidas" in sent_messages[0]
    assert "de momento todo lo cerrado va en perdidas" in sent_messages[0]
    assert "Torneo: Mantener live pausado | activa arb-micro-v1 -> candidata vidarx-tilted-v1" in sent_messages[0]

    assert service.send_if_due(now_utc=base_now.replace(minute=10)) is False
    assert len(sent_messages) == 1

    db.set_bot_state(service.STATUS_FORCE_SEND_KEY, "1")
    assert service.send_if_due(now_utc=base_now.replace(minute=10)) is True
    assert len(sent_messages) == 2
    assert db.get_bot_state(service.STATUS_FORCE_SEND_KEY) == "0"
    db.close()
