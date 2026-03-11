from __future__ import annotations

import logging

from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide
from app.services.telegram_trade_notifier import TelegramTradeNotifierService
from app.settings import BotConfig, EnvSettings


class _FakeNotifier(TelegramTradeNotifierService):
    def __init__(self) -> None:
        super().__init__(
            BotConfig(watched_wallets=["0xabc"], telegram_execution_notifications_enabled=True),
            EnvSettings(telegram_bot_token="token", telegram_chat_id="123"),
            logging.getLogger("test-telegram-trade-notifier"),
        )
        self.messages: list[str] = []

    def _send_message(self, text: str) -> bool:
        self.messages.append(text)
        return True


def _instruction() -> CopyInstruction:
    return CopyInstruction(
        action=SignalAction.CLOSE,
        side=TradeSide.SELL,
        asset="asset-1",
        condition_id="cond-1",
        size=10.0,
        price=0.6,
        notional=6.0,
        source_wallet="autonomous",
        source_signal_id=0,
        title="Bitcoin Up or Down",
        slug="btc-updown-5m",
        outcome="Up",
        category="crypto",
        reason="autonomous take_profit",
    )


def test_notifier_skips_zero_pnl() -> None:
    notifier = _FakeNotifier()
    sent = notifier.send_realized_result(
        instruction=_instruction(),
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-1",
            size=10.0,
            price=0.6,
            notional=6.0,
            pnl_delta=0.0,
            message="ok",
        ),
    )
    assert sent is False
    assert notifier.messages == []


def test_notifier_sends_realized_profit_message() -> None:
    notifier = _FakeNotifier()
    sent = notifier.send_realized_result(
        instruction=_instruction(),
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-1",
            size=10.0,
            price=0.6,
            notional=6.0,
            pnl_delta=1.25,
            message="ok",
        ),
    )
    assert sent is True
    assert notifier.messages
    assert "ganado $1.2500" in notifier.messages[0]
