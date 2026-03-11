from __future__ import annotations

import logging

import requests

from app.models import CopyInstruction, ExecutionResult
from app.settings import BotConfig, EnvSettings


class TelegramTradeNotifierService:
    def __init__(self, config: BotConfig, env: EnvSettings, logger: logging.Logger) -> None:
        self.config = config
        self.env = env
        self.logger = logger
        self.session = requests.Session()
        self._warned_missing_telegram = False

    @property
    def enabled(self) -> bool:
        if not self.config.telegram_execution_notifications_enabled:
            return False
        if self.env.telegram_bot_token and self.env.telegram_chat_id:
            return True
        if not self._warned_missing_telegram:
            self.logger.warning(
                "telegram_execution_notifications_enabled=true but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are missing"
            )
            self._warned_missing_telegram = True
        return False

    def send_realized_result(self, *, instruction: CopyInstruction, result: ExecutionResult) -> bool:
        if not self.enabled:
            return False
        if result.status != "filled":
            return False
        if abs(float(result.pnl_delta or 0.0)) <= 1e-9:
            return False

        outcome = "ganado" if result.pnl_delta > 0 else "perdido"
        text = (
            "Resultado de decision ejecutada\n"
            f"mercado: {instruction.title or instruction.asset}\n"
            f"outcome: {instruction.outcome or '-'}\n"
            f"accion: {instruction.side.value.upper()} | {instruction.action.value}\n"
            f"monto: ${result.notional:.2f} | precio: {result.price:.4f}\n"
            f"resultado: {outcome} ${abs(result.pnl_delta):.4f}\n"
            f"motivo: {instruction.reason or '-'}"
        )
        return self._send_message(text)

    def _send_message(self, text: str) -> bool:
        payload = {
            "chat_id": self.env.telegram_chat_id,
            "text": text,
        }
        try:
            response = self.session.post(
                f"https://api.telegram.org/bot{self.env.telegram_bot_token}/sendMessage",
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            body = response.json()
            if not body.get("ok"):
                self.logger.warning("telegram trade notifier failed: %s", body)
                return False
            return True
        except requests.RequestException as error:
            self.logger.warning("telegram trade notifier error: %s", error)
            return False
