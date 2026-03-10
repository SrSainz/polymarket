from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

from app.db import Database
from app.settings import BotConfig, EnvSettings


class TelegramDailySummaryService:
    LAST_SENT_KEY = "telegram_daily_summary_last_day"

    def __init__(self, db: Database, config: BotConfig, env: EnvSettings, logger: logging.Logger) -> None:
        self.db = db
        self.config = config
        self.env = env
        self.logger = logger
        self.session = requests.Session()
        self._warned_missing_telegram = False

    @property
    def enabled(self) -> bool:
        if not self.config.telegram_daily_summary_enabled:
            return False
        if self.env.telegram_bot_token and self.env.telegram_chat_id:
            return True
        if not self._warned_missing_telegram:
            self.logger.warning(
                "telegram_daily_summary_enabled=true but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are missing"
            )
            self._warned_missing_telegram = True
        return False

    def send_if_due(self) -> bool:
        if not self.enabled:
            return False

        tz = self._resolve_timezone(self.config.telegram_daily_summary_timezone)
        now_local = datetime.now(tz)
        if now_local.hour < self.config.telegram_daily_summary_hour:
            return False

        local_day = now_local.date().isoformat()
        last_sent = self.db.get_bot_state(self.LAST_SENT_KEY)
        if last_sent == local_day:
            return False

        today_utc = datetime.now(timezone.utc).date().isoformat()
        daily_pnl = self.db.get_daily_pnl(today_utc)
        gross_profit = self.db.get_daily_profit_gross(today_utc)
        gross_loss = self.db.get_daily_loss_gross(today_utc)
        counts = self.db.get_daily_execution_counts(today_utc)
        open_positions = len(self.db.list_copy_positions())
        exposure = self.db.get_total_exposure()

        text = (
            "Resumen diario paper trading\n"
            f"Fecha UTC: {today_utc}\n"
            f"PnL neto: ${daily_pnl:.2f}\n"
            f"Ganancias brutas: ${gross_profit:.2f}\n"
            f"Perdidas brutas: ${gross_loss:.2f}\n"
            f"Operaciones: {counts['total']} (buy {counts['buys']} / sell {counts['sells']})\n"
            f"Posiciones abiertas: {open_positions}\n"
            f"Exposicion actual: ${exposure:.2f}"
        )

        if not self._send_message(text):
            return False
        self.db.set_bot_state(self.LAST_SENT_KEY, local_day)
        return True

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
                self.logger.warning("telegram daily summary failed: %s", body)
                return False
            return True
        except requests.RequestException as error:
            self.logger.warning("telegram daily summary error: %s", error)
            return False

    def _resolve_timezone(self, tz_name: str):
        try:
            return ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            return timezone.utc
