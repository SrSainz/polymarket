from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from app.core.market_classifier import is_dynamic_market
from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.settings import BotConfig, EnvSettings


class ManualApprovalService:
    OFFSET_KEY = "telegram_update_offset"

    def __init__(self, db: Database, config: BotConfig, env: EnvSettings, logger: logging.Logger) -> None:
        self.db = db
        self.config = config
        self.env = env
        self.logger = logger
        self.session = requests.Session()
        self._warned_missing_telegram = False

    @property
    def enabled(self) -> bool:
        if not self.config.manual_confirmation_enabled:
            return False
        if self.env.telegram_bot_token and self.env.telegram_chat_id:
            return True
        if not self._warned_missing_telegram:
            self.logger.warning(
                "manual_confirmation_enabled=true but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are missing; auto mode continues"
            )
            self._warned_missing_telegram = True
        return False

    def is_within_confirmation_window(self) -> bool:
        tz = ZoneInfo(self.config.confirmation_timezone)
        hour = datetime.now(tz).hour
        return self.config.confirmation_start_hour <= hour < self.config.confirmation_end_hour

    def request_confirmation(self, instruction: CopyInstruction, source_signal_id: int | None) -> bool:
        if not self.enabled:
            return False
        if not self.is_within_confirmation_window():
            return False
        if self.config.dynamic_skip_manual_confirmation and is_dynamic_market(
            title=instruction.title,
            slug=instruction.slug,
            category=instruction.category,
            keywords=self.config.dynamic_keywords,
        ):
            return False

        approval_id = self.db.create_trade_approval(
            source_signal_id=source_signal_id,
            asset=instruction.asset,
            condition_id=instruction.condition_id,
            action=instruction.action.value,
            side_proposed=instruction.side.value,
            size=instruction.size,
            price=instruction.price,
            notional=instruction.notional,
            source_wallet=instruction.source_wallet,
            title=instruction.title,
            slug=instruction.slug,
            outcome=instruction.outcome,
            category=instruction.category,
            reason=instruction.reason,
            timeout_minutes=self.config.confirmation_timeout_minutes,
        )
        message_id = self._send_approval_message(approval_id, instruction)
        if message_id is None:
            self.db.mark_trade_approval_failed(approval_id, "telegram_send_failed")
            return False

        self.db.set_trade_approval_message_id(approval_id, message_id)
        return True

    def sync_user_decisions(self) -> None:
        if not self.enabled:
            return
        self._poll_telegram_updates()

    def collect_ready_approvals(self) -> list[dict[str, str | int | float]]:
        ready: list[dict[str, str | int | float]] = []
        now_ts = int(time.time())
        for row in self.db.list_pending_trade_approvals(limit=500):
            side_decided = str(row["side_decided"] or "").strip().lower()
            if side_decided in {"buy", "sell"}:
                ready.append(dict(row))
                continue

            if int(row["expires_at"]) <= now_ts:
                self.db.set_trade_approval_decision(
                    approval_id=int(row["id"]),
                    side_decided=str(row["side_proposed"]),
                    decision_source="timeout_auto",
                    decision_note="No user response in time; auto execution",
                )
                refreshed = self.db.get_trade_approval(int(row["id"]))
                if refreshed is not None:
                    ready.append(dict(refreshed))
        return ready

    def instruction_from_approval(self, row: dict[str, str | int | float]) -> CopyInstruction | None:
        side_raw = str(row.get("side_decided") or row.get("side_proposed") or "").lower().strip()
        if side_raw not in {"buy", "sell"}:
            return None

        action_raw = str(row.get("action") or "").lower().strip()
        try:
            action = SignalAction(action_raw)
            side = TradeSide(side_raw)
        except ValueError:
            return None

        reason_parts = [str(row.get("reason") or "").strip(), str(row.get("decision_source") or "").strip()]
        reason = " | ".join([part for part in reason_parts if part])

        return CopyInstruction(
            action=action,
            side=side,
            asset=str(row.get("asset") or ""),
            condition_id=str(row.get("condition_id") or ""),
            size=float(row.get("size") or 0),
            price=float(row.get("price") or 0),
            notional=float(row.get("notional") or 0),
            source_wallet=str(row.get("source_wallet") or ""),
            source_signal_id=int(row.get("source_signal_id") or 0),
            title=str(row.get("title") or ""),
            slug=str(row.get("slug") or ""),
            outcome=str(row.get("outcome") or ""),
            category=str(row.get("category") or ""),
            reason=reason,
        )

    def _send_approval_message(self, approval_id: int, instruction: CopyInstruction) -> int | None:
        text = (
            "Nueva decision de trading\n"
            f"id: {approval_id}\n"
            f"mercado: {instruction.title or instruction.asset}\n"
            f"outcome: {instruction.outcome or '-'}\n"
            f"sugerencia: {instruction.side.value.upper()} | {instruction.action.value}\n"
            f"monto: ${instruction.notional:.2f} | size: {instruction.size:.4f} | precio: {instruction.price:.4f}\n"
            f"timeout: {self.config.confirmation_timeout_minutes} min"
        )
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "Comprar", "callback_data": f"pm:ap:{approval_id}:buy"},
                    {"text": "Vender", "callback_data": f"pm:ap:{approval_id}:sell"},
                ],
                [
                    {"text": "Saltar", "callback_data": f"pm:ap:{approval_id}:skip"},
                ],
            ]
        }
        payload = {
            "chat_id": self.env.telegram_chat_id,
            "text": text,
            "reply_markup": keyboard,
        }
        try:
            response = self.session.post(
                f"https://api.telegram.org/bot{self.env.telegram_bot_token}/sendMessage",
                json=payload,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                self.logger.warning("telegram sendMessage failed: %s", data)
                return None
            return int(data["result"]["message_id"])
        except requests.RequestException as error:
            self.logger.warning("telegram sendMessage error: %s", error)
            return None

    def _poll_telegram_updates(self) -> None:
        offset_raw = self.db.get_bot_state(self.OFFSET_KEY)
        offset = int(offset_raw) if offset_raw else 0

        params = {
            "timeout": 0,
            "offset": offset,
            "allowed_updates": json.dumps(["callback_query"]),
        }
        try:
            response = self.session.get(
                f"https://api.telegram.org/bot{self.env.telegram_bot_token}/getUpdates",
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as error:
            self.logger.warning("telegram getUpdates error: %s", error)
            return

        if not payload.get("ok"):
            self.logger.warning("telegram getUpdates failed: %s", payload)
            return

        results = payload.get("result") or []
        max_offset = offset
        for update in results:
            update_id = int(update.get("update_id") or 0)
            if update_id >= max_offset:
                max_offset = update_id + 1

            callback = update.get("callback_query") or {}
            callback_id = str(callback.get("id") or "")
            data = str(callback.get("data") or "")
            message = self._handle_callback(data)
            if callback_id:
                self._answer_callback(callback_id, message)

        if max_offset != offset:
            self.db.set_bot_state(self.OFFSET_KEY, str(max_offset))

    def _handle_callback(self, data: str) -> str:
        parts = data.split(":")
        if len(parts) != 4 or parts[0] != "pm" or parts[1] != "ap":
            return "Comando invalido"

        try:
            approval_id = int(parts[2])
        except ValueError:
            return "ID invalido"
        action = parts[3].strip().lower()

        approval = self.db.get_trade_approval(approval_id)
        if approval is None:
            return "No encontrada"
        if str(approval["status"]) != "pending":
            return "Ya procesada"

        source_signal_id = int(approval["source_signal_id"]) if approval["source_signal_id"] is not None else 0

        if action == "skip":
            self.db.reject_trade_approval(
                approval_id=approval_id,
                decision_source="user_telegram",
                decision_note="User skipped",
            )
            if source_signal_id > 0:
                self.db.mark_signal_status(source_signal_id, "skipped", "rejected by user")
            self._send_followup_message(
                approval_id=approval_id,
                decision="SALTAR",
                market=str(approval["title"] or approval["asset"] or ""),
                outcome=str(approval["outcome"] or "-"),
            )
            return "Saltada"

        if action not in {"buy", "sell"}:
            return "Accion invalida"

        self.db.set_trade_approval_decision(
            approval_id=approval_id,
            side_decided=action,
            decision_source="user_telegram",
            decision_note=f"user selected {action}",
        )
        if source_signal_id > 0:
            self.db.mark_signal_status(source_signal_id, "awaiting_execution", f"user selected {action}")
        self._send_followup_message(
            approval_id=approval_id,
            decision="COMPRAR" if action == "buy" else "VENDER",
            market=str(approval["title"] or approval["asset"] or ""),
            outcome=str(approval["outcome"] or "-"),
        )
        return "Decision recibida"

    def _send_followup_message(self, *, approval_id: int, decision: str, market: str, outcome: str) -> None:
        text = (
            "Accion recibida\n"
            f"id: {approval_id}\n"
            f"decision: {decision}\n"
            f"mercado: {market or '-'}\n"
            f"outcome: {outcome or '-'}"
        )
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
        except requests.RequestException as error:
            self.logger.warning("telegram followup sendMessage error: %s", error)

    def _answer_callback(self, callback_id: str, text: str) -> None:
        payload = {
            "callback_query_id": callback_id,
            "text": text[:180],
            "show_alert": False,
        }
        try:
            self.session.post(
                f"https://api.telegram.org/bot{self.env.telegram_bot_token}/answerCallbackQuery",
                json=payload,
                timeout=10,
            )
        except requests.RequestException:
            return
