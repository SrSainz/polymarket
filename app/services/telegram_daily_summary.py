from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

from app.core.lab_artifacts import load_tournament_summary, research_root_from_db
from app.db import Database
from app.settings import BotConfig, EnvSettings


class TelegramDailySummaryService:
    LAST_SENT_KEY = "telegram_daily_summary_last_day"
    STATUS_LAST_SENT_TS_KEY = "telegram_status_summary_last_sent_ts"
    STATUS_FORCE_SEND_KEY = "telegram_status_summary_force_send"

    def __init__(self, db: Database, config: BotConfig, env: EnvSettings, logger: logging.Logger) -> None:
        self.db = db
        self.config = config
        self.env = env
        self.logger = logger
        self.session = requests.Session()
        self._warned_missing_telegram = False

    @property
    def enabled(self) -> bool:
        force_requested = self.db.get_bot_state(self.STATUS_FORCE_SEND_KEY) == "1"
        if not (
            self.config.telegram_daily_summary_enabled
            or self.config.telegram_status_summary_enabled
            or force_requested
        ):
            return False
        if self.env.telegram_bot_token and self.env.telegram_chat_id:
            return True
        if not self._warned_missing_telegram:
            self.logger.warning(
                "telegram summaries enabled but TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID are missing"
            )
            self._warned_missing_telegram = True
        return False

    def send_if_due(self, now_utc: datetime | None = None) -> bool:
        if not self.enabled:
            return False

        reference_utc = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
        sent = False
        sent = self._send_status_summary_if_due(reference_utc) or sent
        sent = self._send_daily_summary_if_due(reference_utc) or sent
        return sent

    def _send_daily_summary_if_due(self, reference_utc: datetime) -> bool:
        if not self.config.telegram_daily_summary_enabled:
            return False

        tz = self._resolve_timezone(self.config.telegram_daily_summary_timezone)
        now_local = reference_utc.astimezone(tz)
        if now_local.hour < self.config.telegram_daily_summary_hour:
            return False

        local_day = now_local.date().isoformat()
        last_sent = self.db.get_bot_state(self.LAST_SENT_KEY)
        if last_sent == local_day:
            return False

        today_utc = reference_utc.date().isoformat()
        daily_pnl = self.db.get_daily_pnl(today_utc)
        gross_profit = self.db.get_daily_profit_gross(today_utc)
        gross_loss = self.db.get_daily_loss_gross(today_utc)
        counts = self.db.get_daily_execution_counts(today_utc)
        open_positions = len(self.db.list_copy_positions())
        exposure = self.db.get_total_exposure()
        tournament_brief = self._tournament_brief()

        mode_label = "live" if self.env.live_trading and self.config.execution_mode == "live" else "paper"
        text = (
            f"Resumen diario {mode_label}\n"
            f"Fecha UTC: {today_utc}\n"
            f"PnL neto: ${daily_pnl:.2f}\n"
            f"Ganancias brutas: ${gross_profit:.2f}\n"
            f"Perdidas brutas: ${gross_loss:.2f}\n"
            f"Operaciones: {counts['total']} (buy {counts['buys']} / sell {counts['sells']})\n"
            f"Posiciones abiertas: {open_positions}\n"
            f"Exposicion actual: ${exposure:.2f}"
        )
        if tournament_brief:
            text += f"\nTorneo: {tournament_brief}"

        if not self._send_message(text):
            return False
        self.db.set_bot_state(self.LAST_SENT_KEY, local_day)
        return True

    def _send_status_summary_if_due(self, reference_utc: datetime) -> bool:
        force_requested = self.db.get_bot_state(self.STATUS_FORCE_SEND_KEY) == "1"
        if not self.config.telegram_status_summary_enabled and not force_requested:
            return False

        interval_minutes = max(int(self.config.telegram_status_summary_interval_minutes), 5)
        interval_seconds = interval_minutes * 60
        now_ts = int(reference_utc.timestamp())
        last_sent_ts = _safe_int(self.db.get_bot_state(self.STATUS_LAST_SENT_TS_KEY))
        if not force_requested and last_sent_ts > 0 and (now_ts - last_sent_ts) < interval_seconds:
            return False

        since_ts = last_sent_ts if last_sent_ts > 0 else max(now_ts - interval_seconds, 0)
        text = self._build_status_summary_text(
            reference_utc=reference_utc,
            since_ts=since_ts,
            interval_minutes=interval_minutes,
        )
        if not self._send_message(text):
            return False

        self.db.set_bot_state(self.STATUS_LAST_SENT_TS_KEY, str(now_ts))
        self.db.set_bot_state(self.STATUS_FORCE_SEND_KEY, "0")
        return True

    def _build_status_summary_text(
        self,
        *,
        reference_utc: datetime,
        since_ts: int,
        interval_minutes: int,
    ) -> str:
        today_utc = reference_utc.date().isoformat()
        daily_pnl = self.db.get_daily_pnl(today_utc)
        gross_profit = self.db.get_daily_profit_gross(today_utc)
        gross_loss = self.db.get_daily_loss_gross(today_utc)
        counts = self.db.get_daily_execution_counts(today_utc)
        open_positions = len(self.db.list_copy_positions())
        exposure = self.db.get_total_exposure()
        recent_limit = max(int(self.config.telegram_status_summary_recent_limit), 1)
        interval_stats = self.db.get_execution_stats_since(since_ts)
        interval_rows = self.db.get_recent_executions_since(since_ts, limit=recent_limit)
        interval_total = int(interval_stats["total"] or 0)
        interval_buys = int(interval_stats["buys"] or 0)
        interval_sells = int(interval_stats["sells"] or 0)
        interval_pnl = float(interval_stats["pnl"] or 0.0)
        mode_label = self._mode_label()
        control_label = self._control_label(mode_label)
        control_reason = str(self.db.get_bot_state("live_control_reason") or "").strip()
        strategy_variant = str(self.db.get_bot_state("strategy_variant") or "default").strip() or "default"
        strategy_market = str(self.db.get_bot_state("strategy_market_title") or self.db.get_bot_state("strategy_market_slug") or "-")
        operability_label = str(self.db.get_bot_state("strategy_operability_label") or self.db.get_bot_state("strategy_last_note") or "-")
        operability_reason = str(self.db.get_bot_state("strategy_operability_reason") or "").strip()
        tournament_brief = self._tournament_brief()

        lines = [
            f"Resumen {interval_minutes}m {mode_label}",
            f"Control: {control_label}" + (f" | {control_reason}" if control_reason else ""),
            (
                f"Tramo: {datetime.fromtimestamp(max(since_ts, 0), timezone.utc).isoformat()} -> "
                f"{reference_utc.isoformat()}"
            ),
            (
                f"Ultimos {interval_minutes}m: {interval_total} ops "
                f"(buy {interval_buys} / sell {interval_sells}) | pnl {self._fmt_usd(interval_pnl)}"
            ),
            (
                f"Hoy: pnl {self._fmt_usd(daily_pnl)} | bruto {self._fmt_usd(gross_profit)} / "
                f"{self._fmt_usd(-gross_loss)} | ops {counts['total']}"
            ),
            f"Abiertas: {open_positions} | exposicion {self._fmt_usd(exposure)}",
            f"Variante: {strategy_variant} | mercado: {strategy_market}",
            f"Estado: {operability_label}" + (f" | {operability_reason}" if operability_reason else ""),
        ]
        if tournament_brief:
            lines.append(f"Torneo: {tournament_brief}")
        if interval_total <= 0:
            lines.append(f"Sin operaciones nuevas en los ultimos {interval_minutes} minutos.")
        else:
            lines.extend(self._recent_execution_lines(interval_rows[:recent_limit]))
        if daily_pnl < 0:
            if gross_profit <= 1e-9 and gross_loss > 0:
                lines.append("Lectura: de momento todo lo cerrado va en perdidas.")
            else:
                lines.append("Lectura: seguimos en rojo; no escalar aun.")
        return "\n".join(lines)

    def _recent_execution_lines(self, rows: list) -> list[str]:
        lines = ["Ultimas operaciones:"]
        for row in rows:
            action = str(row["action"] or "").upper()
            side = str(row["side"] or "").upper()
            outcome = str(row["notes"] or row["asset"] or "-")
            notional = float(row["notional"] or 0.0)
            pnl_delta = float(row["pnl_delta"] or 0.0)
            lines.append(
                f"- {side} {action} {self._fmt_usd(notional)} | pnl {self._fmt_usd(pnl_delta)} | {outcome[:72]}"
            )
        return lines

    def _mode_label(self) -> str:
        runtime_mode = str(self.db.get_bot_state("strategy_runtime_mode") or "").strip().lower()
        if runtime_mode in {"live", "paper"}:
            return runtime_mode
        if self.env.live_trading and self.config.execution_mode == "live":
            return "live"
        return "paper"

    def _control_label(self, mode_label: str) -> str:
        if mode_label != "live":
            return "solo paper"
        state = str(self.db.get_bot_state("live_control_state") or self.config.live_control_default_state).strip().lower()
        if state == "armed":
            return "live armado"
        return "live pausado"

    def _fmt_usd(self, value: float) -> str:
        sign = "+" if value > 0 else ""
        return f"{sign}${value:.2f}"

    def _tournament_brief(self) -> str:
        research_root = research_root_from_db(self.db.db_path)
        payload = load_tournament_summary(research_root)
        recommendation = payload.get("recommendation") if isinstance(payload.get("recommendation"), dict) else {}
        active_variant = str(payload.get("active_variant") or "").strip() or str(self.db.get_bot_state("strategy_variant") or "-")
        candidate_variant = str(recommendation.get("candidate_variant") or "").strip()
        label = str(recommendation.get("label") or "").strip()
        if not label and not candidate_variant:
            return ""
        if candidate_variant and candidate_variant != active_variant:
            return f"{label} | activa {active_variant} -> candidata {candidate_variant}"
        if candidate_variant:
            return f"{label} | candidata {candidate_variant}"
        return label

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


def _safe_int(raw_value: str | None) -> int:
    try:
        return int(float(raw_value or 0))
    except (TypeError, ValueError):
        return 0
