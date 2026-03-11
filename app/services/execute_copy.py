from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.core.autonomous_decider import AutonomousDecider
from app.core.copier import Copier
from app.core.market_classifier import is_btc5m_market
from app.core.live_broker import LiveBroker
from app.core.market_classifier import is_dynamic_market
from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import CopyInstruction
from app.models import SignalAction
from app.polymarket.clob_client import CLOBClient
from app.services.manual_approval import ManualApprovalService
from app.services.telegram_daily_summary import TelegramDailySummaryService
from app.settings import AppSettings


def calculate_effective_bankroll(
    *,
    base_bankroll: float,
    prior_realized_pnl: float,
    prior_profit_gross: float,
    profit_keep_ratio: float,
) -> float:
    reserved_profit = max(prior_profit_gross, 0.0) * max(min(profit_keep_ratio, 1.0), 0.0)
    return max(base_bankroll + prior_realized_pnl - reserved_profit, 0.0)


class ExecuteCopyService:
    def __init__(
        self,
        db: Database,
        copier: Copier,
        paper_broker: PaperBroker,
        live_broker: LiveBroker,
        clob_client: CLOBClient,
        autonomous_decider: AutonomousDecider,
        manual_approval: ManualApprovalService,
        daily_summary: TelegramDailySummaryService,
        settings: AppSettings,
        logger: logging.Logger,
    ) -> None:
        self.db = db
        self.copier = copier
        self.paper_broker = paper_broker
        self.live_broker = live_broker
        self.clob_client = clob_client
        self.autonomous_decider = autonomous_decider
        self.manual_approval = manual_approval
        self.daily_summary = daily_summary
        self.settings = settings
        self.logger = logger

    def run(self, mode: str = "paper") -> dict[str, int]:
        pending_signals = self.db.list_pending_signals()
        today = datetime.now(timezone.utc).date().isoformat()
        prior_realized_pnl = self.db.get_cumulative_pnl_before(today)
        prior_profit_gross = self.db.get_cumulative_profit_gross_before(today)
        effective_bankroll = calculate_effective_bankroll(
            base_bankroll=self.settings.config.bankroll,
            prior_realized_pnl=prior_realized_pnl,
            prior_profit_gross=prior_profit_gross,
            profit_keep_ratio=self.settings.config.profit_keep_ratio,
        )

        stats = {
            "pending": len(pending_signals),
            "filled": 0,
            "blocked": 0,
            "failed": 0,
            "skipped": 0,
            "auto_candidates": 0,
            "auto_filled": 0,
            "auto_failed": 0,
            "approvals_requested": 0,
            "approvals_user_filled": 0,
            "approvals_timeout_filled": 0,
            "approvals_failed": 0,
        }

        if mode == "live":
            self.manual_approval.sync_user_decisions()

        for signal in pending_signals:
            try:
                skip_reason = self._skip_reason_for_mode(signal=signal, mode=mode)
                if skip_reason:
                    self.db.mark_signal_status(signal.id or 0, "skipped", skip_reason)
                    stats["skipped"] += 1
                    continue

                execution_price = self.clob_client.get_midpoint(signal.asset) or signal.reference_price
                copy_position = self.db.get_copy_position(signal.asset)
                copy_size = float(copy_position["size"]) if copy_position else 0.0
                copy_avg_price = float(copy_position["avg_price"]) if copy_position else execution_price
                current_market_notional = self._get_condition_exposure(signal.condition_id)
                total_exposure = self.db.get_total_exposure()
                dynamic_exposure = self._get_dynamic_exposure()
                btc5m_exposure = self._get_btc5m_exposure()
                daily_pnl = self.db.get_daily_pnl(today)
                daily_profit_gross = self.db.get_daily_profit_gross(today)

                instruction, reason = self.copier.build_instruction(
                    mode=mode,
                    signal=signal,
                    copy_position_size=copy_size,
                    copy_position_avg_price=copy_avg_price,
                    execution_price=execution_price,
                    current_total_exposure=total_exposure,
                    current_dynamic_exposure=dynamic_exposure,
                    current_btc5m_exposure=btc5m_exposure,
                    current_market_notional=current_market_notional,
                    daily_pnl=daily_pnl,
                    daily_profit_gross=daily_profit_gross,
                    effective_bankroll=effective_bankroll,
                )

                if instruction is None:
                    status = "blocked"
                    if signal.action in (SignalAction.REDUCE, SignalAction.CLOSE) and reason == "size below minimum":
                        status = "skipped"
                    self.db.mark_signal_status(signal.id or 0, status, reason)
                    stats[status] += 1
                    continue

                if mode == "live" and self.manual_approval.request_confirmation(instruction, signal.id):
                    self.db.mark_signal_status(signal.id or 0, "awaiting_approval", "manual confirmation pending")
                    stats["approvals_requested"] += 1
                    continue

                result = self._execute_instruction(mode=mode, instruction=instruction)
                self.db.mark_signal_status(signal.id or 0, "executed", result.message)
                if result.status == "filled":
                    stats["filled"] += 1
                else:
                    stats["skipped"] += 1

            except Exception as error:  # noqa: BLE001
                error_text = _normalize_error_text(error)
                if _is_no_match_error(error_text):
                    self.db.mark_signal_status(signal.id or 0, "skipped", "no_match_liquidity")
                    stats["skipped"] += 1
                    self.logger.warning("signal_id=%s skipped: no orderbook match/liquidity", signal.id)
                    continue

                self.db.mark_signal_status(signal.id or 0, "failed", str(error))
                stats["failed"] += 1
                if _is_invalid_signature_error(error_text):
                    self.logger.error(
                        "signal_id=%s failed: live auth/signature mismatch. Check POLYMARKET_SIGNATURE_TYPE and POLYMARKET_FUNDER.",
                        signal.id,
                    )
                    # Stop this cycle to avoid spamming repeated auth failures.
                    break
                self.logger.exception("signal_id=%s failed: %s", signal.id, error)

        self._run_autonomous_exits(mode=mode, stats=stats)
        if mode == "live":
            self.manual_approval.sync_user_decisions()
            self._execute_ready_approvals(mode=mode, stats=stats)
        self.daily_summary.send_if_due()
        return stats

    def _skip_reason_for_mode(self, *, signal, mode: str) -> str:
        if mode != "live" or not self.settings.config.live_only_btc5m:
            return ""
        if signal.action not in (SignalAction.OPEN, SignalAction.ADD):
            return ""
        if not is_btc5m_market(title=signal.title, slug=signal.slug, category=signal.category):
            return "live_only_btc5m"

        copy_position = self.db.get_copy_position(signal.asset)
        if copy_position is not None:
            return ""

        if self._get_open_btc5m_positions_count() >= self.settings.config.live_btc5m_max_open_positions:
            return "live_btc5m_max_open_positions"
        if self._has_live_condition_conflict(asset=signal.asset, condition_id=signal.condition_id):
            return "live_condition_conflict"
        return ""

    def _get_condition_exposure(self, condition_id: str) -> float:
        exposure = 0.0
        for row in self.db.list_copy_positions():
            if str(row["condition_id"] or "") != condition_id:
                continue
            exposure += abs(float(row["size"]) * float(row["avg_price"]))
        return exposure

    def _has_live_condition_conflict(self, *, asset: str, condition_id: str) -> bool:
        for row in self.db.list_copy_positions():
            if str(row["condition_id"] or "") != condition_id:
                continue
            if str(row["asset"] or "") == asset:
                continue
            if float(row["size"] or 0.0) <= 0:
                continue
            return True
        return False

    def _get_dynamic_exposure(self) -> float:
        exposure = 0.0
        for row in self.db.list_copy_positions():
            if is_dynamic_market(
                title=str(row["title"] or ""),
                slug=str(row["slug"] or ""),
                category=str(row["category"] or ""),
                keywords=self.settings.config.dynamic_keywords,
            ):
                exposure += abs(float(row["size"]) * float(row["avg_price"]))
        return exposure

    def _get_btc5m_exposure(self) -> float:
        exposure = 0.0
        for row in self.db.list_copy_positions():
            if is_btc5m_market(
                title=str(row["title"] or ""),
                slug=str(row["slug"] or ""),
                category=str(row["category"] or ""),
            ) or is_dynamic_market(
                title=str(row["title"] or ""),
                slug=str(row["slug"] or ""),
                category=str(row["category"] or ""),
                keywords=self.settings.config.btc5m_reserve_keywords,
            ):
                exposure += abs(float(row["size"]) * float(row["avg_price"]))
        return exposure

    def _get_open_btc5m_positions_count(self) -> int:
        total = 0
        for row in self.db.list_copy_positions():
            if is_btc5m_market(
                title=str(row["title"] or ""),
                slug=str(row["slug"] or ""),
                category=str(row["category"] or ""),
            ):
                total += 1
        return total

    def _run_autonomous_exits(self, *, mode: str, stats: dict[str, int]) -> None:
        if not self.settings.config.autonomous_decisions_enabled:
            return

        positions = self.db.list_copy_positions()
        for position in positions:
            asset = str(position["asset"])
            avg_price = float(position["avg_price"])
            size = float(position["size"])
            if size <= 0:
                continue

            mark_price = self.clob_client.get_midpoint(asset) or avg_price
            self.db.record_position_mark(asset, mark_price)

            instruction = self.autonomous_decider.build_exit_instruction(
                asset=asset,
                condition_id=str(position["condition_id"]),
                size=size,
                avg_price=avg_price,
                mark_price=mark_price,
                title=str(position["title"] or ""),
                slug=str(position["slug"] or ""),
                outcome=str(position["outcome"] or ""),
                category=str(position["category"] or ""),
            )
            if instruction is None:
                continue

            stats["auto_candidates"] += 1
            try:
                if mode == "live" and self.manual_approval.request_confirmation(instruction, source_signal_id=None):
                    stats["approvals_requested"] += 1
                    continue

                result = self._execute_instruction(mode=mode, instruction=instruction)
                if result.status == "filled":
                    stats["auto_filled"] += 1
                    self.logger.info("autonomous fill asset=%s reason=%s", asset, instruction.reason)
            except Exception as error:  # noqa: BLE001
                stats["auto_failed"] += 1
                self.logger.exception("autonomous execution failed asset=%s: %s", asset, error)

    def _execute_ready_approvals(self, *, mode: str, stats: dict[str, int]) -> None:
        ready_rows = self.manual_approval.collect_ready_approvals()
        for row in ready_rows:
            approval_id = int(row["id"])
            source_signal_id = int(row["source_signal_id"]) if row["source_signal_id"] is not None else 0
            decision_source = str(row["decision_source"] or "")
            instruction = self.manual_approval.instruction_from_approval(row)
            if instruction is None:
                self.db.mark_trade_approval_failed(approval_id, "invalid approval payload")
                if source_signal_id > 0:
                    self.db.mark_signal_status(source_signal_id, "failed", "invalid approval payload")
                stats["approvals_failed"] += 1
                continue

            try:
                result = self._execute_instruction(mode=mode, instruction=instruction)
                if result.status == "filled":
                    if source_signal_id > 0:
                        self.db.mark_signal_status(source_signal_id, "executed", result.message)
                    self.db.mark_trade_approval_executed(approval_id, result.message)
                    if decision_source == "timeout_auto":
                        stats["approvals_timeout_filled"] += 1
                    else:
                        stats["approvals_user_filled"] += 1
                else:
                    self.db.mark_trade_approval_failed(approval_id, f"not filled: {result.message}")
                    if source_signal_id > 0:
                        self.db.mark_signal_status(source_signal_id, "skipped", result.message)
            except Exception as error:  # noqa: BLE001
                self.db.mark_trade_approval_failed(approval_id, str(error))
                if source_signal_id > 0:
                    self.db.mark_signal_status(source_signal_id, "failed", str(error))
                stats["approvals_failed"] += 1
                self.logger.exception("approval execution failed id=%s: %s", approval_id, error)

    def _execute_instruction(self, *, mode: str, instruction: CopyInstruction):
        if mode == "live":
            return self.live_broker.execute(instruction)
        return self.paper_broker.execute(instruction)


def _normalize_error_text(error: Exception) -> str:
    return str(error or "").strip().lower()


def _is_no_match_error(error_text: str) -> bool:
    # Market FOK orders may be rejected when book liquidity cannot fully fill.
    liquidity_patterns = (
        "no match",
        "couldn't be fully filled",
        "could not be fully filled",
        "fully filled or killed",
    )
    return any(pattern in error_text for pattern in liquidity_patterns)


def _is_invalid_signature_error(error_text: str) -> bool:
    return "invalid signature" in error_text or "unauthorized/invalid api key" in error_text
