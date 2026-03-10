from __future__ import annotations

import logging
from datetime import datetime

from app.core.autonomous_decider import AutonomousDecider
from app.core.copier import Copier
from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import SignalAction
from app.polymarket.clob_client import CLOBClient
from app.settings import AppSettings


class ExecuteCopyService:
    def __init__(
        self,
        db: Database,
        copier: Copier,
        paper_broker: PaperBroker,
        live_broker: LiveBroker,
        clob_client: CLOBClient,
        autonomous_decider: AutonomousDecider,
        settings: AppSettings,
        logger: logging.Logger,
    ) -> None:
        self.db = db
        self.copier = copier
        self.paper_broker = paper_broker
        self.live_broker = live_broker
        self.clob_client = clob_client
        self.autonomous_decider = autonomous_decider
        self.settings = settings
        self.logger = logger

    def run(self, mode: str = "paper") -> dict[str, int]:
        pending_signals = self.db.list_pending_signals()
        today = datetime.utcnow().date().isoformat()

        stats = {
            "pending": len(pending_signals),
            "filled": 0,
            "blocked": 0,
            "failed": 0,
            "skipped": 0,
            "auto_candidates": 0,
            "auto_filled": 0,
            "auto_failed": 0,
        }

        for signal in pending_signals:
            try:
                execution_price = self.clob_client.get_midpoint(signal.asset) or signal.reference_price
                copy_position = self.db.get_copy_position(signal.asset)
                copy_size = float(copy_position["size"]) if copy_position else 0.0
                copy_avg_price = float(copy_position["avg_price"]) if copy_position else execution_price
                total_exposure = self.db.get_total_exposure()
                daily_pnl = self.db.get_daily_pnl(today)

                instruction, reason = self.copier.build_instruction(
                    signal=signal,
                    copy_position_size=copy_size,
                    copy_position_avg_price=copy_avg_price,
                    execution_price=execution_price,
                    current_total_exposure=total_exposure,
                    daily_pnl=daily_pnl,
                )

                if instruction is None:
                    status = "blocked"
                    if signal.action in (SignalAction.REDUCE, SignalAction.CLOSE) and reason == "size below minimum":
                        status = "skipped"
                    self.db.mark_signal_status(signal.id or 0, status, reason)
                    stats[status] += 1
                    continue

                if mode == "live":
                    result = self.live_broker.execute(instruction)
                else:
                    result = self.paper_broker.execute(instruction)

                self.db.mark_signal_status(signal.id or 0, "executed", result.message)
                if result.status == "filled":
                    stats["filled"] += 1
                else:
                    stats["skipped"] += 1

            except Exception as error:  # noqa: BLE001
                self.db.mark_signal_status(signal.id or 0, "failed", str(error))
                stats["failed"] += 1
                self.logger.exception("signal_id=%s failed: %s", signal.id, error)

        self._run_autonomous_exits(mode=mode, stats=stats)
        return stats

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
                if mode == "live":
                    result = self.live_broker.execute(instruction)
                else:
                    result = self.paper_broker.execute(instruction)

                if result.status == "filled":
                    stats["auto_filled"] += 1
                    self.logger.info("autonomous fill asset=%s reason=%s", asset, instruction.reason)
            except Exception as error:  # noqa: BLE001
                stats["auto_failed"] += 1
                self.logger.exception("autonomous execution failed asset=%s: %s", asset, error)
