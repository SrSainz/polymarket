from __future__ import annotations

from datetime import datetime

from app.db import Database
from app.models import CopyInstruction, ExecutionResult, TradeSide


class PaperBroker:
    def __init__(self, db: Database) -> None:
        self.db = db

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        existing = self.db.get_copy_position(instruction.asset)
        current_size = float(existing["size"]) if existing else 0.0
        current_avg = float(existing["avg_price"]) if existing else instruction.price
        current_realized = float(existing["realized_pnl"]) if existing else 0.0

        if instruction.side == TradeSide.BUY:
            new_size = current_size + instruction.size
            if new_size <= 0:
                return ExecutionResult(
                    mode="paper",
                    status="skipped",
                    action=instruction.action,
                    asset=instruction.asset,
                    size=0.0,
                    price=instruction.price,
                    notional=0.0,
                    pnl_delta=0.0,
                    message="invalid resulting size",
                )

            new_avg = ((current_size * current_avg) + (instruction.size * instruction.price)) / new_size
            self.db.upsert_copy_position(
                asset=instruction.asset,
                condition_id=instruction.condition_id,
                size=new_size,
                avg_price=new_avg,
                realized_pnl=current_realized,
                title=instruction.title,
                slug=instruction.slug,
                outcome=instruction.outcome,
                category=instruction.category,
            )
            pnl_delta = 0.0
            filled_size = instruction.size

        else:
            if current_size <= 0:
                return ExecutionResult(
                    mode="paper",
                    status="skipped",
                    action=instruction.action,
                    asset=instruction.asset,
                    size=0.0,
                    price=instruction.price,
                    notional=0.0,
                    pnl_delta=0.0,
                    message="no position to reduce/close",
                )

            filled_size = min(instruction.size, current_size)
            pnl_delta = (instruction.price - current_avg) * filled_size
            remaining_size = current_size - filled_size
            new_realized = current_realized + pnl_delta

            if remaining_size <= 1e-9:
                self.db.delete_copy_position(instruction.asset)
            else:
                self.db.upsert_copy_position(
                    asset=instruction.asset,
                    condition_id=instruction.condition_id,
                    size=remaining_size,
                    avg_price=current_avg,
                    realized_pnl=new_realized,
                    title=instruction.title,
                    slug=instruction.slug,
                    outcome=instruction.outcome,
                    category=instruction.category,
                )

            self.db.add_daily_pnl(datetime.utcnow().date().isoformat(), pnl_delta)

        result = ExecutionResult(
            mode="paper",
            status="filled",
            action=instruction.action,
            asset=instruction.asset,
            size=filled_size,
            price=instruction.price,
            notional=filled_size * instruction.price,
            pnl_delta=pnl_delta,
            message="paper fill",
        )
        self.db.record_execution(
            result=result,
            side=instruction.side.value,
            condition_id=instruction.condition_id,
            source_wallet=instruction.source_wallet,
            source_signal_id=instruction.source_signal_id,
            notes=instruction.reason,
        )
        return result
