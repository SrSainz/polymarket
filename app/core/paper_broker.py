from __future__ import annotations

from app.core.execution_engine import apply_fill_to_database, estimate_fill_fee_paid
from app.db import Database
from app.models import CopyInstruction, ExecutionResult


class PaperBroker:
    def __init__(self, db: Database, clob_client: object | None = None) -> None:
        self.db = db
        self.clob_client = clob_client

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        fee_lookup = getattr(self.clob_client, "get_fee_rate_bps", None)
        fill_price = instruction.price
        fill_notional = instruction.size * instruction.price
        return apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode="paper",
            filled_size=instruction.size,
            fill_price=fill_price,
            fill_notional=fill_notional,
            fee_paid=estimate_fill_fee_paid(
                instruction=instruction,
                fill_size=instruction.size,
                fill_price=fill_price,
                fee_lookup=fee_lookup,
            ),
            message="paper fill",
            status="filled",
            notes=instruction.reason,
        )
