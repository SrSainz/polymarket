from __future__ import annotations

from app.core.execution_engine import apply_fill_to_database
from app.db import Database
from app.models import CopyInstruction, ExecutionResult


class PaperBroker:
    def __init__(self, db: Database) -> None:
        self.db = db

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        return apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode="paper",
            filled_size=instruction.size,
            fill_price=instruction.price,
            fill_notional=instruction.size * instruction.price,
            message="paper fill",
            status="filled",
            notes=instruction.reason,
        )
