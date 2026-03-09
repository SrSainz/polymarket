from __future__ import annotations

from app.db import Database
from app.models import CopyInstruction, ExecutionResult
from app.polymarket.clob_client import CLOBClient
from app.settings import EnvSettings


class LiveBroker:
    def __init__(self, db: Database, clob_client: CLOBClient, env: EnvSettings) -> None:
        self.db = db
        self.clob_client = clob_client
        self.env = env

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        if not self.env.live_trading:
            raise RuntimeError("LIVE_TRADING=false. Live broker is disabled.")

        response = self.clob_client.place_market_order(
            token_id=instruction.asset,
            side=instruction.side.value,
            size=instruction.size,
        )

        result = ExecutionResult(
            mode="live",
            status="submitted",
            action=instruction.action,
            asset=instruction.asset,
            size=instruction.size,
            price=instruction.price,
            notional=instruction.notional,
            pnl_delta=0.0,
            message=str(response),
        )

        self.db.record_execution(
            result=result,
            side=instruction.side.value,
            condition_id=instruction.condition_id,
            source_wallet=instruction.source_wallet,
            source_signal_id=instruction.source_signal_id,
            notes="live order submitted",
        )
        return result
