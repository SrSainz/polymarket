from __future__ import annotations

import json
from pathlib import Path

from app.core.execution_engine import ExecutionEngine
from app.core.lab_artifacts import events_log_path, load_latency_snapshot
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide


class _StubBroker:
    def __init__(self, result: ExecutionResult) -> None:
        self.result = result
        self.calls: list[CopyInstruction] = []

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        self.calls.append(instruction)
        return self.result


def _instruction() -> CopyInstruction:
    return CopyInstruction(
        action=SignalAction.OPEN,
        side=TradeSide.BUY,
        asset="asset-up",
        condition_id="cond-1",
        size=10.0,
        price=0.42,
        notional=4.2,
        source_wallet="strategy:test",
        source_signal_id=7,
        title="Bitcoin Up or Down",
        slug="btc-updown-5m-1773913500",
        outcome="Up",
        category="crypto",
        reason="unit-test",
    )


def test_execution_engine_records_paper_trace(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    paper = _StubBroker(
        ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-up",
            size=10.0,
            price=0.42,
            notional=4.2,
            pnl_delta=0.0,
            message="paper fill",
        )
    )
    live = _StubBroker(
        ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-up",
            size=10.0,
            price=0.42,
            notional=4.2,
            pnl_delta=0.0,
            message="live fill",
        )
    )
    engine = ExecutionEngine(db=db, research_dir=tmp_path, paper_broker=paper, live_broker=live)

    result = engine.execute(mode="paper", instruction=_instruction())

    trace_path = events_log_path(tmp_path, "execution_traces")
    assert result.status == "filled"
    assert len(paper.calls) == 1
    assert len(live.calls) == 0
    rows = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[-1]["mode"] == "paper"
    assert rows[-1]["asset"] == "asset-up"
    latency = load_latency_snapshot(tmp_path)
    assert latency["latencies"]["order_to_fill_ms"] >= 0.0
    db.close()


def test_execution_engine_shadow_simulates_fill_without_broker_calls(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    paper = _StubBroker(
        ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-up",
            size=10.0,
            price=0.42,
            notional=4.2,
            pnl_delta=0.0,
            message="paper fill",
        )
    )
    live = _StubBroker(
        ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-up",
            size=10.0,
            price=0.42,
            notional=4.2,
            pnl_delta=0.0,
            message="live fill",
        )
    )
    engine = ExecutionEngine(db=db, research_dir=tmp_path, paper_broker=paper, live_broker=live)

    result = engine.execute(mode="shadow", instruction=_instruction())

    assert result.status == "filled"
    assert result.mode == "shadow"
    assert paper.calls == []
    assert live.calls == []
    assert db.get_bot_state("shadow_last_instruction") is not None
    assert db.get_bot_state("shadow_last_instruction_at") is not None
    assert db.get_bot_state("position_ledger_mode") == "shadow"
    position = db.get_copy_position("asset-up")
    assert position is not None
    assert float(position["size"]) == 10.0
    latency = load_latency_snapshot(tmp_path)
    assert latency["latencies"]["expected_slippage_bps"] == 0.0
    db.close()


def test_execution_engine_settle_resolved_updates_ledger_without_hitting_live_broker(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-1",
        size=10.0,
        avg_price=0.42,
        realized_pnl=0.0,
        title="Bitcoin Up or Down",
        slug="btc-updown-5m-1773913500",
        outcome="Up",
        category="crypto",
    )
    db.set_bot_state("position_ledger_mode", "live")
    paper = _StubBroker(
        ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-up",
            size=10.0,
            price=0.42,
            notional=4.2,
            pnl_delta=0.0,
            message="paper fill",
        )
    )
    live = _StubBroker(
        ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset="asset-up",
            size=10.0,
            price=0.42,
            notional=4.2,
            pnl_delta=0.0,
            message="live fill",
        )
    )
    engine = ExecutionEngine(db=db, research_dir=tmp_path, paper_broker=paper, live_broker=live)
    instruction = CopyInstruction(
        action=SignalAction.CLOSE,
        side=TradeSide.SELL,
        asset="asset-up",
        condition_id="cond-1",
        size=10.0,
        price=1.0,
        notional=10.0,
        source_wallet="strategy:settlement",
        source_signal_id=0,
        title="Bitcoin Up or Down",
        slug="btc-updown-5m-1773913500",
        outcome="Up",
        category="crypto",
        reason="strategy_resolution:test",
    )

    result = engine.settle_resolved(mode="live", instruction=instruction)

    assert result.status == "filled"
    assert paper.calls == []
    assert live.calls == []
    assert db.get_copy_position("asset-up") is None
    assert db.get_bot_state("position_ledger_mode") == ""
    db.close()
