from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from app.core.lab_artifacts import append_jsonl, dump_json, events_log_path, latency_snapshot_path, load_latency_snapshot
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, TradeSide
from app.polymarket.fee_model import fee_cost_usdc


class BrokerAdapter(Protocol):
    def execute(self, instruction: CopyInstruction) -> ExecutionResult: ...


@dataclass(frozen=True, slots=True)
class ExecutionTrace:
    ts: int
    mode: str
    status: str
    asset: str
    condition_id: str
    side: str
    action: str
    size: float
    price: float
    notional: float
    pnl_delta: float
    fee_paid: float
    source_wallet: str
    source_signal_id: int
    outcome: str
    slug: str
    reason: str
    message: str
    execution_profile: str
    signal_to_order_ms: float
    order_to_fill_ms: float
    expected_slippage_bps: float
    realized_slippage_bps: float
    edge_decay_bps: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ExecutionEngine:
    def __init__(
        self,
        *,
        db: Database,
        research_dir: Path,
        paper_broker: BrokerAdapter,
        shadow_broker: BrokerAdapter,
        live_broker: BrokerAdapter,
    ) -> None:
        self.db = db
        self.research_dir = research_dir
        self.paper_broker = paper_broker
        self.shadow_broker = shadow_broker
        self.live_broker = live_broker

    def execute(self, *, mode: str, instruction: CopyInstruction) -> ExecutionResult:
        safe_mode = str(mode or "").strip().lower() or "paper"
        started_ns = time.time_ns()
        if safe_mode == "live":
            result = self.live_broker.execute(instruction)
            self._record_trace(result=result, instruction=instruction, started_ns=started_ns)
            return result
        if safe_mode == "shadow":
            result = self.shadow_broker.execute(instruction)
            self._record_trace(result=result, instruction=instruction, started_ns=started_ns)
            self.db.set_bot_state("shadow_last_instruction_at", str(int(time.time())))
            self.db.set_bot_state(
                "shadow_last_instruction",
                (
                    f"{result.status} {instruction.side.value} {instruction.outcome or instruction.asset} "
                    f"req={instruction.size:.4f}@{instruction.price:.4f} "
                    f"fill={float(result.size or 0.0):.4f}@{float(result.price or instruction.price):.4f}"
                ),
            )
            return result
        result = self.paper_broker.execute(instruction)
        self._record_trace(result=result, instruction=instruction, started_ns=started_ns)
        return result

    def settle_resolved(self, *, mode: str, instruction: CopyInstruction) -> ExecutionResult:
        safe_mode = str(mode or "").strip().lower() or "paper"
        started_ns = time.time_ns()
        result = apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode=safe_mode,
            filled_size=instruction.size,
            fill_price=instruction.price,
            fill_notional=instruction.notional,
            message="resolved_settlement",
            status="filled",
            notes=instruction.reason,
        )
        self._record_trace(result=result, instruction=instruction, started_ns=started_ns)
        return result

    def _record_trace(self, *, result: ExecutionResult, instruction: CopyInstruction, started_ns: int) -> None:
        now_ns = time.time_ns()
        signal_to_order_ms = _signal_to_order_ms(self.db, started_ns=started_ns)
        order_to_fill_ms = max((now_ns - started_ns) / 1_000_000, 0.0)
        expected_slippage_bps = _expected_slippage_bps(instruction)
        realized_slippage_bps = _realized_slippage_bps(instruction=instruction, result=result)
        edge_decay_bps = _edge_decay_bps(self.db, realized_slippage_bps=realized_slippage_bps)
        append_jsonl(
            events_log_path(self.research_dir, "execution_traces"),
            ExecutionTrace(
                ts=int(time.time()),
                mode=str(result.mode or ""),
                status=str(result.status or ""),
                asset=instruction.asset,
                condition_id=instruction.condition_id,
                side=instruction.side.value,
                action=instruction.action.value,
                size=float(result.size or 0.0),
                price=float(result.price or 0.0),
            notional=float(result.notional or 0.0),
            pnl_delta=float(result.pnl_delta or 0.0),
            fee_paid=float(result.fee_paid or 0.0),
            source_wallet=instruction.source_wallet,
                source_signal_id=int(instruction.source_signal_id or 0),
                outcome=instruction.outcome,
                slug=instruction.slug,
                reason=instruction.reason,
                message=str(result.message or ""),
                execution_profile=str(instruction.execution_profile or ""),
                signal_to_order_ms=round(signal_to_order_ms, 4),
                order_to_fill_ms=round(order_to_fill_ms, 4),
                expected_slippage_bps=round(expected_slippage_bps, 4),
                realized_slippage_bps=round(realized_slippage_bps, 4),
                edge_decay_bps=round(edge_decay_bps, 4),
            ).to_dict(),
        )
        self._update_latency_snapshot(
            signal_to_order_ms=signal_to_order_ms,
            order_to_fill_ms=order_to_fill_ms,
            expected_slippage_bps=expected_slippage_bps,
            realized_slippage_bps=realized_slippage_bps,
            edge_decay_bps=edge_decay_bps,
        )

    def _update_latency_snapshot(
        self,
        *,
        signal_to_order_ms: float,
        order_to_fill_ms: float,
        expected_slippage_bps: float,
        realized_slippage_bps: float,
        edge_decay_bps: float,
    ) -> None:
        snapshot = load_latency_snapshot(self.research_dir)
        latencies = snapshot.get("latencies") if isinstance(snapshot.get("latencies"), dict) else {}
        latencies.update(
            {
                "signal_to_order_ms": round(signal_to_order_ms, 4),
                "order_to_fill_ms": round(order_to_fill_ms, 4),
                "expected_slippage_bps": round(expected_slippage_bps, 4),
                "realized_slippage_bps": round(realized_slippage_bps, 4),
                "edge_decay_bps": round(edge_decay_bps, 4),
            }
        )
        dump_json(
            latency_snapshot_path(self.research_dir),
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "latencies": latencies,
            },
        )
        self.db.set_bot_state("strategy_signal_to_order_ms", f"{signal_to_order_ms:.4f}")
        self.db.set_bot_state("strategy_order_to_fill_ms", f"{order_to_fill_ms:.4f}")
        self.db.set_bot_state("strategy_expected_slippage_bps", f"{expected_slippage_bps:.4f}")
        self.db.set_bot_state("strategy_realized_slippage_bps", f"{realized_slippage_bps:.4f}")
        self.db.set_bot_state("strategy_edge_decay_bps", f"{edge_decay_bps:.4f}")


def apply_fill_to_database(
    *,
    db: Database,
    instruction: CopyInstruction,
    mode: str,
    filled_size: float,
    fill_price: float,
    fill_notional: float,
    fee_paid: float = 0.0,
    message: str,
    status: str = "filled",
    notes: str = "",
    execution_ts: int | None = None,
) -> ExecutionResult:
    existing = db.get_copy_position(instruction.asset)
    current_size = float(existing["size"]) if existing else 0.0
    current_avg = float(existing["avg_price"]) if existing else instruction.price
    current_realized = float(existing["realized_pnl"]) if existing else 0.0
    effective_fee_paid = max(float(fee_paid or 0.0), 0.0)
    recorded_ts = int(execution_ts or time.time())
    recorded_day = datetime.fromtimestamp(recorded_ts, timezone.utc).date().isoformat()

    if instruction.side == TradeSide.BUY:
        new_size = current_size + filled_size
        if new_size <= 0:
            return ExecutionResult(
                mode=mode,
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message="invalid resulting size",
            )
        new_avg = ((current_size * current_avg) + (filled_size * fill_price) + effective_fee_paid) / new_size
        db.upsert_copy_position(
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
        db.set_bot_state("position_ledger_mode", mode)
        pnl_delta = 0.0
    else:
        if current_size <= 0:
            return ExecutionResult(
                mode=mode,
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message="no position to reduce/close",
            )
        filled_size = min(filled_size, current_size)
        gross_pnl_delta = (fill_price - current_avg) * filled_size
        pnl_delta = gross_pnl_delta - effective_fee_paid
        remaining_size = current_size - filled_size
        new_realized = current_realized + pnl_delta
        if remaining_size <= 1e-9:
            db.delete_copy_position(instruction.asset)
        else:
            db.upsert_copy_position(
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
        if db.list_copy_positions():
            db.set_bot_state("position_ledger_mode", mode)
        else:
            db.set_bot_state("position_ledger_mode", "")
        db.add_daily_pnl(recorded_day, pnl_delta)

    result = ExecutionResult(
        mode=mode,
        status=status,
        action=instruction.action,
        asset=instruction.asset,
        size=filled_size,
        price=fill_price,
        notional=fill_notional,
        pnl_delta=pnl_delta,
        fee_paid=effective_fee_paid,
        message=message,
    )
    db.record_execution(
        result=result,
        side=instruction.side.value,
        condition_id=instruction.condition_id,
        source_wallet=instruction.source_wallet,
        source_signal_id=instruction.source_signal_id,
        notes=notes or instruction.reason,
        title=instruction.title,
        slug=instruction.slug,
        outcome=instruction.outcome,
        category=instruction.category,
        ts=recorded_ts,
    )
    return result


def estimate_fill_fee_paid(
    *,
    instruction: CopyInstruction,
    fill_size: float,
    fill_price: float,
    fee_lookup,
) -> float:
    if str(instruction.reason or "").strip().startswith("strategy_resolution:"):
        return 0.0
    if not callable(fee_lookup):
        return 0.0
    try:
        fee_rate_bps = float(fee_lookup(str(instruction.asset)) or 0.0)
    except (TypeError, ValueError):
        fee_rate_bps = 0.0
    if fee_rate_bps <= 0:
        return 0.0
    return fee_cost_usdc(
        size=fill_size,
        price=fill_price,
        fee_rate_bps=fee_rate_bps,
        category=instruction.category,
    )


def _signal_to_order_ms(db: Database, *, started_ns: int) -> float:
    raw = db.get_bot_state("strategy_last_decision_at_ns")
    try:
        decision_ns = int(str(raw or "0"))
    except ValueError:
        decision_ns = 0
    if decision_ns <= 0:
        return 0.0
    return max((started_ns - decision_ns) / 1_000_000, 0.0)


def _expected_slippage_bps(instruction: CopyInstruction) -> float:
    profile = str(instruction.execution_profile or "").strip().lower()
    if profile == "taker_fak":
        return 3.0
    if profile == "taker_fok":
        return 5.0
    return 0.5 if profile.startswith("maker_") else 0.0


def _realized_slippage_bps(*, instruction: CopyInstruction, result: ExecutionResult) -> float:
    reference_price = float(instruction.price or 0.0)
    fill_price = float(result.price or 0.0)
    if reference_price <= 0 or fill_price <= 0 or result.status != "filled":
        return 0.0
    if instruction.side == TradeSide.BUY:
        return ((fill_price - reference_price) / reference_price) * 10_000
    return ((reference_price - fill_price) / reference_price) * 10_000


def _edge_decay_bps(db: Database, *, realized_slippage_bps: float) -> float:
    raw = db.get_bot_state("strategy_last_expected_edge_bps")
    try:
        expected_edge_bps = float(str(raw or "0"))
    except ValueError:
        expected_edge_bps = 0.0
    realized_cost = abs(realized_slippage_bps)
    if expected_edge_bps <= 0:
        return realized_cost
    return min(realized_cost, expected_edge_bps)
