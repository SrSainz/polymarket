from __future__ import annotations

from dataclasses import dataclass

from app.core.execution_engine import apply_fill_to_database, estimate_fill_fee_paid
from app.core.live_broker import _marketable_limit_price, _passive_limit_price, _resolve_execution_profile
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, TradeSide
from app.polymarket.clob_client import CLOBClient


@dataclass(frozen=True, slots=True)
class _BookLevel:
    price: float
    size: float


class ShadowBroker:
    def __init__(
        self,
        db: Database,
        clob_client: CLOBClient,
        *,
        slippage_limit: float = 0.03,
        execution_profile: str = "taker_fak",
    ) -> None:
        self.db = db
        self.clob_client = clob_client
        self.slippage_limit = max(float(slippage_limit), 0.0)
        self.execution_profile = str(execution_profile or "taker_fak").strip().lower()

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        profile = _resolve_execution_profile(
            instruction=instruction,
            default_profile=self.execution_profile,
        )
        if profile in {"maker_gtd", "maker_post_only_gtc"}:
            return self._submit_passive(instruction=instruction, profile=profile)
        return self._execute_taker(instruction=instruction, profile=profile)

    def _submit_passive(self, *, instruction: CopyInstruction, profile: str) -> ExecutionResult:
        passive_price = _passive_limit_price(
            reference_price=instruction.price,
            side=instruction.side,
        )
        return ExecutionResult(
            mode="shadow",
            status="submitted",
            action=instruction.action,
            asset=instruction.asset,
            size=0.0,
            price=passive_price if passive_price > 0 else instruction.price,
            notional=0.0,
            pnl_delta=0.0,
            message=f"shadow maker resting | profile={profile}",
        )

    def _execute_taker(self, *, instruction: CopyInstruction, profile: str) -> ExecutionResult:
        try:
            book = self.clob_client.get_book(instruction.asset)
        except Exception as error:  # noqa: BLE001
            return ExecutionResult(
                mode="shadow",
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message=f"shadow missing_orderbook: {error}",
            )

        levels = _book_levels(book=book, side=instruction.side)
        if not levels:
            return ExecutionResult(
                mode="shadow",
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message="shadow missing_orderbook",
            )

        limit_price = _marketable_limit_price(
            reference_price=instruction.price,
            side=instruction.side,
            slippage_limit=self.slippage_limit,
        )
        requested_size = max(float(instruction.size or 0.0), 0.0)
        if requested_size <= 0:
            return ExecutionResult(
                mode="shadow",
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message="shadow invalid_size",
            )

        remaining_size = requested_size
        consumed: list[_BookLevel] = []
        for level in levels:
            if remaining_size <= 1e-9:
                break
            if not _level_is_marketable(level=level, side=instruction.side, limit_price=limit_price):
                break
            fill_size = min(remaining_size, level.size)
            if fill_size <= 1e-9:
                continue
            consumed.append(_BookLevel(price=level.price, size=fill_size))
            remaining_size -= fill_size

        filled_size = max(requested_size - remaining_size, 0.0)
        if filled_size <= 1e-9:
            return ExecutionResult(
                mode="shadow",
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message=f"shadow {profile} unmatched",
            )

        if profile == "taker_fok" and remaining_size > 1e-9:
            return ExecutionResult(
                mode="shadow",
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message=f"shadow {profile} unfilled",
            )

        fill_notional = sum(level.price * level.size for level in consumed)
        fill_price = fill_notional / max(filled_size, 1e-9)
        partial = filled_size + 1e-9 < requested_size
        message = (
            f"shadow partial fill | profile={profile} | {filled_size:.4f}/{requested_size:.4f} across {len(consumed)} levels"
            if partial
            else f"shadow fill | profile={profile} | {len(consumed)} levels"
        )
        notes = instruction.reason
        if partial:
            notes = f"{instruction.reason} | shadow_partial:{filled_size:.4f}/{requested_size:.4f}"
        return apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode="shadow",
            filled_size=filled_size,
            fill_price=fill_price,
            fill_notional=fill_notional,
            fee_paid=estimate_fill_fee_paid(
                instruction=instruction,
                fill_size=filled_size,
                fill_price=fill_price,
                fee_lookup=getattr(self.clob_client, "get_fee_rate_bps", None),
            ),
            message=message,
            status="filled",
            notes=notes,
        )


def _book_levels(*, book: dict, side: TradeSide) -> tuple[_BookLevel, ...]:
    raw_levels = (book.get("asks") or []) if side == TradeSide.BUY else (book.get("bids") or [])
    parsed: list[_BookLevel] = []
    for raw in raw_levels:
        price, size = _parse_book_level(raw)
        if price <= 0 or size <= 0:
            continue
        parsed.append(_BookLevel(price=price, size=size))
    reverse = side == TradeSide.SELL
    parsed.sort(key=lambda level: level.price, reverse=reverse)
    return tuple(parsed)


def _parse_book_level(raw: object) -> tuple[float, float]:
    if isinstance(raw, dict):
        price = _safe_float(raw.get("price") or raw.get("p") or raw.get("px"))
        size = _safe_float(raw.get("size") or raw.get("s") or raw.get("quantity"))
        return price, size
    if isinstance(raw, (list, tuple)) and len(raw) >= 2:
        return _safe_float(raw[0]), _safe_float(raw[1])
    return 0.0, 0.0


def _level_is_marketable(*, level: _BookLevel, side: TradeSide, limit_price: float) -> bool:
    if limit_price <= 0:
        return False
    if side == TradeSide.BUY:
        return level.price <= limit_price + 1e-12
    return level.price >= limit_price - 1e-12


def _safe_float(value: object) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
