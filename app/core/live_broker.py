from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_UP
from datetime import datetime, timezone

from app.db import Database
from app.models import CopyInstruction, ExecutionResult, TradeSide
from app.polymarket.clob_client import CLOBClient
from app.settings import EnvSettings


class LiveBroker:
    def __init__(self, db: Database, clob_client: CLOBClient, env: EnvSettings, *, slippage_limit: float = 0.03) -> None:
        self.db = db
        self.clob_client = clob_client
        self.env = env
        self.slippage_limit = max(float(slippage_limit), 0.0)

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        if not self.env.live_trading:
            raise RuntimeError("LIVE_TRADING=false. Live broker is disabled.")

        try:
            response = self.clob_client.place_market_order(
                token_id=instruction.asset,
                side=instruction.side.value,
                size=instruction.size,
                notional=instruction.notional,
                limit_price=_marketable_limit_price(
                    reference_price=instruction.price,
                    side=instruction.side,
                    slippage_limit=self.slippage_limit,
                ),
                order_type="FOK",
            )
        except Exception as error:  # noqa: BLE001
            if _is_missing_orderbook_error(str(error or "")):
                return ExecutionResult(
                    mode="live",
                    status="skipped",
                    action=instruction.action,
                    asset=instruction.asset,
                    size=0.0,
                    price=instruction.price,
                    notional=0.0,
                    pnl_delta=0.0,
                    message="missing_orderbook",
                )
            raise
        fill = _extract_live_fill(response=response, instruction=instruction)
        if not fill["filled"]:
            return ExecutionResult(
                mode="live",
                status="skipped",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message=_live_execution_status(response),
            )

        existing = self.db.get_copy_position(instruction.asset)
        current_size = float(existing["size"]) if existing else 0.0
        current_avg = float(existing["avg_price"]) if existing else instruction.price
        current_realized = float(existing["realized_pnl"]) if existing else 0.0
        fill_size = float(fill["size"])
        fill_price = float(fill["price"])
        fill_notional = float(fill["notional"])

        if instruction.side == TradeSide.BUY:
            new_size = current_size + fill_size
            if new_size <= 0:
                return ExecutionResult(
                    mode="live",
                    status="skipped",
                    action=instruction.action,
                    asset=instruction.asset,
                    size=0.0,
                    price=instruction.price,
                    notional=0.0,
                    pnl_delta=0.0,
                    message="invalid resulting size",
                )

            new_avg = ((current_size * current_avg) + (fill_size * fill_price)) / new_size
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
            filled_size = fill_size
        else:
            if current_size <= 0:
                return ExecutionResult(
                    mode="live",
                    status="skipped",
                    action=instruction.action,
                    asset=instruction.asset,
                    size=0.0,
                    price=instruction.price,
                    notional=0.0,
                    pnl_delta=0.0,
                    message="no position to reduce/close",
                )

            filled_size = min(fill_size, current_size)
            pnl_delta = (fill_price - current_avg) * filled_size
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

            self.db.add_daily_pnl(datetime.now(timezone.utc).date().isoformat(), pnl_delta)

        result = ExecutionResult(
            mode="live",
            status="filled",
            action=instruction.action,
            asset=instruction.asset,
            size=filled_size,
            price=fill_price,
            notional=fill_notional,
            pnl_delta=pnl_delta,
            message=str(response),
        )

        self.db.record_execution(
            result=result,
            side=instruction.side.value,
            condition_id=instruction.condition_id,
            source_wallet=instruction.source_wallet,
            source_signal_id=instruction.source_signal_id,
            notes=_live_execution_notes(response),
        )
        return result


def _marketable_limit_price(*, reference_price: float, side: TradeSide, slippage_limit: float) -> float:
    price = max(float(reference_price), 0.0)
    if price <= 0:
        return 0.0
    if side == TradeSide.BUY:
        bounded = min(price * (1.0 + slippage_limit), 0.99)
        return _quantize_price(bounded, rounding=ROUND_UP)
    bounded = max(price * (1.0 - slippage_limit), 0.01)
    return _quantize_price(bounded, rounding=ROUND_DOWN)


def _quantize_price(value: float, *, rounding) -> float:  # noqa: ANN001
    return float(Decimal(str(value)).quantize(Decimal("0.0001"), rounding=rounding))


def _extract_live_fill(*, response: object, instruction: CopyInstruction) -> dict[str, float | bool]:
    if not isinstance(response, dict):
        return {
            "filled": True,
            "size": instruction.size,
            "price": instruction.price,
            "notional": instruction.notional,
        }

    making_amount = _safe_float(response.get("makingAmount"))
    taking_amount = _safe_float(response.get("takingAmount"))
    direct_size = _safe_float(
        response.get("size")
        or response.get("filledSize")
        or response.get("matchedSize")
        or response.get("baseFilled")
        or response.get("filledBaseAmount")
    )
    direct_notional = _safe_float(
        response.get("notional")
        or response.get("filledNotional")
        or response.get("quoteFilled")
        or response.get("filledQuoteAmount")
    )
    direct_price = _safe_float(response.get("price") or response.get("avgPrice") or response.get("averagePrice"))
    status = _live_execution_status(response)
    trade_ids = response.get("tradeIDs") or []

    if instruction.side == TradeSide.BUY:
        filled_size = taking_amount if taking_amount > 0 else direct_size
        filled_notional = making_amount if making_amount > 0 else direct_notional
    else:
        filled_size = making_amount if making_amount > 0 else direct_size
        filled_notional = taking_amount if taking_amount > 0 else direct_notional

    if filled_size > 0 and filled_notional > 0:
        return {
            "filled": True,
            "size": filled_size,
            "price": filled_notional / max(filled_size, 1e-9),
            "notional": filled_notional,
        }
    if filled_size > 0:
        inferred_price = direct_price if direct_price > 0 else instruction.price
        return {
            "filled": True,
            "size": filled_size,
            "price": inferred_price,
            "notional": filled_size * inferred_price,
        }
    if status == "matched":
        return {
            "filled": True,
            "size": instruction.size,
            "price": direct_price if direct_price > 0 else instruction.price,
            "notional": instruction.notional if direct_notional <= 0 else direct_notional,
        }
    if status in {"delayed", "unmatched", "cancelled"} and not trade_ids:
        return {"filled": False, "size": 0.0, "price": 0.0, "notional": 0.0}
    if trade_ids:
        return {
            "filled": True,
            "size": instruction.size,
            "price": direct_price if direct_price > 0 else instruction.price,
            "notional": instruction.notional if direct_notional <= 0 else direct_notional,
        }
    return {"filled": False, "size": 0.0, "price": 0.0, "notional": 0.0}


def _live_execution_notes(response: object) -> str:
    if not isinstance(response, dict):
        return "live fill"

    order_id = response.get("orderID") or response.get("orderId") or response.get("id")
    status = response.get("status")
    parts = ["live fill"]
    if status:
        parts.append(f"status={status}")
    if order_id:
        parts.append(f"order_id={order_id}")
    return " | ".join(parts)


def _live_execution_status(response: object) -> str:
    if not isinstance(response, dict):
        return "live_submitted"
    return str(response.get("status") or "live_submitted")


def _is_missing_orderbook_error(error_text: str) -> bool:
    normalized = str(error_text or "").strip().lower()
    return "no orderbook exists for the requested token id" in normalized


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
