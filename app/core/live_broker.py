from __future__ import annotations

import json
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from app.core.execution_engine import apply_fill_to_database
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, TradeSide
from app.polymarket.clob_client import CLOBClient
from app.settings import EnvSettings


class LiveBroker:
    def __init__(
        self,
        db: Database,
        clob_client: CLOBClient,
        env: EnvSettings,
        *,
        slippage_limit: float = 0.03,
        execution_profile: str = "taker_fok",
        dry_run: bool = False,
    ) -> None:
        self.db = db
        self.clob_client = clob_client
        self.env = env
        self.slippage_limit = max(float(slippage_limit), 0.0)
        self.execution_profile = str(execution_profile or "taker_fok").strip().lower()
        self.dry_run = bool(dry_run)

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        if not self.env.live_trading:
            raise RuntimeError("LIVE_TRADING=false. Live broker is disabled.")
        if self.dry_run:
            raise RuntimeError("dry_run=true. Live broker is disabled until explicitly disabled.")

        profile = _resolve_execution_profile(
            instruction=instruction,
            default_profile=self.execution_profile,
        )
        try:
            if profile in {"maker_gtd", "maker_post_only_gtc"}:
                response = self.clob_client.place_limit_order(
                    token_id=instruction.asset,
                    side=instruction.side.value,
                    price=_passive_limit_price(
                        reference_price=instruction.price,
                        side=instruction.side,
                    ),
                    size=instruction.size,
                    order_type="GTD" if profile == "maker_gtd" else "GTC",
                    post_only=profile == "maker_post_only_gtc",
                )
            else:
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
                    order_type="FAK" if profile == "taker_fak" else "FOK",
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
        order_id = _extract_order_id(response)
        status = _live_execution_status(response)
        if order_id:
            _persist_pending_live_order(
                db=self.db,
                order_id=order_id,
                instruction=instruction,
                profile=profile,
                response=response,
            )
        if profile in {"maker_gtd", "maker_post_only_gtc"}:
            return ExecutionResult(
                mode="live",
                status="submitted",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message=_live_execution_notes(response, prefix=f"{profile} submitted"),
            )
        if status in {"failed", "rejected", "cancelled", "canceled", "expired", "unmatched"}:
            if order_id:
                self.db.delete_bot_state(_pending_live_order_key(order_id))
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
        fill = _extract_live_fill(response=response, instruction=instruction)
        if order_id:
            return ExecutionResult(
                mode="live",
                status="submitted",
                action=instruction.action,
                asset=instruction.asset,
                size=0.0,
                price=instruction.price,
                notional=0.0,
                pnl_delta=0.0,
                message=_live_execution_notes(response, prefix=f"{profile} awaiting_confirmation"),
            )
        if bool(fill.get("ambiguous")):
            raise RuntimeError(str(fill.get("message") or "ambiguous live fill response"))
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

        fill_size = float(fill["size"])
        fill_price = float(fill["price"])
        fill_notional = float(fill["notional"])
        result = apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode="live",
            filled_size=fill_size,
            fill_price=fill_price,
            fill_notional=fill_notional,
            message=str(response),
            status="filled",
            notes=_live_execution_notes(response, prefix=profile),
        )
        if order_id:
            self.db.delete_bot_state(_pending_live_order_key(order_id))
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


def _passive_limit_price(*, reference_price: float, side: TradeSide) -> float:
    price = max(float(reference_price), 0.0)
    if price <= 0:
        return 0.0
    if side == TradeSide.BUY:
        return _quantize_price(max(price - 0.0001, 0.01), rounding=ROUND_DOWN)
    return _quantize_price(min(price + 0.0001, 0.99), rounding=ROUND_UP)


def _extract_live_fill(*, response: object, instruction: CopyInstruction) -> dict[str, float | bool]:
    if not isinstance(response, dict):
        return {
            "filled": False,
            "ambiguous": True,
            "size": 0.0,
            "price": 0.0,
            "notional": 0.0,
            "message": "live response is not a dict; cannot confirm fill",
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
    if filled_notional > 0:
        inferred_price = direct_price if direct_price > 0 else instruction.price
        if inferred_price > 0:
            return {
                "filled": True,
                "size": filled_notional / inferred_price,
                "price": inferred_price,
                "notional": filled_notional,
            }
    if status in {"live", "delayed", "unmatched", "cancelled"} and not trade_ids:
        return {"filled": False, "size": 0.0, "price": 0.0, "notional": 0.0}
    if status == "matched" or trade_ids:
        return {
            "filled": False,
            "ambiguous": True,
            "size": 0.0,
            "price": 0.0,
            "notional": 0.0,
            "message": "matched live order without explicit fill quantities",
        }
    return {"filled": False, "size": 0.0, "price": 0.0, "notional": 0.0}


def _live_execution_notes(response: object, *, prefix: str = "live fill") -> str:
    if not isinstance(response, dict):
        return prefix

    order_id = response.get("orderID") or response.get("orderId") or response.get("id")
    status = response.get("status")
    parts = [prefix]
    if status:
        parts.append(f"status={status}")
    if order_id:
        parts.append(f"order_id={order_id}")
    return " | ".join(parts)


def _live_execution_status(response: object) -> str:
    if not isinstance(response, dict):
        return "live_submitted"
    return str(response.get("status") or "live_submitted").strip().lower()


def _is_missing_orderbook_error(error_text: str) -> bool:
    normalized = str(error_text or "").strip().lower()
    return "no orderbook exists for the requested token id" in normalized


def _resolve_execution_profile(*, instruction: CopyInstruction, default_profile: str) -> str:
    profile = str(instruction.execution_profile or default_profile or "taker_fok").strip().lower()
    if profile in {"taker_fok", "taker_fak", "maker_gtd", "maker_post_only_gtc"}:
        return profile
    return "taker_fok"


def _extract_order_id(response: object) -> str:
    if not isinstance(response, dict):
        return ""
    return str(response.get("orderID") or response.get("orderId") or response.get("id") or "").strip()


def _pending_live_order_key(order_id: str) -> str:
    return f"live_pending_order:{str(order_id or '').strip()}"


def _persist_pending_live_order(
    *,
    db: Database,
    order_id: str,
    instruction: CopyInstruction,
    profile: str,
    response: object,
) -> None:
    payload = {
        "order_id": str(order_id or "").strip(),
        "action": instruction.action.value,
        "side": instruction.side.value,
        "asset": instruction.asset,
        "condition_id": instruction.condition_id,
        "size": float(instruction.size or 0.0),
        "price": float(instruction.price or 0.0),
        "notional": float(instruction.notional or 0.0),
        "source_wallet": instruction.source_wallet,
        "source_signal_id": int(instruction.source_signal_id or 0),
        "title": instruction.title,
        "slug": instruction.slug,
        "outcome": instruction.outcome,
        "category": instruction.category,
        "reason": instruction.reason,
        "execution_profile": str(profile or ""),
        "response_status": _live_execution_status(response),
    }
    db.set_bot_state(_pending_live_order_key(order_id), json.dumps(payload, separators=(",", ":"), sort_keys=True))


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
