from __future__ import annotations

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
    ) -> None:
        self.db = db
        self.clob_client = clob_client
        self.env = env
        self.slippage_limit = max(float(slippage_limit), 0.0)
        self.execution_profile = str(execution_profile or "taker_fok").strip().lower()

    def execute(self, instruction: CopyInstruction) -> ExecutionResult:
        if not self.env.live_trading:
            raise RuntimeError("LIVE_TRADING=false. Live broker is disabled.")

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

        fill_size = float(fill["size"])
        fill_price = float(fill["price"])
        fill_notional = float(fill["notional"])
        return apply_fill_to_database(
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
    return str(response.get("status") or "live_submitted")


def _is_missing_orderbook_error(error_text: str) -> bool:
    normalized = str(error_text or "").strip().lower()
    return "no orderbook exists for the requested token id" in normalized


def _resolve_execution_profile(*, instruction: CopyInstruction, default_profile: str) -> str:
    profile = str(instruction.execution_profile or default_profile or "taker_fok").strip().lower()
    if profile in {"taker_fok", "taker_fak", "maker_gtd", "maker_post_only_gtc"}:
        return profile
    return "taker_fok"


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
