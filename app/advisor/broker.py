from __future__ import annotations

from typing import Any

from app.advisor.approved_execution import SubmissionResult
from app.advisor.models import MarketQuote, TradeProposal


class PolymarketCLOBSubmitter:
    """Explicit adapter for the repository's authenticated CLOB client.

    It never retries an ambiguous response. The caller must reconcile that
    response before the reservation can be settled.
    """

    def __init__(self, clob_client: Any, *, order_type: str = "FOK") -> None:
        self.clob_client = clob_client
        self.order_type = str(order_type or "FOK").upper()

    def __call__(self, proposal: TradeProposal, quote: MarketQuote, idempotency_key: str) -> SubmissionResult:
        if proposal.side.lower() != "buy":
            return SubmissionResult("invalid", note="only_buy_proposals_supported")
        size = proposal.max_notional_usdc / quote.execution_price
        if size <= 0:
            return SubmissionResult("invalid", note="computed_size_invalid")
        payload = self.clob_client.place_market_order(
            quote.token_id,
            "BUY",
            size,
            notional=proposal.max_notional_usdc,
            limit_price=proposal.max_price,
            order_type=self.order_type,
        )
        if not isinstance(payload, dict):
            return SubmissionResult("ambiguous", note="clob_response_not_object")
        order_id = str(payload.get("orderID") or payload.get("order_id") or payload.get("id") or "")
        status = str(payload.get("status") or "").lower()
        if not order_id:
            return SubmissionResult("ambiguous", note="clob_order_id_missing")
        if status in {"matched", "filled", "executed"}:
            # CLOB making/taking amounts can be raw integer units. Do not
            # guess a share fill from them; reconcile the order for exact size.
            filled_size = _positive_float(payload.get("filled_size") or payload.get("size_matched"))
            return SubmissionResult("filled" if filled_size > 0 else "submitted", order_id, status, filled_size or None)
        if status in {"live", "open", "unmatched", "pending"}:
            return SubmissionResult("submitted", order_id, status)
        if status in {"cancelled", "canceled"}:
            return SubmissionResult("cancelled", order_id, status)
        if status in {"rejected", "failed", "invalid"}:
            return SubmissionResult("rejected", order_id, status)
        return SubmissionResult("ambiguous", order_id, status or "unknown_clob_status")


def _positive_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed > 0 else 0.0
