from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from app.advisor.execution import ExecutionDecision, ExecutionFlags, authorize_execution
from app.advisor.config import AdvisorRuntimeConfig
from app.advisor.models import MarketQuote, TradeProposal
from app.advisor.proposals import proposal_from_payload
from app.advisor.store import AdvisorStore


@dataclass(frozen=True)
class SubmissionResult:
    status: str
    order_id: str = ""
    note: str = ""
    filled_size: float | None = None


@dataclass(frozen=True)
class ApprovedExecutionResult:
    status: str
    reason: str
    decision: ExecutionDecision | None = None
    order_id: str = ""


class ApprovedExecutionService:
    """Single-claim execution shell; the real broker is an explicit injection."""

    def __init__(self, store: AdvisorStore, *, runtime_config: AdvisorRuntimeConfig) -> None:
        self.store = store
        self.runtime_config = runtime_config

    def execute(
        self,
        proposal_id: str,
        current_quote: MarketQuote,
        *,
        submit_order: Callable[[TradeProposal, MarketQuote, str], SubmissionResult],
        available_balance_usdc: float,
        now: float | None = None,
    ) -> ApprovedExecutionResult:
        config = self.runtime_config
        if (
            not config.live_ready
            or self.store.integrity_key != config.integrity_key
            or self.store.confirmation_key != config.confirmation_key
        ):
            return ApprovedExecutionResult("rejected", "runtime_live_gate_closed")
        flags = config.execution_flags(integrity_key_loaded=bool(self.store.integrity_key))
        if not self.store.claim_confirmed(proposal_id, now=now):
            return ApprovedExecutionResult("not_claimed", "proposal_not_confirmed_or_integrity_invalid")
        row = self.store.get_proposal(proposal_id)
        if row is None:
            return ApprovedExecutionResult("failed", "proposal_missing")
        if not self.store.proposal_integrity_valid(proposal_id):
            self.store.mark_execution_failed(proposal_id, "proposal_integrity_changed_after_claim")
            return ApprovedExecutionResult("failed", "proposal_integrity_changed_after_claim")
        try:
            proposal = _proposal_from_payload(
                str(row["payload_json"]),
                code_key=self.store.confirmation_key,
            )
        except (TypeError, ValueError, KeyError) as error:
            self.store.mark_execution_failed(proposal_id, error.__class__.__name__)
            return ApprovedExecutionResult("failed", error.__class__.__name__)
        decision = authorize_execution(
            proposal,
            current_quote,
            flags,
            proposal_status="revalidating",
            available_balance_usdc=available_balance_usdc,
            daily_loss_usdc=self.store.daily_risk_usdc(now=now),
            max_daily_loss_usdc=config.maximum_daily_loss_usdc,
            integrity_verified=self.store.integrity_key == config.integrity_key,
            max_quote_age_seconds=config.maximum_quote_age_seconds,
            max_model_age_seconds=config.maximum_model_age_seconds,
            max_stake_usdc=config.maximum_stake_usdc,
            now=now,
        )
        if not decision.authorized:
            self.store.mark_execution_failed(proposal_id, decision.reason)
            return ApprovedExecutionResult("rejected", decision.reason, decision=decision)
        if not self.store.reserve_daily_loss(
            proposal_id,
            proposal.max_notional_usdc,
            config.maximum_daily_loss_usdc,
            now=now,
        ):
            self.store.mark_execution_failed(proposal_id, "daily_loss_reservation_failed")
            return ApprovedExecutionResult("rejected", "daily_loss_limit_reached", decision=decision)

        try:
            submission = submit_order(proposal, current_quote, proposal.proposal_id)
        except Exception as error:  # noqa: BLE001
            self.store.mark_reconciliation_required(proposal_id, error.__class__.__name__)
            return ApprovedExecutionResult("reconciliation_required", error.__class__.__name__, decision=decision)

        normalized_status = str(submission.status or "").lower()
        if normalized_status in {"submitted", "filled", "partial"}:
            if not self.store.mark_execution_submitted(
                proposal_id,
                submission.order_id,
                submission.note or normalized_status,
                execution_status=normalized_status,
                filled_size=submission.filled_size,
                now=now,
            ):
                self.store.mark_reconciliation_required(
                    proposal_id,
                    "submission_transition_failed",
                    order_id=submission.order_id,
                )
                return ApprovedExecutionResult(
                    "reconciliation_required",
                    "submission_transition_failed",
                    decision=decision,
                    order_id=submission.order_id,
                )
            return ApprovedExecutionResult(
                normalized_status,
                submission.note or normalized_status,
                decision=decision,
                order_id=submission.order_id,
            )
        if normalized_status in {"ambiguous", "unknown", "pending_reconciliation"}:
            self.store.mark_reconciliation_required(
                proposal_id,
                submission.note or normalized_status,
                order_id=submission.order_id,
            )
            return ApprovedExecutionResult(
                "reconciliation_required",
                submission.note or normalized_status,
                decision=decision,
                order_id=submission.order_id,
            )
        if normalized_status == "cancelled":
            self.store.mark_reconciliation_required(
                proposal_id,
                "cancelled_fill_count_unknown",
                order_id=submission.order_id,
            )
            return ApprovedExecutionResult(
                "reconciliation_required",
                "cancelled_fill_count_unknown",
                decision=decision,
                order_id=submission.order_id,
            )
        if normalized_status in {"rejected", "invalid", "insufficient_funds"}:
            self.store.mark_execution_failed(proposal_id, submission.note or normalized_status)
            return ApprovedExecutionResult("failed", submission.note or normalized_status, decision=decision)
        self.store.mark_reconciliation_required(
            proposal_id,
            submission.note or "unknown_submission_status",
            order_id=submission.order_id,
        )
        return ApprovedExecutionResult(
            "reconciliation_required",
            submission.note or "unknown_submission_status",
            decision=decision,
            order_id=submission.order_id,
        )


def _proposal_from_payload(payload_json: str, *, code_key: str = "") -> TradeProposal:
    return proposal_from_payload(payload_json, code_key=code_key)
