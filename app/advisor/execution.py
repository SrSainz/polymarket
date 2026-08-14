from __future__ import annotations

from dataclasses import dataclass
import math
import time

from app.advisor.models import MarketQuote, TradeProposal


@dataclass(frozen=True)
class ExecutionFlags:
    live_trading: bool = False
    execution_mode: str = "paper"
    dry_run: bool = True
    advisor_live_enabled: bool = False
    control_state: str = "paused"
    integrity_key_loaded: bool = False


@dataclass(frozen=True)
class ExecutionDecision:
    authorized: bool
    reason: str


def authorize_execution(
    proposal: TradeProposal,
    current_quote: MarketQuote,
    flags: ExecutionFlags,
    *,
    proposal_status: str,
    available_balance_usdc: float,
    daily_loss_usdc: float,
    max_daily_loss_usdc: float,
    integrity_verified: bool = False,
    max_quote_age_seconds: float = 60.0,
    max_model_age_seconds: float = 86_400.0,
    max_stake_usdc: float | None = None,
    now: float | None = None,
) -> ExecutionDecision:
    current_time = time.time() if now is None else float(now)
    if (
        not math.isfinite(current_time)
        or not math.isfinite(float(max_quote_age_seconds))
        or max_quote_age_seconds <= 0
        or not math.isfinite(float(max_model_age_seconds))
        or max_model_age_seconds <= 0
    ):
        return ExecutionDecision(False, "time_window_invalid")
    required_flags = (
        flags.live_trading,
        flags.execution_mode == "live",
        not flags.dry_run,
        flags.advisor_live_enabled,
        flags.control_state == "armed",
        flags.integrity_key_loaded,
        integrity_verified,
    )
    if not all(required_flags):
        return ExecutionDecision(False, "live_execution_gate_closed")
    if proposal_status != "revalidating":
        return ExecutionDecision(False, "proposal_not_confirmed")
    proposal_numeric_values = (
        proposal.created_at,
        proposal.expires_at,
        proposal.quote_expires_at,
        proposal.max_price,
        proposal.max_notional_usdc,
        proposal.probability_used,
        proposal.net_edge_bps,
        proposal.expected_profit_usdc,
        proposal.fee_bps,
        proposal.slippage_bps,
        proposal.model_brier_score,
        proposal.model_as_of,
    )
    if not all(math.isfinite(float(value)) for value in proposal_numeric_values):
        return ExecutionDecision(False, "non_finite_proposal")
    if proposal.max_price <= 0 or proposal.max_price >= 1 or proposal.max_notional_usdc <= 0:
        return ExecutionDecision(False, "proposal_terms_invalid")
    if max_stake_usdc is not None:
        if not math.isfinite(float(max_stake_usdc)) or max_stake_usdc <= 0:
            return ExecutionDecision(False, "max_stake_limit_invalid")
        if proposal.max_notional_usdc > max_stake_usdc:
            return ExecutionDecision(False, "stake_above_runtime_limit")
    if current_time >= min(proposal.expires_at, proposal.quote_expires_at):
        return ExecutionDecision(False, "proposal_or_quote_expired")
    if proposal.model_as_of > current_time or current_time - proposal.model_as_of > max_model_age_seconds:
        return ExecutionDecision(False, "model_evidence_stale")
    if (
        current_quote.market_id != proposal.market_id
        or current_quote.condition_id != proposal.condition_id
        or current_quote.token_id != proposal.token_id
        or current_quote.resolution_source != proposal.resolution_source
    ):
        return ExecutionDecision(False, "quote_identity_changed")
    if current_quote.market_status.lower() not in {"open", "active"}:
        return ExecutionDecision(False, "market_not_open")
    numeric_values = (
        current_quote.execution_price,
        current_quote.available_size,
        current_quote.observed_at,
        current_quote.fee_bps,
        current_quote.slippage_bps,
        current_quote.min_order_size,
        available_balance_usdc,
        daily_loss_usdc,
        max_daily_loss_usdc,
    )
    if not all(math.isfinite(float(value)) for value in numeric_values):
        return ExecutionDecision(False, "non_finite_revalidation")
    if current_quote.execution_price <= 0 or current_quote.execution_price >= 1:
        return ExecutionDecision(False, "quote_price_invalid")
    if current_quote.fee_bps < 0 or current_quote.slippage_bps < 0:
        return ExecutionDecision(False, "quote_cost_invalid")
    if current_quote.available_size <= 0 or current_quote.min_order_size < 0:
        return ExecutionDecision(False, "quote_liquidity_invalid")
    if current_quote.observed_at > current_time or current_time - current_quote.observed_at > max_quote_age_seconds:
        return ExecutionDecision(False, "quote_stale")
    if current_quote.observed_at < proposal.created_at:
        return ExecutionDecision(False, "quote_not_newer_than_proposal")
    if current_quote.execution_price > proposal.max_price:
        return ExecutionDecision(False, "price_worse_than_approved")
    if current_quote.fee_bps + current_quote.slippage_bps > proposal.fee_bps + proposal.slippage_bps:
        return ExecutionDecision(False, "execution_cost_worse_than_approved")
    approved_shares = proposal.max_notional_usdc / current_quote.execution_price
    if current_quote.available_size < approved_shares:
        return ExecutionDecision(False, "liquidity_below_approved_amount")
    if current_quote.min_order_size > 0 and approved_shares < current_quote.min_order_size:
        return ExecutionDecision(False, "below_exchange_minimum")
    if available_balance_usdc < proposal.max_notional_usdc:
        return ExecutionDecision(False, "balance_below_approved_amount")
    if daily_loss_usdc < 0:
        return ExecutionDecision(False, "daily_loss_invalid")
    if daily_loss_usdc + proposal.max_notional_usdc > max_daily_loss_usdc:
        return ExecutionDecision(False, "daily_loss_limit_reached")
    return ExecutionDecision(True, "authorized_after_revalidation")
