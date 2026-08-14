from __future__ import annotations

from dataclasses import dataclass
import math
import time

from app.advisor.models import EvaluationResult, Opportunity


@dataclass(frozen=True)
class AdvisorPolicy:
    """Conservative gates applied before a proposal can be created."""

    minimum_probability: float = 0.60
    minimum_edge_bps: float = 150.0
    maximum_edge_bps: float = 5_000.0
    minimum_model_sample_size: int = 200
    maximum_uncertainty_width: float = 0.20
    maximum_quote_age_seconds: float = 60.0
    maximum_model_age_seconds: float = 86_400.0
    maximum_stake_usdc: float = 5.0
    maximum_bankroll_fraction: float = 0.05
    maximum_daily_loss_usdc: float = 5.0
    minimum_stake_usdc: float = 1.0
    proposal_ttl_seconds: float = 900.0
    maximum_price_drift_bps: float = 0.0

    def __post_init__(self) -> None:
        positive = {
            "minimum_probability": self.minimum_probability,
            "minimum_edge_bps": self.minimum_edge_bps,
            "maximum_edge_bps": self.maximum_edge_bps,
            "maximum_quote_age_seconds": self.maximum_quote_age_seconds,
            "maximum_model_age_seconds": self.maximum_model_age_seconds,
            "maximum_stake_usdc": self.maximum_stake_usdc,
            "maximum_bankroll_fraction": self.maximum_bankroll_fraction,
            "maximum_daily_loss_usdc": self.maximum_daily_loss_usdc,
            "minimum_stake_usdc": self.minimum_stake_usdc,
            "proposal_ttl_seconds": self.proposal_ttl_seconds,
        }
        if not all(math.isfinite(float(value)) and float(value) > 0 for value in positive.values()):
            raise ValueError("advisor policy limits must be finite and positive")
        if not 0 < self.minimum_probability < 1:
            raise ValueError("minimum_probability must be between 0 and 1")
        if not 0 < self.maximum_bankroll_fraction <= 1:
            raise ValueError("maximum_bankroll_fraction must be in (0, 1]")
        if not math.isfinite(float(self.minimum_model_sample_size)) or self.minimum_model_sample_size <= 0:
            raise ValueError("minimum_model_sample_size must be positive")
        if not math.isfinite(float(self.maximum_uncertainty_width)) or not 0 <= self.maximum_uncertainty_width < 1:
            raise ValueError("maximum_uncertainty_width must be in [0, 1)")
        if not math.isfinite(float(self.maximum_price_drift_bps)) or self.maximum_price_drift_bps < 0:
            raise ValueError("maximum_price_drift_bps must be non-negative")
        if self.maximum_edge_bps < self.minimum_edge_bps:
            raise ValueError("maximum_edge_bps must be >= minimum_edge_bps")


class OpportunityEvaluator:
    def __init__(self, policy: AdvisorPolicy | None = None) -> None:
        self.policy = policy or AdvisorPolicy()

    def evaluate(self, opportunity: Opportunity, *, now: float | None = None) -> EvaluationResult:
        current_time = time.time() if now is None else float(now)
        quote = opportunity.quote
        evidence = opportunity.evidence
        reasons: list[str] = []

        if not math.isfinite(current_time):
            reasons.append("invalid_now")

        numeric_values = [
            quote.execution_price,
            quote.available_size,
            quote.observed_at,
            quote.fee_bps,
            quote.slippage_bps,
            quote.min_order_size,
            evidence.probability,
            evidence.lower_probability,
            evidence.upper_probability,
            evidence.as_of,
            opportunity.bankroll_usdc,
            opportunity.daily_loss_usdc,
        ]
        if opportunity.requested_stake_usdc is not None:
            numeric_values.append(opportunity.requested_stake_usdc)
        if not all(math.isfinite(float(value)) for value in numeric_values):
            reasons.append("non_finite_input")

        if quote.market_status.lower() not in {"open", "active"}:
            reasons.append("market_not_open")
        if not quote.market_id or not quote.condition_id or not quote.token_id:
            reasons.append("market_identity_missing")
        if not quote.resolution_source:
            reasons.append("resolution_source_missing")
        if (
            evidence.market_id != quote.market_id
            or evidence.condition_id != quote.condition_id
            or evidence.token_id != quote.token_id
            or evidence.outcome != quote.outcome
        ):
            reasons.append("evidence_market_mismatch")
        if quote.execution_price <= 0 or quote.execution_price >= 1:
            reasons.append("execution_price_invalid")
        if quote.fee_bps < 0 or quote.slippage_bps < 0:
            reasons.append("execution_cost_invalid")
        if quote.available_size <= 0 or quote.min_order_size < 0:
            reasons.append("liquidity_invalid")
        if quote.available_size <= 0:
            reasons.append("visible_liquidity_missing")
        if quote.observed_at > current_time:
            reasons.append("quote_from_future")
        elif current_time - quote.observed_at > self.policy.maximum_quote_age_seconds:
            reasons.append("quote_stale")
        if evidence.as_of > current_time:
            reasons.append("model_evidence_from_future")
        elif current_time - evidence.as_of > self.policy.maximum_model_age_seconds:
            reasons.append("model_evidence_stale")
        if not evidence.calibrated:
            reasons.append("model_not_calibrated")
        if not evidence.independent:
            reasons.append("independent_model_required")
        if not str(evidence.model_name or "").strip():
            reasons.append("model_name_missing")
        if not str(evidence.model_version or "").strip():
            reasons.append("model_version_missing")
        if not evidence.source_refs:
            reasons.append("evidence_source_missing")
        if evidence.sample_size < self.policy.minimum_model_sample_size:
            reasons.append("model_sample_too_small")
        if evidence.brier_score is None or not math.isfinite(float(evidence.brier_score)):
            reasons.append("calibration_metric_missing")
        elif not 0 <= float(evidence.brier_score) <= 1:
            reasons.append("calibration_metric_invalid")
        if not 0 < evidence.lower_probability <= evidence.probability <= evidence.upper_probability < 1:
            reasons.append("probability_interval_invalid")
        if evidence.upper_probability - evidence.lower_probability > self.policy.maximum_uncertainty_width:
            reasons.append("uncertainty_too_wide")
        if opportunity.daily_loss_usdc >= self.policy.maximum_daily_loss_usdc:
            reasons.append("daily_loss_limit_reached")
        if opportunity.bankroll_usdc <= 0:
            reasons.append("bankroll_missing")

        probability_used = max(0.0, min(1.0, evidence.lower_probability))
        price = float(quote.execution_price)
        cost_drag = price * max(quote.fee_bps + quote.slippage_bps, 0.0) / 10_000.0
        net_delta_per_share = probability_used - price - cost_drag
        net_edge_bps = (net_delta_per_share / price * 10_000.0) if price > 0 else 0.0

        if probability_used <= self.policy.minimum_probability:
            reasons.append("probability_below_threshold")
        if net_edge_bps <= 0:
            reasons.append("net_edge_non_positive")
        if net_edge_bps <= self.policy.minimum_edge_bps:
            reasons.append("net_edge_below_threshold")
        if net_edge_bps > self.policy.maximum_edge_bps:
            reasons.append("net_edge_above_safety_band")

        bankroll_cap = opportunity.bankroll_usdc * self.policy.maximum_bankroll_fraction
        requested = opportunity.requested_stake_usdc
        if requested is None:
            requested_cap = float("inf")
        elif requested <= 0:
            reasons.append("requested_stake_invalid")
            requested_cap = 0.0
        else:
            requested_cap = requested
        depth_cap = quote.available_size * price
        remaining_loss_cap = max(
            self.policy.maximum_daily_loss_usdc - max(opportunity.daily_loss_usdc, 0.0),
            0.0,
        )
        stake = min(
            self.policy.maximum_stake_usdc,
            max(bankroll_cap, 0.0),
            max(depth_cap, 0.0),
            remaining_loss_cap,
            requested_cap,
        )
        if stake < self.policy.minimum_stake_usdc:
            reasons.append("stake_below_minimum")
        if quote.min_order_size > 0 and stake / price < quote.min_order_size:
            reasons.append("below_exchange_minimum")

        expected_profit = (stake / price * net_delta_per_share) if price > 0 else 0.0
        eligible = not reasons
        return EvaluationResult(
            eligible=eligible,
            reason="eligible" if eligible else reasons[0],
            reasons=tuple(reasons),
            probability_used=probability_used,
            net_edge_bps=net_edge_bps,
            expected_profit_usdc=expected_profit,
            recommended_stake_usdc=stake if eligible else 0.0,
            price_limit=price,
        )
