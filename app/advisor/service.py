from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Iterable

from app.advisor.config import AdvisorRuntimeConfig
from app.advisor.evaluator import AdvisorPolicy, OpportunityEvaluator
from app.advisor.models import EvaluationResult, Opportunity, TradeProposal
from app.advisor.proposals import build_proposal
from app.advisor.store import AdvisorStore


@dataclass(frozen=True)
class AdvisorRun:
    evaluation: EvaluationResult
    proposal: TradeProposal | None = None


class DailyAdvisor:
    """Turn one independently modelled opportunity into a durable proposal."""

    def __init__(
        self,
        store: AdvisorStore,
        *,
        policy: AdvisorPolicy | None = None,
        confirmation_key: str = "",
        enabled: bool | None = None,
        enforce_daily_cycle: bool = False,
    ) -> None:
        self.store = store
        self.policy = policy or AdvisorPolicy()
        self.confirmation_key = str(confirmation_key or store.confirmation_key)
        self.enabled = enabled
        self.enforce_daily_cycle = bool(enforce_daily_cycle)
        self.evaluator = OpportunityEvaluator(self.policy)

    @classmethod
    def from_runtime_config(cls, store: AdvisorStore, config: AdvisorRuntimeConfig) -> "DailyAdvisor":
        if not config.paper_ready:
            raise RuntimeError("Advisor disabled or missing server-side integrity/confirmation keys")
        return cls(
            store,
            policy=config.policy(),
            confirmation_key=config.confirmation_key,
            enabled=True,
            enforce_daily_cycle=True,
        )

    def analyze(self, opportunity: Opportunity, *, now: float | None = None) -> AdvisorRun:
        if self.enabled is False:
            raise RuntimeError("advisor disabled")
        if not self.confirmation_key or not self.store.integrity_key:
            raise RuntimeError("server-side confirmation and integrity keys are required before creating a proposal")
        if self.enforce_daily_cycle:
            return self.run_daily([opportunity], now=now)
        evaluation = self.evaluator.evaluate(opportunity, now=now)
        if not evaluation.eligible:
            return AdvisorRun(evaluation=evaluation)
        proposal = build_proposal(
            opportunity,
            evaluation,
            policy=self.policy,
            confirmation_key=self.confirmation_key,
            now=now,
        )
        self.store.save_proposal(proposal)
        self.store.enqueue(proposal.proposal_id, now=now)
        return AdvisorRun(evaluation=evaluation, proposal=proposal)

    def run_daily(
        self,
        opportunities: Iterable[Opportunity],
        *,
        cycle_day: str | None = None,
        now: float | None = None,
        cycle_claimed: bool = False,
    ) -> AdvisorRun:
        """Evaluate a candidate set and persist at most one proposal per UTC day.

        Market discovery and probability modelling remain explicit inputs. This
        method only selects the strongest eligible candidate and owns the
        once-per-day persistence boundary.
        """
        current_time = time.time() if now is None else float(now)
        if self.enabled is False:
            return AdvisorRun(evaluation=_runtime_keys_missing("advisor_disabled"))
        if not self.confirmation_key or not self.store.integrity_key:
            return AdvisorRun(evaluation=_runtime_keys_missing())
        if not math.isfinite(current_time):
            return AdvisorRun(evaluation=_runtime_keys_missing("invalid_now"))
        expected_day = time.strftime("%Y-%m-%d", time.gmtime(current_time))
        if cycle_day is not None and str(cycle_day).strip() != expected_day:
            evaluation = EvaluationResult(
                eligible=False,
                reason="cycle_day_mismatch",
                reasons=("cycle_day_mismatch",),
                probability_used=0.0,
                net_edge_bps=0.0,
                expected_profit_usdc=0.0,
                recommended_stake_usdc=0.0,
                price_limit=0.0,
            )
            return AdvisorRun(evaluation=evaluation)
        day = expected_day
        if cycle_claimed:
            cycle = self.store.get_daily_cycle(day)
            if cycle is None or str(cycle["status"]) != "running":
                return AdvisorRun(evaluation=_daily_cycle_taken(_empty_evaluation("daily_cycle_already_processed")))
        elif not self.store.begin_daily_cycle(day, now=current_time):
            return AdvisorRun(evaluation=_daily_cycle_taken(_empty_evaluation("daily_cycle_already_processed")))
        candidates = list(opportunities)
        evaluations = [(candidate, self.evaluator.evaluate(candidate, now=current_time)) for candidate in candidates]
        if not evaluations:
            evaluation = EvaluationResult(
                eligible=False,
                reason="no_candidates",
                reasons=("no_candidates",),
                probability_used=0.0,
                net_edge_bps=0.0,
                expected_profit_usdc=0.0,
                recommended_stake_usdc=0.0,
                price_limit=0.0,
            )
            if not self.store.complete_daily_cycle(day, "no_opportunity", decision_note="no_candidates", now=current_time):
                evaluation = _daily_cycle_taken(evaluation)
            return AdvisorRun(evaluation=evaluation)

        candidate, evaluation = max(
            evaluations,
            key=lambda item: (item[1].eligible, item[1].net_edge_bps, item[1].expected_profit_usdc),
        )
        if not evaluation.eligible:
            if not self.store.complete_daily_cycle(
                day,
                "no_opportunity",
                decision_note=evaluation.reason,
                now=current_time,
            ):
                evaluation = _daily_cycle_taken(evaluation)
            return AdvisorRun(evaluation=evaluation)

        proposal = build_proposal(
            candidate,
            evaluation,
            policy=self.policy,
            confirmation_key=self.confirmation_key,
            now=current_time,
        )
        if not self.store.create_daily_proposal(day, proposal):
            return AdvisorRun(evaluation=_daily_cycle_taken(evaluation))
        return AdvisorRun(evaluation=evaluation, proposal=proposal)


def _daily_cycle_taken(evaluation: EvaluationResult) -> EvaluationResult:
    reasons = tuple(dict.fromkeys(("daily_cycle_already_processed", *evaluation.reasons)))
    return EvaluationResult(
        eligible=False,
        reason="daily_cycle_already_processed",
        reasons=reasons,
        probability_used=evaluation.probability_used,
        net_edge_bps=evaluation.net_edge_bps,
        expected_profit_usdc=evaluation.expected_profit_usdc,
        recommended_stake_usdc=0.0,
        price_limit=evaluation.price_limit,
    )


def _runtime_keys_missing(reason: str = "runtime_keys_missing") -> EvaluationResult:
    return EvaluationResult(
        eligible=False,
        reason=reason,
        reasons=(reason,),
        probability_used=0.0,
        net_edge_bps=0.0,
        expected_profit_usdc=0.0,
        recommended_stake_usdc=0.0,
        price_limit=0.0,
    )


def _empty_evaluation(reason: str) -> EvaluationResult:
    return EvaluationResult(
        eligible=False,
        reason=reason,
        reasons=(reason,),
        probability_used=0.0,
        net_edge_bps=0.0,
        expected_profit_usdc=0.0,
        recommended_stake_usdc=0.0,
        price_limit=0.0,
    )
