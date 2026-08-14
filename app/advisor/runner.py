from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable

from app.advisor.approved_execution import ApprovedExecutionResult, ApprovedExecutionService, SubmissionResult
from app.advisor.models import EvaluationResult
from app.advisor.models import MarketQuote, TradeProposal
from app.advisor.notifications import AdvisorNotificationService
from app.advisor.proposals import proposal_from_payload
from app.advisor.service import AdvisorRun, DailyAdvisor
from app.advisor.sports import ProbabilityModel, SportsMarketDiscovery, opportunities_from_quotes


@dataclass(frozen=True)
class DailyRunResult:
    run: AdvisorRun
    discovered_quotes: int
    modelled_opportunities: int
    notifications: int


class DailyAdvisorRunner:
    """One paper-first run from public market data to a WhatsApp outbox."""

    def __init__(
        self,
        advisor: DailyAdvisor,
        discovery: SportsMarketDiscovery,
        probability_model: ProbabilityModel,
        *,
        bankroll_usdc: float,
        daily_loss_usdc: float = 0.0,
        notifications: AdvisorNotificationService | None = None,
        enabled: bool = True,
    ) -> None:
        self.advisor = advisor
        self.discovery = discovery
        self.probability_model = probability_model
        self.bankroll_usdc = float(bankroll_usdc)
        self.daily_loss_usdc = float(daily_loss_usdc)
        self.notifications = notifications
        self.enabled = bool(enabled)

    def run_once(self, *, now: float | None = None) -> DailyRunResult:
        current_time = time.time() if now is None else float(now)
        if not self.enabled or self.advisor.enabled is False:
            run = self.advisor.run_daily([], now=current_time)
            return DailyRunResult(run, 0, 0, 0)
        if not self.advisor.confirmation_key or not self.advisor.store.integrity_key:
            run = self.advisor.run_daily([], now=current_time)
            return DailyRunResult(run, 0, 0, 0)
        today = time.strftime("%Y-%m-%d", time.gmtime(current_time))
        existing = self.advisor.store.get_daily_cycle(today)
        if existing is not None and str(existing["status"]) != "running":
            return _already_processed_run()
        if existing is not None and float(existing["created_at"]) > current_time - 1_800.0:
            return _already_processed_run()
        if not self.advisor.store.begin_daily_cycle(today, now=current_time):
            return _already_processed_run()
        try:
            quotes = self.discovery.discover(now=current_time)
            durable_daily_risk = self.advisor.store.daily_risk_usdc(now=current_time)
            opportunities = opportunities_from_quotes(
                quotes,
                self.probability_model,
                bankroll_usdc=self.bankroll_usdc,
                daily_loss_usdc=durable_daily_risk,
                now=current_time,
            )
            run = self.advisor.run_daily(opportunities, now=current_time, cycle_claimed=True)
        except Exception as error:  # noqa: BLE001
            self.advisor.store.complete_daily_cycle(
                today,
                "failed",
                decision_note=f"runner_{error.__class__.__name__}",
                now=current_time,
            )
            run = AdvisorRun(_failed_evaluation(error.__class__.__name__))
            return DailyRunResult(run, 0, 0, 0)
        notification_count = 0
        if run.proposal is not None and self.notifications is not None:
            notification_count = len(self.notifications.dispatch_once(now=current_time))
        return DailyRunResult(run, len(quotes), len(opportunities), notification_count)


class DailyAdvisorScheduler:
    """Small dependency-free scheduler; deployment decides whether to run it."""

    def __init__(self, runner: DailyAdvisorRunner, *, poll_seconds: float = 60.0) -> None:
        self.runner = runner
        self.poll_seconds = max(float(poll_seconds), 5.0)
        self._stop = threading.Event()

    def run_forever(self, *, on_result: Callable[[DailyRunResult], None] | None = None) -> None:
        while not self._stop.is_set():
            today = time.strftime("%Y-%m-%d", time.gmtime())
            cycle = self.runner.advisor.store.get_daily_cycle(today)
            if cycle is not None and str(cycle["status"]) != "running":
                self._stop.wait(self.poll_seconds)
                continue
            if cycle is not None and float(cycle["created_at"]) > time.time() - 1_800.0:
                self._stop.wait(self.poll_seconds)
                continue
            if not self.runner.enabled or self.runner.advisor.enabled is False:
                self._stop.wait(self.poll_seconds)
                continue
            result = self.runner.run_once()
            if on_result is not None:
                on_result(result)
            self._stop.wait(self.poll_seconds)

    def stop(self) -> None:
        self._stop.set()


class ConfirmedExecutionWorker:
    """Composition root for the explicit SI -> revalidate -> broker path."""

    def __init__(
        self,
        executor: ApprovedExecutionService,
        *,
        quote_loader: Callable[[TradeProposal], MarketQuote],
        balance_loader: Callable[[], float],
        submit_order: Callable[[TradeProposal, MarketQuote, str], SubmissionResult],
    ) -> None:
        self.executor = executor
        self.quote_loader = quote_loader
        self.balance_loader = balance_loader
        self.submit_order = submit_order

    def run_once(self, *, now: float | None = None, limit: int = 20) -> list[ApprovedExecutionResult]:
        results: list[ApprovedExecutionResult] = []
        for row in self.executor.store.confirmed_proposals(limit=limit):
            try:
                proposal = proposal_from_payload(
                    str(row["payload_json"]),
                    code_key=self.executor.store.confirmation_key,
                )
                quote = self.quote_loader(proposal)
                result = self.executor.execute(
                    proposal.proposal_id,
                    quote,
                    submit_order=self.submit_order,
                    available_balance_usdc=float(self.balance_loader()),
                    now=now,
                )
            except Exception as error:  # noqa: BLE001
                result = ApprovedExecutionResult("retry", error.__class__.__name__)
            results.append(result)
        return results


def _already_processed_run() -> DailyRunResult:
    return DailyRunResult(
        AdvisorRun(_failed_evaluation("daily_cycle_already_processed")),
        0,
        0,
        0,
    )


def _failed_evaluation(reason: str) -> EvaluationResult:
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
