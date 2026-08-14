from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from app.advisor.approval import ApprovalService
from app.advisor.approved_execution import ApprovedExecutionService, SubmissionResult
from app.advisor.broker import PolymarketCLOBSubmitter
from app.advisor.config import AdvisorRuntimeConfig
from app.advisor.notifications import AdvisorNotificationService
from app.advisor.probability import JsonProbabilityModel
from app.advisor.runner import ConfirmedExecutionWorker, DailyAdvisorRunner
from app.advisor.service import DailyAdvisor
from app.advisor.sports import ProbabilityModel, SportsMarketDiscovery
from app.advisor.store import AdvisorStore
from app.advisor.webhook import build_webhook_handler
from app.advisor.whatsapp import WhatsAppGateway
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.settings import EnvSettings


@dataclass
class AdvisorRuntime:
    """All server-side dependencies for the paper-first advisor."""

    config: AdvisorRuntimeConfig
    env: EnvSettings
    store: AdvisorStore
    advisor: DailyAdvisor
    discovery: SportsMarketDiscovery
    notifications: AdvisorNotificationService
    approval: ApprovalService
    runner: DailyAdvisorRunner
    executor: ApprovedExecutionService
    webhook_handler: type

    def execution_worker(
        self,
        *,
        quote_loader: Callable,
        balance_loader: Callable[[], float],
        submit_order: Callable[[object, object, str], SubmissionResult],
    ) -> ConfirmedExecutionWorker:
        return ConfirmedExecutionWorker(
            self.executor,
            quote_loader=quote_loader,
            balance_loader=balance_loader,
            submit_order=submit_order,
        )

    def authenticated_execution_worker(self) -> ConfirmedExecutionWorker:
        """Build the guarded CLOB v2 worker without starting it."""

        if not self.config.live_ready:
            raise RuntimeError("runtime_live_gate_closed")
        submitter = PolymarketCLOBSubmitter(self.discovery.clob)

        def quote_loader(proposal):  # noqa: ANN001
            return self.discovery.quote_for_identity(
                market_id=proposal.market_id,
                condition_id=proposal.condition_id,
                token_id=proposal.token_id,
                outcome=proposal.outcome,
            )

        def balance_loader() -> float:
            balance = self.discovery.clob.get_collateral_balance()
            return float(balance.get("balance", 0.0))

        return self.execution_worker(
            quote_loader=quote_loader,
            balance_loader=balance_loader,
            submit_order=submitter,
        )

    def close(self) -> None:
        self.runner.discovery.clob.close()
        self.store.close()


def build_advisor_runtime(
    *,
    db_path: Path,
    probability_model: ProbabilityModel | None = None,
    bankroll_usdc: float,
    daily_loss_usdc: float = 0.0,
    evidence_path: Path | None = None,
) -> AdvisorRuntime:
    """Compose the complete server-side graph without enabling live by itself."""
    config = AdvisorRuntimeConfig.from_env()
    env = EnvSettings.from_env()
    store = AdvisorStore(
        db_path,
        integrity_key=config.integrity_key,
        confirmation_key=config.confirmation_key,
    )
    if config.paper_ready:
        advisor = DailyAdvisor.from_runtime_config(store, config)
    else:
        advisor = DailyAdvisor(
            store,
            policy=config.policy(),
            confirmation_key=config.confirmation_key,
            enabled=False,
        )
    gamma = GammaClient(env.gamma_api_host)
    clob = CLOBClient(env.clob_host, env)
    discovery = SportsMarketDiscovery(gamma, clob)
    model = probability_model or JsonProbabilityModel(
        evidence_path or Path(config.evidence_path)
    )
    gateway = WhatsAppGateway(config.whatsapp)
    notifications = AdvisorNotificationService(store, gateway, recipient=config.recipient)
    approval = ApprovalService(
        store,
        allowed_numbers=config.whatsapp.allowed_numbers,
        app_secret=config.whatsapp.app_secret,
    )
    runner = DailyAdvisorRunner(
        advisor,
        discovery,
        model,
        bankroll_usdc=bankroll_usdc,
        daily_loss_usdc=daily_loss_usdc,
        notifications=notifications,
        enabled=config.paper_ready,
    )
    return AdvisorRuntime(
        config=config,
        env=env,
        store=store,
        advisor=advisor,
        discovery=discovery,
        notifications=notifications,
        approval=approval,
        runner=runner,
        executor=ApprovedExecutionService(store, runtime_config=config),
        webhook_handler=build_webhook_handler(approval, config.whatsapp),
    )
