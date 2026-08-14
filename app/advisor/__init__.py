"""Safe daily opportunity advisor primitives.

This package is deliberately separate from the archived BTC5m runtime. It can
produce and persist proposals, but it does not start a live process or place
orders by itself.
"""

from app.advisor.evaluator import AdvisorPolicy, OpportunityEvaluator
from app.advisor.config import AdvisorRuntimeConfig
from app.advisor.approved_execution import ApprovedExecutionService, SubmissionResult
from app.advisor.models import MarketQuote, ModelEvidence, Opportunity
from app.advisor.notifications import AdvisorNotificationService, NotificationResult
from app.advisor.proposals import TradeProposal, build_proposal
from app.advisor.service import DailyAdvisor
from app.advisor.sports import ProbabilityModel, SportsMarketDiscovery
from app.advisor.probability import JsonProbabilityModel
from app.advisor.runner import ConfirmedExecutionWorker, DailyAdvisorRunner, DailyAdvisorScheduler
from app.advisor.broker import PolymarketCLOBSubmitter
from app.advisor.webhook import build_webhook_handler, run_webhook_server
from app.advisor.runtime import AdvisorRuntime, build_advisor_runtime

__all__ = [
    "AdvisorPolicy",
    "AdvisorRuntimeConfig",
    "ApprovedExecutionService",
    "DailyAdvisor",
    "MarketQuote",
    "ModelEvidence",
    "AdvisorNotificationService",
    "NotificationResult",
    "Opportunity",
    "OpportunityEvaluator",
    "SubmissionResult",
    "TradeProposal",
    "build_proposal",
    "DailyAdvisorRunner",
    "DailyAdvisorScheduler",
    "ConfirmedExecutionWorker",
    "PolymarketCLOBSubmitter",
    "SportsMarketDiscovery",
    "JsonProbabilityModel",
    "build_webhook_handler",
    "run_webhook_server",
    "AdvisorRuntime",
    "build_advisor_runtime",
]
