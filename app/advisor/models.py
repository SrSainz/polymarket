from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MarketQuote:
    """Executable public quote captured at one point in time.

    ``execution_price`` must be the price used for sizing, normally the best
    ask for a BUY. A midpoint is not sufficient for an executable proposal.
    """

    market_id: str
    condition_id: str
    token_id: str
    title: str
    outcome: str
    execution_price: float
    available_size: float
    observed_at: float
    market_status: str = "open"
    resolution_source: str = ""
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    source_url: str = ""
    min_order_size: float = 0.0


@dataclass(frozen=True)
class ModelEvidence:
    """Versioned independent probability evidence.

    The evaluator refuses uncalibrated or incomplete evidence. A market price
    alone is not an independent probability estimate.
    """

    model_name: str
    model_version: str
    probability: float
    lower_probability: float
    upper_probability: float
    calibrated: bool
    sample_size: int
    brier_score: float | None
    as_of: float
    source_refs: tuple[str, ...] = field(default_factory=tuple)
    independent: bool = False
    market_id: str = ""
    condition_id: str = ""
    token_id: str = ""
    outcome: str = ""


@dataclass(frozen=True)
class Opportunity:
    quote: MarketQuote
    evidence: ModelEvidence
    bankroll_usdc: float
    daily_loss_usdc: float = 0.0
    requested_stake_usdc: float | None = None


@dataclass(frozen=True)
class EvaluationResult:
    eligible: bool
    reason: str
    reasons: tuple[str, ...]
    probability_used: float
    net_edge_bps: float
    expected_profit_usdc: float
    recommended_stake_usdc: float
    price_limit: float


@dataclass(frozen=True)
class TradeProposal:
    """Immutable terms the user is asked to approve."""

    proposal_id: str
    confirmation_code: str
    created_at: float
    expires_at: float
    quote_expires_at: float
    market_id: str
    condition_id: str
    token_id: str
    title: str
    outcome: str
    side: str
    max_price: float
    max_notional_usdc: float
    probability_used: float
    probability_interval: tuple[float, float]
    net_edge_bps: float
    expected_profit_usdc: float
    fee_bps: float
    slippage_bps: float
    model_name: str
    model_version: str
    model_sample_size: int
    model_brier_score: float
    model_independent: bool
    resolution_source: str
    evidence_refs: tuple[str, ...] = field(default_factory=tuple)
    source_url: str = ""
    model_as_of: float = 0.0

    @property
    def display_code(self) -> str:
        return self.confirmation_code
