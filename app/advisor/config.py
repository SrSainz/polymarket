from __future__ import annotations

from dataclasses import dataclass, field
import math
import os
import re

from app.advisor.evaluator import AdvisorPolicy
from app.advisor.whatsapp import WhatsAppConfig


@dataclass(frozen=True, repr=False)
class AdvisorRuntimeConfig:
    """Server-side advisor configuration; defaults are fully disabled."""

    enabled: bool = False
    live_enabled: bool = False
    broker_configured: bool = False
    control_state: str = "paused"
    global_live_trading: bool = False
    global_execution_mode: str = "paper"
    global_dry_run: bool = True
    integrity_key: str = field(default="", repr=False)
    confirmation_key: str = field(default="", repr=False)
    recipient: str = ""
    minimum_probability: float = 0.60
    minimum_edge_bps: float = 150.0
    maximum_edge_bps: float = 5_000.0
    maximum_stake_usdc: float = 5.0
    maximum_daily_loss_usdc: float = 5.0
    maximum_quote_age_seconds: float = 60.0
    maximum_model_age_seconds: float = 86_400.0
    proposal_ttl_seconds: float = 900.0
    evidence_path: str = ""
    whatsapp: WhatsAppConfig = field(default_factory=WhatsAppConfig, repr=False)

    def __post_init__(self) -> None:
        if self.integrity_key and (self.integrity_key != self.integrity_key.strip() or len(self.integrity_key.strip()) < 32):
            raise ValueError("integrity_key must contain at least 32 characters")
        if self.confirmation_key and (self.confirmation_key != self.confirmation_key.strip() or len(self.confirmation_key.strip()) < 32):
            raise ValueError("confirmation_key must contain at least 32 characters")
        if self.integrity_key and self.confirmation_key and self.integrity_key == self.confirmation_key:
            raise ValueError("integrity_key and confirmation_key must be different")
        self.policy()

    @classmethod
    def from_env(cls) -> "AdvisorRuntimeConfig":
        return cls(
            enabled=_to_bool(os.getenv("ADVISOR_ENABLED", "false")),
            live_enabled=_to_bool(os.getenv("ADVISOR_LIVE_ENABLED", "false")),
            broker_configured=_to_bool(os.getenv("ADVISOR_BROKER_CONFIGURED", "false")),
            control_state=os.getenv("ADVISOR_CONTROL_STATE", "paused").strip().lower(),
            global_live_trading=_to_bool(os.getenv("LIVE_TRADING", "false")),
            global_execution_mode=os.getenv("EXECUTION_MODE", "paper").strip().lower(),
            global_dry_run=_to_bool(os.getenv("DRY_RUN", "true")),
            integrity_key=os.getenv("ADVISOR_INTEGRITY_KEY", ""),
            confirmation_key=os.getenv("ADVISOR_CONFIRMATION_KEY", ""),
            recipient=os.getenv("ADVISOR_RECIPIENT", ""),
            minimum_probability=_float_env("ADVISOR_MIN_PROBABILITY", 0.60),
            minimum_edge_bps=_float_env("ADVISOR_MIN_EDGE_BPS", 150.0),
            maximum_edge_bps=_float_env("ADVISOR_MAX_EDGE_BPS", 5_000.0),
            maximum_stake_usdc=_float_env("ADVISOR_MAX_STAKE_USDC", 5.0),
            maximum_daily_loss_usdc=_float_env("ADVISOR_MAX_DAILY_LOSS_USDC", 5.0),
            maximum_quote_age_seconds=_float_env("ADVISOR_MAX_QUOTE_AGE_SECONDS", 60.0),
            maximum_model_age_seconds=_float_env("ADVISOR_MAX_MODEL_AGE_SECONDS", 86_400.0),
            proposal_ttl_seconds=_float_env("ADVISOR_PROPOSAL_TTL_SECONDS", 900.0),
            evidence_path=os.getenv("ADVISOR_EVIDENCE_PATH", "").strip(),
            whatsapp=WhatsAppConfig.from_env(),
        )

    @property
    def paper_ready(self) -> bool:
        return bool(
            self.enabled
            and self.integrity_key
            and self.confirmation_key
            and self.integrity_key != self.confirmation_key
            and self.configuration_valid
        )

    @property
    def configuration_valid(self) -> bool:
        try:
            if self.integrity_key and (self.integrity_key != self.integrity_key.strip() or len(self.integrity_key.strip()) < 32):
                return False
            if self.confirmation_key and (self.confirmation_key != self.confirmation_key.strip() or len(self.confirmation_key.strip()) < 32):
                return False
            if self.integrity_key and self.confirmation_key and self.integrity_key == self.confirmation_key:
                return False
            self.policy()
        except (TypeError, ValueError, OverflowError):
            return False
        return True

    @property
    def live_ready(self) -> bool:
        return bool(
            self.paper_ready
            and self.live_enabled
            and self.broker_configured
            and self.control_state == "armed"
            and self.global_live_trading
            and self.global_execution_mode == "live"
            and not self.global_dry_run
            and bool(self.recipient)
            and re.sub(r"\D", "", self.recipient) in self.whatsapp.allowed_numbers
            and self.whatsapp.ready
        )

    def execution_flags(self, *, integrity_key_loaded: bool) -> "ExecutionFlags":
        from app.advisor.execution import ExecutionFlags

        return ExecutionFlags(
            live_trading=self.global_live_trading,
            execution_mode=self.global_execution_mode,
            dry_run=self.global_dry_run,
            advisor_live_enabled=self.live_enabled,
            control_state=self.control_state,
            integrity_key_loaded=bool(integrity_key_loaded),
        )

    def policy(self) -> AdvisorPolicy:
        return AdvisorPolicy(
            minimum_probability=self.minimum_probability,
            minimum_edge_bps=self.minimum_edge_bps,
            maximum_edge_bps=self.maximum_edge_bps,
            maximum_stake_usdc=self.maximum_stake_usdc,
            maximum_daily_loss_usdc=self.maximum_daily_loss_usdc,
            maximum_quote_age_seconds=self.maximum_quote_age_seconds,
            maximum_model_age_seconds=self.maximum_model_age_seconds,
            proposal_ttl_seconds=self.proposal_ttl_seconds,
        )

    def __repr__(self) -> str:
        return (
            "AdvisorRuntimeConfig(enabled={!r}, live_enabled={!r}, broker_configured={!r}, control_state={!r}, "
            "global_live_trading={!r}, global_execution_mode={!r}, global_dry_run={!r}, recipient={!r}, "
            "minimum_probability={!r}, minimum_edge_bps={!r}, maximum_stake_usdc={!r}, "
            "maximum_edge_bps={!r}, maximum_daily_loss_usdc={!r}, maximum_model_age_seconds={!r}, "
            "evidence_path={!r}, whatsapp={!r})"
        ).format(
            self.enabled,
            self.live_enabled,
            self.broker_configured,
            self.control_state,
            self.global_live_trading,
            self.global_execution_mode,
            self.global_dry_run,
            self.recipient,
            self.minimum_probability,
            self.minimum_edge_bps,
            self.maximum_stake_usdc,
            self.maximum_edge_bps,
            self.maximum_daily_loss_usdc,
            self.maximum_model_age_seconds,
            self.evidence_path,
            self.whatsapp,
        )


def _to_bool(raw: str) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be numeric") from error
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value
