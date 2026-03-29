from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict


class SignalAction(str, Enum):
    OPEN = "open"
    ADD = "add"
    REDUCE = "reduce"
    CLOSE = "close"


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class SourcePosition(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wallet: str
    asset: str
    condition_id: str
    size: float
    avg_price: float
    current_price: float
    title: str
    slug: str
    outcome: str
    category: str
    observed_at: int


class NormalizedSignal(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    event_key: str
    wallet: str
    asset: str
    condition_id: str
    action: SignalAction
    prev_size: float
    new_size: float
    delta_size: float
    reference_price: float
    title: str
    slug: str
    outcome: str
    category: str
    detected_at: int


class CopyInstruction(BaseModel):
    model_config = ConfigDict(extra="ignore")

    action: SignalAction
    side: TradeSide
    asset: str
    condition_id: str
    size: float
    price: float
    notional: float
    source_wallet: str
    source_signal_id: int
    title: str
    slug: str
    outcome: str
    category: str
    reason: str = ""
    execution_profile: str = ""
    time_in_force: str = ""
    post_only: bool = False
    good_till_seconds: int = 0


class ExecutionResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    mode: str
    status: str
    action: SignalAction
    asset: str
    size: float
    price: float
    notional: float
    pnl_delta: float = 0.0
    fee_paid: float = 0.0
    message: str = ""
