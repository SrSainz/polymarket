from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from app.models import NormalizedSignal, SignalAction
from app.settings import BotConfig


class SizingEngine:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def calculate_buy_size(
        self,
        signal: NormalizedSignal,
        *,
        execution_price: float,
        current_total_exposure: float,
    ) -> float:
        if execution_price <= 0:
            return 0.0

        if self.config.sizing_mode == "fixed_amount_per_trade":
            desired_notional = self.config.fixed_amount_per_trade
        else:
            source_position_notional = max(signal.new_size * signal.reference_price, 1e-9)
            ratio = (self.config.bankroll * self.config.proportional_scale) / source_position_notional
            ratio = max(0.0, min(1.0, ratio))
            desired_notional = abs(signal.delta_size) * signal.reference_price * ratio

        budget_left = max(self.config.bankroll - current_total_exposure, 0.0)
        desired_notional = min(desired_notional, budget_left)

        if desired_notional < self.config.min_trade_amount:
            return 0.0

        raw_size = desired_notional / execution_price
        return _round_down(raw_size, precision="0.0001")

    def calculate_reduce_size(self, signal: NormalizedSignal, copy_position_size: float) -> float:
        if copy_position_size <= 0:
            return 0.0

        if signal.action == SignalAction.CLOSE:
            return _round_down(copy_position_size, precision="0.0001")

        if signal.action != SignalAction.REDUCE:
            return 0.0

        if signal.prev_size <= 0:
            return 0.0

        fraction = min(abs(signal.delta_size) / signal.prev_size, 1.0)
        return _round_down(copy_position_size * fraction, precision="0.0001")


def _round_down(value: float, precision: str) -> float:
    quant = Decimal(precision)
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_DOWN))
