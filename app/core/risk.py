from __future__ import annotations

from app.models import CopyInstruction, TradeSide
from app.settings import BotConfig


class RiskManager:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def is_tag_allowed(self, category: str) -> bool:
        category = (category or "").strip().lower()
        allowed = set(self.config.allowed_tags)
        blocked = set(self.config.blocked_tags)

        if category and category in blocked:
            return False
        if allowed and category not in allowed:
            return False
        return True

    def evaluate_instruction(
        self,
        instruction: CopyInstruction,
        *,
        current_market_notional: float,
        current_total_exposure: float,
        daily_pnl: float,
        reference_price: float,
    ) -> tuple[bool, str]:
        if not self.is_tag_allowed(instruction.category):
            return False, "category blocked by allowed_tags/blocked_tags"

        if instruction.side == TradeSide.BUY:
            if instruction.price < self.config.min_price:
                return False, "min_price filter"

            if instruction.price > self.config.max_price:
                return False, "max_price filter"

            if daily_pnl <= -abs(self.config.max_daily_loss):
                return False, "max_daily_loss reached"

            resulting_market_notional = current_market_notional + instruction.notional
            if resulting_market_notional > self.config.max_position_per_market:
                return False, "max_position_per_market exceeded"

            resulting_exposure = current_total_exposure + instruction.notional
            if resulting_exposure > self.config.max_total_exposure:
                return False, "max_total_exposure exceeded"

        if reference_price > 0:
            slippage = abs(instruction.price - reference_price) / reference_price
            if slippage > self.config.slippage_limit:
                return False, "slippage_limit exceeded"

        return True, "ok"
