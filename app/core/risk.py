from __future__ import annotations

from app.models import CopyInstruction, TradeSide
from app.settings import BotConfig


class RiskManager:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    def daily_loss_limit(
        self,
        daily_profit_gross: float = 0.0,
        *,
        effective_bankroll: float | None = None,
    ) -> float:
        bankroll = self._resolve_bankroll(effective_bankroll)
        absolute_limit = abs(self.config.max_daily_loss)
        pct_limit = bankroll * self.config.max_daily_loss_pct
        base_limit = min(absolute_limit, pct_limit)
        return base_limit + max(daily_profit_gross, 0.0)

    def _resolve_bankroll(self, effective_bankroll: float | None) -> float:
        if effective_bankroll is None:
            return self.config.bankroll
        return max(float(effective_bankroll), self.config.bankroll)

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
        daily_profit_gross: float,
        effective_bankroll: float | None = None,
        reference_price: float,
    ) -> tuple[bool, str]:
        bankroll = self._resolve_bankroll(effective_bankroll)
        if not self.is_tag_allowed(instruction.category):
            return False, "category blocked by allowed_tags/blocked_tags"

        if instruction.side == TradeSide.BUY:
            if instruction.price < self.config.min_price:
                return False, "min_price filter"

            if instruction.price > self.config.max_price:
                return False, "max_price filter"

            if daily_pnl <= -self.daily_loss_limit(daily_profit_gross, effective_bankroll=bankroll):
                return False, "max_daily_loss reached"

            market_limit = max(self.config.max_position_per_market, bankroll)
            resulting_market_notional = current_market_notional + instruction.notional
            if resulting_market_notional > market_limit:
                return False, "max_position_per_market exceeded"

            exposure_limit = max(self.config.max_total_exposure, bankroll)
            resulting_exposure = current_total_exposure + instruction.notional
            if resulting_exposure > exposure_limit:
                return False, "max_total_exposure exceeded"

        if reference_price > 0:
            slippage = abs(instruction.price - reference_price) / reference_price
            if slippage > self.config.slippage_limit:
                return False, "slippage_limit exceeded"

        return True, "ok"
