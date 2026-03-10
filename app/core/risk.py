from __future__ import annotations

from app.core.market_classifier import is_dynamic_market
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
        return max(float(effective_bankroll), 0.0)

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
        current_dynamic_exposure: float = 0.0,
        current_btc5m_exposure: float = 0.0,
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

            market_limit = min(self.config.max_position_per_market, bankroll)
            resulting_market_notional = current_market_notional + instruction.notional
            if resulting_market_notional > market_limit:
                return False, "max_position_per_market exceeded"

            exposure_limit = min(self.config.max_total_exposure, bankroll)
            market_is_btc5m = self.config.btc5m_reserve_enabled and is_dynamic_market(
                title=instruction.title,
                slug=instruction.slug,
                category=instruction.category,
                keywords=self.config.btc5m_reserve_keywords,
            )
            btc5m_cap = min(self.config.btc5m_reserved_notional, bankroll) if self.config.btc5m_reserve_enabled else 0.0
            if market_is_btc5m:
                resulting_btc5m_exposure = current_btc5m_exposure + instruction.notional
                if resulting_btc5m_exposure > btc5m_cap:
                    return False, "btc5m_reserved_cap exceeded"
            elif self.config.btc5m_reserve_enabled and btc5m_cap > 0:
                non_btc5m_cap = max(exposure_limit - btc5m_cap, 0.0)
                current_non_btc5m_exposure = max(current_total_exposure - current_btc5m_exposure, 0.0)
                resulting_non_btc5m_exposure = current_non_btc5m_exposure + instruction.notional
                if resulting_non_btc5m_exposure > non_btc5m_cap:
                    return False, "reserved_for_btc5m"

            resulting_exposure = current_total_exposure + instruction.notional
            if resulting_exposure > exposure_limit:
                return False, "max_total_exposure exceeded"

            if self.config.dynamic_max_allocation_pct > 0:
                market_is_dynamic = is_dynamic_market(
                    title=instruction.title,
                    slug=instruction.slug,
                    category=instruction.category,
                    keywords=self.config.dynamic_keywords,
                )
                if market_is_dynamic:
                    dynamic_cap = bankroll * self.config.dynamic_max_allocation_pct
                    resulting_dynamic_exposure = current_dynamic_exposure + instruction.notional
                    if resulting_dynamic_exposure > dynamic_cap:
                        return False, "dynamic_allocation_cap exceeded"

        if reference_price > 0:
            slippage = abs(instruction.price - reference_price) / reference_price
            if slippage > self.config.slippage_limit:
                return False, "slippage_limit exceeded"

        return True, "ok"
