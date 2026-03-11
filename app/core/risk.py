from __future__ import annotations

from app.core.market_classifier import is_btc5m_market, is_dynamic_market
from app.models import CopyInstruction, TradeSide
from app.settings import BotConfig

_BTC5M_RELAXED_MIN_PRICE = 0.02


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
        mode: str = "paper",
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

        market_is_btc5m = self.config.btc5m_reserve_enabled and (
            is_btc5m_market(
                title=instruction.title,
                slug=instruction.slug,
                category=instruction.category,
            )
            or is_dynamic_market(
                title=instruction.title,
                slug=instruction.slug,
                category=instruction.category,
                keywords=self.config.btc5m_reserve_keywords,
            )
        )
        btc5m_relaxed = market_is_btc5m and self.config.btc5m_relaxed_risk

        if instruction.side == TradeSide.BUY:
            effective_min_price = self.config.min_price
            if btc5m_relaxed:
                effective_min_price = min(self.config.min_price, _BTC5M_RELAXED_MIN_PRICE)

            if instruction.price < effective_min_price:
                return False, "min_price filter"

            if instruction.price > self.config.max_price:
                return False, "max_price filter"

            market_limit = min(self.config.max_position_per_market, bankroll)
            if mode == "live" and market_is_btc5m:
                market_limit = min(market_limit, bankroll * self.config.live_btc5m_ticket_allocation_pct)
            resulting_market_notional = current_market_notional + instruction.notional
            if resulting_market_notional > market_limit:
                return False, "max_position_per_market exceeded"

            if not btc5m_relaxed:
                if daily_pnl <= -self.daily_loss_limit(daily_profit_gross, effective_bankroll=bankroll):
                    return False, "max_daily_loss reached"

            exposure_limit = min(self.config.max_total_exposure, bankroll)
            btc5m_cap = 0.0
            if self.config.btc5m_reserve_enabled:
                if self.config.btc5m_reserved_allocation_pct > 0:
                    btc5m_cap = bankroll * self.config.btc5m_reserved_allocation_pct
                else:
                    btc5m_cap = min(self.config.btc5m_reserved_notional, bankroll)
            if market_is_btc5m:
                resulting_btc5m_exposure = current_btc5m_exposure + instruction.notional
                if resulting_btc5m_exposure > btc5m_cap:
                    return False, "btc5m_reserved_cap exceeded"
            elif (
                self.config.btc5m_reserve_enabled
                and btc5m_cap > 0
                and self.config.btc5m_reserve_protected_pct > 0
            ):
                protected_reserve = btc5m_cap * self.config.btc5m_reserve_protected_pct
                non_btc5m_cap = max(exposure_limit - protected_reserve, 0.0)
                current_non_btc5m_exposure = max(current_total_exposure - current_btc5m_exposure, 0.0)
                resulting_non_btc5m_exposure = current_non_btc5m_exposure + instruction.notional
                if resulting_non_btc5m_exposure > non_btc5m_cap:
                    return False, "reserved_for_btc5m"

            if not btc5m_relaxed and not (market_is_btc5m and self.config.btc5m_ignore_global_exposure_limit):
                resulting_exposure = current_total_exposure + instruction.notional
                if resulting_exposure > exposure_limit:
                    return False, "max_total_exposure exceeded"

            if not btc5m_relaxed and self.config.dynamic_max_allocation_pct > 0:
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

        if reference_price > 0 and not (instruction.side == TradeSide.BUY and btc5m_relaxed):
            slippage = abs(instruction.price - reference_price) / reference_price
            if slippage > self.config.slippage_limit:
                return False, "slippage_limit exceeded"

        return True, "ok"
