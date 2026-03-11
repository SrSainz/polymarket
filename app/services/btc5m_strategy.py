from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.core.risk import RiskManager
from app.core.autonomous_decider import AutonomousDecider
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.services.telegram_daily_summary import TelegramDailySummaryService
from app.services.telegram_trade_notifier import TelegramTradeNotifierService
from app.settings import AppSettings

_OPERATIVE_TRIGGER_PRICE = 0.80
_OPERATIVE_MAX_OPPOSITE_PRICE = 0.20
_OPERATIVE_MAX_TARGET_SPREAD = 0.05
_OPERATIVE_MAX_SECONDS_INTO_WINDOW = 270
_VIDARX_MIN_SECONDS = 20
_VIDARX_MAX_SECONDS = 260
_VIDARX_RICH_TRIGGER_FLOOR = 0.58
_VIDARX_RICH_ENTRY_CEILING = 0.86
_VIDARX_MAX_SPREAD = 0.08
_VIDARX_TILTED_RICH_MIN = 0.66
_VIDARX_TILTED_RICH_MAX = 0.80
_VIDARX_TILTED_CHEAP_MIN = 0.20
_VIDARX_TILTED_CHEAP_MAX = 0.34
_VIDARX_EXTREME_RICH_MIN = 0.75
_VIDARX_EXTREME_RICH_MAX = 0.88
_VIDARX_EXTREME_CHEAP_MIN = 0.12
_VIDARX_EXTREME_CHEAP_MAX = 0.25
_VIDARX_BALANCED_RICH_MIN = 0.52
_VIDARX_BALANCED_RICH_MAX = 0.72
_VIDARX_BALANCED_CHEAP_MIN = 0.28
_VIDARX_BALANCED_CHEAP_MAX = 0.48
_VIDARX_EARLY_MID_END = 125
_VIDARX_MID_LATE_START = 140
_VIDARX_BUCKET_TOLERANCE = 0.015


@dataclass(frozen=True)
class AskLevel:
    price: float
    size: float


@dataclass(frozen=True)
class MarketOutcome:
    label: str
    asset_id: str
    best_ask: float
    best_bid: float
    best_ask_size: float
    ask_levels: tuple[AskLevel, ...]


@dataclass(frozen=True)
class StrategyOpportunity:
    slug: str
    condition_id: str
    title: str
    category: str
    target: MarketOutcome
    trigger: MarketOutcome
    rationale: str
    event_start_ts: int


@dataclass(frozen=True)
class StrategyPlan:
    instructions: tuple[CopyInstruction, ...]
    note: str
    primary_target: MarketOutcome
    secondary_target: MarketOutcome | None
    trigger: MarketOutcome
    window_seconds: int
    cycle_budget: float
    market_bias: str
    timing_regime: str
    price_mode: str
    primary_ratio: float
    primary_notional: float
    secondary_notional: float
    replenishment_count: int


@dataclass(frozen=True)
class VidarxEntryLevel:
    price: float
    size: float
    bucket_price: float
    is_replenishment: bool


class BTC5mStrategyService:
    def __init__(
        self,
        db: Database,
        gamma_client: GammaClient,
        clob_client: CLOBClient,
        paper_broker: PaperBroker,
        live_broker: LiveBroker,
        autonomous_decider: AutonomousDecider,
        daily_summary: TelegramDailySummaryService,
        trade_notifier: TelegramTradeNotifierService,
        settings: AppSettings,
        logger: logging.Logger,
    ) -> None:
        self.db = db
        self.gamma_client = gamma_client
        self.clob_client = clob_client
        self.paper_broker = paper_broker
        self.live_broker = live_broker
        self.autonomous_decider = autonomous_decider
        self.daily_summary = daily_summary
        self.trade_notifier = trade_notifier
        self.settings = settings
        self.logger = logger
        self.risk = RiskManager(settings.config)

    def run(self, mode: str = "paper") -> dict[str, int]:
        if self.settings.config.strategy_entry_mode == "vidarx_micro":
            return self._run_vidarx_micro(mode=mode)

        stats = {
            "pending": 0,
            "filled": 0,
            "blocked": 0,
            "failed": 0,
            "skipped": 0,
            "opportunities": 0,
        }
        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode=mode)
        live_total_capital = cash_balance + total_exposure
        self._record_balance_snapshot(
            mode=mode,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
        )

        market = self._discover_market()
        if market is None:
            stats["skipped"] += 1
            note = "no active btc5m market"
            self._record_strategy_snapshot(note=note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        opportunity = self._build_opportunity(market)
        if opportunity is None:
            stats["skipped"] += 1
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=self.db.get_bot_state("strategy_last_note") or "no opportunity",
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        stats["pending"] = 1
        stats["opportunities"] = 1
        self._record_strategy_snapshot(
            market=market,
            opportunity=opportunity,
            note=opportunity.rationale,
        )

        if self._get_open_btc5m_positions_count() >= self.settings.config.strategy_max_open_positions:
            stats["blocked"] += 1
            note = "strategy_max_open_positions reached"
            self._record_strategy_snapshot(market=market, opportunity=opportunity, note=note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        if self._has_condition_conflict(opportunity.condition_id):
            stats["blocked"] += 1
            note = "condition already open"
            self._record_strategy_snapshot(market=market, opportunity=opportunity, note=note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        try:
            instruction = self._build_instruction(
                opportunity=opportunity,
                current_total_exposure=total_exposure,
                effective_bankroll=live_total_capital,
                cash_balance=cash_balance,
                mode=mode,
            )
        except ValueError as error:
            stats["blocked"] += 1
            note = str(error)
            self._record_strategy_snapshot(market=market, opportunity=opportunity, note=note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        try:
            result = self._execute_instruction(mode=mode, instruction=instruction)
        except Exception as error:  # noqa: BLE001
            stats["failed"] += 1
            note = f"execution failed: {error}"
            self._record_strategy_snapshot(market=market, opportunity=opportunity, note=note)
            self.logger.exception("btc5m strategy execution failed: %s", error)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        if result.status == "filled":
            stats["filled"] += 1
            note = f"filled {instruction.side.value} {instruction.outcome} @ {instruction.price:.3f}"
            self._record_strategy_snapshot(market=market, opportunity=opportunity, note=note)
            if mode == "live":
                self.trade_notifier.send_realized_result(instruction=instruction, result=result)
        else:
            stats["skipped"] += 1
            note = result.message or "not filled"
            self._record_strategy_snapshot(market=market, opportunity=opportunity, note=note)
        return self._complete_cycle(
            mode=mode,
            stats=stats,
            note=note,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
        )

    def _run_vidarx_micro(self, *, mode: str) -> dict[str, int]:
        stats = {
            "pending": 0,
            "filled": 0,
            "blocked": 0,
            "failed": 0,
            "skipped": 0,
            "opportunities": 0,
        }
        if mode == "live":
            total_exposure = self.db.get_total_exposure()
            cash_balance, allowance = self._live_cash_snapshot(mode="paper")
            live_total_capital = cash_balance + total_exposure
            note = "vidarx_micro is paper-only; use `python run.py paper` or `python run.py once`"
            stats["blocked"] += 1
            self._record_balance_snapshot(
                mode="paper",
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )
            self._record_strategy_snapshot(
                note=note,
                extra_state=self._vidarx_state_defaults(
                    strategy_resolution_mode="paper-settle-at-close",
                ),
            )
            return self._complete_cycle(
                mode="paper",
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        self._settle_resolved_paper_positions(stats)
        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode="paper")
        live_total_capital = cash_balance + total_exposure
        self._record_balance_snapshot(
            mode="paper",
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
        )

        market = self._discover_market()
        if market is None:
            stats["skipped"] += 1
            note = "no active btc5m market"
            self._record_strategy_snapshot(
                note=note,
                extra_state=self._vidarx_state_defaults(
                    strategy_resolution_mode="paper-settle-at-close",
                ),
            )
            return self._complete_cycle(
                mode="paper",
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        plan = self._build_vidarx_plan(
            market=market,
            cash_balance=cash_balance,
            effective_bankroll=live_total_capital,
            current_total_exposure=total_exposure,
        )
        if plan is None:
            stats["skipped"] += 1
            return self._complete_cycle(
                mode="paper",
                stats=stats,
                note=self.db.get_bot_state("strategy_last_note") or "no vidarx plan",
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        stats["pending"] = len(plan.instructions)
        stats["opportunities"] = len(plan.instructions)
        self._record_strategy_snapshot(
            market=market,
            note=plan.note,
            extra_state={
                "strategy_target_outcome": plan.primary_target.label,
                "strategy_target_price": f"{plan.primary_target.best_ask:.6f}",
                "strategy_trigger_outcome": plan.trigger.label,
                "strategy_trigger_price_seen": f"{plan.trigger.best_ask:.6f}",
                "strategy_market_bias": plan.market_bias,
                "strategy_plan_legs": str(len(plan.instructions)),
                "strategy_window_seconds": str(plan.window_seconds),
                "strategy_cycle_budget": f"{plan.cycle_budget:.6f}",
                "strategy_current_market_exposure": f"{self._get_condition_exposure(str(market.get('conditionId') or '')):.6f}",
                "strategy_resolution_mode": "paper-settle-at-close",
                "strategy_timing_regime": plan.timing_regime,
                "strategy_price_mode": plan.price_mode,
                "strategy_primary_ratio": f"{plan.primary_ratio:.6f}",
                "strategy_primary_outcome": plan.primary_target.label,
                "strategy_hedge_outcome": plan.secondary_target.label if plan.secondary_target else "",
                "strategy_primary_exposure": f"{plan.primary_notional:.6f}",
                "strategy_hedge_exposure": f"{plan.secondary_notional:.6f}",
                "strategy_replenishment_count": str(plan.replenishment_count),
            },
        )

        note = plan.note
        for instruction in plan.instructions:
            try:
                result = self.paper_broker.execute(instruction)
            except Exception as error:  # noqa: BLE001
                stats["failed"] += 1
                note = f"vidarx_micro execution failed: {error}"
                self.logger.exception("vidarx_micro paper execution failed: %s", error)
                continue

            if result.status == "filled":
                stats["filled"] += 1
                fill_state = self._vidarx_fill_state_from_reason(instruction.reason)
                if fill_state is not None:
                    bucket_price, is_replenishment = fill_state
                    self._mark_vidarx_bucket_seen(
                        slug=instruction.slug,
                        asset=instruction.asset,
                        price=bucket_price,
                    )
                    if is_replenishment:
                        self._increment_vidarx_bucket_count(
                            slug=instruction.slug,
                            asset=instruction.asset,
                            price=bucket_price,
                        )
            elif result.status == "skipped":
                stats["skipped"] += 1
                note = result.message or note

        self._record_strategy_snapshot(
            market=market,
            note=note,
            extra_state={
                "strategy_target_outcome": plan.primary_target.label,
                "strategy_target_price": f"{plan.primary_target.best_ask:.6f}",
                "strategy_trigger_outcome": plan.trigger.label,
                "strategy_trigger_price_seen": f"{plan.trigger.best_ask:.6f}",
                "strategy_market_bias": plan.market_bias,
                "strategy_plan_legs": str(len(plan.instructions)),
                "strategy_window_seconds": str(plan.window_seconds),
                "strategy_cycle_budget": f"{plan.cycle_budget:.6f}",
                "strategy_current_market_exposure": f"{self._get_condition_exposure(str(market.get('conditionId') or '')):.6f}",
                "strategy_resolution_mode": "paper-settle-at-close",
                "strategy_timing_regime": plan.timing_regime,
                "strategy_price_mode": plan.price_mode,
                "strategy_primary_ratio": f"{plan.primary_ratio:.6f}",
                "strategy_primary_outcome": plan.primary_target.label,
                "strategy_hedge_outcome": plan.secondary_target.label if plan.secondary_target else "",
                "strategy_primary_exposure": f"{plan.primary_notional:.6f}",
                "strategy_hedge_exposure": f"{plan.secondary_notional:.6f}",
                "strategy_replenishment_count": str(plan.replenishment_count),
            },
        )
        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode="paper")
        live_total_capital = cash_balance + total_exposure
        self._record_balance_snapshot(
            mode="paper",
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
        )
        return self._complete_cycle(
            mode="paper",
            stats=stats,
            note=note,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
        )

    def _discover_market(self) -> dict | None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        base_start = now_ts - (now_ts % 300)
        for candidate_ts in (base_start, base_start + 300, base_start - 300):
            slug = f"btc-updown-5m-{candidate_ts}"
            market = self.gamma_client.get_market_by_slug(slug)
            if not market:
                continue
            if bool(market.get("closed")):
                continue
            if not bool(market.get("acceptingOrders", False)):
                continue
            return market
        return None

    def _build_opportunity(self, market: dict) -> StrategyOpportunity | None:
        outcomes = _parse_json_list(market.get("outcomes"))
        token_ids = _parse_json_list(market.get("clobTokenIds"))
        if len(outcomes) != 2 or len(token_ids) != 2:
            self._record_strategy_snapshot(market=market, note="market outcomes unavailable")
            return None

        priced_outcomes: list[MarketOutcome] = []
        for label, token_id in zip(outcomes, token_ids):
            book = self._safe_book(token_id)
            if not book:
                self._record_strategy_snapshot(market=market, note=f"no orderbook for {label}")
                return None
            best_ask = _best_ask(book)
            best_bid = _best_bid(book)
            best_ask_size = _best_ask_size(book)
            ask_levels = _ask_levels(book)
            if best_ask is None or best_bid is None or best_ask_size is None or not ask_levels:
                self._record_strategy_snapshot(market=market, note=f"incomplete book for {label}")
                return None
            priced_outcomes.append(
                MarketOutcome(
                    label=str(label),
                    asset_id=str(token_id),
                    best_ask=best_ask,
                    best_bid=best_bid,
                    best_ask_size=best_ask_size,
                    ask_levels=ask_levels,
                )
            )

        priced_outcomes.sort(key=lambda item: item.best_ask, reverse=True)
        rich_side = priced_outcomes[0]
        cheap_side = priced_outcomes[1]
        trigger_price = self._effective_trigger_price()

        if rich_side.best_ask < trigger_price:
            self._record_strategy_snapshot(
                market=market,
                note=f"no trigger: richest ask {rich_side.best_ask:.3f} < {trigger_price:.3f}",
            )
            return None

        seconds_into_window = self._seconds_into_window(market)
        if seconds_into_window < self.settings.config.strategy_min_seconds_into_window:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"too early in window: {seconds_into_window}s < "
                    f"{self.settings.config.strategy_min_seconds_into_window}s"
                ),
            )
            return None
        effective_max_seconds = self._effective_max_seconds_into_window()
        if seconds_into_window > effective_max_seconds:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"too late in window: {seconds_into_window}s > "
                    f"{effective_max_seconds}s"
                ),
            )
            return None

        if self.settings.config.strategy_entry_mode == "buy_above":
            target = rich_side
            rationale = f"buy_above trigger {rich_side.label} ask={rich_side.best_ask:.3f}"
        else:
            max_opposite_price = self._effective_max_opposite_price()
            if cheap_side.best_ask > max_opposite_price:
                self._record_strategy_snapshot(
                    market=market,
                    note=(
                        f"opposite too expensive: {cheap_side.label} ask="
                        f"{cheap_side.best_ask:.3f} > {max_opposite_price:.3f}"
                    ),
                )
                return None
            target = cheap_side
            rationale = (
                f"buy_opposite trigger {rich_side.label} ask={rich_side.best_ask:.3f} -> "
                f"buy {cheap_side.label} ask={cheap_side.best_ask:.3f}"
            )

        target_spread = max(target.best_ask - target.best_bid, 0.0)
        max_target_spread = self._effective_max_target_spread()
        if target_spread > max_target_spread:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"spread too wide: {target.label} spread={target_spread:.3f} > "
                    f"{max_target_spread:.3f}"
                ),
            )
            return None

        event_start_ts = _to_timestamp(str((market.get("events") or [{}])[0].get("startTime") or ""))
        return StrategyOpportunity(
            slug=str(market.get("slug") or ""),
            condition_id=str(market.get("conditionId") or ""),
            title=str(market.get("question") or market.get("slug") or ""),
            category="crypto",
            target=target,
            trigger=rich_side,
            rationale=rationale,
            event_start_ts=event_start_ts,
        )

    def _build_vidarx_plan(
        self,
        *,
        market: dict,
        cash_balance: float,
        effective_bankroll: float,
        current_total_exposure: float,
    ) -> StrategyPlan | None:
        outcomes = _parse_json_list(market.get("outcomes"))
        token_ids = _parse_json_list(market.get("clobTokenIds"))
        if len(outcomes) != 2 or len(token_ids) != 2:
            self._record_strategy_snapshot(market=market, note="market outcomes unavailable")
            return None

        priced_outcomes: list[MarketOutcome] = []
        for label, token_id in zip(outcomes, token_ids):
            book = self._safe_book(token_id)
            if not book:
                self._record_strategy_snapshot(market=market, note=f"no orderbook for {label}")
                return None
            best_ask = _best_ask(book)
            best_bid = _best_bid(book)
            best_ask_size = _best_ask_size(book)
            ask_levels = _ask_levels(book)
            if best_ask is None or best_bid is None or best_ask_size is None or not ask_levels:
                self._record_strategy_snapshot(market=market, note=f"incomplete book for {label}")
                return None
            priced_outcomes.append(
                MarketOutcome(
                    label=str(label),
                    asset_id=str(token_id),
                    best_ask=best_ask,
                    best_bid=best_bid,
                    best_ask_size=best_ask_size,
                    ask_levels=ask_levels,
                )
            )

        priced_outcomes.sort(key=lambda item: item.best_ask, reverse=True)
        rich_side = priced_outcomes[0]
        cheap_side = priced_outcomes[1]
        rich_spread = max(rich_side.best_ask - rich_side.best_bid, 0.0)
        cheap_spread = max(cheap_side.best_ask - cheap_side.best_bid, 0.0)
        seconds_into_window = self._seconds_into_window(market)
        price_mode, primary_ratio = self._classify_vidarx_market(rich_side=rich_side, cheap_side=cheap_side)
        if not price_mode:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"vidarx fuera de rango: {rich_side.label} {rich_side.best_ask:.3f} / "
                    f"{cheap_side.label} {cheap_side.best_ask:.3f}"
                ),
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_trigger_outcome=rich_side.label,
                    strategy_trigger_price_seen=f"{rich_side.best_ask:.6f}",
                ),
            )
            return None

        timing_regime, timing_note = self._select_vidarx_timing_regime(
            seconds_into_window=seconds_into_window,
            price_mode=price_mode,
        )
        if timing_regime is None:
            self._record_strategy_snapshot(
                market=market,
                note=timing_note,
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_price_mode=price_mode,
                    strategy_trigger_outcome=rich_side.label,
                    strategy_trigger_price_seen=f"{rich_side.best_ask:.6f}",
                ),
            )
            return None

        if rich_side.best_ask < _VIDARX_RICH_TRIGGER_FLOOR:
            self._record_strategy_snapshot(
                market=market,
                note=f"vidarx no pressure: richest ask {rich_side.best_ask:.3f} < {_VIDARX_RICH_TRIGGER_FLOOR:.3f}",
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_price_mode=price_mode,
                    strategy_trigger_outcome=rich_side.label,
                    strategy_trigger_price_seen=f"{rich_side.best_ask:.6f}",
                ),
            )
            return None

        if rich_spread > _VIDARX_MAX_SPREAD or cheap_spread > _VIDARX_MAX_SPREAD:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"vidarx spreads wide: {rich_side.label} {rich_spread:.3f} / "
                    f"{cheap_side.label} {cheap_spread:.3f}"
                ),
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_price_mode=price_mode,
                ),
            )
            return None

        primary_target = rich_side
        hedge_target = cheap_side
        primary_in_band = self._price_matches_vidarx_band(rich_side.best_ask, price_mode=price_mode, role="primary")
        hedge_in_band = self._price_matches_vidarx_band(cheap_side.best_ask, price_mode=price_mode, role="hedge")
        if not primary_in_band:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"vidarx precio fuerte fuera de banda: {rich_side.label} ask={rich_side.best_ask:.3f}"
                ),
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_price_mode=price_mode,
                ),
            )
            return None

        cycle_budget = self._target_vidarx_cycle_budget(
            cash_balance=cash_balance,
            effective_bankroll=effective_bankroll,
            current_total_exposure=current_total_exposure,
            timing_regime=timing_regime,
            price_mode=price_mode,
        )
        if cycle_budget < self.settings.config.min_trade_amount:
            self._record_strategy_snapshot(
                market=market,
                note="vidarx cash below min_trade_amount",
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_price_mode=price_mode,
                    strategy_timing_regime=timing_regime,
                ),
            )
            return None

        condition_id = str(market.get("conditionId") or "")
        existing_market_notional = self._get_condition_exposure(condition_id)
        budget_cap = max(self.settings.config.max_position_per_market - existing_market_notional, 0.0)
        cycle_budget = min(cycle_budget, budget_cap, cash_balance)
        if cycle_budget < self.settings.config.min_trade_amount:
            self._record_strategy_snapshot(
                market=market,
                note="vidarx market cap exhausted",
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                    strategy_price_mode=price_mode,
                    strategy_timing_regime=timing_regime,
                ),
            )
            return None

        primary_budget = cycle_budget * primary_ratio
        hedge_budget = cycle_budget - primary_budget
        if not hedge_in_band:
            primary_budget = cycle_budget
            hedge_budget = 0.0
        elif hedge_budget < self.settings.config.min_trade_amount:
            primary_budget = cycle_budget
            hedge_budget = 0.0

        instructions: list[CopyInstruction] = []
        projected_market_notional = existing_market_notional
        projected_total_exposure = current_total_exposure
        rejection_reasons: list[str] = []
        primary_notional = 0.0
        hedge_notional = 0.0
        replenishment_count = 0

        for target, budget, label in (
            (primary_target, primary_budget, "primary"),
            (hedge_target, hedge_budget, "hedge"),
        ):
            if budget < self.settings.config.min_trade_amount:
                continue
            ladder_levels = self._select_vidarx_entry_levels(
                slug=str(market.get("slug") or ""),
                target=target,
                price_mode=price_mode,
                timing_regime=timing_regime,
                role=label,
            )
            if not ladder_levels:
                rejection_reasons.append(f"{label} ladder unavailable")
                continue
            tranches = self._build_vidarx_tranches(
                budget,
                timing_regime=timing_regime,
                price_mode=price_mode,
                level_count=len(ladder_levels),
                role=label,
            )
            selected_levels = ladder_levels[: len(tranches)]
            for idx, (level, tranche_notional) in enumerate(zip(selected_levels, tranches), start=1):
                if tranche_notional < self.settings.config.min_trade_amount:
                    continue
                instruction = self._build_vidarx_instruction(
                    market=market,
                    target=target,
                    price=level.price,
                    available_size=level.size,
                    tranche_notional=tranche_notional,
                    reason_label=label,
                    entry_kind="replenish" if level.is_replenishment else "ladder",
                    tranche_index=idx,
                )
                if instruction is None:
                    rejection_reasons.append(f"{label} size unavailable")
                    continue
                allowed, reason = self.risk.evaluate_instruction(
                    instruction,
                    mode="paper",
                    current_market_notional=projected_market_notional,
                    current_total_exposure=projected_total_exposure,
                    current_dynamic_exposure=0.0,
                    current_btc5m_exposure=projected_total_exposure,
                    daily_pnl=self.db.get_daily_pnl(datetime.now(timezone.utc).date().isoformat()),
                    daily_profit_gross=self.db.get_daily_profit_gross(datetime.now(timezone.utc).date().isoformat()),
                    effective_bankroll=effective_bankroll,
                    reference_price=instruction.price,
                )
                if not allowed:
                    rejection_reasons.append(reason)
                    continue
                instructions.append(instruction)
                projected_market_notional += instruction.notional
                projected_total_exposure += instruction.notional
                if label == "primary":
                    primary_notional += instruction.notional
                else:
                    hedge_notional += instruction.notional
                if level.is_replenishment:
                    replenishment_count += 1

        if not instructions:
            note = rejection_reasons[-1] if rejection_reasons else "vidarx no viable tranche"
            self._record_strategy_snapshot(
                market=market,
                note=note,
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                    strategy_price_mode=price_mode,
                    strategy_timing_regime=timing_regime,
                    strategy_primary_ratio=f"{primary_ratio:.6f}",
                    strategy_primary_outcome=primary_target.label,
                    strategy_hedge_outcome=hedge_target.label,
                ),
            )
            return None

        actual_primary_ratio = primary_notional / max(primary_notional + hedge_notional, 1e-9)
        market_bias = (
            f"{primary_target.label} lidera {int(round(actual_primary_ratio * 100))}% / "
            f"{hedge_target.label} cubre {int(round((1 - actual_primary_ratio) * 100))}%"
            if hedge_notional > 0
            else f"{primary_target.label} en solitario"
        )
        note = (
            f"{market_bias} | {timing_regime} | banda {price_mode} | "
            f"mercado {seconds_into_window}s | compras {len(instructions)}"
        )
        return StrategyPlan(
            instructions=tuple(instructions),
            note=note,
            primary_target=primary_target,
            secondary_target=hedge_target if hedge_notional > 0 else None,
            trigger=rich_side,
            window_seconds=seconds_into_window,
            cycle_budget=cycle_budget,
            market_bias=market_bias,
            timing_regime=timing_regime,
            price_mode=price_mode,
            primary_ratio=actual_primary_ratio,
            primary_notional=primary_notional,
            secondary_notional=hedge_notional,
            replenishment_count=replenishment_count,
        )

    def _build_instruction(
        self,
        *,
        opportunity: StrategyOpportunity,
        current_total_exposure: float,
        effective_bankroll: float,
        cash_balance: float,
        mode: str,
    ) -> CopyInstruction:
        trade_notional = self._target_trade_notional(
            cash_balance=cash_balance,
            effective_bankroll=effective_bankroll,
            current_total_exposure=current_total_exposure,
        )
        if trade_notional < self.settings.config.min_trade_amount:
            raise ValueError("cash below min_trade_amount")

        raw_size = trade_notional / opportunity.target.best_ask
        size = _round_down(raw_size, "0.0001")
        if size <= 0:
            raise ValueError("size below minimum")
        if size > opportunity.target.best_ask_size:
            raise ValueError("insufficient size at best ask")

        instruction = CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset=opportunity.target.asset_id,
            condition_id=opportunity.condition_id,
            size=size,
            price=opportunity.target.best_ask,
            notional=size * opportunity.target.best_ask,
            source_wallet="strategy:btc5m",
            source_signal_id=0,
            title=opportunity.title,
            slug=opportunity.slug,
            outcome=opportunity.target.label,
            category=opportunity.category,
            reason=opportunity.rationale,
        )

        allowed, reason = self.risk.evaluate_instruction(
            instruction,
            mode=mode,
            current_market_notional=0.0,
            current_total_exposure=current_total_exposure,
            current_dynamic_exposure=0.0,
            current_btc5m_exposure=current_total_exposure,
            daily_pnl=self.db.get_daily_pnl(datetime.now(timezone.utc).date().isoformat()),
            daily_profit_gross=self.db.get_daily_profit_gross(datetime.now(timezone.utc).date().isoformat()),
            effective_bankroll=effective_bankroll,
            reference_price=opportunity.target.best_ask,
        )
        if not allowed:
            raise ValueError(reason)
        return instruction

    def _target_trade_notional(
        self,
        *,
        cash_balance: float,
        effective_bankroll: float,
        current_total_exposure: float,
    ) -> float:
        if self.settings.config.strategy_fixed_trade_amount > 0:
            desired = self.settings.config.strategy_fixed_trade_amount
        else:
            desired = cash_balance * self.settings.config.strategy_trade_allocation_pct

        budget_left = max(cash_balance, 0.0)
        if desired < self.settings.config.min_trade_amount and budget_left >= self.settings.config.min_trade_amount:
            desired = self.settings.config.min_trade_amount
        desired = min(desired, budget_left)
        desired = min(desired, self.settings.config.max_position_per_market)
        desired = min(desired, max(effective_bankroll - current_total_exposure, 0.0))
        return max(desired, 0.0)

    def _target_vidarx_cycle_budget(
        self,
        *,
        cash_balance: float,
        effective_bankroll: float,
        current_total_exposure: float,
        timing_regime: str,
        price_mode: str,
    ) -> float:
        if self.settings.config.strategy_fixed_trade_amount > 0:
            desired = self.settings.config.strategy_fixed_trade_amount
        else:
            desired = cash_balance * self.settings.config.strategy_trade_allocation_pct

        if cash_balance >= self.settings.config.min_trade_amount * 2:
            desired = max(desired, self.settings.config.min_trade_amount * 2)
        elif cash_balance >= self.settings.config.min_trade_amount:
            desired = max(desired, self.settings.config.min_trade_amount)

        if timing_regime == "early-mid":
            desired *= 0.85
        else:
            desired *= 1.0

        if price_mode == "extreme":
            desired *= 1.25
        elif price_mode == "tilted":
            desired *= 1.05
        else:
            desired *= 0.90

        budget_left = max(cash_balance, 0.0)
        desired = min(desired, budget_left)
        desired = min(desired, max(effective_bankroll - current_total_exposure, 0.0))
        desired = min(desired, self.settings.config.max_position_per_market)
        return max(desired, 0.0)

    def _live_cash_snapshot(self, *, mode: str) -> tuple[float, float]:
        if mode != "live":
            paper_equity = self.settings.config.bankroll + self.db.get_cumulative_pnl()
            return max(paper_equity - self.db.get_total_exposure(), 0.0), 0.0
        try:
            balance = self.clob_client.get_collateral_balance()
            return float(balance.get("balance") or 0.0), float(balance.get("allowance") or 0.0)
        except Exception as error:  # noqa: BLE001
            self.logger.warning("live balance snapshot failed: %s", error)
            return 0.0, 0.0

    def _record_balance_snapshot(
        self,
        *,
        mode: str,
        cash_balance: float,
        allowance: float,
        total_exposure: float,
        live_total_capital: float,
    ) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        self.db.set_bot_state("live_cash_balance", f"{cash_balance:.8f}")
        self.db.set_bot_state("live_cash_allowance", f"{allowance:.8f}")
        self.db.set_bot_state("live_total_capital", f"{live_total_capital:.8f}")
        self.db.set_bot_state("live_balance_updated_at", str(now_ts))
        self.db.set_bot_state("strategy_runtime_mode", mode)
        self.db.set_bot_state("strategy_total_exposure", f"{total_exposure:.8f}")

    def _record_strategy_snapshot(
        self,
        *,
        market: dict | None = None,
        opportunity: StrategyOpportunity | None = None,
        note: str = "",
        extra_state: dict[str, str] | None = None,
    ) -> None:
        self.db.set_bot_state("strategy_mode", self.settings.config.strategy_mode)
        self.db.set_bot_state("strategy_entry_mode", self.settings.config.strategy_entry_mode)
        self.db.set_bot_state("strategy_last_note", note)
        self.db.set_bot_state("strategy_last_updated_at", str(int(datetime.now(timezone.utc).timestamp())))
        if market is not None:
            self.db.set_bot_state("strategy_market_slug", str(market.get("slug") or ""))
            self.db.set_bot_state("strategy_market_title", str(market.get("question") or market.get("slug") or ""))
        if opportunity is not None:
            self.db.set_bot_state("strategy_target_outcome", opportunity.target.label)
            self.db.set_bot_state("strategy_target_price", f"{opportunity.target.best_ask:.6f}")
            self.db.set_bot_state("strategy_trigger_outcome", opportunity.trigger.label)
            self.db.set_bot_state("strategy_trigger_price_seen", f"{opportunity.trigger.best_ask:.6f}")
        for key, value in (extra_state or {}).items():
            self.db.set_bot_state(key, value)

    def _has_condition_conflict(self, condition_id: str) -> bool:
        for row in self.db.list_copy_positions():
            if str(row["condition_id"] or "") != condition_id:
                continue
            if float(row["size"] or 0.0) <= 0:
                continue
            return True
        return False

    def _get_open_btc5m_positions_count(self) -> int:
        total = 0
        for row in self.db.list_copy_positions():
            if "btc-updown-5m-" not in str(row["slug"] or ""):
                continue
            if float(row["size"] or 0.0) <= 0:
                continue
            total += 1
        return total

    def _get_condition_exposure(self, condition_id: str) -> float:
        exposure = 0.0
        for row in self.db.list_copy_positions():
            if str(row["condition_id"] or "") != condition_id:
                continue
            size = float(row["size"] or 0.0)
            avg_price = float(row["avg_price"] or 0.0)
            if size <= 0:
                continue
            exposure += abs(size * avg_price)
        return exposure

    def _seconds_into_window(self, market: dict) -> int:
        start_ts = _to_timestamp(str((market.get("events") or [{}])[0].get("startTime") or market.get("eventStartTime") or ""))
        if start_ts <= 0:
            return 0
        return max(int(datetime.now(timezone.utc).timestamp()) - start_ts, 0)

    def _safe_book(self, token_id: str) -> dict | None:
        try:
            return self.clob_client.get_book(token_id)
        except Exception:  # noqa: BLE001
            return None

    def _effective_trigger_price(self) -> float:
        return min(self.settings.config.strategy_trigger_price, _OPERATIVE_TRIGGER_PRICE)

    def _effective_max_opposite_price(self) -> float:
        return max(self.settings.config.strategy_max_opposite_price, _OPERATIVE_MAX_OPPOSITE_PRICE)

    def _effective_max_target_spread(self) -> float:
        return max(self.settings.config.strategy_max_target_spread, _OPERATIVE_MAX_TARGET_SPREAD)

    def _effective_max_seconds_into_window(self) -> int:
        return max(self.settings.config.strategy_max_seconds_into_window, _OPERATIVE_MAX_SECONDS_INTO_WINDOW)

    def _complete_cycle(
        self,
        *,
        mode: str,
        stats: dict[str, int],
        note: str,
        cash_balance: float,
        allowance: float,
        total_exposure: float,
        live_total_capital: float,
    ) -> dict[str, int]:
        self._run_autonomous_exits(mode=mode, stats=stats)
        self.daily_summary.send_if_due()
        self._log_cycle_summary(
            mode=mode,
            stats=stats,
            note=note,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
        )
        return stats

    def _log_cycle_summary(
        self,
        *,
        mode: str,
        stats: dict[str, int],
        note: str,
        cash_balance: float,
        allowance: float,
        total_exposure: float,
        live_total_capital: float,
    ) -> None:
        available_to_trade = self._available_to_trade(cash_balance=cash_balance, allowance=allowance)
        self.logger.info(
            "strategy => mode=%s pending=%s filled=%s blocked=%s skipped=%s failed=%s opportunities=%s note=%s "
            "cash_balance=%.4f available_to_trade=%.4f allowance=%.4f exposure=%.4f live_total_capital=%.4f",
            mode,
            stats["pending"],
            stats["filled"],
            stats["blocked"],
            stats["skipped"],
            stats["failed"],
            stats["opportunities"],
            note,
            cash_balance,
            available_to_trade,
            allowance,
            total_exposure,
            live_total_capital,
        )

    def _available_to_trade(self, *, cash_balance: float, allowance: float) -> float:
        normalized_balance = max(cash_balance, 0.0)
        normalized_allowance = max(allowance, 0.0)
        if normalized_allowance <= 0:
            return normalized_balance
        return min(normalized_balance, normalized_allowance)

    def _execute_instruction(self, *, mode: str, instruction: CopyInstruction) -> ExecutionResult:
        if mode == "live":
            return self.live_broker.execute(instruction)
        return self.paper_broker.execute(instruction)

    def _run_autonomous_exits(self, *, mode: str, stats: dict[str, int]) -> None:
        if self.settings.config.strategy_entry_mode == "vidarx_micro":
            return
        positions = self.db.list_copy_positions()
        for position in positions:
            asset = str(position["asset"])
            avg_price = float(position["avg_price"])
            size = float(position["size"])
            if size <= 0:
                continue

            midpoint = self.clob_client.get_midpoint(asset)
            if midpoint is None:
                self._remember_missing_midpoint(asset)
                continue
            self._clear_missing_midpoint(asset)
            mark_price = midpoint
            self.db.record_position_mark(asset, mark_price)

            instruction = self.autonomous_decider.build_exit_instruction(
                asset=asset,
                condition_id=str(position["condition_id"]),
                size=size,
                avg_price=avg_price,
                mark_price=mark_price,
                title=str(position["title"] or ""),
                slug=str(position["slug"] or ""),
                outcome=str(position["outcome"] or ""),
                category=str(position["category"] or ""),
            )
            if instruction is None:
                continue

            stats["opportunities"] += 1
            try:
                result = self._execute_instruction(mode=mode, instruction=instruction)
                if mode == "live":
                    self.trade_notifier.send_realized_result(instruction=instruction, result=result)
                if result.status == "filled":
                    stats["filled"] += 1
                elif result.status == "skipped":
                    stats["skipped"] += 1
            except Exception as error:  # noqa: BLE001
                stats["failed"] += 1
                self.logger.exception("btc5m autonomous exit failed asset=%s: %s", asset, error)

    def _vidarx_state_defaults(self, **overrides: str) -> dict[str, str]:
        state = {
            "strategy_market_bias": "sin reparto",
            "strategy_plan_legs": "0",
            "strategy_window_seconds": "0",
            "strategy_cycle_budget": "0.000000",
            "strategy_current_market_exposure": "0.000000",
            "strategy_resolution_mode": "paper-settle-at-close",
            "strategy_timing_regime": "",
            "strategy_price_mode": "",
            "strategy_primary_ratio": "0.000000",
            "strategy_primary_outcome": "",
            "strategy_hedge_outcome": "",
            "strategy_primary_exposure": "0.000000",
            "strategy_hedge_exposure": "0.000000",
            "strategy_replenishment_count": "0",
            "strategy_target_outcome": "",
            "strategy_target_price": "0.000000",
            "strategy_trigger_outcome": "",
            "strategy_trigger_price_seen": "0.000000",
        }
        state.update({key: value for key, value in overrides.items() if value is not None})
        return state

    def _classify_vidarx_market(self, *, rich_side: MarketOutcome, cheap_side: MarketOutcome) -> tuple[str | None, float]:
        rich = rich_side.best_ask
        cheap = cheap_side.best_ask
        gap = rich - cheap

        if self._band_contains(rich, _VIDARX_EXTREME_RICH_MIN, _VIDARX_EXTREME_RICH_MAX) and self._band_contains(
            cheap, _VIDARX_EXTREME_CHEAP_MIN, _VIDARX_EXTREME_CHEAP_MAX
        ):
            return "extreme", 0.80

        if self._band_contains(rich, _VIDARX_TILTED_RICH_MIN, _VIDARX_TILTED_RICH_MAX) and self._band_contains(
            cheap, _VIDARX_TILTED_CHEAP_MIN, _VIDARX_TILTED_CHEAP_MAX
        ):
            return "tilted", 0.68

        if self._band_contains(rich, _VIDARX_BALANCED_RICH_MIN, _VIDARX_BALANCED_RICH_MAX) and self._band_contains(
            cheap, _VIDARX_BALANCED_CHEAP_MIN, _VIDARX_BALANCED_CHEAP_MAX
        ):
            return "balanced", 0.55

        if rich >= 0.77 and cheap <= 0.23:
            return "extreme", 0.80
        if rich >= 0.66 and cheap <= 0.34 and gap >= 0.30:
            return "tilted", 0.68
        if rich >= 0.52 and cheap >= 0.33 and gap <= 0.24:
            return "balanced", 0.55
        return None, 0.0

    def _select_vidarx_timing_regime(self, *, seconds_into_window: int, price_mode: str) -> tuple[str | None, str]:
        if _VIDARX_MIN_SECONDS <= seconds_into_window <= _VIDARX_EARLY_MID_END:
            return "early-mid", ""
        if _VIDARX_MID_LATE_START <= seconds_into_window <= _VIDARX_MAX_SECONDS:
            return "mid-late", ""
        if seconds_into_window < _VIDARX_MIN_SECONDS:
            return None, f"vidarx demasiado pronto: {seconds_into_window}s < {_VIDARX_MIN_SECONDS}s"
        if seconds_into_window < _VIDARX_MID_LATE_START:
            return None, (
                f"vidarx esperando segunda oleada: {seconds_into_window}s fuera de "
                f"{_VIDARX_MIN_SECONDS}-{_VIDARX_EARLY_MID_END}s y {_VIDARX_MID_LATE_START}-{_VIDARX_MAX_SECONDS}s ({price_mode})"
            )
        return None, f"vidarx tarde: {seconds_into_window}s > {_VIDARX_MAX_SECONDS}s"

    def _price_matches_vidarx_band(self, price: float, *, price_mode: str, role: str) -> bool:
        if price_mode == "extreme":
            bounds = (
                (_VIDARX_EXTREME_RICH_MIN, _VIDARX_EXTREME_RICH_MAX)
                if role == "primary"
                else (_VIDARX_EXTREME_CHEAP_MIN, _VIDARX_EXTREME_CHEAP_MAX)
            )
        elif price_mode == "tilted":
            bounds = (
                (_VIDARX_TILTED_RICH_MIN, _VIDARX_TILTED_RICH_MAX)
                if role == "primary"
                else (_VIDARX_TILTED_CHEAP_MIN, _VIDARX_TILTED_CHEAP_MAX)
            )
        else:
            bounds = (
                (_VIDARX_BALANCED_RICH_MIN, _VIDARX_BALANCED_RICH_MAX)
                if role == "primary"
                else (_VIDARX_BALANCED_CHEAP_MIN, _VIDARX_BALANCED_CHEAP_MAX)
            )
        return self._band_contains(price, bounds[0], bounds[1], tolerance=_VIDARX_BUCKET_TOLERANCE)

    def _band_contains(self, value: float, low: float, high: float, *, tolerance: float = 0.0) -> bool:
        return (low - tolerance) <= value <= (high + tolerance)

    def _get_vidarx_bucket_count(self, *, slug: str, asset: str, price: float) -> int:
        row = self.db.get_bot_state(self._vidarx_bucket_replenish_key(slug=slug, asset=asset, price=price))
        if row is None:
            return 0
        try:
            return int(row)
        except ValueError:
            return 0

    def _increment_vidarx_bucket_count(self, *, slug: str, asset: str, price: float) -> None:
        key = self._vidarx_bucket_replenish_key(slug=slug, asset=asset, price=price)
        current = self._get_vidarx_bucket_count(slug=slug, asset=asset, price=price)
        self.db.set_bot_state(key, str(current + 1))

    def _has_vidarx_bucket_seen(self, *, slug: str, asset: str, price: float) -> bool:
        return self.db.get_bot_state(self._vidarx_bucket_seen_key(slug=slug, asset=asset, price=price)) == "1"

    def _mark_vidarx_bucket_seen(self, *, slug: str, asset: str, price: float) -> None:
        self.db.set_bot_state(self._vidarx_bucket_seen_key(slug=slug, asset=asset, price=price), "1")

    def _vidarx_bucket_seen_key(self, *, slug: str, asset: str, price: float) -> str:
        return f"vidarx_bucket_seen:{slug}:{asset}:{price:.2f}"

    def _vidarx_bucket_replenish_key(self, *, slug: str, asset: str, price: float) -> str:
        return f"vidarx_bucket_replenish:{slug}:{asset}:{price:.2f}"

    def _vidarx_bucket_price(self, price: float) -> float:
        return _round_down(price, "0.01")

    def _vidarx_bucket_fill_cap(self, *, price_mode: str, timing_regime: str, role: str) -> int:
        if price_mode == "extreme":
            return 6 if timing_regime == "mid-late" or role == "primary" else 4
        if price_mode == "tilted":
            return 5 if role == "primary" else 3
        return 4 if role == "primary" else 3

    def _vidarx_initial_level_cap(self, *, price_mode: str, timing_regime: str, role: str) -> int:
        if price_mode == "extreme":
            return 4 if timing_regime == "mid-late" and role == "primary" else 3
        if price_mode == "tilted":
            return 3 if role == "primary" else 2
        return 2

    def _select_vidarx_entry_levels(
        self,
        *,
        slug: str,
        target: MarketOutcome,
        price_mode: str,
        timing_regime: str,
        role: str,
    ) -> list[VidarxEntryLevel]:
        levels_in_band = [
            level
            for level in target.ask_levels
            if self._price_matches_vidarx_band(level.price, price_mode=price_mode, role=role)
        ]
        selected: list[VidarxEntryLevel] = []
        selected_buckets: set[float] = set()
        initial_limit = self._vidarx_initial_level_cap(price_mode=price_mode, timing_regime=timing_regime, role=role)
        initial_used = 0
        replenish_limit = self._vidarx_bucket_fill_cap(price_mode=price_mode, timing_regime=timing_regime, role=role)

        for level in levels_in_band:
            bucket_price = self._vidarx_bucket_price(level.price)
            if bucket_price in selected_buckets:
                continue
            if self._has_vidarx_bucket_seen(slug=slug, asset=target.asset_id, price=bucket_price):
                if self._get_vidarx_bucket_count(slug=slug, asset=target.asset_id, price=bucket_price) >= replenish_limit:
                    continue
                selected.append(
                    VidarxEntryLevel(
                        price=level.price,
                        size=level.size,
                        bucket_price=bucket_price,
                        is_replenishment=True,
                    )
                )
                selected_buckets.add(bucket_price)
                continue

            if initial_used >= initial_limit:
                continue
            selected.append(
                VidarxEntryLevel(
                    price=level.price,
                    size=level.size,
                    bucket_price=bucket_price,
                    is_replenishment=False,
                )
            )
            initial_used += 1
            selected_buckets.add(bucket_price)
        return selected

    def _vidarx_fill_state_from_reason(self, reason: str) -> tuple[float, bool] | None:
        parts = reason.split(":")
        if len(parts) < 5 or parts[0] != "vidarx_micro":
            return None
        try:
            bucket_price = float(parts[3])
        except ValueError:
            return None
        return bucket_price, parts[2] == "replenish"

    def _remember_missing_midpoint(self, asset: str) -> None:
        state_key = self._missing_midpoint_state_key(asset)
        if self.db.get_bot_state(state_key) == "1":
            return
        self.db.set_bot_state(state_key, "1")
        self.logger.info("btc5m autonomous exit skipped asset=%s: missing midpoint/orderbook", asset)

    def _clear_missing_midpoint(self, asset: str) -> None:
        state_key = self._missing_midpoint_state_key(asset)
        if self.db.get_bot_state(state_key) != "1":
            return
        self.db.set_bot_state(state_key, "0")

    def _missing_midpoint_state_key(self, asset: str) -> str:
        return f"btc5m_missing_midpoint:{asset}"

    def _build_vidarx_tranches(
        self,
        budget: float,
        *,
        timing_regime: str,
        price_mode: str,
        level_count: int,
        role: str,
    ) -> list[float]:
        if budget < self.settings.config.min_trade_amount:
            return []
        if budget < self.settings.config.min_trade_amount * 2 or level_count <= 1:
            return [_round_down(budget, "0.01")]

        if price_mode == "extreme":
            weights = [0.34, 0.28, 0.22, 0.16] if timing_regime == "mid-late" else [0.42, 0.33, 0.25]
        elif price_mode == "tilted":
            weights = [0.42, 0.32, 0.26] if timing_regime == "mid-late" else [0.55, 0.45]
        else:
            weights = [0.56, 0.44]

        if role == "hedge" and len(weights) > 1:
            weights = weights[:-1]
        weights = weights[:level_count]
        total_weight = sum(weights)
        if total_weight <= 0:
            return [_round_down(budget, "0.01")]

        tranches: list[float] = []
        allocated = 0.0
        for idx, weight in enumerate(weights, start=1):
            if idx == len(weights):
                tranche = _round_down(max(budget - allocated, 0.0), "0.01")
            else:
                tranche = _round_down(budget * (weight / total_weight), "0.01")
            if tranche >= self.settings.config.min_trade_amount:
                tranches.append(tranche)
                allocated += tranche
        if not tranches:
            return [_round_down(budget, "0.01")]
        if len(tranches) == 1:
            return tranches
        if any(tranche < self.settings.config.min_trade_amount for tranche in tranches):
            return [_round_down(budget, "0.01")]
        return tranches

    def _build_vidarx_instruction(
        self,
        *,
        market: dict,
        target: MarketOutcome,
        price: float,
        available_size: float,
        tranche_notional: float,
        reason_label: str,
        entry_kind: str,
        tranche_index: int,
    ) -> CopyInstruction | None:
        max_notional = price * available_size
        effective_notional = min(tranche_notional, max_notional)
        if effective_notional < self.settings.config.min_trade_amount:
            return None
        raw_size = effective_notional / price
        size = _round_down(raw_size, "0.0001")
        if size <= 0:
            return None
        notional = size * price
        if notional < self.settings.config.min_trade_amount:
            return None
        bucket_price = self._vidarx_bucket_price(price)
        return CopyInstruction(
            action=SignalAction.ADD if self.db.get_copy_position(target.asset_id) else SignalAction.OPEN,
            side=TradeSide.BUY,
            asset=target.asset_id,
            condition_id=str(market.get("conditionId") or ""),
            size=size,
            price=price,
            notional=notional,
            source_wallet="strategy:vidarx_micro",
            source_signal_id=0,
            title=str(market.get("question") or market.get("slug") or ""),
            slug=str(market.get("slug") or ""),
            outcome=target.label,
            category="crypto",
            reason=f"vidarx_micro:{reason_label}:{entry_kind}:{bucket_price:.2f}:tranche-{tranche_index}",
        )

    def _settle_resolved_paper_positions(self, stats: dict[str, int]) -> None:
        if not self.db.list_copy_positions():
            return

        for row in list(self.db.list_copy_positions()):
            slug = str(row["slug"] or "")
            asset = str(row["asset"] or "")
            size = float(row["size"] or 0.0)
            if size <= 0 or "btc-updown-5m-" not in slug:
                continue
            market = self.gamma_client.get_market_by_slug(slug)
            if not market or not bool(market.get("closed")):
                continue
            settlement_price = self._resolved_price_for_asset(market, asset)
            if settlement_price is None:
                continue
            instruction = CopyInstruction(
                action=SignalAction.CLOSE,
                side=TradeSide.SELL,
                asset=asset,
                condition_id=str(row["condition_id"] or ""),
                size=size,
                price=settlement_price,
                notional=size * settlement_price,
                source_wallet="strategy:vidarx_micro",
                source_signal_id=0,
                title=str(row["title"] or ""),
                slug=slug,
                outcome=str(row["outcome"] or ""),
                category=str(row["category"] or "crypto"),
                reason=f"vidarx_resolution:{slug}:{row['outcome'] or ''}",
            )
            result = self.paper_broker.execute(instruction)
            stats["opportunities"] += 1
            if result.status == "filled":
                stats["filled"] += 1

    def _resolved_price_for_asset(self, market: dict, asset: str) -> float | None:
        token_ids = _parse_json_list(market.get("clobTokenIds"))
        prices = _parse_json_list(market.get("outcomePrices"))
        if len(token_ids) != len(prices):
            return None
        for token_id, price in zip(token_ids, prices):
            if str(token_id) != asset:
                continue
            try:
                return float(price)
            except (TypeError, ValueError):
                return None
        return None


def _parse_json_list(raw_value: object) -> list[str]:
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value]
    if isinstance(raw_value, str) and raw_value:
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if isinstance(payload, list):
            return [str(item) for item in payload]
    return []


def _best_ask(book: dict) -> float | None:
    ask_levels = _ask_levels(book)
    if not ask_levels:
        return None
    return ask_levels[0].price


def _best_bid(book: dict) -> float | None:
    bids = book.get("bids") or []
    prices: list[float] = []
    for bid in bids:
        try:
            prices.append(float(bid.get("price")))
        except (AttributeError, TypeError, ValueError):
            continue
    if not prices:
        return None
    return max(prices)


def _best_ask_size(book: dict) -> float | None:
    ask_levels = _ask_levels(book)
    if not ask_levels:
        return None
    return ask_levels[0].size


def _ask_levels(book: dict) -> tuple[AskLevel, ...]:
    asks = book.get("asks") or []
    aggregated: dict[float, float] = {}
    for ask in asks:
        try:
            price = float(ask.get("price"))
            size = float(ask.get("size") or 0.0)
        except (AttributeError, TypeError, ValueError):
            continue
        if price <= 0 or size <= 0:
            continue
        aggregated[price] = aggregated.get(price, 0.0) + size
    return tuple(AskLevel(price=price, size=aggregated[price]) for price in sorted(aggregated))


def _to_timestamp(raw_value: str) -> int:
    if not raw_value:
        return 0
    try:
        return int(datetime.fromisoformat(raw_value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def _round_down(value: float, precision: str) -> float:
    quant = Decimal(precision)
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_DOWN))
