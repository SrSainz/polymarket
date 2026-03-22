from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN

from app.core.autonomous_decider import AutonomousDecider
from app.core.bankroll import calculate_effective_bankroll, calculate_reserved_profit
from app.core.decision_engine import ReadinessScorer, RegimeDetector, StrategyEngine
from app.core.event_bus import EventBus
from app.core.execution_engine import ExecutionEngine
from app.core.feature_engine import FeatureEngine
from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.core.risk import RiskManager
from app.core.state_store import StateStore
from app.core.strategy_registry import active_variant_metadata
from app.core.telemetry import MicrostructureTelemetry
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.polymarket.spot_feed import SpotFeed, SpotSnapshot
from app.polymarket.user_feed import UserFeed
from app.services.liquidation_feed import LiquidationFeed
from app.services.runtime_diagnostics import RuntimeDiagnosticsService, evaluate_runtime_guard
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
_VIDARX_MAX_DRAWDOWN_PCT = 0.25
_VIDARX_BALANCED_CYCLE_FRACTION = 0.025
_VIDARX_TILTED_CYCLE_FRACTION = 0.035
_VIDARX_EXTREME_CYCLE_FRACTION = 0.05
_VIDARX_SETUP_DISABLE_MIN_WINDOWS = 4
_VIDARX_SETUP_DISABLE_MAX_WIN_RATE = 0.50
_VIDARX_ALLOWED_SETUPS = {
    ("tilted", "early-mid"),
    ("tilted", "mid-late"),
}
_ARB_PAIR_SUM_MAX = 0.992
_ARB_CHEAP_SIDE_SUM_MAX = 1.03
_ARB_FAIR_VALUE_EDGE_MIN = 0.025
_ARB_SINGLE_SIDE_BUDGET_FRACTION = 0.035
_ARB_PAIR_OVERLAY_FRACTION = 0.0
_ARB_MAX_PAIR_LEVELS = 20
_ARB_EARLY_MID_END = 150
_ARB_MID_LATE_START = 151
_ARB_ENABLE_CHEAP_SIDE = True
_ARB_ENABLE_PAIR_OVERLAY = False
_ARB_MIN_SECONDS = 10
_ARB_MAX_SECONDS = 275
_ARB_STRATEGY_MIN_NOTIONAL = 1.00
_ARB_CHEAP_SIDE_MIN_DELTA_BPS = 2.5
_ARB_CHEAP_SIDE_STRONG_EDGE_MIN = 0.11
_ARB_CHEAP_SIDE_SOFT_DELTA_BPS = 0.75
_ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN = 0.04
_ARB_CHEAP_SIDE_NET_EDGE_MIN = 0.025
_ARB_CHEAP_SIDE_OVERROUND_DRAG_WEIGHT = 0.65
_ARB_CHEAP_SIDE_SPREAD_DRAG_WEIGHT = 0.35
_ARB_CHEAP_SIDE_FEE_ESTIMATE = 0.0025
_ARB_REBALANCE_RATIO_TRIGGER = 0.06
_ARB_REBALANCE_BUDGET_FRACTION = 0.35
_ARB_PAIR_BURST_BASE = (1.0, 1.5, 2.5, 4.0, 6.0, 8.0, 12.0, 18.0)
_ARB_PAIR_BURST_MID_LATE = (1.0, 1.5, 2.5, 4.0, 6.0, 8.0, 12.0, 18.0, 27.0, 40.0)
_ARB_SINGLE_BURST_BASE = (0.7, 1.0, 1.5, 2.5, 4.0, 6.0, 8.0, 12.0)
_ARB_SINGLE_BURST_MID_LATE = (0.7, 1.0, 1.5, 2.5, 4.0, 6.0, 8.0, 12.0, 18.0, 27.0)
_ARB_SPOT_EDGE_MIN = 0.008
_ARB_SPOT_ANCHOR_GRACE_SECONDS = 20
_ARB_SPOT_ANCHOR_CAPTURE_WINDOW_SECONDS = 2.0
_ARB_MAX_MARKET_EXPOSURE_FRACTION = 0.05
_ARB_MAX_TOTAL_EXPOSURE_FRACTION = 0.20
_ARB_CHEAP_SIDE_BASE_PAIR_MAX = 1.02
_ARB_CHEAP_SIDE_MID_PAIR_MAX = 1.025
_ARB_CHEAP_SIDE_HIGH_PAIR_MAX = 1.03
_ARB_PAIR_RELAXED_SUM_MAX = 1.0
_ARB_BIASED_BRACKET_SUM_MAX = 1.018
_ARB_BIASED_BRACKET_NET_EDGE_MIN = 0.008
_ARB_BIASED_BRACKET_HEDGE_TOLERANCE = -0.012
_ARB_BRACKET_HEDGE_FLOOR_EARLY = 0.18
_ARB_BRACKET_HEDGE_FLOOR_MID_LATE = 0.12
_ARB_REPAIR_RATIO_TRIGGER = 0.14
_ARB_REPAIR_SUM_MAX = 1.04
_ARB_REPAIR_NET_EDGE_FLOOR = 0.005
_ARB_REPAIR_FAIR_SLACK = 0.02
_ARB_REPAIR_PROGRESS_FRACTION = 0.65
_ARB_REPAIR_BUDGET_FRACTION = 0.45
_ARB_STABILIZE_RATIO_TRIGGER = 0.22
_ARB_STABILIZE_EXTREME_RATIO = 0.90
_ARB_STABILIZE_MAX_PAIR_SUM = 1.015
_ARB_STABILIZE_MAX_NEG_NET_EDGE = -0.015
_ARB_STABILIZE_PROGRESS_FRACTION = 0.35
_ARB_STABILIZE_BUDGET_FRACTION = 0.18
_ARB_STABILIZE_MIN_DELTA_BPS = 1.0
_ARB_STABILIZE_CATCHUP_RATIO_GAP = 0.18
_ARB_STABILIZE_CATCHUP_TARGET_MIN_RATIO = 0.22
_ARB_STABILIZE_CATCHUP_MAX_PAIR_SUM = 1.03
_ARB_STABILIZE_CATCHUP_MAX_NEG_NET_EDGE = -0.030
_ARB_STABILIZE_CATCHUP_PROGRESS_FRACTION = 0.24
_ARB_STABILIZE_CATCHUP_BUDGET_FRACTION = 0.14
_ARB_MIN_OPERABLE_BUDGET_SLACK = 0.05
_ARB_UNWIND_RATIO_TRIGGER = 0.18
_ARB_UNWIND_EXTREME_RATIO = 0.88
_ARB_UNWIND_MAX_FAIR_DISCOUNT = 0.03
_ARB_UNWIND_PROGRESS_FRACTION = 0.50
_ARB_UNWIND_BUDGET_FRACTION = 0.35
_ARB_UNWIND_MIN_DELTA_BPS = 1.5
_ARB_BURST_COOLDOWN_MIN_SECONDS = 0.8
_ARB_BURST_COOLDOWN_MAX_SECONDS = 2.6
_ARB_MAX_PLAN_INSTRUCTIONS = 72
_ARB_NO_NEW_ENTRY_SECONDS_REMAINING = 25
_ARB_LATE_DIRECTIONAL_SECONDS_REMAINING = 90
_ARB_LATE_DIRECTIONAL_MAX_SPOT_AGE_MS = 600
_ARB_LATE_DIRECTIONAL_MIN_DELTA_BPS = 5.0
_ARB_LATE_DIRECTIONAL_MIN_EDGE = 0.015
_ARB_LATE_DIRECTIONAL_MIN_FAIR = 0.82
_MARKET_METADATA_CACHE_SECONDS = 10.0


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
    trigger_value: float
    pair_sum: float
    edge_pct: float
    fair_value: float
    spot_price: float = 0.0
    spot_anchor: float = 0.0
    spot_delta_bps: float = 0.0
    spot_fair_up: float = 0.0
    spot_fair_down: float = 0.0
    spot_source: str = ""
    spot_age_ms: int = 0
    spot_binance_price: float = 0.0
    spot_chainlink_price: float = 0.0
    spot_price_mode: str = ""
    desired_up_ratio: float = 0.5
    current_up_ratio: float = 0.5
    bracket_phase: str = ""
    reference_quality: str = ""
    reference_comparable: bool = False
    reference_note: str = ""


@dataclass(frozen=True)
class VidarxEntryLevel:
    price: float
    size: float
    bucket_price: float
    is_replenishment: bool


@dataclass(frozen=True)
class ArbPairLevel:
    up_price: float
    down_price: float
    shares: float
    pair_sum: float
    total_notional: float


@dataclass(frozen=True)
class ArbSingleSideLevel:
    price: float
    shares: float
    notional: float


@dataclass(frozen=True)
class ArbSingleSideSignal:
    target: MarketOutcome
    fair_value: float
    raw_edge: float
    net_edge: float
    edge_source: str


@dataclass(frozen=True)
class ArbSpotContext:
    current_price: float
    reference_price: float
    lead_price: float
    anchor_price: float
    local_anchor_price: float
    official_price_to_beat: float
    anchor_source: str
    fair_up: float
    fair_down: float
    delta_bps: float
    price_mode: str
    source: str
    age_ms: int
    binance_price: float | None
    chainlink_price: float | None


@dataclass(frozen=True)
class ArbCarryState:
    total_open_windows: int
    active_open_windows: int
    previous_open_windows: int
    carry_exposure: float
    current_market_exposure: float


@dataclass(frozen=True)
class ArbReferenceState:
    comparable: bool
    quality: str
    note: str
    budget_scale: float = 1.0


@dataclass(frozen=True)
class StrategyOperabilityState:
    state: str
    label: str
    reason: str
    blocking: bool = False


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
        runtime_diagnostics: RuntimeDiagnosticsService | None = None,
        spot_feed: SpotFeed | None = None,
        liquidation_feed: LiquidationFeed | None = None,
        user_feed: UserFeed | None = None,
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
        self.runtime_diagnostics = runtime_diagnostics
        self.spot_feed = spot_feed
        self.liquidation_feed = liquidation_feed
        self.user_feed = user_feed
        self.risk = RiskManager(settings.config)
        self.execution_engine = ExecutionEngine(
            db=self.db,
            research_dir=self.settings.paths.research_dir,
            paper_broker=self.paper_broker,
            live_broker=self.live_broker,
        )
        self.event_bus = EventBus()
        self.state_store = StateStore()
        self.feature_engine = FeatureEngine()
        self.readiness_scorer = ReadinessScorer(min_score=self.settings.config.decision_readiness_min_score)
        self.regime_detector = RegimeDetector()
        self.micro_strategy_engine = StrategyEngine(
            min_taker_edge_bps=self.settings.config.decision_min_taker_edge_bps,
            min_maker_edge_bps=self.settings.config.decision_min_maker_edge_bps,
        )
        self.telemetry = MicrostructureTelemetry(
            db=self.db,
            research_dir=self.settings.paths.research_dir,
            bus=self.event_bus,
            state_store=self.state_store,
            feature_engine=self.feature_engine,
            readiness_scorer=self.readiness_scorer,
            regime_detector=self.regime_detector,
            strategy_engine=self.micro_strategy_engine,
            log_events=self.settings.config.microstructure_log_events,
        )
        self._cached_market: dict | None = None
        self._cached_market_expires_at = 0.0
        self._official_price_cache: dict[str, tuple[float, float]] = {}
        self._arb_exchange_min_order_size_cache: dict[str, float] = {}
        self._last_cycle_log_signature = ""
        self._last_cycle_log_at = 0.0
        self._last_market_lookup_warning_signature = ""
        self._last_market_lookup_warning_at = 0.0
        self._attach_runtime_feeds()

    def run(self, mode: str = "paper") -> dict[str, int]:
        handler_name = str(self.settings.config.strategy_entry_mode or "").strip()
        if self.settings.strategy_registry is not None:
            handler_name = self.settings.strategy_registry.resolve(
                self.settings.config.strategy_variant,
                entry_mode=self.settings.config.strategy_entry_mode,
            ).runtime_handler
        handler = {
            "arb_micro": self._run_arb_micro,
            "vidarx_micro": self._run_vidarx_micro,
        }.get(handler_name)
        if handler is not None:
            return handler(mode=mode)

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
        operating_bankroll, reserved_profit = self._operating_bankroll_snapshot(
            mode=mode,
            live_total_capital=live_total_capital
        )
        self._record_balance_snapshot(
            mode=mode,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
            operating_bankroll=operating_bankroll,
            reserved_profit=reserved_profit,
        )
        ledger_allowed, ledger_note = self._position_ledger_can_run(mode=mode)
        if not ledger_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(note=ledger_note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=ledger_note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )
        live_allowed, live_control_note = self._live_control_can_execute(mode=mode)
        if not live_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(note=live_control_note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=live_control_note,
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

        guard_allowed, guard_note = self._runtime_guard_can_open(mode=mode)
        if not guard_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(market=market, note=guard_note)
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=guard_note,
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
                effective_bankroll=operating_bankroll,
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

    def _run_arb_micro(self, *, mode: str) -> dict[str, int]:
        stats = {
            "pending": 0,
            "filled": 0,
            "blocked": 0,
            "failed": 0,
            "skipped": 0,
            "opportunities": 0,
        }
        resolution_mode = self._strategy_resolution_mode_label(mode=mode)
        self._settle_resolved_paper_positions(mode=mode, stats=stats)
        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode=mode)
        marked_exposure, unrealized_pnl = self._paper_mark_to_market_snapshot()
        live_total_capital = cash_balance + marked_exposure
        operating_bankroll, reserved_profit = self._operating_bankroll_snapshot(
            mode=mode,
            live_total_capital=live_total_capital
        )
        self._record_balance_snapshot(
            mode=mode,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
            marked_exposure=marked_exposure,
            unrealized_pnl=unrealized_pnl,
            operating_bankroll=operating_bankroll,
            reserved_profit=reserved_profit,
        )
        ledger_allowed, ledger_note = self._position_ledger_can_run(mode=mode)
        if not ledger_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(
                note=ledger_note,
                extra_state=self._arb_state_defaults(strategy_resolution_mode=resolution_mode),
            )
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=ledger_note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        live_allowed, live_control_note = self._live_control_can_execute(mode=mode)
        if not live_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(
                note=live_control_note,
                extra_state=self._arb_state_defaults(strategy_resolution_mode=resolution_mode),
            )
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=live_control_note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        self._maybe_prime_arb_spot_anchor()

        drawdown_floor = self._mode_drawdown_floor(mode=mode, live_total_capital=live_total_capital)
        if live_total_capital <= drawdown_floor:
            capital_target = self._mode_capital_target(mode=mode, live_total_capital=live_total_capital)
            max_total_loss = max(capital_target - drawdown_floor, 0.0)
            note = (
                f"arb_micro drawdown stop: capital {live_total_capital:.2f} <= "
                f"{drawdown_floor:.2f} (max total loss {max_total_loss:.2f})"
            )
            stats["blocked"] += 1
            self._record_strategy_snapshot(
                note=note,
                extra_state=self._arb_state_defaults(strategy_resolution_mode=resolution_mode),
            )
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
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
                extra_state=self._arb_state_defaults(strategy_resolution_mode=resolution_mode),
            )
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        seconds_into_window = self._seconds_into_window(market)
        guard_allowed, guard_note = self._runtime_guard_can_open(mode=mode)
        if not guard_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(
                market=market,
                note=guard_note,
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_resolution_mode=resolution_mode,
                ),
            )
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=guard_note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        current_condition_id = str(market.get("conditionId") or "")
        carry_state = self._arb_carry_state(
            current_condition_id=current_condition_id,
            current_market_start_ts=self._btc5m_market_start_ts(market),
        )
        if (
            current_condition_id
            and carry_state.current_market_exposure <= 0
            and carry_state.active_open_windows >= self.settings.config.strategy_max_open_positions
        ):
            note = "arb_micro concurrent market limit reached"
            stats["blocked"] += 1
            self._record_strategy_snapshot(
                market=market,
                note=note,
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=self._seconds_into_window(market),
                    strategy_current_market_exposure=f"{carry_state.current_market_exposure:.6f}",
                    strategy_window_seconds=str(self._seconds_into_window(market)),
                    strategy_resolution_mode=resolution_mode,
                ),
            )
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        plan = self._build_arb_micro_plan(
            mode=mode,
            market=market,
            cash_balance=cash_balance,
            effective_bankroll=operating_bankroll,
            current_total_exposure=total_exposure,
            carry_exposure=carry_state.carry_exposure,
            carry_window_count=carry_state.previous_open_windows,
        )
        if plan is None:
            stats["skipped"] += 1
            return self._complete_cycle(
                mode=mode,
                stats=stats,
                note=self.db.get_bot_state("strategy_last_note") or "no arb plan",
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
                "strategy_trigger_outcome": self._arb_trigger_outcome(plan),
                "strategy_trigger_price_seen": f"{plan.trigger_value:.6f}",
                "strategy_market_bias": plan.market_bias,
                "strategy_plan_legs": str(len(plan.instructions)),
                "strategy_window_seconds": str(plan.window_seconds),
                "strategy_cycle_budget": f"{plan.cycle_budget:.6f}",
                "strategy_current_market_exposure": f"{self._get_condition_exposure(str(market.get('conditionId') or '')):.6f}",
                "strategy_resolution_mode": resolution_mode,
                "strategy_timing_regime": plan.timing_regime,
                "strategy_price_mode": plan.price_mode,
                "strategy_primary_ratio": f"{plan.primary_ratio:.6f}",
                "strategy_primary_outcome": plan.primary_target.label,
                "strategy_hedge_outcome": plan.secondary_target.label if plan.secondary_target else "",
                "strategy_primary_exposure": f"{plan.primary_notional:.6f}",
                "strategy_hedge_exposure": f"{plan.secondary_notional:.6f}",
                "strategy_replenishment_count": str(plan.replenishment_count),
                "strategy_pair_sum": f"{plan.pair_sum:.6f}",
                "strategy_edge_pct": f"{plan.edge_pct:.6f}",
                "strategy_fair_value": f"{plan.fair_value:.6f}",
                "strategy_spot_price": f"{plan.spot_price:.6f}",
                "strategy_spot_anchor": f"{plan.spot_anchor:.6f}",
                "strategy_spot_delta_bps": f"{plan.spot_delta_bps:.4f}",
                "strategy_spot_fair_up": f"{plan.spot_fair_up:.6f}",
                "strategy_spot_fair_down": f"{plan.spot_fair_down:.6f}",
                "strategy_spot_source": plan.spot_source,
                "strategy_spot_price_mode": plan.spot_price_mode or "missing",
                "strategy_spot_age_ms": str(plan.spot_age_ms),
                "strategy_spot_binance": f"{plan.spot_binance_price:.6f}",
                "strategy_spot_chainlink": f"{plan.spot_chainlink_price:.6f}",
                "strategy_desired_up_ratio": f"{plan.desired_up_ratio:.6f}",
                "strategy_desired_down_ratio": f"{max(1.0 - plan.desired_up_ratio, 0.0):.6f}",
                "strategy_current_up_ratio": f"{plan.current_up_ratio:.6f}",
                "strategy_bracket_phase": plan.bracket_phase or "observando",
                "strategy_reference_quality": plan.reference_quality,
                "strategy_reference_comparable": "1" if plan.reference_comparable else "0",
                "strategy_reference_note": plan.reference_note,
            },
        )
        self.db.upsert_strategy_window(
            slug=str(market.get("slug") or ""),
            condition_id=str(market.get("conditionId") or ""),
            title=str(market.get("question") or market.get("slug") or ""),
            price_mode=plan.price_mode,
            timing_regime=plan.timing_regime,
            primary_outcome=plan.primary_target.label,
            hedge_outcome=plan.secondary_target.label if plan.secondary_target else "",
            primary_ratio=plan.primary_ratio,
            planned_budget=plan.cycle_budget,
            current_exposure=self._get_condition_exposure(str(market.get("conditionId") or "")),
            notes=plan.note,
        )

        note = plan.note
        filled_notional = 0.0
        for instruction in plan.instructions:
            try:
                result = self._execute_instruction(mode=mode, instruction=instruction)
            except Exception as error:  # noqa: BLE001
                stats["failed"] += 1
                note = f"arb_micro execution failed: {error}"
                self.logger.exception("arb_micro execution failed: %s", error)
                continue

            if result.status == "filled":
                stats["filled"] += 1
                filled_notional += result.notional
            elif result.status == "skipped":
                stats["skipped"] += 1
                note = result.message or note

        if stats["filled"] > 0:
            self.db.record_strategy_window_fills(
                slug=str(market.get("slug") or ""),
                fill_count=stats["filled"],
                added_notional=filled_notional,
                replenishment_count=0,
                notes=note,
            )

        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode=mode)
        marked_exposure, unrealized_pnl = self._paper_mark_to_market_snapshot()
        live_total_capital = cash_balance + marked_exposure
        operating_bankroll, reserved_profit = self._operating_bankroll_snapshot(
            mode=mode,
            live_total_capital=live_total_capital
        )
        self._record_balance_snapshot(
            mode=mode,
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
            marked_exposure=marked_exposure,
            unrealized_pnl=unrealized_pnl,
            operating_bankroll=operating_bankroll,
            reserved_profit=reserved_profit,
        )
        self._record_strategy_snapshot(
            market=market,
            note=note,
            extra_state={
                "strategy_target_outcome": plan.primary_target.label,
                "strategy_target_price": f"{plan.primary_target.best_ask:.6f}",
                "strategy_trigger_outcome": self._arb_trigger_outcome(plan),
                "strategy_trigger_price_seen": f"{plan.trigger_value:.6f}",
                "strategy_market_bias": plan.market_bias,
                "strategy_plan_legs": str(len(plan.instructions)),
                "strategy_window_seconds": str(plan.window_seconds),
                "strategy_cycle_budget": f"{plan.cycle_budget:.6f}",
                "strategy_current_market_exposure": f"{self._get_condition_exposure(str(market.get('conditionId') or '')):.6f}",
                "strategy_resolution_mode": resolution_mode,
                "strategy_timing_regime": plan.timing_regime,
                "strategy_price_mode": plan.price_mode,
                "strategy_primary_ratio": f"{plan.primary_ratio:.6f}",
                "strategy_primary_outcome": plan.primary_target.label,
                "strategy_hedge_outcome": plan.secondary_target.label if plan.secondary_target else "",
                "strategy_primary_exposure": f"{plan.primary_notional:.6f}",
                "strategy_hedge_exposure": f"{plan.secondary_notional:.6f}",
                "strategy_replenishment_count": "0",
                "strategy_pair_sum": f"{plan.pair_sum:.6f}",
                "strategy_edge_pct": f"{plan.edge_pct:.6f}",
                "strategy_fair_value": f"{plan.fair_value:.6f}",
                "strategy_spot_price": f"{plan.spot_price:.6f}",
                "strategy_spot_anchor": f"{plan.spot_anchor:.6f}",
                "strategy_spot_delta_bps": f"{plan.spot_delta_bps:.4f}",
                "strategy_spot_fair_up": f"{plan.spot_fair_up:.6f}",
                "strategy_spot_fair_down": f"{plan.spot_fair_down:.6f}",
                "strategy_spot_source": plan.spot_source,
                "strategy_spot_price_mode": plan.spot_price_mode or "missing",
                "strategy_spot_age_ms": str(plan.spot_age_ms),
                "strategy_spot_binance": f"{plan.spot_binance_price:.6f}",
                "strategy_spot_chainlink": f"{plan.spot_chainlink_price:.6f}",
                "strategy_desired_up_ratio": f"{plan.desired_up_ratio:.6f}",
                "strategy_desired_down_ratio": f"{max(1.0 - plan.desired_up_ratio, 0.0):.6f}",
                "strategy_current_up_ratio": f"{plan.current_up_ratio:.6f}",
                "strategy_bracket_phase": plan.bracket_phase or "observando",
                "strategy_reference_quality": plan.reference_quality,
                "strategy_reference_comparable": "1" if plan.reference_comparable else "0",
                "strategy_reference_note": plan.reference_note,
            },
        )
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

        self._settle_resolved_paper_positions(mode=mode, stats=stats)
        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode="paper")
        marked_exposure, unrealized_pnl = self._paper_mark_to_market_snapshot()
        live_total_capital = cash_balance + marked_exposure
        operating_bankroll, reserved_profit = self._operating_bankroll_snapshot(
            live_total_capital=live_total_capital
        )
        self._record_balance_snapshot(
            mode="paper",
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
            marked_exposure=marked_exposure,
            unrealized_pnl=unrealized_pnl,
            operating_bankroll=operating_bankroll,
            reserved_profit=reserved_profit,
        )

        drawdown_floor = self.settings.config.bankroll * (1.0 - _VIDARX_MAX_DRAWDOWN_PCT)
        if live_total_capital <= drawdown_floor:
            note = (
                f"vidarx drawdown stop: capital {live_total_capital:.2f} <= "
                f"{drawdown_floor:.2f} ({_VIDARX_MAX_DRAWDOWN_PCT:.0%} loss)"
            )
            stats["blocked"] += 1
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

        guard_allowed, guard_note = self._runtime_guard_can_open(mode="paper")
        if not guard_allowed:
            stats["blocked"] += 1
            self._record_strategy_snapshot(
                market=market,
                note=guard_note,
                extra_state=self._vidarx_state_defaults(strategy_resolution_mode="paper-settle-at-close"),
            )
            return self._complete_cycle(
                mode="paper",
                stats=stats,
                note=guard_note,
                cash_balance=cash_balance,
                allowance=allowance,
                total_exposure=total_exposure,
                live_total_capital=live_total_capital,
            )

        plan = self._build_vidarx_plan(
            market=market,
            cash_balance=cash_balance,
            effective_bankroll=operating_bankroll,
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
                "strategy_spot_price": f"{plan.spot_price:.6f}",
                "strategy_spot_anchor": f"{plan.spot_anchor:.6f}",
                "strategy_spot_delta_bps": f"{plan.spot_delta_bps:.4f}",
                "strategy_spot_fair_up": f"{plan.spot_fair_up:.6f}",
                "strategy_spot_fair_down": f"{plan.spot_fair_down:.6f}",
                "strategy_spot_source": plan.spot_source,
                "strategy_spot_price_mode": plan.spot_price_mode or "missing",
                "strategy_spot_age_ms": str(plan.spot_age_ms),
                "strategy_spot_binance": f"{plan.spot_binance_price:.6f}",
                "strategy_spot_chainlink": f"{plan.spot_chainlink_price:.6f}",
            },
        )
        self.db.upsert_strategy_window(
            slug=str(market.get("slug") or ""),
            condition_id=str(market.get("conditionId") or ""),
            title=str(market.get("question") or market.get("slug") or ""),
            price_mode=plan.price_mode,
            timing_regime=plan.timing_regime,
            primary_outcome=plan.primary_target.label,
            hedge_outcome=plan.secondary_target.label if plan.secondary_target else "",
            primary_ratio=plan.primary_ratio,
            planned_budget=plan.cycle_budget,
            current_exposure=self._get_condition_exposure(str(market.get("conditionId") or "")),
            notes=plan.note,
        )

        note = plan.note
        filled_notional = 0.0
        for instruction in plan.instructions:
            try:
                result = self._execute_instruction(mode=mode, instruction=instruction)
            except Exception as error:  # noqa: BLE001
                stats["failed"] += 1
                note = f"vidarx_micro execution failed: {error}"
                self.logger.exception("vidarx_micro paper execution failed: %s", error)
                continue

            if result.status == "filled":
                stats["filled"] += 1
                filled_notional += result.notional
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

        if stats["filled"] > 0:
            self.db.record_strategy_window_fills(
                slug=str(market.get("slug") or ""),
                fill_count=stats["filled"],
                added_notional=filled_notional,
                replenishment_count=plan.replenishment_count,
                notes=note,
            )

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
                "strategy_spot_price": f"{plan.spot_price:.6f}",
                "strategy_spot_anchor": f"{plan.spot_anchor:.6f}",
                "strategy_spot_delta_bps": f"{plan.spot_delta_bps:.4f}",
                "strategy_spot_fair_up": f"{plan.spot_fair_up:.6f}",
                "strategy_spot_fair_down": f"{plan.spot_fair_down:.6f}",
                "strategy_spot_source": plan.spot_source,
                "strategy_spot_price_mode": plan.spot_price_mode or "missing",
                "strategy_spot_age_ms": str(plan.spot_age_ms),
                "strategy_spot_binance": f"{plan.spot_binance_price:.6f}",
                "strategy_spot_chainlink": f"{plan.spot_chainlink_price:.6f}",
            },
        )
        total_exposure = self.db.get_total_exposure()
        cash_balance, allowance = self._live_cash_snapshot(mode="paper")
        marked_exposure, unrealized_pnl = self._paper_mark_to_market_snapshot()
        live_total_capital = cash_balance + marked_exposure
        operating_bankroll, reserved_profit = self._operating_bankroll_snapshot(
            live_total_capital=live_total_capital
        )
        self._record_balance_snapshot(
            mode="paper",
            cash_balance=cash_balance,
            allowance=allowance,
            total_exposure=total_exposure,
            live_total_capital=live_total_capital,
            marked_exposure=marked_exposure,
            unrealized_pnl=unrealized_pnl,
            operating_bankroll=operating_bankroll,
            reserved_profit=reserved_profit,
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
        candidate_slugs = tuple(f"btc-updown-5m-{candidate_ts}" for candidate_ts in (base_start, base_start + 300, base_start - 300))

        cached_market = self._cached_market
        if (
            cached_market is not None
            and time.monotonic() < self._cached_market_expires_at
            and str(cached_market.get("slug") or "") in candidate_slugs
            and not bool(cached_market.get("closed"))
            and bool(cached_market.get("acceptingOrders", False))
        ):
            self._prime_market_feed(cached_market)
            return dict(cached_market)

        lookup_failed = False
        for slug in candidate_slugs:
            try:
                market = self.gamma_client.get_market_by_slug(slug)
            except Exception as error:  # noqa: BLE001
                lookup_failed = True
                self._log_market_lookup_warning(slug=slug, error=error)
                continue
            if not market:
                continue
            if bool(market.get("closed")):
                continue
            if not bool(market.get("acceptingOrders", False)):
                continue
            self._cached_market = dict(market)
            self._cached_market_expires_at = time.monotonic() + _MARKET_METADATA_CACHE_SECONDS
            self._prime_market_feed(market)
            self._prefetch_next_window(market)
            return market
        if (
            lookup_failed
            and cached_market is not None
            and str(cached_market.get("slug") or "") in candidate_slugs
            and not bool(cached_market.get("closed"))
            and bool(cached_market.get("acceptingOrders", False))
        ):
            self._prime_market_feed(cached_market)
            return dict(cached_market)
        self._cached_market = None
        self._cached_market_expires_at = 0.0
        return None

    def _log_market_lookup_warning(self, *, slug: str, error: Exception) -> None:
        signature = f"{slug}:{type(error).__name__}:{error}"
        now = time.monotonic()
        if signature == self._last_market_lookup_warning_signature and (now - self._last_market_lookup_warning_at) < 10.0:
            return
        self._last_market_lookup_warning_signature = signature
        self._last_market_lookup_warning_at = now
        self.logger.warning("market lookup failed slug=%s: %s", slug, error)

    def _prime_market_feed(self, market: dict) -> None:
        token_ids = _parse_json_list(market.get("clobTokenIds"))
        if not token_ids:
            return
        track_assets = getattr(self.clob_client, "track_assets", None)
        if callable(track_assets):
            try:
                track_assets(token_ids)
            except Exception as error:  # noqa: BLE001
                self.logger.debug("market feed prime skipped: %s", error)

    def _prefetch_next_window(self, market: dict) -> None:
        slug = str(market.get("slug") or "").strip()
        if not slug:
            return
        prefetch = getattr(self.gamma_client, "prefetch_next_btc5m_window", None)
        if not callable(prefetch):
            return
        try:
            prefetch(slug)
        except Exception as error:  # noqa: BLE001
            self.logger.debug("next window prefetch skipped: %s", error)

    def _incomplete_book_note(self, *, label: str, book: dict) -> str:
        missing: list[str] = []
        if _best_ask(book) is None:
            missing.append("best_ask")
        if _best_bid(book) is None:
            missing.append("best_bid")
        if _best_ask_size(book) is None:
            missing.append("best_ask_size")
        if not _ask_levels(book):
            missing.append("ask_levels")
        if not missing:
            return f"incomplete book for {label}"
        return f"incomplete book for {label}: missing {', '.join(missing)}"

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
                self._record_strategy_snapshot(market=market, note=self._incomplete_book_note(label=str(label), book=book))
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

    def _build_arb_micro_plan(
        self,
        *,
        mode: str,
        market: dict,
        cash_balance: float,
        effective_bankroll: float,
        current_total_exposure: float,
        carry_exposure: float = 0.0,
        carry_window_count: int = 0,
    ) -> StrategyPlan | None:
        outcomes = _parse_json_list(market.get("outcomes"))
        token_ids = _parse_json_list(market.get("clobTokenIds"))
        if len(outcomes) != 2 or len(token_ids) != 2:
            self._record_strategy_snapshot(market=market, note="arb_micro: market outcomes unavailable")
            return None

        priced_outcomes: list[MarketOutcome] = []
        for label, token_id in zip(outcomes, token_ids):
            book = self._safe_book(token_id)
            if not book:
                self._record_strategy_snapshot(market=market, note=f"arb_micro: no orderbook for {label}")
                return None
            best_ask = _best_ask(book)
            best_bid = _best_bid(book)
            best_ask_size = _best_ask_size(book)
            ask_levels = _ask_levels(book)
            if best_ask is None or best_bid is None or best_ask_size is None or not ask_levels:
                self._record_strategy_snapshot(market=market, note=self._incomplete_book_note(label=str(label), book=book))
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

        seconds_into_window = self._seconds_into_window(market)
        timing_regime, timing_note = self._select_arb_timing_regime(seconds_into_window=seconds_into_window)
        if timing_regime is None:
            self._record_strategy_snapshot(
                market=market,
                note=timing_note,
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_window_seconds=str(seconds_into_window),
                ),
            )
            return None

        spot_context = self._arb_spot_context(market=market, seconds_into_window=seconds_into_window)
        reference_state = self._arb_reference_state(
            mode=mode,
            source=spot_context.source if spot_context is not None else "",
            age_ms=spot_context.age_ms if spot_context is not None else 0,
            chainlink_price=float(spot_context.chainlink_price or 0.0) if spot_context is not None else 0.0,
            official_price_to_beat=spot_context.official_price_to_beat if spot_context is not None else 0.0,
            local_anchor_price=spot_context.local_anchor_price if spot_context is not None else 0.0,
            anchor_source=spot_context.anchor_source if spot_context is not None else "",
        )
        if self.settings.config.btc5m_strict_realism_mode and not reference_state.comparable:
            self._record_strategy_snapshot(
                market=market,
                note=f"arb_micro realism gate: {reference_state.note}",
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_timing_regime=timing_regime,
                    **self._arb_reference_state_entries(reference_state),
                ),
            )
            return None
        slug = str(market.get("slug") or "")
        condition_id = str(market.get("conditionId") or "")
        existing_market_notional = self._get_condition_exposure(condition_id)
        window_state = self.db.get_strategy_window(slug)
        market_cap = self._arb_market_exposure_cap(mode=mode, effective_bankroll=effective_bankroll)
        total_cap = self._arb_total_exposure_cap(mode=mode, effective_bankroll=effective_bankroll)

        if existing_market_notional >= market_cap:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"arb_micro market cap exhausted: {existing_market_notional:.2f} >= "
                    f"{market_cap:.2f}"
                ),
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                ),
            )
            return None

        if current_total_exposure >= total_cap:
            self._record_strategy_snapshot(
                market=market,
                note=(
                    f"arb_micro total cap exhausted: {current_total_exposure:.2f} >= "
                    f"{total_cap:.2f}"
                ),
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                ),
            )
            return None

        up_outcome, down_outcome = priced_outcomes[0], priced_outcomes[1]
        if up_outcome.best_ask > down_outcome.best_ask:
            primary_target, secondary_target = down_outcome, up_outcome
        else:
            primary_target, secondary_target = up_outcome, down_outcome

        current_up_notional, current_down_notional = self._get_condition_outcome_exposures(
            condition_id,
            price_marks={
                up_outcome.asset_id: self._arb_mark_price(up_outcome),
                down_outcome.asset_id: self._arb_mark_price(down_outcome),
            },
            basis="committed",
        )
        current_up_ratio = self._arb_current_up_ratio(
            up_exposure=current_up_notional,
            down_exposure=current_down_notional,
        )
        pair_sum = up_outcome.best_ask + down_outcome.best_ask
        directional_target = self._arb_directional_target(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            spot_context=spot_context,
            seconds_remaining=max(300 - seconds_into_window, 0),
        )
        desired_up_ratio, fair_up, fair_down, edge_up, edge_down = self._arb_desired_up_ratio(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            timing_regime=timing_regime,
            spot_context=spot_context,
            seconds_remaining=max(300 - seconds_into_window, 0),
        )
        desired_down_ratio = max(1.0 - desired_up_ratio, 0.0)
        bracket_phase = self._arb_bracket_phase(
            existing_market_notional=existing_market_notional,
            current_up_ratio=current_up_ratio,
            desired_up_ratio=desired_up_ratio,
        )
        carry_note = self._arb_carry_note(
            carry_exposure=carry_exposure,
            carry_window_count=carry_window_count,
        )
        effective_min_notional = self._arb_min_notional(up_outcome, down_outcome)
        up_exchange_min_size = self._arb_exchange_min_order_size(up_outcome.asset_id)
        down_exchange_min_size = self._arb_exchange_min_order_size(down_outcome.asset_id)
        up_exchange_min_notional = self._arb_exchange_min_notional(up_outcome)
        down_exchange_min_notional = self._arb_exchange_min_notional(down_outcome)
        self.db.set_bot_state("strategy_exchange_min_order_size_up", f"{up_exchange_min_size:.6f}")
        self.db.set_bot_state("strategy_exchange_min_order_size_down", f"{down_exchange_min_size:.6f}")
        self.db.set_bot_state("strategy_exchange_min_notional_up", f"{up_exchange_min_notional:.6f}")
        self.db.set_bot_state("strategy_exchange_min_notional_down", f"{down_exchange_min_notional:.6f}")

        cycle_budget = self._target_arb_cycle_budget(
            mode=mode,
            cash_balance=cash_balance,
            effective_bankroll=effective_bankroll,
            current_total_exposure=current_total_exposure,
            timing_regime=timing_regime,
            carry_exposure=carry_exposure,
        )
        cycle_budget = min(
            cycle_budget,
            max(market_cap - existing_market_notional, 0.0),
            max(total_cap - current_total_exposure, 0.0),
            cash_balance,
        )
        remaining_instruction_capacity = self._arb_dynamic_instruction_capacity(
            cycle_budget=cycle_budget,
            timing_regime=timing_regime,
            pair_sum=pair_sum,
            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
            ratio_gap=abs(desired_up_ratio - current_up_ratio),
            carry_exposure=carry_exposure,
        )
        if cycle_budget < effective_min_notional:
            self._record_strategy_snapshot(
                market=market,
                note="arb_micro budget below minimum after caps",
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_timing_regime=timing_regime,
                    strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                    strategy_cycle_budget=f"{cycle_budget:.6f}",
                    strategy_effective_min_notional=f"{effective_min_notional:.6f}",
                ),
            )
            return None

        up_net_edge = self._arb_estimated_single_side_net_edge(
            target=up_outcome,
            fair_value=fair_up,
            pair_sum=pair_sum,
            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
        )
        down_net_edge = self._arb_estimated_single_side_net_edge(
            target=down_outcome,
            fair_value=fair_down,
            pair_sum=pair_sum,
            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
        )
        pair_net_edge = self._arb_pair_net_edge(
            pair_sum=pair_sum,
            desired_up_ratio=desired_up_ratio,
            up_net_edge=up_net_edge,
            down_net_edge=down_net_edge,
        )
        pair_sum_cap = self._arb_effective_pair_sum_cap(
            pair_sum=pair_sum,
            pair_net_edge=pair_net_edge,
            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
        )
        cycle_budget = self._arb_scaled_cycle_budget(
            base_budget=cycle_budget,
            max_budget=min(
                max(market_cap - existing_market_notional, 0.0),
                max(total_cap - current_total_exposure, 0.0),
                cash_balance,
            ),
            timing_regime=timing_regime,
            pair_sum=pair_sum,
            pair_net_edge=pair_net_edge,
            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
            ratio_gap=abs(desired_up_ratio - current_up_ratio),
            carry_exposure=carry_exposure,
        )
        cycle_budget = _round_down(cycle_budget * reference_state.budget_scale, "0.01")
        remaining_instruction_capacity = self._arb_dynamic_instruction_capacity(
            cycle_budget=cycle_budget,
            timing_regime=timing_regime,
            pair_sum=pair_sum,
            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
            ratio_gap=abs(desired_up_ratio - current_up_ratio),
            carry_exposure=carry_exposure,
        )
        if cycle_budget < effective_min_notional:
            self._record_strategy_snapshot(
                market=market,
                note=f"arb_micro budget below minimum after reference gate ({reference_state.quality})",
                extra_state=self._arb_state_defaults(
                    market=market,
                    seconds_into_window=seconds_into_window,
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_timing_regime=timing_regime,
                    strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                    strategy_cycle_budget=f"{cycle_budget:.6f}",
                    strategy_effective_min_notional=f"{effective_min_notional:.6f}",
                    **self._arb_reference_state_entries(reference_state),
                ),
            )
            return None
        if window_state is not None:
            last_trade_at = int(window_state["last_trade_at"] or 0)
            if last_trade_at > 0:
                seconds_since_last_trade = max(time.time() - last_trade_at, 0.0)
                dynamic_cooldown = self._arb_dynamic_cooldown_seconds(
                    timing_regime=timing_regime,
                    pair_sum=pair_sum,
                    pair_net_edge=pair_net_edge,
                    delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
                    ratio_gap=abs(desired_up_ratio - current_up_ratio),
                    carry_exposure=carry_exposure,
                )
                if seconds_since_last_trade < dynamic_cooldown:
                    self._record_strategy_snapshot(
                        market=market,
                        note=(
                            f"arb_micro cooldown: {seconds_since_last_trade:.1f}s < "
                            f"{dynamic_cooldown:.1f}s"
                        ),
                        extra_state=self._arb_state_defaults(
                            market=market,
                            seconds_into_window=seconds_into_window,
                            strategy_window_seconds=str(seconds_into_window),
                            strategy_current_market_exposure=f"{existing_market_notional:.6f}",
                        ),
                    )
                    return None

        late_directional_pair_block = directional_target is not None and pair_sum >= 0.998
        if pair_sum <= pair_sum_cap and not late_directional_pair_block:
            overlay_reserve_fraction = 0.0
            if _ARB_ENABLE_PAIR_OVERLAY and abs(desired_up_ratio - 0.5) >= _ARB_REBALANCE_RATIO_TRIGGER:
                overlay_reserve_fraction = _ARB_REBALANCE_BUDGET_FRACTION
            pair_core_budget = cycle_budget * (1.0 - overlay_reserve_fraction)
            pair_levels = self._build_arb_pair_levels(
                up_outcome=up_outcome,
                down_outcome=down_outcome,
                budget=pair_core_budget,
                timing_regime=timing_regime,
                pair_sum_cap=pair_sum_cap,
                net_edge=pair_net_edge,
                delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
                ratio_gap=abs(desired_up_ratio - current_up_ratio),
            )
            if pair_levels:
                instructions: list[CopyInstruction] = []
                up_notional = 0.0
                down_notional = 0.0
                for idx, level in enumerate(pair_levels, start=1):
                    if len(instructions) + 2 > remaining_instruction_capacity:
                        break
                    up_instruction = self._build_arb_instruction(
                        market=market,
                        target=up_outcome,
                        shares=level.shares,
                        price=level.up_price,
                        pair_sum=level.pair_sum,
                        tranche_index=idx,
                    )
                    down_instruction = self._build_arb_instruction(
                        market=market,
                        target=down_outcome,
                        shares=level.shares,
                        price=level.down_price,
                        pair_sum=level.pair_sum,
                        tranche_index=idx,
                    )
                    if up_instruction is None or down_instruction is None:
                        continue
                    instructions.extend([up_instruction, down_instruction])
                    up_notional += up_instruction.notional
                    down_notional += down_instruction.notional

                projected_up_notional = current_up_notional + up_notional
                projected_down_notional = current_down_notional + down_notional
                projected_up_ratio = self._arb_current_up_ratio(
                    up_exposure=projected_up_notional,
                    down_exposure=projected_down_notional,
                )
                ratio_gap = desired_up_ratio - projected_up_ratio

                overlay_target: MarketOutcome | None = None
                overlay_fair = 0.0
                overlay_edge = 0.0
                if (
                    _ARB_ENABLE_PAIR_OVERLAY
                    and ratio_gap >= _ARB_REBALANCE_RATIO_TRIGGER
                    and edge_up >= _ARB_FAIR_VALUE_EDGE_MIN * 0.8
                ):
                    overlay_target = up_outcome
                    overlay_fair = fair_up
                    overlay_edge = max(edge_up, 0.0)
                elif (
                    _ARB_ENABLE_PAIR_OVERLAY
                    and ratio_gap <= -_ARB_REBALANCE_RATIO_TRIGGER
                    and edge_down >= _ARB_FAIR_VALUE_EDGE_MIN * 0.8
                ):
                    overlay_target = down_outcome
                    overlay_fair = fair_down
                    overlay_edge = max(edge_down, 0.0)

                if _ARB_ENABLE_PAIR_OVERLAY and instructions and overlay_target is not None:
                    projected_total = projected_up_notional + projected_down_notional
                    if overlay_target.asset_id == up_outcome.asset_id:
                        needed_notional = max((desired_up_ratio * projected_total) - projected_up_notional, 0.0)
                    else:
                        needed_notional = max((desired_down_ratio * projected_total) - projected_down_notional, 0.0)
                    remaining_plan_budget = max(cycle_budget - (up_notional + down_notional), 0.0)
                    overlay_budget = _round_down(
                        min(
                            cycle_budget * _ARB_REBALANCE_BUDGET_FRACTION,
                            needed_notional,
                            remaining_plan_budget,
                            max(cash_balance - (up_notional + down_notional), 0.0),
                        ),
                        "0.01",
                    )
                    if overlay_budget >= self._arb_min_notional(overlay_target):
                        overlay_levels = self._build_arb_single_side_levels(
                            target=overlay_target,
                            budget=overlay_budget,
                            fair_value=overlay_fair,
                            timing_regime=timing_regime,
                            relative_edge=overlay_edge,
                            pair_sum=pair_sum,
                            delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
                            ratio_gap=abs(desired_up_ratio - current_up_ratio),
                        )
                        base_index = len(instructions) + 1
                        for offset, level in enumerate(overlay_levels, start=0):
                            if len(instructions) + 1 > remaining_instruction_capacity:
                                break
                            instruction = self._build_arb_instruction(
                                market=market,
                                target=overlay_target,
                                shares=level.shares,
                                price=level.price,
                                pair_sum=pair_sum,
                                tranche_index=base_index + offset,
                            )
                            if instruction is None:
                                continue
                            instructions.append(instruction)
                            if overlay_target.asset_id == up_outcome.asset_id:
                                up_notional += instruction.notional
                            else:
                                down_notional += instruction.notional

                if instructions:
                    total_pair_notional = up_notional + down_notional
                    captured_edge_pct = pair_net_edge
                    primary_target, secondary_target = self._ordered_targets_by_notional(
                        up_outcome=up_outcome,
                        down_outcome=down_outcome,
                        up_notional=up_notional,
                        down_notional=down_notional,
                    )
                    primary_notional = up_notional if primary_target.asset_id == up_outcome.asset_id else down_notional
                    secondary_notional = down_notional if primary_target.asset_id == up_outcome.asset_id else up_notional
                    primary_ratio = primary_notional / total_pair_notional if total_pair_notional > 0 else 0.5
                    current_ratio_after = self._arb_current_up_ratio(
                        up_exposure=current_up_notional + up_notional,
                        down_exposure=current_down_notional + down_notional,
                    )
                    bias_note = (
                        f" | objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=desired_down_ratio)}"
                        f" | actual {self._arb_ratio_label(up_ratio=current_ratio_after, down_ratio=max(1.0 - current_ratio_after, 0.0))}"
                        f" | fase {bracket_phase}"
                    )
                    if spot_context is not None:
                        bias_note += f" | delta {spot_context.delta_bps:+.1f}bps"
                    note = (
                        f"underround {pair_levels[0].pair_sum:.3f} | net {captured_edge_pct * 100:.2f}% | "
                        f"{timing_regime} | niveles {len(pair_levels)} | patas {len(instructions)}{bias_note}{carry_note}"
                    )
                    return self._with_arb_reference_state(
                        StrategyPlan(
                        instructions=tuple(instructions),
                        note=note,
                        primary_target=primary_target,
                        secondary_target=secondary_target,
                        trigger=primary_target,
                        window_seconds=seconds_into_window,
                        cycle_budget=round(total_pair_notional, 6),
                        market_bias=(
                            f"Arbitraje + sesgo {primary_target.label} {primary_ratio * 100:.0f} / "
                            f"{secondary_target.label if secondary_target else '-'} {max((1 - primary_ratio) * 100, 0):.0f}"
                        ),
                        timing_regime=timing_regime,
                        price_mode="underround",
                        primary_ratio=primary_ratio,
                        primary_notional=primary_notional,
                        secondary_notional=secondary_notional,
                        replenishment_count=0,
                        trigger_value=pair_levels[0].pair_sum,
                        pair_sum=pair_levels[0].pair_sum,
                        edge_pct=captured_edge_pct,
                        fair_value=max(spot_context.fair_up, spot_context.fair_down) if spot_context is not None else 0.0,
                        spot_price=spot_context.current_price if spot_context is not None else 0.0,
                        spot_anchor=spot_context.anchor_price if spot_context is not None else 0.0,
                        spot_delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
                        spot_fair_up=spot_context.fair_up if spot_context is not None else 0.0,
                        spot_fair_down=spot_context.fair_down if spot_context is not None else 0.0,
                        spot_source=spot_context.source if spot_context is not None else "",
                        spot_price_mode=spot_context.price_mode if spot_context is not None else "",
                        spot_age_ms=spot_context.age_ms if spot_context is not None else 0,
                        spot_binance_price=spot_context.binance_price or 0.0 if spot_context is not None else 0.0,
                        spot_chainlink_price=spot_context.chainlink_price or 0.0 if spot_context is not None else 0.0,
                        desired_up_ratio=desired_up_ratio,
                        current_up_ratio=current_ratio_after,
                        bracket_phase=bracket_phase,
                        ),
                        reference_state,
                    )

        bracket_plan = self._build_arb_biased_bracket_plan(
            market=market,
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            pair_sum=pair_sum,
            fair_up=fair_up,
            fair_down=fair_down,
            up_net_edge=up_net_edge,
            down_net_edge=down_net_edge,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_up_ratio,
            timing_regime=timing_regime,
            cycle_budget=cycle_budget,
            cash_balance=cash_balance,
            remaining_instruction_capacity=remaining_instruction_capacity,
            current_up_notional=current_up_notional,
            current_down_notional=current_down_notional,
            spot_context=spot_context,
            bracket_phase=bracket_phase,
            carry_note=carry_note,
        )
        if bracket_plan is not None:
            return self._with_arb_reference_state(bracket_plan, reference_state)

        cheap_side = None
        if _ARB_ENABLE_CHEAP_SIDE:
            cheap_side = self._select_cheap_side_target(
                up_outcome=up_outcome,
                down_outcome=down_outcome,
                pair_sum=pair_sum,
                spot_context=spot_context,
                desired_up_ratio=desired_up_ratio,
                current_up_ratio=current_up_ratio,
            )
        if cheap_side is not None:
            target = cheap_side.target
            fair_value = cheap_side.fair_value
            relative_edge = cheap_side.raw_edge
            net_edge = cheap_side.net_edge
            edge_source = cheap_side.edge_source
            ratio_gap_abs = abs(desired_up_ratio - current_up_ratio)
            single_budget_fraction = _ARB_SINGLE_SIDE_BUDGET_FRACTION
            if existing_market_notional > 0:
                single_budget_fraction *= 1.0 + min(ratio_gap_abs * 3.0, 0.75)
            single_budget = _round_down(cycle_budget * single_budget_fraction, "0.01")
            single_budget = min(max(single_budget, self._arb_min_operable_budget(target)), cash_balance)
            single_levels = self._build_arb_single_side_levels(
                target=target,
                budget=single_budget,
                fair_value=fair_value,
                timing_regime=timing_regime,
                relative_edge=net_edge,
                pair_sum=pair_sum,
                delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
                ratio_gap=ratio_gap_abs,
            )
            if single_levels:
                instructions = [
                    instruction
                    for idx, level in enumerate(single_levels[:remaining_instruction_capacity], start=1)
                    if (
                        instruction := self._build_arb_instruction(
                            market=market,
                            target=target,
                            shares=level.shares,
                            price=level.price,
                            pair_sum=pair_sum,
                            tranche_index=idx,
                        )
                    )
                    is not None
                ]
                if instructions:
                    primary_notional = sum(item.notional for item in instructions)
                    projected_up_notional = current_up_notional + (primary_notional if target.asset_id == up_outcome.asset_id else 0.0)
                    projected_down_notional = current_down_notional + (primary_notional if target.asset_id == down_outcome.asset_id else 0.0)
                    current_ratio_after = self._arb_current_up_ratio(
                        up_exposure=projected_up_notional,
                        down_exposure=projected_down_notional,
                    )
                    note = (
                        f"cheap {target.label} ask {target.best_ask:.3f} < fair {fair_value:.3f} | "
                        f"edge {relative_edge * 100:.2f}% net {net_edge * 100:.2f}% | "
                        f"delta {spot_context.delta_bps:+.1f}bps | "
                        f"{edge_source} | {timing_regime} | niveles {len(single_levels)} | "
                        f"compras {len(instructions)} | objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=desired_down_ratio)} | "
                        f"actual {self._arb_ratio_label(up_ratio=current_ratio_after, down_ratio=max(1.0 - current_ratio_after, 0.0))} | fase {bracket_phase}{carry_note}"
                    )
                    return self._with_arb_reference_state(
                        StrategyPlan(
                        instructions=tuple(instructions),
                        note=note,
                        primary_target=target,
                        secondary_target=None,
                        trigger=target,
                        window_seconds=seconds_into_window,
                        cycle_budget=round(primary_notional, 6),
                        market_bias=f"Valor relativo {target.label}",
                        timing_regime=timing_regime,
                        price_mode="cheap-side",
                        primary_ratio=1.0,
                        primary_notional=primary_notional,
                        secondary_notional=0.0,
                        replenishment_count=0,
                        trigger_value=target.best_ask,
                        pair_sum=pair_sum,
                        edge_pct=max(net_edge, 0.0),
                        fair_value=fair_value,
                        spot_price=spot_context.current_price if spot_context is not None else 0.0,
                        spot_anchor=spot_context.anchor_price if spot_context is not None else 0.0,
                        spot_delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
                        spot_fair_up=spot_context.fair_up if spot_context is not None else 0.0,
                        spot_fair_down=spot_context.fair_down if spot_context is not None else 0.0,
                        spot_source=spot_context.source if spot_context is not None else "",
                        spot_price_mode=spot_context.price_mode if spot_context is not None else "",
                        spot_age_ms=spot_context.age_ms if spot_context is not None else 0,
                        spot_binance_price=spot_context.binance_price or 0.0 if spot_context is not None else 0.0,
                        spot_chainlink_price=spot_context.chainlink_price or 0.0 if spot_context is not None else 0.0,
                        desired_up_ratio=desired_up_ratio,
                        current_up_ratio=current_ratio_after,
                        bracket_phase=bracket_phase,
                        ),
                        reference_state,
                    )
        repair_plan = self._build_arb_repair_plan(
            market=market,
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            pair_sum=pair_sum,
            fair_up=fair_up,
            fair_down=fair_down,
            up_net_edge=up_net_edge,
            down_net_edge=down_net_edge,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_up_ratio,
            timing_regime=timing_regime,
            cycle_budget=cycle_budget,
            cash_balance=cash_balance,
            remaining_instruction_capacity=remaining_instruction_capacity,
            current_up_notional=current_up_notional,
            current_down_notional=current_down_notional,
            spot_context=spot_context,
            bracket_phase=bracket_phase,
            carry_note=carry_note,
        )
        if repair_plan is not None:
            return self._with_arb_reference_state(repair_plan, reference_state)
        stabilize_plan = self._build_arb_stabilize_plan(
            mode=mode,
            market=market,
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            pair_sum=pair_sum,
            fair_up=fair_up,
            fair_down=fair_down,
            up_net_edge=up_net_edge,
            down_net_edge=down_net_edge,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_up_ratio,
            timing_regime=timing_regime,
            cycle_budget=cycle_budget,
            cash_balance=cash_balance,
            remaining_instruction_capacity=remaining_instruction_capacity,
            current_up_notional=current_up_notional,
            current_down_notional=current_down_notional,
            spot_context=spot_context,
            bracket_phase=bracket_phase,
            carry_note=carry_note,
        )
        if stabilize_plan is not None:
            return self._with_arb_reference_state(stabilize_plan, reference_state)
        unwind_plan = self._build_arb_inventory_unwind_plan(
            market=market,
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            pair_sum=pair_sum,
            fair_up=fair_up,
            fair_down=fair_down,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_up_ratio,
            timing_regime=timing_regime,
            cycle_budget=cycle_budget,
            current_up_notional=current_up_notional,
            current_down_notional=current_down_notional,
            spot_context=spot_context,
            bracket_phase=bracket_phase,
            carry_note=carry_note,
        )
        if unwind_plan is not None:
            return self._with_arb_reference_state(unwind_plan, reference_state)
        delta_note = f" | delta {spot_context.delta_bps:+.1f}bps" if spot_context is not None else ""
        self._record_strategy_snapshot(
            market=market,
            note=(
                f"arb_micro no locked edge: pair sum {pair_sum:.3f} | "
                f"Up edge {edge_up * 100:.2f}% net {self._arb_estimated_single_side_net_edge(target=up_outcome, fair_value=fair_up, pair_sum=pair_sum, delta_bps=spot_context.delta_bps if spot_context is not None else 0.0) * 100:.2f}% | "
                f"Down edge {edge_down * 100:.2f}% net {self._arb_estimated_single_side_net_edge(target=down_outcome, fair_value=fair_down, pair_sum=pair_sum, delta_bps=spot_context.delta_bps if spot_context is not None else 0.0) * 100:.2f}% | "
                f"{delta_note.lstrip()}"
                f"{' | ' if delta_note else ''}objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=desired_down_ratio)} | "
                f"actual {self._arb_ratio_label(up_ratio=current_up_ratio, down_ratio=max(1.0 - current_up_ratio, 0.0))} | fase {bracket_phase}{carry_note}"
            ),
            extra_state=self._arb_state_defaults(
                market=market,
                seconds_into_window=seconds_into_window,
                strategy_window_seconds=str(seconds_into_window),
                strategy_timing_regime=timing_regime,
                strategy_trigger_price_seen=f"{pair_sum:.6f}",
                strategy_pair_sum=f"{pair_sum:.6f}",
                strategy_cycle_budget=f"{cycle_budget:.6f}",
                strategy_edge_pct=(
                    f"{max(self._arb_estimated_single_side_net_edge(target=up_outcome, fair_value=fair_up, pair_sum=pair_sum, delta_bps=spot_context.delta_bps if spot_context is not None else 0.0), self._arb_estimated_single_side_net_edge(target=down_outcome, fair_value=fair_down, pair_sum=pair_sum, delta_bps=spot_context.delta_bps if spot_context is not None else 0.0), 0.0):.6f}"
                ),
                strategy_fair_value=f"{max(fair_up, fair_down, 0.0):.6f}",
                strategy_spot_price=f"{spot_context.current_price:.6f}" if spot_context is not None else "0.000000",
                strategy_spot_anchor=f"{spot_context.anchor_price:.6f}" if spot_context is not None else "0.000000",
                strategy_spot_delta_bps=f"{spot_context.delta_bps:.4f}" if spot_context is not None else "0.0000",
                strategy_spot_fair_up=f"{spot_context.fair_up:.6f}" if spot_context is not None else "0.000000",
                strategy_spot_fair_down=f"{spot_context.fair_down:.6f}" if spot_context is not None else "0.000000",
                strategy_spot_source=spot_context.source if spot_context is not None else "",
                strategy_spot_price_mode=spot_context.price_mode if spot_context is not None else "missing",
                strategy_spot_age_ms=str(spot_context.age_ms if spot_context is not None else 0),
                strategy_spot_binance=f"{(spot_context.binance_price or 0.0):.6f}" if spot_context is not None else "0.000000",
                strategy_spot_chainlink=f"{(spot_context.chainlink_price or 0.0):.6f}" if spot_context is not None else "0.000000",
                strategy_desired_up_ratio=f"{desired_up_ratio:.6f}",
                strategy_desired_down_ratio=f"{desired_down_ratio:.6f}",
                strategy_current_up_ratio=f"{current_up_ratio:.6f}",
                strategy_bracket_phase=bracket_phase,
            ),
        )
        return None

    def _select_arb_timing_regime(self, *, seconds_into_window: int) -> tuple[str | None, str]:
        if _ARB_MIN_SECONDS <= seconds_into_window <= _ARB_EARLY_MID_END:
            return "early-mid", ""
        if _ARB_MID_LATE_START <= seconds_into_window <= _ARB_MAX_SECONDS:
            return "mid-late", ""
        if seconds_into_window < _ARB_MIN_SECONDS:
            return None, f"arb_micro demasiado pronto: {seconds_into_window}s < {_ARB_MIN_SECONDS}s"
        if seconds_into_window < _ARB_MID_LATE_START:
            return None, (
                f"arb_micro fuera de banda temporal: {seconds_into_window}s fuera de "
                f"{_ARB_MIN_SECONDS}-{_ARB_EARLY_MID_END}s y {_ARB_MID_LATE_START}-{_ARB_MAX_SECONDS}s"
            )
        return None, (
            f"arb_micro demasiado tarde para nueva entrada: {seconds_into_window}s > "
            f"{_ARB_MAX_SECONDS}s"
        )

    def _target_arb_cycle_budget(
        self,
        *,
        mode: str,
        cash_balance: float,
        effective_bankroll: float,
        current_total_exposure: float,
        timing_regime: str,
        carry_exposure: float = 0.0,
    ) -> float:
        if self.settings.config.strategy_fixed_trade_amount > 0:
            desired = self.settings.config.strategy_fixed_trade_amount
        else:
            desired = effective_bankroll * self.settings.config.strategy_trade_allocation_pct
        if timing_regime == "early-mid":
            desired *= 0.8
        else:
            desired *= 1.0

        if current_total_exposure > 0:
            desired *= 0.95
        if carry_exposure > 0 and effective_bankroll > 0:
            carry_ratio = min(carry_exposure / effective_bankroll, 0.25)
            desired *= max(0.45, 1.0 - (carry_ratio * 2.2))

        if mode in {"live", "shadow"}:
            live_cycle_budget = self._live_cycle_budget_target(
                mode=mode,
                live_total_capital=cash_balance + current_total_exposure,
            )
            if live_cycle_budget > 0:
                desired = min(max(desired, live_cycle_budget), live_cycle_budget)

        min_pair_budget = self._arb_strategy_min_notional() * 2
        desired = max(desired, min_pair_budget)
        desired = min(desired, cash_balance)
        return _round_down(desired, "0.01")

    def _build_arb_pair_levels(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        budget: float,
        timing_regime: str,
        pair_sum_cap: float,
        net_edge: float,
        delta_bps: float,
        ratio_gap: float,
    ) -> list[ArbPairLevel]:
        up_levels = [[level.price, level.size] for level in up_outcome.ask_levels[:_ARB_MAX_PAIR_LEVELS]]
        down_levels = [[level.price, level.size] for level in down_outcome.ask_levels[:_ARB_MAX_PAIR_LEVELS]]
        pairs: list[ArbPairLevel] = []
        up_idx = 0
        down_idx = 0
        remaining_budget = budget
        up_min_notional = self._arb_min_notional(up_outcome)
        down_min_notional = self._arb_min_notional(down_outcome)
        min_pair_budget = self._arb_pair_min_operable_budget(up_outcome=up_outcome, down_outcome=down_outcome)

        tranche_targets = self._arb_pair_tranche_targets(
            timing_regime=timing_regime,
            edge_pct=max(1.0 - min(up_outcome.best_ask + down_outcome.best_ask, pair_sum_cap), 0.0),
            pair_sum=min(up_outcome.best_ask + down_outcome.best_ask, pair_sum_cap),
            net_edge=net_edge,
            delta_bps=delta_bps,
            ratio_gap=ratio_gap,
            budget=budget,
            min_pair_budget=min_pair_budget,
        )

        for tranche_target in tranche_targets:
            if (
                up_idx >= len(up_levels)
                or down_idx >= len(down_levels)
                or remaining_budget < min_pair_budget
                or len(pairs) >= _ARB_MAX_PAIR_LEVELS
            ):
                break

            while up_idx < len(up_levels) and down_idx < len(down_levels):
                up_price, up_size = float(up_levels[up_idx][0]), float(up_levels[up_idx][1])
                down_price, down_size = float(down_levels[down_idx][0]), float(down_levels[down_idx][1])
                pair_sum = up_price + down_price
                if pair_sum > pair_sum_cap:
                    return pairs

                available_shares = min(up_size, down_size)
                if available_shares <= 0:
                    if up_size <= down_size:
                        up_idx += 1
                    else:
                        down_idx += 1
                    continue

                tranche_budget = min(tranche_target, remaining_budget)
                if tranche_budget < min_pair_budget and remaining_budget >= min_pair_budget:
                    tranche_budget = min_pair_budget
                max_shares_budget = tranche_budget / max(pair_sum, 1e-9)
                shares = _round_down(min(available_shares, max_shares_budget), "0.0001")
                if shares <= 0:
                    break

                up_notional = _round_down(shares * up_price, "0.01")
                down_notional = _round_down(shares * down_price, "0.01")
                if up_notional < up_min_notional or down_notional < down_min_notional:
                    up_level_capacity = _round_down(available_shares * up_price, "0.01")
                    down_level_capacity = _round_down(available_shares * down_price, "0.01")
                    if up_level_capacity < up_min_notional or down_level_capacity < down_min_notional:
                        if up_size <= down_size:
                            up_idx += 1
                        if down_size <= up_size:
                            down_idx += 1
                        continue
                    break

                total_notional = up_notional + down_notional
                pairs.append(
                    ArbPairLevel(
                        up_price=up_price,
                        down_price=down_price,
                        shares=shares,
                        pair_sum=pair_sum,
                        total_notional=total_notional,
                    )
                )
                remaining_budget -= total_notional
                up_levels[up_idx][1] = max(up_levels[up_idx][1] - shares, 0.0)
                down_levels[down_idx][1] = max(down_levels[down_idx][1] - shares, 0.0)
                if up_levels[up_idx][1] <= 1e-9:
                    up_idx += 1
                if down_levels[down_idx][1] <= 1e-9:
                    down_idx += 1
                break
        return pairs

    def _select_cheap_side_target(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        pair_sum: float,
        spot_context: ArbSpotContext | None = None,
        desired_up_ratio: float = 0.5,
        current_up_ratio: float = 0.5,
    ) -> ArbSingleSideSignal | None:
        if spot_context is None:
            return None

        fair_up_book = max(1.0 - down_outcome.best_bid, 0.0)
        fair_down_book = max(1.0 - up_outcome.best_bid, 0.0)
        fair_up = fair_up_book
        fair_down = fair_down_book
        edge_source_up = "book"
        edge_source_down = "book"
        if spot_context is not None:
            if spot_context.fair_up > fair_up:
                fair_up = spot_context.fair_up
                edge_source_up = "spot"
            if spot_context.fair_down > fair_down:
                fair_down = spot_context.fair_down
                edge_source_down = "spot"
        up_edge = fair_up - up_outcome.best_ask
        down_edge = fair_down - down_outcome.best_ask
        up_net_edge = self._arb_estimated_single_side_net_edge(
            target=up_outcome,
            fair_value=fair_up,
            pair_sum=pair_sum,
            delta_bps=spot_context.delta_bps,
        )
        down_net_edge = self._arb_estimated_single_side_net_edge(
            target=down_outcome,
            fair_value=fair_down,
            pair_sum=pair_sum,
            delta_bps=spot_context.delta_bps,
        )
        max_edge = max(up_edge, down_edge, 0.0)
        max_net_edge = max(up_net_edge, down_net_edge, 0.0)
        cheap_side_pair_max = _ARB_CHEAP_SIDE_BASE_PAIR_MAX
        if max_edge >= 0.12 or abs(spot_context.delta_bps) >= 16:
            cheap_side_pair_max = _ARB_CHEAP_SIDE_MID_PAIR_MAX
        if max_edge >= 0.16 and abs(spot_context.delta_bps) >= _ARB_CHEAP_SIDE_MIN_DELTA_BPS:
            cheap_side_pair_max = _ARB_CHEAP_SIDE_HIGH_PAIR_MAX
        effective_pair_cap = min(cheap_side_pair_max, _ARB_CHEAP_SIDE_SUM_MAX)
        if pair_sum > effective_pair_cap:
            return None
        strong_single_side_signal = (
            pair_sum <= min(_ARB_CHEAP_SIDE_BASE_PAIR_MAX, _ARB_CHEAP_SIDE_SUM_MAX)
            and max_edge >= _ARB_CHEAP_SIDE_STRONG_EDGE_MIN
        )
        soft_single_side_signal = (
            pair_sum <= min(_ARB_CHEAP_SIDE_MID_PAIR_MAX, _ARB_CHEAP_SIDE_SUM_MAX)
            and max_net_edge >= _ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN
            and abs(spot_context.delta_bps) >= _ARB_CHEAP_SIDE_SOFT_DELTA_BPS
        )
        if (
            abs(spot_context.delta_bps) < _ARB_CHEAP_SIDE_MIN_DELTA_BPS
            and max_edge < 0.10
            and not strong_single_side_signal
            and not soft_single_side_signal
        ):
            return None

        strong_ratio_gap = _ARB_REBALANCE_RATIO_TRIGGER * 0.5
        required_net_edge = _ARB_CHEAP_SIDE_NET_EDGE_MIN + min(max(pair_sum - 1.0, 0.0) * 0.35, 0.01)
        if abs(spot_context.delta_bps) < _ARB_CHEAP_SIDE_MIN_DELTA_BPS:
            required_net_edge = max(required_net_edge, _ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN)

        if (
            desired_up_ratio > current_up_ratio + strong_ratio_gap
            and up_net_edge >= required_net_edge
            and (
                spot_context.delta_bps >= _ARB_CHEAP_SIDE_MIN_DELTA_BPS
                or (spot_context.delta_bps >= _ARB_CHEAP_SIDE_SOFT_DELTA_BPS and up_net_edge >= _ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN)
                or (strong_single_side_signal and up_net_edge >= down_net_edge)
            )
        ):
            return ArbSingleSideSignal(
                target=up_outcome,
                fair_value=fair_up,
                raw_edge=up_edge,
                net_edge=up_net_edge,
                edge_source=edge_source_up,
            )
        if (
            desired_up_ratio < current_up_ratio - strong_ratio_gap
            and down_net_edge >= required_net_edge
            and (
                spot_context.delta_bps <= -_ARB_CHEAP_SIDE_MIN_DELTA_BPS
                or (spot_context.delta_bps <= -_ARB_CHEAP_SIDE_SOFT_DELTA_BPS and down_net_edge >= _ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN)
                or (strong_single_side_signal and down_net_edge > up_net_edge)
            )
        ):
            return ArbSingleSideSignal(
                target=down_outcome,
                fair_value=fair_down,
                raw_edge=down_edge,
                net_edge=down_net_edge,
                edge_source=edge_source_down,
            )

        wrong_side_block_gap = max(_ARB_REBALANCE_RATIO_TRIGGER * 2.0, 0.12)
        if desired_up_ratio > current_up_ratio + wrong_side_block_gap:
            return None
        if desired_up_ratio < current_up_ratio - wrong_side_block_gap:
            return None

        if (
            up_net_edge >= down_net_edge
            and up_net_edge >= required_net_edge
            and (
                spot_context.delta_bps >= _ARB_CHEAP_SIDE_MIN_DELTA_BPS
                or (spot_context.delta_bps >= _ARB_CHEAP_SIDE_SOFT_DELTA_BPS and up_net_edge >= _ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN)
                or strong_single_side_signal
            )
        ):
            return ArbSingleSideSignal(
                target=up_outcome,
                fair_value=fair_up,
                raw_edge=up_edge,
                net_edge=up_net_edge,
                edge_source=edge_source_up,
            )
        if (
            down_net_edge > up_net_edge
            and down_net_edge >= required_net_edge
            and (
                spot_context.delta_bps <= -_ARB_CHEAP_SIDE_MIN_DELTA_BPS
                or (spot_context.delta_bps <= -_ARB_CHEAP_SIDE_SOFT_DELTA_BPS and down_net_edge >= _ARB_CHEAP_SIDE_SOFT_NET_EDGE_MIN)
                or strong_single_side_signal
            )
        ):
            return ArbSingleSideSignal(
                target=down_outcome,
                fair_value=fair_down,
                raw_edge=down_edge,
                net_edge=down_net_edge,
                edge_source=edge_source_down,
            )
        return None

    def _build_arb_single_side_levels(
        self,
        *,
        target: MarketOutcome,
        budget: float,
        fair_value: float,
        timing_regime: str,
        relative_edge: float,
        pair_sum: float = 1.0,
        delta_bps: float = 0.0,
        ratio_gap: float = 0.0,
    ) -> list[ArbSingleSideLevel]:
        remaining_budget = budget
        levels: list[ArbSingleSideLevel] = []
        max_price = fair_value - max(_ARB_FAIR_VALUE_EDGE_MIN / 2, 0.005)
        min_notional = self._arb_min_notional(target)
        min_budget = self._arb_min_operable_budget(target)
        tranche_targets = self._arb_single_tranche_targets(
            timing_regime=timing_regime,
            edge_pct=relative_edge,
            pair_sum=pair_sum,
            delta_bps=delta_bps,
            ratio_gap=ratio_gap,
            budget=budget,
            min_budget=min_budget,
        )

        ask_levels = [[float(level.price), float(level.size)] for level in target.ask_levels[:_ARB_MAX_PAIR_LEVELS]]
        level_idx = 0
        for tranche_target in tranche_targets:
            if remaining_budget < min_budget or level_idx >= len(ask_levels):
                break
            while level_idx < len(ask_levels):
                price = float(ask_levels[level_idx][0])
                size = float(ask_levels[level_idx][1])
                if price > max_price:
                    return levels
                if size <= 0:
                    level_idx += 1
                    continue

                tranche_budget = min(tranche_target, remaining_budget)
                if tranche_budget < min_budget and remaining_budget >= min_budget:
                    tranche_budget = min_budget
                max_shares_budget = tranche_budget / max(price, 1e-9)
                shares = _round_down(min(size, max_shares_budget), "0.0001")
                if shares <= 0:
                    break

                notional = _round_down(shares * price, "0.01")
                if notional < min_notional:
                    level_capacity = _round_down(size * price, "0.01")
                    if level_capacity < min_notional:
                        level_idx += 1
                        continue
                    break

                levels.append(ArbSingleSideLevel(price=price, shares=shares, notional=notional))
                remaining_budget -= notional
                ask_levels[level_idx][1] = max(ask_levels[level_idx][1] - shares, 0.0)
                if ask_levels[level_idx][1] <= 1e-9:
                    level_idx += 1
                break

        return levels

    def _build_arb_repair_levels(
        self,
        *,
        target: MarketOutcome,
        budget: float,
        fair_value: float,
    ) -> list[ArbSingleSideLevel]:
        remaining_budget = budget
        levels: list[ArbSingleSideLevel] = []
        max_price = fair_value + _ARB_REPAIR_FAIR_SLACK
        min_notional = self._arb_min_notional(target)
        min_budget = self._arb_min_operable_budget(target)

        for level in target.ask_levels[:_ARB_MAX_PAIR_LEVELS]:
            if remaining_budget < min_budget:
                break
            price = float(level.price)
            size = float(level.size)
            if price > max_price:
                break
            if size <= 0:
                continue

            shares = _round_down(min(size, remaining_budget / max(price, 1e-9)), "0.0001")
            if shares <= 0:
                continue
            notional = _round_down(shares * price, "0.01")
            if notional < min_notional:
                continue

            levels.append(ArbSingleSideLevel(price=price, shares=shares, notional=notional))
            remaining_budget -= notional

        return levels

    def _arb_estimated_single_side_net_edge(
        self,
        *,
        target: MarketOutcome,
        fair_value: float,
        pair_sum: float,
        delta_bps: float,
    ) -> float:
        raw_edge = fair_value - target.best_ask
        overround_drag = max(pair_sum - 1.0, 0.0) * _ARB_CHEAP_SIDE_OVERROUND_DRAG_WEIGHT
        spread = max(target.best_ask - target.best_bid, 0.0)
        slippage_cap = max(float(self.settings.config.slippage_limit), 0.0)
        effective_spread = min(spread, slippage_cap) if slippage_cap > 0 else spread
        spread_drag = effective_spread * _ARB_CHEAP_SIDE_SPREAD_DRAG_WEIGHT
        drift_shortfall = max(_ARB_CHEAP_SIDE_MIN_DELTA_BPS - abs(delta_bps), 0.0)
        drift_drag = min(drift_shortfall / 400.0, 0.006)
        return raw_edge - overround_drag - spread_drag - drift_drag - _ARB_CHEAP_SIDE_FEE_ESTIMATE

    def _arb_strategy_min_notional(self) -> float:
        configured = float(getattr(self.settings.config, "arb_min_trade_amount", 0.0) or 0.0)
        return max(configured, _ARB_STRATEGY_MIN_NOTIONAL)

    def _arb_exchange_min_order_size(self, asset_id: str) -> float:
        asset_key = str(asset_id or "").strip()
        if not asset_key:
            return 0.0
        if asset_key in self._arb_exchange_min_order_size_cache:
            return self._arb_exchange_min_order_size_cache[asset_key]
        min_order_size = 0.0
        try:
            resolved = self.clob_client.get_min_order_size(asset_key)
        except Exception:  # noqa: BLE001
            resolved = None
        if resolved is not None and resolved > 0:
            min_order_size = float(resolved)
        self._arb_exchange_min_order_size_cache[asset_key] = min_order_size
        self.db.set_bot_state(f"strategy_asset_min_order_size:{asset_key}", f"{min_order_size:.6f}")
        return min_order_size

    def _arb_exchange_min_notional(self, target: MarketOutcome) -> float:
        min_order_size = self._arb_exchange_min_order_size(target.asset_id)
        if min_order_size <= 0:
            self.db.set_bot_state(f"strategy_asset_min_notional:{target.asset_id}", "0.000000")
            return 0.0
        reference_price = target.best_ask if target.best_ask > 0 else target.best_bid
        if reference_price <= 0:
            self.db.set_bot_state(f"strategy_asset_min_notional:{target.asset_id}", "0.000000")
            return 0.0
        min_notional = min_order_size * reference_price
        self.db.set_bot_state(f"strategy_asset_min_notional:{target.asset_id}", f"{min_notional:.6f}")
        return min_notional

    def _arb_min_notional(self, *targets: MarketOutcome) -> float:
        strategy_min = self._arb_strategy_min_notional()
        exchange_min = max((self._arb_exchange_min_notional(target) for target in targets), default=0.0)
        effective_min = max(strategy_min, exchange_min)
        self.db.set_bot_state("strategy_strategy_min_notional", f"{strategy_min:.6f}")
        self.db.set_bot_state("strategy_effective_min_notional", f"{effective_min:.6f}")
        return effective_min

    def _arb_min_operable_budget(self, target: MarketOutcome) -> float:
        min_notional = self._arb_min_notional(target)
        reference_price = target.best_ask if target.best_ask > 0 else target.best_bid
        if reference_price <= 0:
            return min_notional
        required_budget = _round_down(min_notional, "0.01")
        for _ in range(25):
            shares = _round_down(required_budget / max(reference_price, 1e-9), "0.0001")
            if shares > 0 and _round_down(shares * reference_price, "0.01") >= min_notional:
                return required_budget
            required_budget = _round_down(required_budget + 0.01, "0.01")
        return _round_down(min_notional + 0.25, "0.01")

    def _arb_pair_min_operable_budget(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
    ) -> float:
        up_price = up_outcome.best_ask if up_outcome.best_ask > 0 else up_outcome.best_bid
        down_price = down_outcome.best_ask if down_outcome.best_ask > 0 else down_outcome.best_bid
        if up_price <= 0 or down_price <= 0:
            return self._arb_min_operable_budget(up_outcome) + self._arb_min_operable_budget(down_outcome)

        up_min_notional = self._arb_min_notional(up_outcome)
        down_min_notional = self._arb_min_notional(down_outcome)
        min_shares = max(
            self._arb_exchange_min_order_size(up_outcome.asset_id),
            self._arb_exchange_min_order_size(down_outcome.asset_id),
            up_min_notional / max(up_price, 1e-9),
            down_min_notional / max(down_price, 1e-9),
        )
        if min_shares <= 0:
            return self._arb_min_operable_budget(up_outcome) + self._arb_min_operable_budget(down_outcome)

        required_shares = _round_up(min_shares, "0.0001")
        for _ in range(25):
            up_budget = _round_down(required_shares * up_price, "0.01")
            down_budget = _round_down(required_shares * down_price, "0.01")
            if up_budget >= up_min_notional and down_budget >= down_min_notional:
                return _round_down(up_budget + down_budget, "0.01")
            required_shares = _round_up(required_shares + 0.0001, "0.0001")
        return _round_down(
            self._arb_min_operable_budget(up_outcome) + self._arb_min_operable_budget(down_outcome),
            "0.01",
        )

    def _arb_floor_budget_to_min_notional(
        self,
        *,
        budget: float,
        target: MarketOutcome,
        needed_notional: float,
        cycle_budget: float,
        cash_balance: float,
    ) -> float:
        floored_budget = _round_down(max(budget, 0.0), "0.01")
        min_budget = self._arb_min_operable_budget(target)
        if floored_budget >= min_budget:
            return floored_budget
        max_operable_budget = min(
            max(needed_notional, 0.0),
            max(cycle_budget, 0.0) + _ARB_MIN_OPERABLE_BUDGET_SLACK,
            max(cash_balance, 0.0),
        )
        if max_operable_budget < min_budget:
            return floored_budget
        return _round_down(min(min_budget, max_operable_budget), "0.01")

    def _arb_market_exposure_cap(self, *, mode: str, effective_bankroll: float) -> float:
        cap = max(self._arb_strategy_min_notional() * 2, effective_bankroll * _ARB_MAX_MARKET_EXPOSURE_FRACTION)
        if mode in {"live", "shadow"}:
            live_cycle_budget = self._live_cycle_budget_target(mode=mode, live_total_capital=effective_bankroll)
            if live_cycle_budget > 0:
                cap = live_cycle_budget
        return cap

    def _arb_total_exposure_cap(self, *, mode: str, effective_bankroll: float) -> float:
        cap = max(self._arb_strategy_min_notional() * 2, effective_bankroll * _ARB_MAX_TOTAL_EXPOSURE_FRACTION)
        if mode in {"live", "shadow"}:
            live_cycle_budget = self._live_cycle_budget_target(mode=mode, live_total_capital=effective_bankroll)
            if live_cycle_budget > 0:
                cap = live_cycle_budget
        return cap

    def _get_condition_outcome_exposures(
        self,
        condition_id: str,
        *,
        price_marks: dict[str, float] | None = None,
        basis: str = "committed",
    ) -> tuple[float, float]:
        up_exposure = 0.0
        down_exposure = 0.0
        marks = price_marks or {}
        for row in self.db.list_copy_positions():
            if str(row["condition_id"] or "") != condition_id:
                continue
            size = float(row["size"] or 0.0)
            avg_price = float(row["avg_price"] or 0.0)
            asset = str(row["asset"] or "")
            mark_price = _safe_float(marks.get(asset))
            if size <= 0:
                continue
            if basis == "mark":
                reference_price = mark_price if mark_price > 0 else avg_price
            else:
                reference_price = avg_price if avg_price > 0 else mark_price
            if reference_price <= 0:
                continue
            exposure = abs(size * reference_price)
            outcome = str(row["outcome"] or "").strip().lower()
            if outcome == "up":
                up_exposure += exposure
            elif outcome == "down":
                down_exposure += exposure
        return up_exposure, down_exposure

    def _arb_mark_price(self, outcome: MarketOutcome) -> float:
        if outcome.best_bid > 0 and outcome.best_ask > 0:
            return (outcome.best_bid + outcome.best_ask) / 2
        if outcome.best_bid > 0:
            return outcome.best_bid
        return outcome.best_ask

    def _arb_current_up_ratio(self, *, up_exposure: float, down_exposure: float) -> float:
        total = up_exposure + down_exposure
        if total <= 0:
            return 0.5
        return up_exposure / total

    def _arb_ratio_bounds(
        self,
        *,
        timing_regime: str,
        seconds_remaining: int,
        directional_target: MarketOutcome | None,
    ) -> tuple[float, float]:
        if directional_target is not None and seconds_remaining <= _ARB_LATE_DIRECTIONAL_SECONDS_REMAINING:
            if directional_target.label.strip().lower() == "up":
                return 0.55, 0.95
            return 0.05, 0.45
        if timing_regime == "mid-late":
            return 0.30, 0.70
        return 0.38, 0.62

    def _arb_desired_up_ratio(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        timing_regime: str,
        spot_context: ArbSpotContext | None,
        seconds_remaining: int,
    ) -> tuple[float, float, float, float, float]:
        fair_up = max(1.0 - down_outcome.best_bid, 0.0)
        fair_down = max(1.0 - up_outcome.best_bid, 0.0)
        if spot_context is not None:
            fair_up = max(fair_up, spot_context.fair_up)
            fair_down = max(fair_down, spot_context.fair_down)

        total_fair = max(fair_up + fair_down, 1e-9)
        base_ratio = fair_up / total_fair
        up_edge = fair_up - up_outcome.best_ask
        down_edge = fair_down - down_outcome.best_ask
        edge_bias = max(min((up_edge - down_edge) * 1.75, 0.16), -0.16)
        ratio = base_ratio + edge_bias
        directional_target = self._arb_directional_target(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            spot_context=spot_context,
            seconds_remaining=seconds_remaining,
        )
        if directional_target is not None:
            ratio = 0.95 if directional_target.asset_id == up_outcome.asset_id else 0.05
        if spot_context is not None:
            spot_bias = max(min(spot_context.delta_bps / 400.0, 0.12), -0.12)
            ratio += spot_bias * (0.45 if timing_regime == "mid-late" else 0.30)
        min_ratio, max_ratio = self._arb_ratio_bounds(
            timing_regime=timing_regime,
            seconds_remaining=seconds_remaining,
            directional_target=directional_target,
        )
        ratio = min(max(ratio, min_ratio), max_ratio)
        return ratio, fair_up, fair_down, up_edge, down_edge

    def _arb_bracket_phase(
        self,
        *,
        existing_market_notional: float,
        current_up_ratio: float,
        desired_up_ratio: float,
    ) -> str:
        if existing_market_notional <= 0:
            return "abrir"
        if abs(desired_up_ratio - current_up_ratio) >= _ARB_REBALANCE_RATIO_TRIGGER:
            return "redistribuir"
        return "acompanar"

    def _arb_ratio_label(self, *, up_ratio: float, down_ratio: float) -> str:
        return f"Sube {up_ratio * 100:.0f}% / Baja {down_ratio * 100:.0f}%"

    def _arb_trigger_outcome(self, plan: StrategyPlan) -> str:
        if plan.price_mode == "underround":
            return "pair_sum"
        if plan.price_mode == "biased-bracket":
            return f"bracket:{plan.primary_target.label}"
        return f"cheap:{plan.primary_target.label}"

    def _arb_pair_net_edge(
        self,
        *,
        pair_sum: float,
        desired_up_ratio: float,
        up_net_edge: float,
        down_net_edge: float,
    ) -> float:
        weighted_edge = (desired_up_ratio * up_net_edge) + (max(1.0 - desired_up_ratio, 0.0) * down_net_edge)
        imbalance_drag = abs(desired_up_ratio - 0.5) * max(pair_sum - 0.99, 0.0) * 0.35
        return weighted_edge - imbalance_drag

    def _arb_effective_pair_sum_cap(
        self,
        *,
        pair_sum: float,
        pair_net_edge: float,
        delta_bps: float,
    ) -> float:
        cap = _ARB_PAIR_SUM_MAX
        if pair_net_edge >= 0.010:
            cap = max(cap, 0.996)
        if pair_net_edge >= 0.015 and abs(delta_bps) >= _ARB_CHEAP_SIDE_SOFT_DELTA_BPS:
            cap = max(cap, 0.998)
        if pair_net_edge >= 0.020 and abs(delta_bps) >= _ARB_CHEAP_SIDE_MIN_DELTA_BPS:
            cap = max(cap, _ARB_PAIR_RELAXED_SUM_MAX)
        return min(max(cap, _ARB_PAIR_SUM_MAX), _ARB_PAIR_RELAXED_SUM_MAX)

    def _arb_scaled_cycle_budget(
        self,
        *,
        base_budget: float,
        max_budget: float,
        timing_regime: str,
        pair_sum: float,
        pair_net_edge: float,
        delta_bps: float,
        ratio_gap: float,
        carry_exposure: float = 0.0,
    ) -> float:
        scale = 1.0
        if timing_regime == "mid-late":
            scale *= 1.10
        if pair_net_edge >= 0.025:
            scale *= 1.35
        elif pair_net_edge >= 0.015:
            scale *= 1.18
        elif pair_net_edge <= 0.006:
            scale *= 0.82
        if pair_sum <= 0.985:
            scale *= 1.12
        elif pair_sum <= 0.992:
            scale *= 1.06
        elif pair_sum >= 1.01:
            scale *= 0.92
        if abs(delta_bps) >= 8:
            scale *= 1.10
        elif abs(delta_bps) >= 3:
            scale *= 1.04
        if ratio_gap >= 0.10:
            scale *= 1.12
        elif ratio_gap >= 0.06:
            scale *= 1.06
        if carry_exposure > 0:
            carry_ratio = min(carry_exposure / max(base_budget + carry_exposure, 1e-9), 0.80)
            scale *= max(0.40, 1.0 - (carry_ratio * 1.05))

        desired = _round_down(min(base_budget * scale, max_budget), "0.01")
        min_pair_budget = self._arb_strategy_min_notional() * 2
        if max_budget < min_pair_budget:
            return _round_down(max_budget, "0.01")
        return max(desired, min_pair_budget)

    def _arb_dynamic_cooldown_seconds(
        self,
        *,
        timing_regime: str,
        pair_sum: float,
        pair_net_edge: float,
        delta_bps: float,
        ratio_gap: float,
        carry_exposure: float = 0.0,
    ) -> float:
        cooldown = 1.5 if timing_regime == "mid-late" else 2.0
        if pair_net_edge >= 0.020:
            cooldown -= 0.6
        elif pair_net_edge >= 0.012:
            cooldown -= 0.3
        elif pair_net_edge <= 0.006:
            cooldown += 0.4
        if pair_sum <= 0.992:
            cooldown -= 0.2
        if abs(delta_bps) >= 8:
            cooldown -= 0.25
        elif abs(delta_bps) >= 3:
            cooldown -= 0.1
        if ratio_gap >= 0.10:
            cooldown -= 0.2
        if carry_exposure > 0:
            carry_ratio = min(carry_exposure / max(carry_exposure + 20.0, 1.0), 0.75)
            cooldown += min(carry_ratio * 0.9, 0.5)
        return min(max(cooldown, _ARB_BURST_COOLDOWN_MIN_SECONDS), _ARB_BURST_COOLDOWN_MAX_SECONDS)

    def _arb_dynamic_instruction_capacity(
        self,
        *,
        cycle_budget: float,
        timing_regime: str,
        pair_sum: float,
        delta_bps: float,
        ratio_gap: float,
        carry_exposure: float = 0.0,
    ) -> int:
        capacity = 18 if timing_regime == "mid-late" else 14
        if cycle_budget >= 25:
            capacity += 4
        if cycle_budget >= 60:
            capacity += 8
        if cycle_budget >= 120:
            capacity += 12
        if pair_sum <= 0.992:
            capacity += 6
        elif pair_sum >= 1.01:
            capacity -= 2
        if abs(delta_bps) >= 8:
            capacity += 6
        elif abs(delta_bps) >= 3:
            capacity += 3
        if ratio_gap >= 0.10:
            capacity += 8
        elif ratio_gap >= 0.06:
            capacity += 4
        if carry_exposure > 0:
            carry_ratio = min(carry_exposure / max(cycle_budget + carry_exposure, 1e-9), 0.80)
            capacity = int(round(capacity * max(0.45, 1.0 - (carry_ratio * 0.70))))
        return int(min(max(capacity, 8), _ARB_MAX_PLAN_INSTRUCTIONS))

    def _scaled_tranche_budgets(
        self,
        *,
        base_targets: tuple[float, ...],
        budget: float,
        min_budget: float,
        level_cap: int,
    ) -> tuple[float, ...]:
        if budget < min_budget or min_budget <= 0 or level_cap <= 0:
            return ()
        feasible_count = int(max(math.floor(budget / min_budget), 0))
        if feasible_count <= 0:
            return ()
        target_count = min(feasible_count, level_cap)
        weights = [float(target) for target in base_targets if float(target) > 0]
        if not weights:
            return (_round_down(budget, "0.01"),)
        if target_count > len(weights):
            if len(weights) >= 2 and weights[-2] > 0:
                growth = weights[-1] / weights[-2]
            else:
                growth = 1.5
            growth = min(max(growth, 1.1), 1.8)
            while len(weights) < target_count:
                weights.append(weights[-1] * growth)
        selected = weights[:target_count]
        total_weight = sum(selected)
        if total_weight <= 0:
            return (_round_down(budget, "0.01"),)

        remaining_budget = _round_down(budget, "0.01")
        tranches: list[float] = []
        for idx, weight in enumerate(selected):
            remaining_slots = target_count - idx
            min_tail_budget = min_budget * (remaining_slots - 1)
            max_for_this = _round_down(max(remaining_budget - min_tail_budget, min_budget), "0.01")
            proportional = _round_down(budget * (weight / total_weight), "0.01")
            tranche = min(max(proportional, min_budget), max_for_this)
            tranches.append(tranche)
            remaining_budget = _round_down(max(remaining_budget - tranche, 0.0), "0.01")
        if tranches:
            tranches[-1] = _round_down(tranches[-1] + remaining_budget, "0.01")
        return tuple(tranche for tranche in tranches if tranche >= min_budget)

    def _arb_pair_tranche_targets(
        self,
        *,
        timing_regime: str,
        edge_pct: float,
        pair_sum: float,
        net_edge: float,
        delta_bps: float,
        ratio_gap: float,
        budget: float,
        min_pair_budget: float,
    ) -> tuple[float, ...]:
        base = _ARB_PAIR_BURST_MID_LATE if timing_regime == "mid-late" else _ARB_PAIR_BURST_BASE
        scale = 1.0
        if net_edge >= 0.025:
            scale *= 1.45
        elif net_edge >= 0.015:
            scale *= 1.25
        elif net_edge <= 0.006:
            scale *= 0.78
        if edge_pct >= 0.015:
            scale *= 1.12
        elif edge_pct <= 0.007:
            scale *= 0.92
        if pair_sum <= 0.985:
            scale *= 1.10
        elif pair_sum >= 0.998:
            scale *= 0.92
        if abs(delta_bps) >= 8:
            scale *= 1.12
        elif abs(delta_bps) >= 3:
            scale *= 1.05
        if ratio_gap >= 0.10:
            scale *= 1.10
        scaled_base = tuple(_round_down(target * scale, "0.01") for target in base)
        return self._scaled_tranche_budgets(
            base_targets=scaled_base,
            budget=budget,
            min_budget=min_pair_budget,
            level_cap=_ARB_MAX_PAIR_LEVELS,
        )

    def _arb_single_tranche_targets(
        self,
        *,
        timing_regime: str,
        edge_pct: float,
        pair_sum: float = 1.0,
        delta_bps: float = 0.0,
        ratio_gap: float = 0.0,
        budget: float,
        min_budget: float,
    ) -> tuple[float, ...]:
        base = _ARB_SINGLE_BURST_MID_LATE if timing_regime == "mid-late" else _ARB_SINGLE_BURST_BASE
        scale = 1.0
        if edge_pct >= 0.03:
            scale *= 1.35
        elif edge_pct <= 0.015:
            scale *= 0.85
        if pair_sum <= 0.992:
            scale *= 1.08
        elif pair_sum >= 1.01:
            scale *= 0.93
        if abs(delta_bps) >= 8:
            scale *= 1.10
        elif abs(delta_bps) >= 3:
            scale *= 1.04
        if ratio_gap >= 0.10:
            scale *= 1.08
        scaled_base = tuple(_round_down(target * scale, "0.01") for target in base)
        return self._scaled_tranche_budgets(
            base_targets=scaled_base,
            budget=budget,
            min_budget=min_budget,
            level_cap=_ARB_MAX_PAIR_LEVELS,
        )

    def _arb_spot_overlay_target(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        spot_context: ArbSpotContext | None,
    ) -> MarketOutcome | None:
        if spot_context is None:
            return None
        up_edge = spot_context.fair_up - up_outcome.best_ask
        down_edge = spot_context.fair_down - down_outcome.best_ask
        if up_edge >= down_edge and up_edge >= _ARB_SPOT_EDGE_MIN:
            return up_outcome
        if down_edge > up_edge and down_edge >= _ARB_SPOT_EDGE_MIN:
            return down_outcome
        return None

    def _build_arb_biased_bracket_plan(
        self,
        *,
        market: dict,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        pair_sum: float,
        fair_up: float,
        fair_down: float,
        up_net_edge: float,
        down_net_edge: float,
        desired_up_ratio: float,
        current_up_ratio: float,
        timing_regime: str,
        cycle_budget: float,
        cash_balance: float,
        remaining_instruction_capacity: int,
        current_up_notional: float,
        current_down_notional: float,
        spot_context: ArbSpotContext | None,
        bracket_phase: str,
        carry_note: str = "",
    ) -> StrategyPlan | None:
        seconds_remaining = max(300 - self._seconds_into_window(market), 0)
        directional_target = self._arb_directional_target(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            spot_context=spot_context,
            seconds_remaining=seconds_remaining,
        )
        if directional_target is not None:
            return None
        if seconds_remaining <= _ARB_LATE_DIRECTIONAL_SECONDS_REMAINING and spot_context is None:
            return None
        if pair_sum > _ARB_BIASED_BRACKET_SUM_MAX:
            return None

        pair_net_edge = self._arb_pair_net_edge(
            pair_sum=pair_sum,
            desired_up_ratio=desired_up_ratio,
            up_net_edge=up_net_edge,
            down_net_edge=down_net_edge,
        )
        if pair_net_edge < _ARB_BIASED_BRACKET_NET_EDGE_MIN:
            return None

        ratio_gap_abs = abs(desired_up_ratio - current_up_ratio)
        if desired_up_ratio >= 0.5:
            primary_target = up_outcome
            hedge_target = down_outcome
            primary_fair = fair_up
            hedge_fair = fair_down
            primary_net_edge = up_net_edge
            hedge_net_edge = down_net_edge
            desired_primary_ratio = desired_up_ratio
        else:
            primary_target = down_outcome
            hedge_target = up_outcome
            primary_fair = fair_down
            hedge_fair = fair_up
            primary_net_edge = down_net_edge
            hedge_net_edge = up_net_edge
            desired_primary_ratio = 1.0 - desired_up_ratio

        if primary_net_edge < max(_ARB_CHEAP_SIDE_NET_EDGE_MIN * 0.75, 0.01):
            return None
        if hedge_net_edge < _ARB_BIASED_BRACKET_HEDGE_TOLERANCE:
            return None

        hedge_floor = _ARB_BRACKET_HEDGE_FLOOR_MID_LATE if timing_regime == "mid-late" else _ARB_BRACKET_HEDGE_FLOOR_EARLY
        desired_hedge_ratio = max(1.0 - desired_primary_ratio, 0.0)
        hedge_ratio = max(desired_hedge_ratio, hedge_floor)
        if hedge_net_edge < 0:
            shrink = min(abs(hedge_net_edge) / abs(_ARB_BIASED_BRACKET_HEDGE_TOLERANCE), 1.0) * 0.65
            hedge_ratio = max(hedge_floor, desired_hedge_ratio * (1.0 - shrink))
        primary_ratio = min(max(1.0 - hedge_ratio, 0.52), 0.88)
        hedge_ratio = max(1.0 - primary_ratio, hedge_floor)
        total_ratio = primary_ratio + hedge_ratio
        if total_ratio <= 0:
            return None
        primary_ratio /= total_ratio
        hedge_ratio /= total_ratio

        primary_budget = _round_down(cycle_budget * primary_ratio, "0.01")
        hedge_budget = _round_down(min(cycle_budget - primary_budget, cash_balance - primary_budget), "0.01")
        if primary_budget < self._arb_min_notional(primary_target) or hedge_budget < self._arb_min_notional(hedge_target):
            return None

        primary_capacity = max(1, min(int(round(remaining_instruction_capacity * primary_ratio)), remaining_instruction_capacity - 1))
        hedge_capacity = max(1, remaining_instruction_capacity - primary_capacity)
        delta_bps = spot_context.delta_bps if spot_context is not None else 0.0

        primary_levels = self._build_arb_single_side_levels(
            target=primary_target,
            budget=primary_budget,
            fair_value=primary_fair,
            timing_regime=timing_regime,
            relative_edge=primary_net_edge,
            pair_sum=pair_sum,
            delta_bps=delta_bps,
            ratio_gap=ratio_gap_abs,
        )
        hedge_levels = self._build_arb_single_side_levels(
            target=hedge_target,
            budget=hedge_budget,
            fair_value=hedge_fair,
            timing_regime=timing_regime,
            relative_edge=max(hedge_net_edge, 0.0),
            pair_sum=pair_sum,
            delta_bps=delta_bps,
            ratio_gap=ratio_gap_abs,
        )
        if not primary_levels or not hedge_levels:
            return None

        primary_instructions: list[CopyInstruction] = []
        for idx, level in enumerate(primary_levels[:primary_capacity], start=1):
            instruction = self._build_arb_instruction(
                market=market,
                target=primary_target,
                shares=level.shares,
                price=level.price,
                pair_sum=pair_sum,
                tranche_index=idx,
            )
            if instruction is not None:
                primary_instructions.append(instruction)

        hedge_instructions: list[CopyInstruction] = []
        for idx, level in enumerate(hedge_levels[:hedge_capacity], start=1):
            instruction = self._build_arb_instruction(
                market=market,
                target=hedge_target,
                shares=level.shares,
                price=level.price,
                pair_sum=pair_sum,
                tranche_index=primary_capacity + idx,
            )
            if instruction is not None:
                hedge_instructions.append(instruction)

        if not primary_instructions or not hedge_instructions:
            return None

        instructions: list[CopyInstruction] = []
        max_len = max(len(primary_instructions), len(hedge_instructions))
        for index in range(max_len):
            if index < len(primary_instructions):
                instructions.append(primary_instructions[index])
            if index < len(hedge_instructions):
                instructions.append(hedge_instructions[index])

        primary_notional = sum(item.notional for item in primary_instructions)
        hedge_notional = sum(item.notional for item in hedge_instructions)
        if primary_notional < self._arb_min_notional(primary_target) or hedge_notional < self._arb_min_notional(hedge_target):
            return None

        added_up_notional = primary_notional if primary_target.asset_id == up_outcome.asset_id else hedge_notional
        added_down_notional = hedge_notional if primary_target.asset_id == up_outcome.asset_id else primary_notional
        current_ratio_after = self._arb_current_up_ratio(
            up_exposure=current_up_notional + added_up_notional,
            down_exposure=current_down_notional + added_down_notional,
        )
        ordered_primary, ordered_secondary = self._ordered_targets_by_notional(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            up_notional=added_up_notional,
            down_notional=added_down_notional,
        )
        ordered_primary_notional = added_up_notional if ordered_primary.asset_id == up_outcome.asset_id else added_down_notional
        ordered_secondary_notional = added_down_notional if ordered_primary.asset_id == up_outcome.asset_id else added_up_notional
        ordered_primary_ratio = ordered_primary_notional / max(ordered_primary_notional + ordered_secondary_notional, 1e-9)
        note = (
            f"biased bracket {pair_sum:.3f} | net {pair_net_edge * 100:.2f}% | "
            f"objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=max(1.0 - desired_up_ratio, 0.0))} | "
            f"actual {self._arb_ratio_label(up_ratio=current_ratio_after, down_ratio=max(1.0 - current_ratio_after, 0.0))} | "
            f"fase {bracket_phase}{carry_note}"
        )
        if spot_context is not None:
            note += f" | delta {spot_context.delta_bps:+.1f}bps"

        return StrategyPlan(
            instructions=tuple(instructions),
            note=note,
            primary_target=ordered_primary,
            secondary_target=ordered_secondary,
            trigger=primary_target,
            window_seconds=self._seconds_into_window(market),
            cycle_budget=round(primary_notional + hedge_notional, 6),
            market_bias=(
                f"Bracket sesgado {ordered_primary.label} {ordered_primary_ratio * 100:.0f}% / "
                f"{ordered_secondary.label} {max((1.0 - ordered_primary_ratio) * 100, 0.0):.0f}%"
            ),
            timing_regime=timing_regime,
            price_mode="biased-bracket",
            primary_ratio=ordered_primary_ratio,
            primary_notional=ordered_primary_notional,
            secondary_notional=ordered_secondary_notional,
            replenishment_count=0,
            trigger_value=pair_sum,
            pair_sum=pair_sum,
            edge_pct=pair_net_edge,
            fair_value=max(fair_up, fair_down, 0.0),
            spot_price=spot_context.current_price if spot_context is not None else 0.0,
            spot_anchor=spot_context.anchor_price if spot_context is not None else 0.0,
            spot_delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
            spot_fair_up=spot_context.fair_up if spot_context is not None else 0.0,
            spot_fair_down=spot_context.fair_down if spot_context is not None else 0.0,
            spot_source=spot_context.source if spot_context is not None else "",
            spot_price_mode=spot_context.price_mode if spot_context is not None else "",
            spot_age_ms=spot_context.age_ms if spot_context is not None else 0,
            spot_binance_price=spot_context.binance_price or 0.0 if spot_context is not None else 0.0,
            spot_chainlink_price=spot_context.chainlink_price or 0.0 if spot_context is not None else 0.0,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_ratio_after,
            bracket_phase=bracket_phase,
        )

    def _build_arb_repair_plan(
        self,
        *,
        market: dict,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        pair_sum: float,
        fair_up: float,
        fair_down: float,
        up_net_edge: float,
        down_net_edge: float,
        desired_up_ratio: float,
        current_up_ratio: float,
        timing_regime: str,
        cycle_budget: float,
        cash_balance: float,
        remaining_instruction_capacity: int,
        current_up_notional: float,
        current_down_notional: float,
        spot_context: ArbSpotContext | None,
        bracket_phase: str,
        carry_note: str = "",
    ) -> StrategyPlan | None:
        if bracket_phase != "redistribuir":
            return None
        seconds_remaining = max(300 - self._seconds_into_window(market), 0)
        directional_target = self._arb_directional_target(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            spot_context=spot_context,
            seconds_remaining=seconds_remaining,
        )
        if seconds_remaining <= _ARB_LATE_DIRECTIONAL_SECONDS_REMAINING and spot_context is None:
            return None

        ratio_gap = desired_up_ratio - current_up_ratio
        ratio_gap_abs = abs(ratio_gap)
        if ratio_gap_abs < _ARB_REPAIR_RATIO_TRIGGER:
            return None
        if pair_sum > _ARB_REPAIR_SUM_MAX:
            return None

        desired_down_ratio = max(1.0 - desired_up_ratio, 0.0)
        if ratio_gap > 0:
            target = up_outcome
            fair_value = fair_up
            net_edge = up_net_edge
            desired_target_ratio = desired_up_ratio
            current_target_notional = current_up_notional
            other_notional = current_down_notional
        else:
            target = down_outcome
            fair_value = fair_down
            net_edge = down_net_edge
            desired_target_ratio = desired_down_ratio
            current_target_notional = current_down_notional
            other_notional = current_up_notional

        if directional_target is not None and target.asset_id != directional_target.asset_id:
            return None

        if desired_target_ratio <= 0 or desired_target_ratio >= 0.999:
            return None
        if target.best_ask > fair_value + _ARB_REPAIR_FAIR_SLACK:
            return None
        if net_edge < _ARB_REPAIR_NET_EDGE_FLOOR:
            return None

        numerator = (desired_target_ratio * (current_target_notional + other_notional)) - current_target_notional
        needed_notional = numerator / max(1.0 - desired_target_ratio, 1e-9)
        if needed_notional <= 0:
            return None

        repair_budget = _round_down(
            min(
                needed_notional * _ARB_REPAIR_PROGRESS_FRACTION,
                cycle_budget * _ARB_REPAIR_BUDGET_FRACTION,
                cash_balance,
            ),
            "0.01",
        )
        repair_budget = self._arb_floor_budget_to_min_notional(
            budget=repair_budget,
            target=target,
            needed_notional=needed_notional,
            cycle_budget=cycle_budget,
            cash_balance=cash_balance,
        )
        if repair_budget < self._arb_min_notional(target):
            return None

        repair_levels = self._build_arb_repair_levels(
            target=target,
            budget=repair_budget,
            fair_value=fair_value,
        )
        if not repair_levels:
            return None

        instructions = [
            instruction
            for idx, level in enumerate(repair_levels[:remaining_instruction_capacity], start=1)
            if (
                instruction := self._build_arb_instruction(
                    market=market,
                    target=target,
                    shares=level.shares,
                    price=level.price,
                    pair_sum=pair_sum,
                    tranche_index=idx,
                )
            )
            is not None
        ]
        if not instructions:
            return None

        primary_notional = sum(item.notional for item in instructions)
        projected_up_notional = current_up_notional + (primary_notional if target.asset_id == up_outcome.asset_id else 0.0)
        projected_down_notional = current_down_notional + (primary_notional if target.asset_id == down_outcome.asset_id else 0.0)
        current_ratio_after = self._arb_current_up_ratio(
            up_exposure=projected_up_notional,
            down_exposure=projected_down_notional,
        )
        note = (
            f"repair {target.label} ask {target.best_ask:.3f} ~ fair {fair_value:.3f} | "
            f"net {net_edge * 100:.2f}% | {timing_regime} | compras {len(instructions)} | "
            f"objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=desired_down_ratio)} | "
            f"actual {self._arb_ratio_label(up_ratio=current_ratio_after, down_ratio=max(1.0 - current_ratio_after, 0.0))} | "
            f"fase {bracket_phase}{carry_note}"
        )
        if spot_context is not None:
            note += f" | delta {spot_context.delta_bps:+.1f}bps"

        return StrategyPlan(
            instructions=tuple(instructions),
            note=note,
            primary_target=target,
            secondary_target=None,
            trigger=target,
            window_seconds=self._seconds_into_window(market),
            cycle_budget=round(primary_notional, 6),
            market_bias=f"Repair {target.label}",
            timing_regime=timing_regime,
            price_mode="repair-bracket",
            primary_ratio=1.0,
            primary_notional=primary_notional,
            secondary_notional=0.0,
            replenishment_count=0,
            trigger_value=target.best_ask,
            pair_sum=pair_sum,
            edge_pct=max(net_edge, 0.0),
            fair_value=fair_value,
            spot_price=spot_context.current_price if spot_context is not None else 0.0,
            spot_anchor=spot_context.anchor_price if spot_context is not None else 0.0,
            spot_delta_bps=spot_context.delta_bps if spot_context is not None else 0.0,
            spot_fair_up=spot_context.fair_up if spot_context is not None else 0.0,
            spot_fair_down=spot_context.fair_down if spot_context is not None else 0.0,
            spot_source=spot_context.source if spot_context is not None else "",
            spot_price_mode=spot_context.price_mode if spot_context is not None else "",
            spot_age_ms=spot_context.age_ms if spot_context is not None else 0,
            spot_binance_price=spot_context.binance_price or 0.0 if spot_context is not None else 0.0,
            spot_chainlink_price=spot_context.chainlink_price or 0.0 if spot_context is not None else 0.0,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_ratio_after,
            bracket_phase=bracket_phase,
        )

    def _build_arb_stabilize_plan(
        self,
        *,
        mode: str = "paper",
        market: dict,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        pair_sum: float,
        fair_up: float,
        fair_down: float,
        up_net_edge: float,
        down_net_edge: float,
        desired_up_ratio: float,
        current_up_ratio: float,
        timing_regime: str,
        cycle_budget: float,
        cash_balance: float,
        remaining_instruction_capacity: int,
        current_up_notional: float,
        current_down_notional: float,
        spot_context: ArbSpotContext | None,
        bracket_phase: str,
        carry_note: str = "",
    ) -> StrategyPlan | None:
        if bracket_phase != "redistribuir" or spot_context is None:
            return None
        ratio_gap = desired_up_ratio - current_up_ratio
        ratio_gap_abs = abs(ratio_gap)
        if ratio_gap_abs < _ARB_STABILIZE_RATIO_TRIGGER:
            return None
        if current_up_ratio < (1.0 - _ARB_STABILIZE_EXTREME_RATIO) or current_up_ratio > _ARB_STABILIZE_EXTREME_RATIO:
            extreme_imbalance = True
        else:
            extreme_imbalance = False
        if not extreme_imbalance:
            return None
        relaxed_live_catchup = str(mode or "").strip().lower() in {"live", "shadow"}

        seconds_remaining = max(300 - self._seconds_into_window(market), 0)
        directional_target = self._arb_directional_target(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            spot_context=spot_context,
            seconds_remaining=seconds_remaining,
        )
        desired_down_ratio = max(1.0 - desired_up_ratio, 0.0)
        if ratio_gap > 0:
            target = up_outcome
            fair_value = fair_up
            net_edge = up_net_edge
            desired_target_ratio = desired_up_ratio
            current_target_notional = current_up_notional
            other_notional = current_down_notional
            delta_supports_target = spot_context.delta_bps >= _ARB_STABILIZE_MIN_DELTA_BPS
        else:
            target = down_outcome
            fair_value = fair_down
            net_edge = down_net_edge
            desired_target_ratio = desired_down_ratio
            current_target_notional = current_down_notional
            other_notional = current_up_notional
            delta_supports_target = spot_context.delta_bps <= -_ARB_STABILIZE_MIN_DELTA_BPS

        current_total_notional = max(current_up_notional + current_down_notional, 1e-9)
        current_target_ratio = current_target_notional / current_total_notional
        severe_target_shortfall = (
            ratio_gap_abs >= _ARB_STABILIZE_CATCHUP_RATIO_GAP
            and desired_target_ratio >= _ARB_STABILIZE_CATCHUP_TARGET_MIN_RATIO
            and current_target_ratio <= max(desired_target_ratio * 0.5, 0.20)
        )
        if relaxed_live_catchup:
            normal_stabilize_viable = (
                delta_supports_target
                and pair_sum <= _ARB_STABILIZE_MAX_PAIR_SUM
                and net_edge >= _ARB_STABILIZE_MAX_NEG_NET_EDGE
            )
            catchup_rebalance = (
                severe_target_shortfall
                and not normal_stabilize_viable
                and net_edge >= _ARB_STABILIZE_CATCHUP_MAX_NEG_NET_EDGE
            )
        else:
            catchup_rebalance = not delta_supports_target and severe_target_shortfall
        if not delta_supports_target and not catchup_rebalance:
            return None
        max_pair_sum = (
            _ARB_STABILIZE_CATCHUP_MAX_PAIR_SUM
            if catchup_rebalance and relaxed_live_catchup
            else _ARB_STABILIZE_MAX_PAIR_SUM
        )
        if pair_sum > max_pair_sum:
            return None

        if directional_target is not None and target.asset_id != directional_target.asset_id:
            return None
        if desired_target_ratio <= 0 or desired_target_ratio >= 0.999:
            return None
        if target.best_ask > fair_value + _ARB_REPAIR_FAIR_SLACK:
            return None
        allowed_negative_edge = (
            _ARB_STABILIZE_CATCHUP_MAX_NEG_NET_EDGE
            if catchup_rebalance and relaxed_live_catchup
            else _ARB_STABILIZE_MAX_NEG_NET_EDGE
        )
        if net_edge < allowed_negative_edge:
            return None

        numerator = (desired_target_ratio * (current_target_notional + other_notional)) - current_target_notional
        needed_notional = numerator / max(1.0 - desired_target_ratio, 1e-9)
        if needed_notional <= 0:
            return None

        progress_fraction = (
            _ARB_STABILIZE_CATCHUP_PROGRESS_FRACTION if catchup_rebalance else _ARB_STABILIZE_PROGRESS_FRACTION
        )
        budget_fraction = (
            _ARB_STABILIZE_CATCHUP_BUDGET_FRACTION if catchup_rebalance else _ARB_STABILIZE_BUDGET_FRACTION
        )
        stabilize_budget = _round_down(
            min(
                needed_notional * progress_fraction,
                cycle_budget * budget_fraction,
                cash_balance,
            ),
            "0.01",
        )
        stabilize_budget = self._arb_floor_budget_to_min_notional(
            budget=stabilize_budget,
            target=target,
            needed_notional=needed_notional,
            cycle_budget=cycle_budget,
            cash_balance=cash_balance,
        )
        if stabilize_budget < self._arb_min_notional(target):
            return None

        stabilize_levels = self._build_arb_repair_levels(
            target=target,
            budget=stabilize_budget,
            fair_value=max(fair_value, target.best_ask - _ARB_REPAIR_FAIR_SLACK),
        )
        if not stabilize_levels:
            return None

        instructions = [
            instruction
            for idx, level in enumerate(stabilize_levels[:remaining_instruction_capacity], start=1)
            if (
                instruction := self._build_arb_instruction(
                    market=market,
                    target=target,
                    shares=level.shares,
                    price=level.price,
                    pair_sum=pair_sum,
                    tranche_index=idx,
                )
            )
            is not None
        ]
        if not instructions:
            return None

        primary_notional = sum(item.notional for item in instructions)
        projected_up_notional = current_up_notional + (primary_notional if target.asset_id == up_outcome.asset_id else 0.0)
        projected_down_notional = current_down_notional + (primary_notional if target.asset_id == down_outcome.asset_id else 0.0)
        current_ratio_after = self._arb_current_up_ratio(
            up_exposure=projected_up_notional,
            down_exposure=projected_down_notional,
        )
        mode_label = "catchup" if catchup_rebalance else "stabilize"
        note = (
            f"{mode_label} {target.label} ask {target.best_ask:.3f} ~ fair {fair_value:.3f} | "
            f"net {net_edge * 100:.2f}% | delta {spot_context.delta_bps:+.1f}bps | "
            f"{timing_regime} | compras {len(instructions)} | "
            f"objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=desired_down_ratio)} | "
            f"actual {self._arb_ratio_label(up_ratio=current_ratio_after, down_ratio=max(1.0 - current_ratio_after, 0.0))} | "
            f"fase {bracket_phase}{carry_note}"
        )
        return StrategyPlan(
            instructions=tuple(instructions),
            note=note,
            primary_target=target,
            secondary_target=None,
            trigger=target,
            window_seconds=self._seconds_into_window(market),
            cycle_budget=round(primary_notional, 6),
            market_bias=f"{'Catch-up' if catchup_rebalance else 'Stabilize'} {target.label}",
            timing_regime=timing_regime,
            price_mode="stabilize-catchup" if catchup_rebalance else "stabilize-bracket",
            primary_ratio=1.0,
            primary_notional=primary_notional,
            secondary_notional=0.0,
            replenishment_count=0,
            trigger_value=target.best_ask,
            pair_sum=pair_sum,
            edge_pct=max(net_edge, 0.0),
            fair_value=fair_value,
            spot_price=spot_context.current_price,
            spot_anchor=spot_context.anchor_price,
            spot_delta_bps=spot_context.delta_bps,
            spot_fair_up=spot_context.fair_up,
            spot_fair_down=spot_context.fair_down,
            spot_source=spot_context.source,
            spot_price_mode=spot_context.price_mode,
            spot_age_ms=spot_context.age_ms,
            spot_binance_price=spot_context.binance_price or 0.0,
            spot_chainlink_price=spot_context.chainlink_price or 0.0,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_ratio_after,
            bracket_phase=bracket_phase,
        )

    def _build_arb_inventory_unwind_plan(
        self,
        *,
        market: dict,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        pair_sum: float,
        fair_up: float,
        fair_down: float,
        desired_up_ratio: float,
        current_up_ratio: float,
        timing_regime: str,
        cycle_budget: float,
        current_up_notional: float,
        current_down_notional: float,
        spot_context: ArbSpotContext | None,
        bracket_phase: str,
        carry_note: str = "",
    ) -> StrategyPlan | None:
        if bracket_phase != "redistribuir" or spot_context is None:
            return None

        desired_down_ratio = max(1.0 - desired_up_ratio, 0.0)
        if current_up_ratio >= _ARB_UNWIND_EXTREME_RATIO:
            target = up_outcome
            target_fair = fair_up
            target_ratio = current_up_ratio
            desired_target_ratio = desired_up_ratio
            current_target_notional = current_up_notional
            other_notional = current_down_notional
            if spot_context.delta_bps >= -_ARB_UNWIND_MIN_DELTA_BPS:
                return None
        elif current_up_ratio <= (1.0 - _ARB_UNWIND_EXTREME_RATIO):
            target = down_outcome
            target_fair = fair_down
            target_ratio = max(1.0 - current_up_ratio, 0.0)
            desired_target_ratio = desired_down_ratio
            current_target_notional = current_down_notional
            other_notional = current_up_notional
            if spot_context.delta_bps <= _ARB_UNWIND_MIN_DELTA_BPS:
                return None
        else:
            return None

        ratio_gap = target_ratio - desired_target_ratio
        if ratio_gap < _ARB_UNWIND_RATIO_TRIGGER:
            return None

        directional_target = self._arb_directional_target(
            up_outcome=up_outcome,
            down_outcome=down_outcome,
            spot_context=spot_context,
            seconds_remaining=max(300 - self._seconds_into_window(market), 0),
        )
        if directional_target is not None and directional_target.asset_id == target.asset_id:
            return None

        sell_edge = target.best_bid - target_fair
        if sell_edge < -_ARB_UNWIND_MAX_FAIR_DISCOUNT:
            return None

        target_position = self.db.get_copy_position(target.asset_id)
        if target_position is None:
            return None
        current_position_size = float(target_position["size"] or 0.0)
        if current_position_size <= 0 or target.best_bid <= 0:
            return None

        numerator = current_target_notional - (desired_target_ratio * (current_target_notional + other_notional))
        needed_notional = numerator / max(1.0 - desired_target_ratio, 1e-9)
        if needed_notional <= 0:
            return None

        unwind_budget = _round_down(
            min(
                needed_notional * _ARB_UNWIND_PROGRESS_FRACTION,
                cycle_budget * _ARB_UNWIND_BUDGET_FRACTION,
                current_target_notional,
                current_position_size * target.best_bid,
            ),
            "0.01",
        )
        if unwind_budget < self._arb_min_notional(target):
            return None

        unwind_size = _round_down(min(unwind_budget / target.best_bid, current_position_size), "0.0001")
        instruction = self._build_arb_sell_instruction(
            market=market,
            target=target,
            shares=unwind_size,
            price=target.best_bid,
            pair_sum=pair_sum,
            tranche_index=1,
            reason_prefix="inventory_unwind",
        )
        if instruction is None:
            return None

        reduced_notional = instruction.notional
        projected_up_notional = current_up_notional - (reduced_notional if target.asset_id == up_outcome.asset_id else 0.0)
        projected_down_notional = current_down_notional - (
            reduced_notional if target.asset_id == down_outcome.asset_id else 0.0
        )
        current_ratio_after = self._arb_current_up_ratio(
            up_exposure=max(projected_up_notional, 0.0),
            down_exposure=max(projected_down_notional, 0.0),
        )
        note = (
            f"unwind {target.label} bid {target.best_bid:.3f} vs fair {target_fair:.3f} | "
            f"sell edge {sell_edge * 100:.2f}% | delta {spot_context.delta_bps:+.1f}bps | "
            f"{timing_regime} | ventas 1 | "
            f"objetivo {self._arb_ratio_label(up_ratio=desired_up_ratio, down_ratio=desired_down_ratio)} | "
            f"actual {self._arb_ratio_label(up_ratio=current_ratio_after, down_ratio=max(1.0 - current_ratio_after, 0.0))} | "
            f"fase {bracket_phase}{carry_note}"
        )
        return StrategyPlan(
            instructions=(instruction,),
            note=note,
            primary_target=target,
            secondary_target=None,
            trigger=target,
            window_seconds=self._seconds_into_window(market),
            cycle_budget=round(reduced_notional, 6),
            market_bias=f"Desapalancar {target.label}",
            timing_regime=timing_regime,
            price_mode="inventory-unwind",
            primary_ratio=1.0,
            primary_notional=reduced_notional,
            secondary_notional=0.0,
            replenishment_count=0,
            trigger_value=target.best_bid,
            pair_sum=pair_sum,
            edge_pct=max(sell_edge, 0.0),
            fair_value=target_fair,
            spot_price=spot_context.current_price,
            spot_anchor=spot_context.anchor_price,
            spot_delta_bps=spot_context.delta_bps,
            spot_fair_up=spot_context.fair_up,
            spot_fair_down=spot_context.fair_down,
            spot_source=spot_context.source,
            spot_price_mode=spot_context.price_mode,
            spot_age_ms=spot_context.age_ms,
            spot_binance_price=spot_context.binance_price or 0.0,
            spot_chainlink_price=spot_context.chainlink_price or 0.0,
            desired_up_ratio=desired_up_ratio,
            current_up_ratio=current_ratio_after,
            bracket_phase=bracket_phase,
        )

    def _ordered_targets_by_notional(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        up_notional: float,
        down_notional: float,
    ) -> tuple[MarketOutcome, MarketOutcome]:
        if up_notional >= down_notional:
            return up_outcome, down_outcome
        return down_outcome, up_outcome

    def _arb_spot_context(self, *, market: dict, seconds_into_window: int) -> ArbSpotContext | None:
        if self.spot_feed is None:
            return None
        try:
            snapshot = self.spot_feed.get_snapshot()
        except Exception:  # noqa: BLE001
            return None
        current_price, price_mode = self._arb_effective_spot_price(snapshot=snapshot)
        reference_price = float(snapshot.reference_price or 0.0)
        lead_price = float(snapshot.lead_price or 0.0)
        if current_price <= 0:
            return None

        slug = str(market.get("slug") or "")
        anchor_key = f"arb_spot_anchor:{slug}"
        local_anchor_price = _safe_float(self.db.get_bot_state(anchor_key))
        local_anchor_source = str(self.db.get_bot_state(f"{anchor_key}:source") or self._arb_anchor_capture_source(snapshot=snapshot))
        if local_anchor_price <= 0 and seconds_into_window <= _ARB_SPOT_ANCHOR_GRACE_SECONDS:
            captured_anchor = self._arb_anchor_capture_price(market=market, snapshot=snapshot)
            if captured_anchor > 0:
                local_anchor_price = captured_anchor
                self.db.set_bot_state(anchor_key, f"{local_anchor_price:.8f}")
                local_anchor_source = self._arb_anchor_capture_source(snapshot=snapshot)
                self.db.set_bot_state(f"{anchor_key}:source", local_anchor_source)
        official_price_to_beat = self._market_official_price_to_beat(market)
        anchor_price = official_price_to_beat if official_price_to_beat > 0 else local_anchor_price
        anchor_source = "polymarket-official" if official_price_to_beat > 0 else local_anchor_source
        if anchor_price <= 0:
            return None

        seconds_remaining = max(300 - seconds_into_window, 0)
        fair_up = self._arb_spot_fair_up(
            anchor_price=anchor_price,
            current_price=current_price,
            seconds_remaining=seconds_remaining,
        )
        fair_down = max(1.0 - fair_up, 0.0)
        delta_bps = ((current_price / anchor_price) - 1.0) * 10000 if anchor_price > 0 else 0.0
        return ArbSpotContext(
            current_price=current_price,
            reference_price=reference_price,
            lead_price=lead_price,
            anchor_price=anchor_price,
            local_anchor_price=local_anchor_price,
            official_price_to_beat=official_price_to_beat,
            anchor_source=anchor_source,
            fair_up=fair_up,
            fair_down=fair_down,
            delta_bps=delta_bps,
            price_mode=price_mode,
            source=snapshot.source,
            age_ms=snapshot.age_ms,
            binance_price=snapshot.binance_price,
            chainlink_price=snapshot.chainlink_price,
        )

    def _maybe_prime_arb_spot_anchor(self) -> None:
        if self.spot_feed is None:
            return
        try:
            snapshot = self.spot_feed.get_snapshot()
        except Exception:  # noqa: BLE001
            return
        if snapshot.reference_price is None or snapshot.reference_price <= 0:
            return

        now = time.time()
        offset_seconds = now % 300
        if offset_seconds > _ARB_SPOT_ANCHOR_CAPTURE_WINDOW_SECONDS:
            return

        base_start = int(now - offset_seconds)
        slug = f"btc-updown-5m-{base_start}"
        anchor_key = f"arb_spot_anchor:{slug}"
        if _safe_float(self.db.get_bot_state(anchor_key)) > 0:
            return

        anchor_price = self._arb_anchor_capture_price_for_slug(slug=slug, snapshot=snapshot)
        if anchor_price <= 0:
            return
        self.db.set_bot_state(anchor_key, f"{anchor_price:.8f}")
        self.db.set_bot_state(f"{anchor_key}:source", self._arb_anchor_capture_source(snapshot=snapshot))
        self.db.set_bot_state(f"{anchor_key}:captured_at", str(int(now)))

    def _arb_spot_fair_up(self, *, anchor_price: float, current_price: float, seconds_remaining: int) -> float:
        if anchor_price <= 0 or current_price <= 0:
            return 0.5
        delta_bps = ((current_price / anchor_price) - 1.0) * 10000
        remaining_fraction = min(max(seconds_remaining / 300, 0.0), 1.0)
        scale_bps = 2.5 + (13.5 * remaining_fraction)
        if seconds_remaining <= _ARB_LATE_DIRECTIONAL_SECONDS_REMAINING:
            scale_bps *= 0.85
        elif seconds_remaining <= 150:
            scale_bps *= 0.92
        if abs(delta_bps) >= 12:
            scale_bps *= 0.92
        z_score = delta_bps / scale_bps
        probability = 1.0 / (1.0 + math.exp(-z_score))
        return min(max(probability, 0.02), 0.98)

    def _arb_directional_target(
        self,
        *,
        up_outcome: MarketOutcome,
        down_outcome: MarketOutcome,
        spot_context: ArbSpotContext | None,
        seconds_remaining: int,
    ) -> MarketOutcome | None:
        if spot_context is None:
            return None
        if seconds_remaining > _ARB_LATE_DIRECTIONAL_SECONDS_REMAINING:
            return None
        if spot_context.age_ms > _ARB_LATE_DIRECTIONAL_MAX_SPOT_AGE_MS:
            return None

        up_edge = spot_context.fair_up - up_outcome.best_ask
        down_edge = spot_context.fair_down - down_outcome.best_ask
        if (
            spot_context.delta_bps >= _ARB_LATE_DIRECTIONAL_MIN_DELTA_BPS
            and spot_context.fair_up >= _ARB_LATE_DIRECTIONAL_MIN_FAIR
            and up_edge >= _ARB_LATE_DIRECTIONAL_MIN_EDGE
            and up_edge >= down_edge
        ):
            return up_outcome
        if (
            spot_context.delta_bps <= -_ARB_LATE_DIRECTIONAL_MIN_DELTA_BPS
            and spot_context.fair_down >= _ARB_LATE_DIRECTIONAL_MIN_FAIR
            and down_edge >= _ARB_LATE_DIRECTIONAL_MIN_EDGE
            and down_edge >= up_edge
        ):
            return down_outcome
        return None

    def _build_arb_instruction(
        self,
        *,
        market: dict,
        target: MarketOutcome,
        shares: float,
        price: float,
        pair_sum: float,
        tranche_index: int,
    ) -> CopyInstruction | None:
        if shares <= 0 or price <= 0:
            return None
        notional = _round_down(shares * price, "0.01")
        size = _round_down(notional / price, "0.0001")
        if notional < self._arb_min_notional(target) or size <= 0:
            return None
        return CopyInstruction(
            action=SignalAction.ADD if self.db.get_copy_position(target.asset_id) else SignalAction.OPEN,
            side=TradeSide.BUY,
            asset=target.asset_id,
            condition_id=str(market.get("conditionId") or ""),
            size=size,
            price=price,
            notional=size * price,
            source_wallet="strategy:arb_micro",
            source_signal_id=0,
            title=str(market.get("question") or market.get("slug") or ""),
            slug=str(market.get("slug") or ""),
            outcome=target.label,
            category="crypto",
            reason=f"arb_micro:pair:{pair_sum:.3f}:tranche-{tranche_index}",
        )

    def _build_arb_sell_instruction(
        self,
        *,
        market: dict,
        target: MarketOutcome,
        shares: float,
        price: float,
        pair_sum: float,
        tranche_index: int,
        reason_prefix: str,
    ) -> CopyInstruction | None:
        existing = self.db.get_copy_position(target.asset_id)
        if existing is None or shares <= 0 or price <= 0:
            return None
        current_size = float(existing["size"] or 0.0)
        if current_size <= 0:
            return None

        size = _round_down(min(shares, current_size), "0.0001")
        notional = _round_down(size * price, "0.01")
        if size <= 0 or notional < self._arb_min_notional(target):
            return None

        action = SignalAction.CLOSE if size >= (current_size - 1e-9) else SignalAction.REDUCE
        return CopyInstruction(
            action=action,
            side=TradeSide.SELL,
            asset=target.asset_id,
            condition_id=str(market.get("conditionId") or ""),
            size=size,
            price=price,
            notional=size * price,
            source_wallet="strategy:arb_micro",
            source_signal_id=0,
            title=str(market.get("question") or market.get("slug") or ""),
            slug=str(market.get("slug") or ""),
            outcome=target.label,
            category="crypto",
            reason=f"arb_micro:{reason_prefix}:{pair_sum:.3f}:tranche-{tranche_index}",
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
                self._record_strategy_snapshot(market=market, note=self._incomplete_book_note(label=str(label), book=book))
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
        condition_id = str(market.get("conditionId") or "")
        existing_market_notional = self._get_condition_exposure(condition_id)
        has_existing_market_exposure = existing_market_notional >= self.settings.config.min_trade_amount
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
            has_existing_market_exposure=has_existing_market_exposure,
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

        setup_allowed, setup_note = self._vidarx_setup_allowed(price_mode=price_mode, timing_regime=timing_regime)
        if not setup_allowed:
            self._record_strategy_snapshot(
                market=market,
                note=setup_note,
                extra_state=self._vidarx_state_defaults(
                    strategy_window_seconds=str(seconds_into_window),
                    strategy_price_mode=price_mode,
                    strategy_timing_regime=timing_regime,
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
            existing_market_notional=existing_market_notional,
            timing_regime=timing_regime,
            price_mode=price_mode,
        )
        if hedge_in_band:
            hedge_share = min(primary_ratio, 1 - primary_ratio)
            if hedge_share > 0:
                min_dual_leg_budget = (self.settings.config.min_trade_amount * 1.10) / hedge_share
                if cash_balance >= min_dual_leg_budget:
                    cycle_budget = max(cycle_budget, min_dual_leg_budget)
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

        cycle_budget = min(cycle_budget, cash_balance)
        if cycle_budget < self.settings.config.min_trade_amount:
            self._record_strategy_snapshot(
                market=market,
                note="vidarx bankroll exhausted",
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
                    ignore_market_cap=True,
                    ignore_total_exposure_cap=True,
                    ignore_reserved_cap=True,
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
            f"{market_bias} | {self._vidarx_timing_label(timing_regime)} | banda {price_mode} | "
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
            trigger_value=rich_side.best_ask,
            pair_sum=primary_target.best_ask + (hedge_target.best_ask if hedge_notional > 0 else 0.0),
            edge_pct=0.0,
            fair_value=0.0,
        )

    def _vidarx_timing_label(self, timing_regime: str) -> str:
        if timing_regime == "second-wave":
            return "segunda oleada"
        return timing_regime

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
            daily_pnl=self._mode_daily_pnl(mode=mode),
            daily_profit_gross=self._mode_daily_profit_gross(mode=mode),
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
        existing_market_notional: float,
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
            desired *= 0.80
        elif timing_regime == "mid-late":
            desired *= 0.90
        else:
            desired *= 1.0

        if price_mode == "extreme":
            desired *= 1.25
        elif price_mode == "tilted":
            desired *= 1.05
        else:
            desired *= 0.90

        equity_ratio = effective_bankroll / max(self.settings.config.bankroll, 1e-9)
        if equity_ratio < 0.85:
            desired *= 0.35
        elif equity_ratio < 0.90:
            desired *= 0.50
        elif equity_ratio < 0.95:
            desired *= 0.70
        elif equity_ratio < 0.98:
            desired *= 0.85

        cycle_fraction = self._vidarx_cycle_fraction(price_mode=price_mode, timing_regime=timing_regime)
        desired = min(desired, effective_bankroll * cycle_fraction)

        budget_left = max(cash_balance, 0.0)
        desired = min(desired, budget_left)
        desired = min(desired, max(effective_bankroll - current_total_exposure, 0.0))
        return max(desired, 0.0)

    def _vidarx_cycle_fraction(self, *, price_mode: str, timing_regime: str) -> float:
        if price_mode == "extreme":
            fraction = _VIDARX_EXTREME_CYCLE_FRACTION
        elif price_mode == "tilted":
            fraction = _VIDARX_TILTED_CYCLE_FRACTION
        else:
            fraction = _VIDARX_BALANCED_CYCLE_FRACTION
        if timing_regime == "mid-late":
            fraction += 0.005
        return fraction

    def _vidarx_setup_allowed(self, *, price_mode: str, timing_regime: str) -> tuple[bool, str]:
        if (price_mode, timing_regime) not in _VIDARX_ALLOWED_SETUPS:
            return False, f"setup desactivado: {price_mode}/{timing_regime} fuera del perfil ganador"
        stats = self.db.get_strategy_setup_stats(price_mode=price_mode, timing_regime=timing_regime)
        windows = int(stats["windows"])
        win_rate = float(stats["win_rate"])
        pnl_total = float(stats["pnl_total"])
        if (
            windows >= _VIDARX_SETUP_DISABLE_MIN_WINDOWS
            and pnl_total < 0
            and win_rate <= _VIDARX_SETUP_DISABLE_MAX_WIN_RATE
        ):
            return (
                False,
                f"setup bloqueado por historial: {price_mode}/{timing_regime} "
                f"{windows} ventanas, win {win_rate * 100:.0f}%, pnl {pnl_total:.2f}",
            )
        return True, ""

    def _configured_live_small_target_capital(self) -> float:
        target = float(self.settings.config.live_small_target_capital or 0.0)
        return max(target, 0.0)

    def _mode_daily_pnl(self, *, mode: str) -> float:
        today = datetime.now(timezone.utc).date().isoformat()
        if mode == "paper":
            return self.db.get_daily_pnl(today)
        return self.db.get_daily_execution_pnl(today, mode=mode)

    def _mode_daily_profit_gross(self, *, mode: str) -> float:
        today = datetime.now(timezone.utc).date().isoformat()
        return self.db.get_daily_profit_gross(today, mode=None if mode == "paper" else mode)

    def _mode_capital_target(self, *, mode: str, live_total_capital: float) -> float:
        if mode == "paper":
            return max(float(self.settings.config.bankroll), 0.0)
        configured_target = self._configured_live_small_target_capital()
        actual_capital = max(float(live_total_capital), 0.0)
        if configured_target <= 0:
            return actual_capital
        if actual_capital <= 0:
            return configured_target
        return min(configured_target, actual_capital)

    def _live_cycle_budget_target(self, *, mode: str, live_total_capital: float) -> float:
        if mode not in {"live", "shadow"}:
            return 0.0
        configured_budget = max(float(self.settings.config.live_btc5m_cycle_budget_usdc or 0.0), 0.0)
        if configured_budget > 0:
            return configured_budget
        capital_target = self._mode_capital_target(mode=mode, live_total_capital=live_total_capital)
        return capital_target * float(self.settings.config.live_btc5m_ticket_allocation_pct)

    def _mode_drawdown_floor(self, *, mode: str, live_total_capital: float) -> float:
        capital_base = self._mode_capital_target(mode=mode, live_total_capital=live_total_capital)
        if mode in {"live", "shadow"}:
            max_drawdown_pct = max(float(self.settings.config.live_small_max_drawdown_pct or 0.0), 0.0)
            if capital_base > 0 and max_drawdown_pct > 0:
                return max(capital_base * (1.0 - max_drawdown_pct), 0.0)
            max_total_loss = max(float(self.settings.config.live_small_max_total_loss or 0.0), 0.0)
            if capital_base > 0 and max_total_loss > 0:
                return max(capital_base - max_total_loss, 0.0)
        return capital_base * (1.0 - _VIDARX_MAX_DRAWDOWN_PCT)

    def _live_cash_snapshot(self, *, mode: str) -> tuple[float, float]:
        if mode == "paper":
            paper_equity = self.settings.config.bankroll + self.db.get_cumulative_pnl()
            return max(paper_equity - self.db.get_total_exposure(), 0.0), 0.0
        if mode == "shadow":
            shadow_capital = self._configured_live_small_target_capital()
            return max(shadow_capital - self.db.get_total_exposure(), 0.0), 0.0
        try:
            balance = self.clob_client.get_collateral_balance()
            return float(balance.get("balance") or 0.0), float(balance.get("allowance") or 0.0)
        except Exception as error:  # noqa: BLE001
            self.logger.warning("live balance snapshot failed: %s", error)
            return 0.0, 0.0

    def _operating_bankroll_snapshot(
        self, *, mode: str = "paper", live_total_capital: float
    ) -> tuple[float, float]:
        today = datetime.now(timezone.utc).date().isoformat()
        if mode == "paper":
            base_bankroll = self.settings.config.bankroll
            realized_pnl = self.db.get_cumulative_pnl()
            realized_profit_gross = self.db.get_cumulative_profit_gross_before(today) + self.db.get_daily_profit_gross(today)
        else:
            base_bankroll = self._mode_capital_target(mode=mode, live_total_capital=live_total_capital)
            realized_pnl = self.db.get_cumulative_execution_pnl(mode=mode)
            realized_profit_gross = self.db.get_cumulative_execution_profit_gross_before(
                today,
                mode=mode,
            ) + self.db.get_daily_profit_gross(today, mode=mode)
        reserved_profit = calculate_reserved_profit(
            profit_gross=realized_profit_gross,
            profit_keep_ratio=self.settings.config.profit_keep_ratio,
        )
        effective_bankroll = calculate_effective_bankroll(
            base_bankroll=base_bankroll,
            realized_pnl=realized_pnl,
            profit_gross=realized_profit_gross,
            profit_keep_ratio=self.settings.config.profit_keep_ratio,
        )
        return min(effective_bankroll, max(live_total_capital, 0.0)), reserved_profit

    def _record_balance_snapshot(
        self,
        *,
        mode: str,
        cash_balance: float,
        allowance: float,
        total_exposure: float,
        live_total_capital: float,
        marked_exposure: float | None = None,
        unrealized_pnl: float | None = None,
        operating_bankroll: float | None = None,
        reserved_profit: float | None = None,
    ) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        capital_target = self._mode_capital_target(mode=mode, live_total_capital=live_total_capital)
        capital_scale_ratio = 0.0
        if self.settings.config.bankroll > 0:
            capital_scale_ratio = capital_target / self.settings.config.bankroll
        self.db.set_bot_state("live_cash_balance", f"{cash_balance:.8f}")
        self.db.set_bot_state("live_cash_allowance", f"{allowance:.8f}")
        self.db.set_bot_state("live_total_capital", f"{live_total_capital:.8f}")
        self.db.set_bot_state("strategy_capital_target", f"{capital_target:.8f}")
        self.db.set_bot_state("strategy_capital_scale_ratio", f"{capital_scale_ratio:.6f}")
        self.db.set_bot_state("live_balance_updated_at", str(now_ts))
        self.db.set_bot_state("strategy_runtime_mode", mode)
        self.db.set_bot_state("strategy_total_exposure", f"{total_exposure:.8f}")
        if marked_exposure is not None:
            self.db.set_bot_state("live_marked_exposure", f"{marked_exposure:.8f}")
        if unrealized_pnl is not None:
            self.db.set_bot_state("live_unrealized_pnl", f"{unrealized_pnl:.8f}")
        if operating_bankroll is not None:
            self.db.set_bot_state("strategy_operating_bankroll", f"{operating_bankroll:.8f}")
        if reserved_profit is not None:
            self.db.set_bot_state("strategy_reserved_profit", f"{reserved_profit:.8f}")
        self._record_market_feed_state()

    def _paper_mark_to_market_snapshot(self) -> tuple[float, float]:
        marked_exposure = 0.0
        unrealized_pnl = 0.0
        for row in self.db.list_copy_positions():
            size = float(row["size"] or 0.0)
            if size <= 0:
                continue
            avg_price = float(row["avg_price"] or 0.0)
            asset = str(row["asset"] or "")
            mark_price = avg_price
            try:
                midpoint = self.clob_client.get_midpoint(asset)
            except Exception:  # noqa: BLE001
                midpoint = None
            if midpoint is not None:
                mark_price = float(midpoint)
                self.db.record_position_mark(asset, mark_price)
            marked_exposure += abs(size * mark_price)
            unrealized_pnl += (mark_price - avg_price) * size
        return marked_exposure, unrealized_pnl

    def _record_strategy_snapshot(
        self,
        *,
        market: dict | None = None,
        opportunity: StrategyOpportunity | None = None,
        note: str = "",
        extra_state: dict[str, str] | None = None,
    ) -> None:
        snapshot_state = dict(extra_state or {})
        official_price_to_beat = self._market_official_price_to_beat(market) if market is not None else 0.0
        if market is not None:
            snapshot_state.setdefault("strategy_market_slug", str(market.get("slug") or ""))
            snapshot_state.setdefault("strategy_market_title", str(market.get("question") or market.get("slug") or ""))
        else:
            snapshot_state.setdefault("strategy_market_slug", "")
            snapshot_state.setdefault("strategy_market_title", "")
        if opportunity is not None:
            snapshot_state.setdefault("strategy_target_outcome", opportunity.target.label)
            snapshot_state.setdefault("strategy_target_price", f"{opportunity.target.best_ask:.6f}")
            snapshot_state.setdefault("strategy_trigger_outcome", opportunity.trigger.label)
            snapshot_state.setdefault("strategy_trigger_price_seen", f"{opportunity.trigger.best_ask:.6f}")
        else:
            snapshot_state.setdefault("strategy_target_outcome", "")
            snapshot_state.setdefault("strategy_target_price", "0.000000")
            snapshot_state.setdefault("strategy_trigger_outcome", "")
            snapshot_state.setdefault("strategy_trigger_price_seen", "0.000000")
        if official_price_to_beat > 0:
            snapshot_state["strategy_official_price_to_beat"] = f"{official_price_to_beat:.6f}"
        else:
            snapshot_state.setdefault("strategy_official_price_to_beat", "0.000000")
        self._snapshot_microstructure_state(market=market, note=note)
        operability_state = self._derive_strategy_operability_state(note=note, extra_state=snapshot_state)
        self.db.set_bot_state("strategy_mode", self.settings.config.strategy_mode)
        self.db.set_bot_state("strategy_entry_mode", self.settings.config.strategy_entry_mode)
        self.db.set_bot_state("strategy_variant", self.settings.config.strategy_variant)
        self.db.set_bot_state("strategy_notes", self.settings.config.strategy_notes)
        self.db.set_bot_state("strategy_incubation_stage", self.settings.config.incubation_stage)
        self.db.set_bot_state(
            "strategy_incubation_auto_promote",
            "1" if self.settings.config.incubation_auto_promote else "0",
        )
        self.db.set_bot_state("strategy_incubation_min_days", str(self.settings.config.incubation_min_days))
        self.db.set_bot_state(
            "strategy_incubation_min_resolutions",
            str(self.settings.config.incubation_min_resolutions),
        )
        self.db.set_bot_state(
            "strategy_incubation_max_drawdown",
            f"{self.settings.config.incubation_max_drawdown:.4f}",
        )
        self.db.set_bot_state(
            "strategy_incubation_min_backtest_pnl",
            f"{self.settings.config.incubation_min_backtest_pnl:.4f}",
        )
        self.db.set_bot_state(
            "strategy_incubation_min_backtest_fill_rate",
            f"{self.settings.config.incubation_min_backtest_fill_rate:.6f}",
        )
        self.db.set_bot_state(
            "strategy_incubation_min_backtest_hit_rate",
            f"{self.settings.config.incubation_min_backtest_hit_rate:.6f}",
        )
        self.db.set_bot_state(
            "strategy_incubation_min_backtest_edge_bps",
            f"{self.settings.config.incubation_min_backtest_edge_bps:.4f}",
        )
        if self.settings.strategy_registry is not None:
            for key, value in active_variant_metadata(
                self.settings.strategy_registry,
                variant_name=self.settings.config.strategy_variant,
                entry_mode=self.settings.config.strategy_entry_mode,
            ).items():
                self.db.set_bot_state(key, value)
        self.db.set_bot_state("strategy_last_note", note)
        self.db.set_bot_state("strategy_last_updated_at", str(int(datetime.now(timezone.utc).timestamp())))
        self._record_market_feed_state()
        for key, value in self._strategy_operability_entries(operability_state).items():
            self.db.set_bot_state(key, value)
        for key, value in snapshot_state.items():
            self.db.set_bot_state(key, value)

    def _derive_strategy_operability_state(
        self,
        *,
        note: str,
        extra_state: dict[str, str],
    ) -> StrategyOperabilityState:
        note_text = str(note or "").strip()
        note_lower = note_text.lower()
        current_exposure = _safe_float(
            extra_state.get("strategy_current_market_exposure") or self.db.get_bot_state("strategy_current_market_exposure")
        )
        plan_legs = int(
            _safe_float(extra_state.get("strategy_plan_legs") or self.db.get_bot_state("strategy_plan_legs"))
        )
        bracket_phase = str(
            extra_state.get("strategy_bracket_phase") or self.db.get_bot_state("strategy_bracket_phase") or ""
        ).strip()

        if "drawdown stop" in note_lower:
            return StrategyOperabilityState(
                state="stopped",
                label="Parado",
                reason="Proteccion de drawdown activada; el simulador no debe abrir nuevas compras.",
                blocking=True,
            )
        if "realism gate" in note_lower:
            return StrategyOperabilityState(
                state="degraded_reference",
                label="Referencia degradada",
                reason=note_text.split(":", 1)[-1].strip() or "La referencia no es comparable a Polymarket.",
                blocking=True,
            )
        if "runtime guard" in note_lower:
            return StrategyOperabilityState(
                state="runtime_guard",
                label="Guardado por riesgo",
                reason=note_text or "La proteccion runtime ha pausado nuevas aperturas tras el rendimiento reciente.",
                blocking=True,
            )
        if "no active btc5m market" in note_lower:
            return StrategyOperabilityState(
                state="no_market",
                label="Sin mercado",
                reason="Todavia no hay una ventana BTC 5m activa y operable.",
                blocking=True,
            )
        if "too early" in note_lower or "demasiado pronto" in note_lower:
            return StrategyOperabilityState(
                state="waiting_window",
                label="Esperando ventana",
                reason=note_text or "Aun es pronto para abrir el bracket con criterio.",
                blocking=True,
            )
        if "too late" in note_lower or " tarde" in note_lower:
            return StrategyOperabilityState(
                state="late_window",
                label="Ventana avanzada",
                reason=note_text or "La ventana ya esta demasiado avanzada para abrir con cabeza.",
                blocking=True,
            )
        if "incomplete book" in note_lower or "no orderbook" in note_lower:
            return StrategyOperabilityState(
                state="waiting_book",
                label="Esperando libro",
                reason="Falta liquidez visible o el libro viene incompleto en una de las patas.",
                blocking=True,
            )
        if "cooldown" in note_lower:
            return StrategyOperabilityState(
                state="cooldown",
                label="Cooldown",
                reason="Acaba de ejecutar y deja una pausa muy corta para no sobrerreaccionar.",
                blocking=True,
            )
        if "no locked edge" in note_lower:
            return StrategyOperabilityState(
                state="waiting_edge",
                label="Esperando edge",
                reason="La ventana esta viva, pero el margen neto aun no justifica abrir o sesgar el bracket.",
                blocking=True,
            )
        if "concurrent market limit reached" in note_lower or "open market limit reached" in note_lower:
            return StrategyOperabilityState(
                state="carry_open",
                label="Carry abierto",
                reason="Todavia queda otra ventana viva y el motor no quiere mezclar dos brackets a la vez.",
                blocking=True,
            )
        if "market cap exhausted" in note_lower or "total cap exhausted" in note_lower:
            return StrategyOperabilityState(
                state="cap_reached",
                label="Capacidad agotada",
                reason="Ya hay bastante dinero metido y el motor prefiere no seguir cargando.",
                blocking=True,
            )
        if "budget below minimum" in note_lower or "cash below min_trade_amount" in note_lower or "bankroll exhausted" in note_lower:
            return StrategyOperabilityState(
                state="budget_limited",
                label="Sin tamano",
                reason="El presupuesto util del ciclo se ha quedado por debajo del minimo operativo.",
                blocking=True,
            )
        if current_exposure > 0 and plan_legs > 0:
            return StrategyOperabilityState(
                state="executing",
                label="Comprando",
                reason="Hay plan activo y el motor esta ejecutando o acompanando el bracket actual.",
                blocking=False,
            )
        if current_exposure > 0 and bracket_phase == "redistribuir":
            return StrategyOperabilityState(
                state="rebalancing",
                label="Rebalanceando",
                reason="Hay exposicion abierta y el motor intenta recomponer el reparto sin empeorarlo.",
                blocking=False,
            )
        if current_exposure > 0:
            return StrategyOperabilityState(
                state="monitoring",
                label="Observando",
                reason="Hay una ventana abierta, pero ahora mismo no hay una nueva compra valida.",
                blocking=False,
            )
        if plan_legs > 0:
            return StrategyOperabilityState(
                state="ready",
                label="Listo para ejecutar",
                reason="Hay un plan valido preparado para este ciclo.",
                blocking=False,
            )
        return StrategyOperabilityState(
            state="observing",
            label="Observando",
            reason=note_text or "Esperando una oportunidad clara y datos comparables.",
            blocking=False,
        )

    def _strategy_operability_entries(self, operability_state: StrategyOperabilityState) -> dict[str, str]:
        return {
            "strategy_operability_state": operability_state.state,
            "strategy_operability_label": operability_state.label,
            "strategy_operability_reason": operability_state.reason,
            "strategy_operability_blocking": "1" if operability_state.blocking else "0",
        }

    def _market_feed_status(self) -> dict[str, object]:
        status_fn = getattr(self.clob_client, "market_feed_status", None)
        if not callable(status_fn):
            return {"mode": "rest-fallback", "connected": False, "tracked_assets": 0, "age_ms": 0}
        try:
            status = status_fn()
        except Exception:  # noqa: BLE001
            return {"mode": "rest-fallback", "connected": False, "tracked_assets": 0, "age_ms": 0}
        return {
            "mode": str(getattr(status, "mode", "rest-fallback") or "rest-fallback"),
            "connected": bool(getattr(status, "connected", False)),
            "tracked_assets": int(getattr(status, "tracked_assets", 0) or 0),
            "age_ms": int(getattr(status, "age_ms", 0) or 0),
        }

    def _record_market_feed_state(self) -> None:
        status = self._market_feed_status()
        self.db.set_bot_state("strategy_data_source", str(status["mode"]))
        self.db.set_bot_state("strategy_feed_connected", "1" if bool(status["connected"]) else "0")
        self.db.set_bot_state("strategy_feed_age_ms", str(int(status["age_ms"])))
        self.db.set_bot_state("strategy_feed_tracked_assets", str(int(status["tracked_assets"])))

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

    def _get_open_btc5m_condition_ids(self) -> set[str]:
        condition_ids: set[str] = set()
        for row in self.db.list_copy_positions():
            if "btc-updown-5m-" not in str(row["slug"] or ""):
                continue
            if float(row["size"] or 0.0) <= 0:
                continue
            condition_id = str(row["condition_id"] or "")
            if condition_id:
                condition_ids.add(condition_id)
        return condition_ids

    def _btc5m_slug_start_ts(self, slug: str) -> int:
        if not slug.startswith("btc-updown-5m-"):
            return 0
        try:
            return int(slug.rsplit("-", 1)[-1])
        except ValueError:
            return 0

    def _btc5m_market_start_ts(self, market: dict) -> int:
        start_ts = _to_timestamp(
            str((market.get("events") or [{}])[0].get("startTime") or market.get("eventStartTime") or "")
        )
        if start_ts > 0:
            return start_ts
        return self._btc5m_slug_start_ts(str(market.get("slug") or ""))

    def _market_official_price_to_beat(self, market: dict | None) -> float:
        if not isinstance(market, dict):
            return 0.0

        slug = str(market.get("slug") or "").strip()
        if slug:
            cached = self._official_price_cache.get(slug)
            if cached is not None:
                cached_price, cached_expires_at = cached
                if time.monotonic() < cached_expires_at and cached_price > 0:
                    return cached_price

        official = self._market_official_price_to_beat_from_payload(market)
        if official > 0:
            if slug:
                self._official_price_cache[slug] = (official, time.monotonic() + _MARKET_METADATA_CACHE_SECONDS)
            return official

        refreshed_market: dict | None = None
        if slug:
            try:
                refreshed_market = self.gamma_client.get_market_by_slug(slug)
            except Exception:  # noqa: BLE001
                refreshed_market = None
            if isinstance(refreshed_market, dict) and refreshed_market is not market:
                refreshed_official = self._market_official_price_to_beat_from_payload(refreshed_market)
                if refreshed_official > 0:
                    self._official_price_cache[slug] = (refreshed_official, time.monotonic() + _MARKET_METADATA_CACHE_SECONDS)
                    return refreshed_official

        event_lookup = getattr(self.gamma_client, "get_event_by_id", None)
        if callable(event_lookup):
            for event_id in self._market_event_ids(refreshed_market if isinstance(refreshed_market, dict) else market):
                try:
                    event_payload = event_lookup(event_id)
                except Exception:  # noqa: BLE001
                    continue
                refreshed_official = self._market_official_price_to_beat_from_payload(event_payload)
                if refreshed_official > 0:
                    if slug:
                        self._official_price_cache[slug] = (
                            refreshed_official,
                            time.monotonic() + _MARKET_METADATA_CACHE_SECONDS,
                        )
                    return refreshed_official
        return 0.0

    def _market_official_price_to_beat_from_payload(self, market: dict) -> float:
        return self._extract_price_to_beat(market)

    def _extract_price_to_beat(self, payload: object) -> float:
        normalized = self._decoded_json_payload(payload)
        if isinstance(normalized, dict):
            for key in ("priceToBeat", "price_to_beat"):
                official = _safe_float(normalized.get(key))
                if official > 0:
                    return official
            for nested_key in ("eventMetadata", "metadata", "marketMetadata", "event"):
                official = self._extract_price_to_beat(normalized.get(nested_key))
                if official > 0:
                    return official
            for list_key in ("events", "markets"):
                raw_items = normalized.get(list_key)
                if not isinstance(raw_items, list):
                    continue
                for item in raw_items:
                    official = self._extract_price_to_beat(item)
                    if official > 0:
                        return official
        return 0.0

    def _decoded_json_payload(self, payload: object) -> object:
        if not isinstance(payload, str):
            return payload
        text = payload.strip()
        if not text:
            return payload
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return payload

    def _market_event_ids(self, market: dict | None) -> list[str]:
        if not isinstance(market, dict):
            return []
        event_ids: list[str] = []
        for raw_event in market.get("events") or []:
            if not isinstance(raw_event, dict):
                continue
            event_id = str(raw_event.get("id") or "").strip()
            if event_id:
                event_ids.append(event_id)
        return list(dict.fromkeys(event_ids))

    def _arb_effective_spot_price(self, *, snapshot: SpotSnapshot) -> tuple[float, str]:
        reference_price = float(snapshot.reference_price or 0.0)
        lead_price = float(snapshot.lead_price or 0.0)
        binance_price = float(snapshot.binance_price or 0.0)
        chainlink_price = float(snapshot.chainlink_price or 0.0)
        basis = float(snapshot.basis or 0.0)

        if lead_price > 0 and binance_price > 0 and reference_price > 0 and (chainlink_price > 0 or basis != 0.0):
            comparable_lead = lead_price + basis
            if comparable_lead > 0:
                return comparable_lead, "lead-basis"
        if reference_price > 0:
            if lead_price > 0 and chainlink_price <= 0 and abs(lead_price - reference_price) < 1e-9:
                return lead_price, "lead"
            return reference_price, "reference"
        if lead_price > 0:
            return lead_price, "lead"
        return 0.0, "missing"

    def _arb_reference_state(
        self,
        *,
        mode: str = "paper",
        source: str,
        age_ms: int,
        chainlink_price: float,
        official_price_to_beat: float,
        local_anchor_price: float,
        anchor_source: str,
    ) -> ArbReferenceState:
        source_text = str(source or "").strip()
        anchor_source_text = str(anchor_source or "").strip()
        has_rtds = source_text.startswith("polymarket-rtds")
        has_chainlink = chainlink_price > 0
        has_official = official_price_to_beat > 0
        has_local_anchor = local_anchor_price > 0
        has_any_anchor = has_official or has_local_anchor
        age_limit = max(int(self.settings.config.btc5m_reference_max_age_ms), 100)
        soft_age_limit = max(int(self.settings.config.btc5m_reference_soft_max_age_ms), age_limit)
        soft_budget_scale = float(self.settings.config.btc5m_reference_soft_budget_scale)
        mode_text = str(mode or "").strip().lower()
        is_live_like = mode_text in {"live", "shadow"}
        is_shadow = mode_text == "shadow"
        shadow_fallback_budget_scale = min(max(soft_budget_scale, 0.50), 0.60)
        shadow_missing_budget_scale = min(max(soft_budget_scale, 0.35), 0.45)

        if not source_text:
            if is_shadow and has_any_anchor:
                return ArbReferenceState(
                    comparable=True,
                    quality="shadow-fallback",
                    note="shadow fallback: sin spot etiquetado; usando ancla reducida",
                    budget_scale=shadow_missing_budget_scale,
                )
            return ArbReferenceState(comparable=False, quality="missing", note="sin spot de referencia")
        if age_ms > age_limit:
            if age_ms <= soft_age_limit and has_rtds and has_chainlink and has_official:
                return ArbReferenceState(
                    comparable=True,
                    quality="soft-stale-official",
                    note=f"RTDS ligeramente vieja: {age_ms}ms > {age_limit}ms; operando reducido",
                    budget_scale=max(soft_budget_scale, 0.70) if is_live_like else soft_budget_scale,
                )
            if (
                age_ms <= soft_age_limit
                and has_rtds
                and has_chainlink
                and self.settings.config.btc5m_allow_rtds_anchor_fallback
                and has_local_anchor
                and anchor_source_text.startswith("polymarket")
            ):
                return ArbReferenceState(
                    comparable=True,
                    quality="soft-stale-rtds",
                    note=f"RTDS ligeramente vieja: {age_ms}ms > {age_limit}ms; ancla RTDS reducida",
                    budget_scale=max(soft_budget_scale, 0.80) if is_live_like else min(soft_budget_scale, 0.45),
                )
            return ArbReferenceState(
                comparable=False,
                quality="stale",
                note=f"referencia vieja: {age_ms}ms > {age_limit}ms",
            )
        if not has_rtds:
            if is_shadow and has_any_anchor:
                return ArbReferenceState(
                    comparable=True,
                    quality="shadow-fallback",
                    note=f"shadow fallback: fuente degradada: {source_text}; operando reducido",
                    budget_scale=shadow_fallback_budget_scale,
                )
            return ArbReferenceState(
                comparable=False,
                quality="degraded",
                note=f"fuente degradada: {source_text}",
            )
        if not has_chainlink:
            if is_shadow and has_any_anchor:
                return ArbReferenceState(
                    comparable=True,
                    quality="shadow-fallback",
                    note="shadow fallback: sin precio Chainlink RTDS; usando ancla reducida",
                    budget_scale=shadow_fallback_budget_scale,
                )
            return ArbReferenceState(
                comparable=False,
                quality="degraded",
                note="sin precio Chainlink RTDS",
            )
        if has_official:
            return ArbReferenceState(
                comparable=True,
                quality="official",
                note="referencia oficial Polymarket + Chainlink RTDS",
                budget_scale=1.0,
            )
        if (
            self.settings.config.btc5m_allow_rtds_anchor_fallback
            and has_local_anchor
            and anchor_source_text.startswith("polymarket")
        ):
            return ArbReferenceState(
                comparable=True,
                quality="rtds-derived",
                note=f"sin beat oficial; usando ancla RTDS ({anchor_source_text})",
                budget_scale=0.65,
            )
        if has_local_anchor:
            return ArbReferenceState(
                comparable=False,
                quality="degraded",
                note=f"ancla no comparable: {anchor_source_text or 'desconocida'}",
            )
        return ArbReferenceState(
            comparable=False,
            quality="missing",
            note="sin beat oficial ni ancla RTDS",
        )

    def _arb_reference_state_entries(self, reference_state: ArbReferenceState) -> dict[str, str]:
        return {
            "strategy_reference_quality": reference_state.quality,
            "strategy_reference_comparable": "1" if reference_state.comparable else "0",
            "strategy_reference_note": reference_state.note,
        }

    def _with_arb_reference_state(self, plan: StrategyPlan, reference_state: ArbReferenceState) -> StrategyPlan:
        return replace(
            plan,
            reference_quality=reference_state.quality,
            reference_comparable=reference_state.comparable,
            reference_note=reference_state.note,
        )

    def _arb_carry_state(self, *, current_condition_id: str, current_market_start_ts: int) -> ArbCarryState:
        open_condition_ids: set[str] = set()
        active_condition_ids: set[str] = set()
        previous_condition_ids: set[str] = set()
        carry_exposure = 0.0
        current_market_exposure = 0.0

        for row in self.db.list_copy_positions():
            slug = str(row["slug"] or "")
            if "btc-updown-5m-" not in slug:
                continue
            size = float(row["size"] or 0.0)
            avg_price = float(row["avg_price"] or 0.0)
            if size <= 0:
                continue

            condition_id = str(row["condition_id"] or "")
            if condition_id:
                open_condition_ids.add(condition_id)

            exposure = abs(size * avg_price) if avg_price > 0 else 0.0
            if condition_id == current_condition_id:
                current_market_exposure += exposure
                if condition_id:
                    active_condition_ids.add(condition_id)
                continue

            row_start_ts = self._btc5m_slug_start_ts(slug)
            is_previous_window = current_market_start_ts > 0 and row_start_ts > 0 and row_start_ts < current_market_start_ts
            if is_previous_window:
                carry_exposure += exposure
                if condition_id:
                    previous_condition_ids.add(condition_id)
                continue

            if condition_id:
                active_condition_ids.add(condition_id)

        return ArbCarryState(
            total_open_windows=len(open_condition_ids),
            active_open_windows=len(active_condition_ids),
            previous_open_windows=len(previous_condition_ids),
            carry_exposure=carry_exposure,
            current_market_exposure=current_market_exposure,
        )

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

    def _arb_carry_note(self, *, carry_exposure: float, carry_window_count: int) -> str:
        if carry_exposure <= 0 or carry_window_count <= 0:
            return ""
        window_label = "ventana" if carry_window_count == 1 else "ventanas"
        return f" | carry previo {carry_exposure:.2f} en {carry_window_count} {window_label}"

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
        if self.runtime_diagnostics is not None:
            try:
                self.runtime_diagnostics.generate_if_due()
            except Exception as error:  # noqa: BLE001
                self.logger.warning("runtime diagnostics generation failed: %s", error)
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

    def _attach_runtime_feeds(self) -> None:
        market_feed = getattr(self.clob_client, "market_feed", None)
        if market_feed is not None and hasattr(market_feed, "register_listener"):
            market_feed.register_listener(self.event_bus.publish)
        if self.spot_feed is not None and hasattr(self.spot_feed, "register_listener"):
            self.spot_feed.register_listener(self.event_bus.publish)
        if self.liquidation_feed is not None and hasattr(self.liquidation_feed, "register_listener"):
            self.liquidation_feed.register_listener(self._handle_liquidation_payload)
        if self.user_feed is not None and hasattr(self.user_feed, "register_listener"):
            self.user_feed.register_listener(self._handle_user_payload)

    def _handle_liquidation_payload(self, payload: dict[str, object]) -> None:
        self.event_bus.emit(
            kind="liquidation",
            source=f"liquidation:{str(payload.get('exchange') or 'unknown')}",
            payload=dict(payload),
            asset_id=str(payload.get("symbol") or ""),
            ts_exchange_ms=int(_safe_float(payload.get("timestamp"))),
        )

    def _handle_user_payload(self, payload: dict[str, object]) -> None:
        event_type = str(payload.get("event_type") or payload.get("type") or "user_update").strip().lower()
        kind = "user_trade" if event_type in {"trade", "matched"} else "user_order"
        self.event_bus.emit(
            kind=kind,
            source="polymarket-user-ws",
            payload=dict(payload),
            asset_id=str(payload.get("asset_id") or payload.get("token_id") or ""),
            market_id=str(payload.get("market") or payload.get("condition_id") or ""),
            ts_exchange_ms=int(_safe_float(payload.get("timestamp") or payload.get("match_time") or payload.get("created_at"))),
        )

    def _snapshot_microstructure_state(
        self,
        *,
        market: dict | None,
        note: str,
    ) -> None:
        if not self.settings.config.microstructure_enabled:
            return
        up_exposure, down_exposure = self._current_market_exposure_split(market)
        seconds_into_window = self._seconds_into_window(market) if market is not None else 0
        spot_snapshot = self.spot_feed.get_snapshot() if self.spot_feed is not None else None
        official_price_to_beat = self._market_official_price_to_beat(market) if market is not None else 0.0
        self.telemetry.snapshot_market(
            market=market,
            official_price_to_beat=official_price_to_beat,
            spot_snapshot=spot_snapshot,
            seconds_into_window=seconds_into_window,
            current_up_exposure=up_exposure,
            current_down_exposure=down_exposure,
            note=note,
        )

    def _current_market_exposure_split(self, market: dict | None) -> tuple[float, float]:
        if market is None:
            return 0.0, 0.0
        slug = str(market.get("slug") or "").strip()
        if not slug:
            return 0.0, 0.0
        up_exposure = 0.0
        down_exposure = 0.0
        for row in self.db.list_copy_positions():
            if str(row["slug"] or "").strip() != slug:
                continue
            notional = abs(float(row["size"] or 0.0) * float(row["avg_price"] or 0.0))
            outcome = str(row["outcome"] or "").strip().lower()
            if outcome == "up":
                up_exposure += notional
            elif outcome == "down":
                down_exposure += notional
        return up_exposure, down_exposure

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
        feed_status = self._market_feed_status()
        signature = "|".join(
            [
                mode,
                note,
                str(stats["pending"]),
                str(stats["filled"]),
                str(stats["blocked"]),
                str(stats["skipped"]),
                str(stats["failed"]),
                str(stats["opportunities"]),
                str(feed_status["mode"]),
                str(feed_status["connected"]),
                str(feed_status["tracked_assets"]),
                f"{cash_balance:.2f}",
                f"{available_to_trade:.2f}",
                f"{total_exposure:.2f}",
                f"{live_total_capital:.2f}",
            ]
        )
        now = time.monotonic()
        if signature == self._last_cycle_log_signature and (now - self._last_cycle_log_at) < 5.0:
            return
        self._last_cycle_log_signature = signature
        self._last_cycle_log_at = now
        self.logger.info(
            "strategy => mode=%s pending=%s filled=%s blocked=%s skipped=%s failed=%s opportunities=%s note=%s "
            "data_source=%s feed_connected=%s feed_age_ms=%s tracked_assets=%s "
            "cash_balance=%.4f available_to_trade=%.4f allowance=%.4f exposure=%.4f live_total_capital=%.4f",
            mode,
            stats["pending"],
            stats["filled"],
            stats["blocked"],
            stats["skipped"],
            stats["failed"],
            stats["opportunities"],
            note,
            feed_status["mode"],
            feed_status["connected"],
            feed_status["age_ms"],
            feed_status["tracked_assets"],
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

    def _strategy_resolution_mode_label(self, *, mode: str) -> str:
        safe_mode = str(mode or "").strip().lower() or "paper"
        return f"{safe_mode}-settle-at-close"

    def _position_ledger_can_run(self, *, mode: str) -> tuple[bool, str]:
        if mode != "live" or not self.settings.config.live_preflight_require_clean_ledger:
            self.db.set_bot_state("position_ledger_preflight", "disabled" if mode != "live" else "not-required")
            return True, ""
        positions = self.db.list_copy_positions()
        if not positions:
            self.db.set_bot_state("position_ledger_mode", "")
            self.db.set_bot_state("position_ledger_preflight", "ready")
            return True, ""
        ledger_mode = str(self.db.get_bot_state("position_ledger_mode") or "").strip().lower() or "paper"
        if ledger_mode == mode:
            self.db.set_bot_state("position_ledger_preflight", "ready")
            return True, ""
        exposure = self.db.get_total_exposure()
        message = (
            f"live preflight: ledger has {len(positions)} open {ledger_mode} position(s) "
            f"with {exposure:.2f} USDC exposure; flatten or reset before live"
        )
        self.db.set_bot_state("position_ledger_preflight", "blocked")
        self.db.set_bot_state("position_ledger_mode", ledger_mode)
        return False, message

    def _live_control_can_execute(self, *, mode: str) -> tuple[bool, str]:
        if mode != "live":
            return True, ""
        raw_state_value = self.db.get_bot_state("live_control_state")
        if raw_state_value is None or not str(raw_state_value).strip():
            return True, ""
        raw_state = str(raw_state_value).strip().lower()
        if raw_state == "armed":
            return True, ""
        reason = str(self.db.get_bot_state("live_control_reason") or "").strip()
        return False, reason or "live pausado desde el live control center"

    def _runtime_guard_can_open(self, *, mode: str = "paper") -> tuple[bool, str]:
        if not self.settings.config.runtime_guard_enabled:
            self.db.set_bot_state("runtime_guard_profile", "global-disabled")
            self.db.set_bot_state("runtime_guard_state", "disabled")
            self.db.set_bot_state("runtime_guard_until", "0")
            self.db.set_bot_state("runtime_guard_reason", "")
            self.db.set_bot_state("runtime_guard_remaining_minutes", "0")
            self.db.set_bot_state("runtime_guard_recent_close_count", "0")
            self.db.set_bot_state("runtime_guard_recent_close_pnl", "0.000000")
            self.db.set_bot_state("runtime_guard_loss_streak", "0")
            return True, ""
        if mode == "paper" and not self.settings.config.paper_runtime_guard_enabled:
            self.db.set_bot_state("runtime_guard_profile", "paper-disabled")
            self.db.set_bot_state("runtime_guard_state", "disabled")
            self.db.set_bot_state("runtime_guard_until", "0")
            self.db.set_bot_state("runtime_guard_reason", "")
            self.db.set_bot_state("runtime_guard_remaining_minutes", "0")
            self.db.set_bot_state("runtime_guard_recent_close_count", "0")
            self.db.set_bot_state("runtime_guard_recent_close_pnl", "0.000000")
            self.db.set_bot_state("runtime_guard_loss_streak", "0")
            return True, ""
        if mode == "paper":
            lookback_minutes = max(self.settings.config.paper_runtime_guard_lookback_minutes, 1)
            loss_streak_limit = max(self.settings.config.paper_runtime_guard_loss_streak, 1)
            max_recent_close_pnl = float(self.settings.config.paper_runtime_guard_max_recent_pnl)
            cooldown_minutes = max(self.settings.config.paper_runtime_guard_cooldown_minutes, 1)
            guard_profile = "paper"
        else:
            lookback_minutes = max(self.settings.config.runtime_guard_lookback_minutes, 1)
            loss_streak_limit = max(self.settings.config.runtime_guard_loss_streak, 1)
            max_recent_close_pnl = float(self.settings.config.runtime_guard_max_recent_pnl)
            cooldown_minutes = max(self.settings.config.runtime_guard_cooldown_minutes, 1)
            guard_profile = "live"
        self.db.set_bot_state("runtime_guard_profile", guard_profile)
        now_ts = int(time.time())
        try:
            existing_until = int(str(self.db.get_bot_state("runtime_guard_until") or "0").strip())
        except (TypeError, ValueError):
            existing_until = 0
        existing_reason = str(self.db.get_bot_state("runtime_guard_reason") or "").strip()
        if existing_until > now_ts:
            remaining_minutes = max(int((existing_until - now_ts) / 60), 1)
            self.db.set_bot_state("runtime_guard_state", "cooldown")
            self.db.set_bot_state("runtime_guard_remaining_minutes", str(remaining_minutes))
            message = existing_reason or "cooldown por mal rendimiento reciente"
            return False, f"runtime guard {remaining_minutes}m: {message}"

        cutoff_ts = now_ts - lookback_minutes * 60
        execution_limit = max(self.settings.config.runtime_diagnostics_execution_limit, 50)
        recent_executions = self.db.get_recent_executions_since(
            cutoff_ts,
            limit=execution_limit,
            mode=mode,
        )
        decision = evaluate_runtime_guard(
            recent_executions,
            now_ts=now_ts,
            lookback_minutes=lookback_minutes,
            loss_streak_limit=loss_streak_limit,
            max_recent_close_pnl=max_recent_close_pnl,
            cooldown_minutes=cooldown_minutes,
        )
        self.db.set_bot_state("runtime_guard_recent_close_count", str(int(decision["recent_close_count"])))
        self.db.set_bot_state("runtime_guard_recent_close_pnl", f"{float(decision['recent_close_pnl']):.6f}")
        self.db.set_bot_state("runtime_guard_loss_streak", str(int(decision["consecutive_losses"])))
        if decision["blocked"]:
            self.db.set_bot_state("runtime_guard_state", "cooldown")
            self.db.set_bot_state("runtime_guard_until", str(int(decision["cooldown_until"])))
            self.db.set_bot_state("runtime_guard_reason", str(decision["reason"]))
            self.db.set_bot_state(
                "runtime_guard_remaining_minutes",
                str(cooldown_minutes),
            )
            return False, f"runtime guard: {decision['reason']}"

        self.db.set_bot_state("runtime_guard_state", "ready")
        self.db.set_bot_state("runtime_guard_until", "0")
        self.db.set_bot_state("runtime_guard_reason", "")
        self.db.set_bot_state("runtime_guard_remaining_minutes", "0")
        return True, ""

    def _execute_instruction(self, *, mode: str, instruction: CopyInstruction) -> ExecutionResult:
        if mode == "live":
            live_allowed, live_control_note = self._live_control_can_execute(mode=mode)
            if not live_allowed:
                raise RuntimeError(live_control_note)
        if mode in {"live", "shadow"} and not str(instruction.execution_profile or "").strip():
            decision = self.telemetry.latest_decision() if self.telemetry is not None else None
            selected_execution = str(getattr(decision, "selected_execution", "") or "").strip().lower()
            if selected_execution and selected_execution != "no_trade":
                instruction = instruction.model_copy(update={"execution_profile": selected_execution})
        return self.execution_engine.execute(mode=mode, instruction=instruction)

    def _run_autonomous_exits(self, *, mode: str, stats: dict[str, int]) -> None:
        if self.settings.config.strategy_entry_mode in {"vidarx_micro", "arb_micro"}:
            return
        live_allowed, _ = self._live_control_can_execute(mode=mode)
        if mode == "live" and not live_allowed:
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

    def _arb_live_spot_state(
        self,
        *,
        market: dict | None = None,
        seconds_into_window: int | None = None,
    ) -> dict[str, str]:
        state = {
            "strategy_spot_price": "0.000000",
            "strategy_spot_anchor": "0.000000",
            "strategy_spot_local_anchor": "0.000000",
            "strategy_official_price_to_beat": "0.000000",
            "strategy_anchor_source": "",
            "strategy_reference_quality": "missing",
            "strategy_reference_comparable": "0",
            "strategy_reference_note": "sin spot de referencia",
            "strategy_spot_delta_bps": "0.0000",
            "strategy_spot_fair_up": "0.000000",
            "strategy_spot_fair_down": "0.000000",
            "strategy_spot_source": "",
            "strategy_spot_price_mode": "missing",
            "strategy_spot_age_ms": "0",
            "strategy_spot_binance": "0.000000",
            "strategy_spot_chainlink": "0.000000",
        }
        if self.spot_feed is None:
            return state

        try:
            snapshot = self.spot_feed.get_snapshot()
        except Exception:  # noqa: BLE001
            return state

        current_price, price_mode = self._arb_effective_spot_price(snapshot=snapshot)
        if current_price <= 0:
            return state

        state.update(
            {
                "strategy_spot_price": f"{current_price:.6f}",
                "strategy_spot_source": snapshot.source,
                "strategy_spot_price_mode": price_mode,
                "strategy_spot_age_ms": str(int(snapshot.age_ms)),
                "strategy_spot_binance": f"{float(snapshot.binance_price or 0.0):.6f}",
                "strategy_spot_chainlink": f"{float(snapshot.chainlink_price or 0.0):.6f}",
            }
        )

        now = time.time()
        current_window_seconds = int(max(now % 300, 0))
        live_market = dict(market) if isinstance(market, dict) else None
        market_slug = str(live_market.get("slug") or "") if live_market is not None else str(self.db.get_bot_state("strategy_market_slug") or "")
        if not market_slug.startswith("btc-updown-5m-"):
            base_start = int(now - current_window_seconds)
            market_slug = f"btc-updown-5m-{base_start}"
        if live_market is None and market_slug.startswith("btc-updown-5m-"):
            cached_slug = str((self._cached_market or {}).get("slug") or "")
            if cached_slug == market_slug:
                live_market = dict(self._cached_market or {})
            else:
                try:
                    fetched_market = self.gamma_client.get_market_by_slug(market_slug)
                except Exception:  # noqa: BLE001
                    fetched_market = None
                if isinstance(fetched_market, dict):
                    live_market = fetched_market
        anchor_key = f"arb_spot_anchor:{market_slug}"
        local_anchor_price = _safe_float(self.db.get_bot_state(anchor_key))
        local_anchor_source = str(self.db.get_bot_state(f"{anchor_key}:source") or self._arb_anchor_capture_source(snapshot=snapshot))
        if local_anchor_price <= 0 and current_window_seconds <= _ARB_SPOT_ANCHOR_GRACE_SECONDS:
            captured_anchor = self._arb_anchor_capture_price_for_slug(slug=market_slug, snapshot=snapshot)
            if captured_anchor > 0:
                local_anchor_price = captured_anchor
                local_anchor_source = self._arb_anchor_capture_source(snapshot=snapshot)
                self.db.set_bot_state(anchor_key, f"{local_anchor_price:.8f}")
                self.db.set_bot_state(f"{anchor_key}:source", local_anchor_source)
        official_price_to_beat = self._market_official_price_to_beat(live_market) if live_market is not None else 0.0
        anchor_price = official_price_to_beat if official_price_to_beat > 0 else local_anchor_price
        anchor_source = "polymarket-official" if official_price_to_beat > 0 else local_anchor_source
        reference_state = self._arb_reference_state(
            mode=str(self.db.get_bot_state("strategy_runtime_mode") or "paper"),
            source=snapshot.source,
            age_ms=int(snapshot.age_ms),
            chainlink_price=float(snapshot.chainlink_price or 0.0),
            official_price_to_beat=official_price_to_beat,
            local_anchor_price=local_anchor_price,
            anchor_source=anchor_source,
        )
        state.update(self._arb_reference_state_entries(reference_state))
        if anchor_price <= 0:
            return state

        seconds_value = seconds_into_window if seconds_into_window is not None else current_window_seconds
        seconds_remaining = max(300 - int(seconds_value), 0)
        fair_up = self._arb_spot_fair_up(
            anchor_price=anchor_price,
            current_price=current_price,
            seconds_remaining=seconds_remaining,
        )
        fair_down = max(1.0 - fair_up, 0.0)
        delta_bps = ((current_price / anchor_price) - 1.0) * 10000 if anchor_price > 0 else 0.0

        state.update(
            {
                "strategy_spot_anchor": f"{anchor_price:.6f}",
                "strategy_spot_local_anchor": f"{local_anchor_price:.6f}",
                "strategy_official_price_to_beat": f"{official_price_to_beat:.6f}",
                "strategy_anchor_source": anchor_source,
                "strategy_spot_delta_bps": f"{delta_bps:.4f}",
                "strategy_spot_fair_up": f"{fair_up:.6f}",
                "strategy_spot_fair_down": f"{fair_down:.6f}",
            }
        )
        return state

    def _arb_anchor_capture_price(self, *, market: dict, snapshot: SpotSnapshot) -> float:
        slug = str(market.get("slug") or "")
        return self._arb_anchor_capture_price_for_slug(slug=slug, snapshot=snapshot)

    def _arb_anchor_capture_price_for_slug(self, *, slug: str, snapshot: SpotSnapshot) -> float:
        start_ts = self._btc5m_slug_start_ts(slug)
        if start_ts > 0 and hasattr(self.spot_feed, "get_anchor_price"):
            try:
                chainlink_anchor = self.spot_feed.get_anchor_price(symbol="btc/usd", target_ts=float(start_ts))
            except Exception:  # noqa: BLE001
                chainlink_anchor = None
            if chainlink_anchor is not None and chainlink_anchor > 0:
                return float(chainlink_anchor)
        return float(snapshot.chainlink_price or snapshot.reference_price or 0.0)

    def _arb_anchor_capture_source(self, *, snapshot: SpotSnapshot) -> str:
        if snapshot.chainlink_price and snapshot.chainlink_price > 0:
            return "polymarket-chainlink"
        return snapshot.source or "local-spot"

    def _arb_state_defaults(self, *, market: dict | None = None, seconds_into_window: int | None = None, **overrides: str) -> dict[str, str]:
        state = {
            "strategy_market_bias": "Arbitraje doble pata 50 / 50",
            "strategy_plan_legs": "0",
            "strategy_window_seconds": "0",
            "strategy_cycle_budget": "0.000000",
            "strategy_current_market_exposure": "0.000000",
            "strategy_resolution_mode": "paper-settle-at-close",
            "strategy_timing_regime": "",
            "strategy_price_mode": "underround",
            "strategy_primary_ratio": "0.500000",
            "strategy_primary_outcome": "",
            "strategy_hedge_outcome": "",
            "strategy_primary_exposure": "0.000000",
            "strategy_hedge_exposure": "0.000000",
            "strategy_replenishment_count": "0",
            "strategy_target_outcome": "",
            "strategy_target_price": "0.000000",
            "strategy_trigger_outcome": "pair_sum",
            "strategy_trigger_price_seen": "0.000000",
            "strategy_pair_sum": "0.000000",
            "strategy_edge_pct": "0.000000",
            "strategy_fair_value": "0.000000",
            "strategy_strategy_min_notional": f"{self._arb_strategy_min_notional():.6f}",
            "strategy_effective_min_notional": f"{self._arb_strategy_min_notional():.6f}",
            "strategy_desired_up_ratio": "0.500000",
            "strategy_desired_down_ratio": "0.500000",
            "strategy_current_up_ratio": "0.500000",
            "strategy_bracket_phase": "observando",
        }
        state.update(self._arb_live_spot_state(market=market, seconds_into_window=seconds_into_window))
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

    def _select_vidarx_timing_regime(
        self,
        *,
        seconds_into_window: int,
        price_mode: str,
        has_existing_market_exposure: bool,
    ) -> tuple[str | None, str]:
        if _VIDARX_MIN_SECONDS <= seconds_into_window <= _VIDARX_EARLY_MID_END:
            return "early-mid", ""
        if has_existing_market_exposure and (_VIDARX_EARLY_MID_END < seconds_into_window <= _VIDARX_MAX_SECONDS):
            return None, f"vidarx segunda oleada desactivada: {seconds_into_window}s con posicion ya abierta"
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
            if role == "primary":
                return 10 if timing_regime == "mid-late" else 8
            return 7 if timing_regime == "mid-late" else 5
        if price_mode == "tilted":
            return 8 if role == "primary" else 5
        return 5 if role == "primary" else 4

    def _vidarx_initial_level_cap(self, *, price_mode: str, timing_regime: str, role: str) -> int:
        if price_mode == "extreme":
            if role == "primary":
                return 6 if timing_regime == "mid-late" else 5
            return 4 if timing_regime == "mid-late" else 3
        if price_mode == "tilted":
            return 5 if role == "primary" else 3
        return 3 if role == "primary" else 2

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
            weights = (
                [0.24, 0.20, 0.17, 0.14, 0.11, 0.08, 0.06]
                if timing_regime == "mid-late"
                else [0.28, 0.23, 0.18, 0.14, 0.10, 0.07]
            )
        elif price_mode == "tilted":
            weights = (
                [0.28, 0.23, 0.19, 0.16, 0.14]
                if timing_regime == "mid-late"
                else [0.34, 0.26, 0.20, 0.12, 0.08]
            )
        else:
            weights = [0.42, 0.33, 0.25]

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

    def _settle_resolved_paper_positions(self, stats: dict[str, int], mode: str = "paper") -> None:
        if not self.db.list_copy_positions():
            return

        resolved_totals: dict[str, dict[str, object]] = {}
        for row in list(self.db.list_copy_positions()):
            slug = str(row["slug"] or "")
            asset = str(row["asset"] or "")
            size = float(row["size"] or 0.0)
            if size <= 0 or "btc-updown-5m-" not in slug:
                continue
            try:
                market = self.gamma_client.get_market_by_slug(slug)
            except Exception as error:  # noqa: BLE001
                self.logger.warning("paper settle skipped slug=%s: market lookup failed: %s", slug, error)
                continue
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
                source_wallet="strategy:settlement",
                source_signal_id=0,
                title=str(row["title"] or ""),
                slug=slug,
                outcome=str(row["outcome"] or ""),
                category=str(row["category"] or "crypto"),
                reason=f"strategy_resolution:{slug}:{row['outcome'] or ''}",
            )
            result = self.execution_engine.settle_resolved(mode=mode, instruction=instruction)
            stats["opportunities"] += 1
            if result.status == "filled":
                stats["filled"] += 1
                bucket = resolved_totals.setdefault(
                    slug,
                    {
                        "pnl": 0.0,
                        "winning_outcome": self._resolved_winning_outcome(market),
                    },
                )
                bucket["pnl"] = float(bucket["pnl"]) + float(result.pnl_delta)

        for slug, payload in resolved_totals.items():
            self.db.close_strategy_window(
                slug=slug,
                realized_pnl=float(payload["pnl"]),
                winning_outcome=str(payload["winning_outcome"] or ""),
                current_exposure=0.0,
                notes=f"resolved {payload['winning_outcome'] or '-'}",
            )

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

    def _resolved_winning_outcome(self, market: dict) -> str:
        outcomes = _parse_json_list(market.get("outcomes"))
        prices = _parse_json_list(market.get("outcomePrices"))
        for outcome, price in zip(outcomes, prices):
            try:
                if float(price) >= 0.999:
                    return str(outcome)
            except (TypeError, ValueError):
                continue
        return ""


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


def _round_up(value: float, precision: str) -> float:
    quant = Decimal(precision)
    return float((Decimal(str(value)) / quant).to_integral_value(rounding=ROUND_CEILING) * quant)


def _safe_float(raw_value: object) -> float:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return 0.0
