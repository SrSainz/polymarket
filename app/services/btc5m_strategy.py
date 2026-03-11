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

_OPERATIVE_TRIGGER_PRICE = 0.75
_OPERATIVE_MAX_OPPOSITE_PRICE = 0.06
_OPERATIVE_MAX_TARGET_SPREAD = 0.03
_OPERATIVE_MAX_SECONDS_INTO_WINDOW = 240


@dataclass(frozen=True)
class MarketOutcome:
    label: str
    asset_id: str
    best_ask: float
    best_bid: float
    best_ask_size: float


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
        live_total_capital = cash_balance + total_exposure if mode == "live" else max(self.settings.config.bankroll, total_exposure)
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
            if best_ask is None or best_bid is None or best_ask_size is None:
                self._record_strategy_snapshot(market=market, note=f"incomplete book for {label}")
                return None
            priced_outcomes.append(
                MarketOutcome(
                    label=str(label),
                    asset_id=str(token_id),
                    best_ask=best_ask,
                    best_bid=best_bid,
                    best_ask_size=best_ask_size,
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
        desired = min(desired, budget_left)
        desired = min(desired, self.settings.config.max_position_per_market)
        desired = min(desired, max(effective_bankroll - current_total_exposure, 0.0))
        return max(desired, 0.0)

    def _live_cash_snapshot(self, *, mode: str) -> tuple[float, float]:
        if mode != "live":
            return max(self.settings.config.bankroll - self.db.get_total_exposure(), 0.0), 0.0
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
    asks = book.get("asks") or []
    prices: list[float] = []
    for ask in asks:
        try:
            prices.append(float(ask.get("price")))
        except (AttributeError, TypeError, ValueError):
            continue
    if not prices:
        return None
    return min(prices)


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
    asks = book.get("asks") or []
    best_price = _best_ask(book)
    if best_price is None:
        return None
    for ask in asks:
        try:
            if float(ask.get("price")) != best_price:
                continue
            return float(ask.get("size") or 0.0)
        except (AttributeError, TypeError, ValueError):
            continue
    return None


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
