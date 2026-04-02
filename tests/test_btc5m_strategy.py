from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from app.core.execution_engine import apply_fill_to_database
from app.core.decision_engine import DecisionTrace
from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import CopyInstruction, ExecutionResult, SignalAction, TradeSide
from app.polymarket.spot_feed import SpotSnapshot
from app.polymarket.fee_model import effective_taker_fee_rate, fee_per_share
from app.services.btc5m_strategy import (
    ArbReferenceState,
    ArbSingleSideSignal,
    ArbSpotContext,
    AskLevel,
    BTC5mStrategyService,
    MarketOutcome,
)
from app.settings import AppPaths, AppSettings, BotConfig, EnvSettings


class _FakeGammaClient:
    def __init__(self, market: dict, events: dict[str, dict] | None = None) -> None:
        self.market = market
        self.events = events or {}

    def get_market_by_slug(self, slug: str) -> dict | None:
        if "question" in self.market or "conditionId" in self.market:
            payload = dict(self.market)
            payload["slug"] = slug
            return payload
        payload = self.market.get(slug)
        if payload is None:
            return None
        copy = dict(payload)
        copy["slug"] = slug
        return copy

    def get_event_by_id(self, event_id: str) -> dict | None:
        payload = self.events.get(event_id)
        return dict(payload) if payload is not None else None


class _FailingGammaClient:
    def get_market_by_slug(self, slug: str) -> dict | None:
        raise ConnectionError(f"dns failed for {slug}")


class _FakeCLOBClient:
    def __init__(self, books: dict[str, dict], balance: float = 50.0, *, feed_mode: str = "auto", feed_connected: bool = False) -> None:
        self.books = books
        self.balance = balance
        self.feed_mode = feed_mode
        self.feed_connected = feed_connected
        self.tracked_assets: tuple[str, ...] = ()

    def get_collateral_balance(self) -> dict[str, float]:
        return {"balance": self.balance, "allowance": self.balance}

    def track_assets(self, token_ids):  # noqa: ANN001
        self.tracked_assets = tuple(str(token_id) for token_id in token_ids)

    def market_feed_status(self):  # noqa: ANN001
        mode = self.feed_mode
        if mode == "auto":
            mode = "websocket-warming" if self.tracked_assets else "websocket-idle"
        return SimpleNamespace(
            mode=mode,
            connected=self.feed_connected,
            tracked_assets=len(self.tracked_assets),
            age_ms=0,
        )

    def get_book(self, token_id: str) -> dict:
        return self.books[token_id]

    def get_min_order_size(self, token_id: str) -> float | None:
        raw_value = (self.books.get(token_id) or {}).get("min_order_size")
        try:
            resolved = float(raw_value)
        except (TypeError, ValueError):
            return None
        return resolved if resolved > 0 else None

    def get_midpoint(self, token_id: str) -> float | None:
        book = self.books.get(token_id) or {}
        asks = book.get("asks") or []
        if not asks:
            return None
        return float(asks[0]["price"])


class _FeeAwareCLOBClient(_FakeCLOBClient):
    def __init__(self, books: dict[str, dict], fee_bps: float) -> None:
        super().__init__(books=books, balance=100.0)
        self.fee_bps = fee_bps

    def get_fee_rate_bps(self, token_id: str) -> float | None:  # noqa: ARG002
        return self.fee_bps


class _FakeBroker:
    def __init__(self) -> None:
        self.instructions = []

    def execute(self, instruction):  # noqa: ANN001
        self.instructions.append(instruction)
        return ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.OPEN,
            asset=instruction.asset,
            size=instruction.size,
            price=instruction.price,
            notional=instruction.notional,
            pnl_delta=0.0,
            message="ok",
        )


class _ApplyingLiveBroker:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.instructions = []

    def execute(self, instruction):  # noqa: ANN001
        self.instructions.append(instruction)
        return apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode="live",
            filled_size=instruction.size,
            fill_price=instruction.price,
            fill_notional=instruction.notional,
            message="live fill",
            status="filled",
            notes="test-live-fill",
        )


class _FakeSpotFeed:
    def __init__(self, snapshot: SpotSnapshot) -> None:
        self.snapshot = snapshot

    def get_snapshot(self) -> SpotSnapshot:
        return self.snapshot

    def wait_for_update(self, timeout_seconds: float) -> bool:  # noqa: ARG002
        return False


def _settings(**overrides) -> AppSettings:
    config = BotConfig(
        **{
            "watched_wallets": ["0xabc"],
            "strategy_mode": "btc5m_orderbook",
            "strategy_entry_mode": "buy_opposite",
            "strategy_trigger_price": 0.98,
            "strategy_trade_allocation_pct": 0.10,
            "strategy_max_opposite_price": 0.20,
            "bankroll": 100.0,
            "max_position_per_market": 10.0,
            "max_total_exposure": 100.0,
            "min_trade_amount": 1.0,
            "btc5m_reserve_enabled": True,
            "btc5m_relaxed_risk": True,
            "btc5m_strict_realism_mode": False,
            "profit_keep_ratio": 0.0,
            **overrides,
        }
    )
    return AppSettings(
        config=config,
        env=EnvSettings(live_trading=True),
        paths=AppPaths(
            root=Path("."),
            db_path=Path("bot.db"),
            logs_dir=Path("."),
            reports_dir=Path("."),
        ),
    )


def test_strategy_buy_opposite_uses_cheap_side_and_records_balance(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.98"}],
                "asks": [{"price": "0.99", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.01"}],
                "asks": [{"price": "0.01", "size": "1000"}],
            },
        },
        balance=50.0,
    )
    broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-strategy"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] == 1
    assert broker.instructions
    assert broker.instructions[0].asset == "asset-down"
    assert broker.instructions[0].outcome == "Down"
    assert db.get_bot_state("live_cash_balance") == "50.00000000"
    assert db.get_bot_state("strategy_target_outcome") == "Down"
    db.close()


def test_execute_instruction_uses_latest_microstructure_execution_profile_for_live(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    live_broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=live_broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-live-profile"),
    )
    service.telemetry._latest_decision = DecisionTrace(
        window_id="btc-updown-5m-test",
        market_slug="btc-updown-5m-test",
        market_title="Bitcoin Up or Down - Test",
        readiness_score=82.0,
        regime="directional_pressure",
        signal_side="up",
        expected_edge_bps=12.0,
        maker_ev_bps=9.0,
        taker_ev_bps=11.0,
        maker_fill_prob=0.55,
        selected_execution="maker_post_only_gtc",
        blocked_by=(),
        latency_penalty_bps=1.0,
        spread_penalty_bps=2.0,
        adverse_selection_penalty_bps=1.5,
    )

    service._execute_instruction(
        mode="live",
        instruction=CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id="cond-1",
            size=10.0,
            price=0.42,
            notional=4.2,
            source_wallet="strategy:test",
            source_signal_id=1,
            title="Bitcoin Up or Down - Test",
            slug="btc-updown-5m-test",
            outcome="Up",
            category="crypto",
            reason="unit-test",
        ),
    )

    assert live_broker.instructions
    assert live_broker.instructions[0].execution_profile == "taker_fak"
    assert "live_maker_disabled:maker_post_only_gtc->taker_fak" in live_broker.instructions[0].reason
    db.close()


def test_execute_instruction_keeps_latest_microstructure_execution_profile_for_shadow(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    shadow_broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=shadow_broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-shadow-profile"),
    )
    service.telemetry._latest_decision = DecisionTrace(
        window_id="btc-updown-5m-test",
        market_slug="btc-updown-5m-test",
        market_title="Bitcoin Up or Down - Test",
        readiness_score=82.0,
        regime="directional_pressure",
        signal_side="up",
        expected_edge_bps=12.0,
        maker_ev_bps=9.0,
        taker_ev_bps=11.0,
        maker_fill_prob=0.55,
        selected_execution="maker_post_only_gtc",
        blocked_by=(),
        latency_penalty_bps=1.0,
        spread_penalty_bps=2.0,
        adverse_selection_penalty_bps=1.5,
    )

    service._execute_instruction(
        mode="shadow",
        instruction=CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id="cond-1",
            size=10.0,
            price=0.42,
            notional=4.2,
            source_wallet="strategy:test",
            source_signal_id=1,
            title="Bitcoin Up or Down - Test",
            slug="btc-updown-5m-test",
            outcome="Up",
            category="crypto",
            reason="unit-test",
        ),
    )

    assert shadow_broker.instructions
    assert shadow_broker.instructions[0].execution_profile == "maker_post_only_gtc"
    db.close()


def test_microstructure_taker_fee_estimate_uses_public_fee_rate(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "feesEnabled": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    books = {
        "asset-up": {"asks": [{"price": 0.40, "size": 50}], "bids": [{"price": 0.39, "size": 50}]},
        "asset-down": {"asks": [{"price": 0.60, "size": 50}], "bids": [{"price": 0.59, "size": 50}]},
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FeeAwareCLOBClient(books=books, fee_bps=2500.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-fee"),
    )

    fee_bps = service._microstructure_taker_fee_bps_estimate(market)  # noqa: SLF001

    expected_bps = effective_taker_fee_rate(
        fee_rate_bps=2500.0,
        price=0.40,
        category="crypto",
    ) * 10_000
    assert round(fee_bps, 2) == round(expected_bps, 2)
    db.close()


def test_arb_single_side_net_edge_uses_dynamic_fee_rate(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FeeAwareCLOBClient(
            books={
                "asset-up": {"asks": [{"price": 0.40, "size": 50}], "bids": [{"price": 0.39, "size": 50}]},
            },
            fee_bps=2500.0,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-single-side-dynamic-fee"),
    )
    target = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.40,
        best_bid=0.39,
        best_ask_size=50.0,
        ask_levels=(AskLevel(price=0.40, size=50.0),),
    )

    net_edge = service._arb_estimated_single_side_net_edge(  # noqa: SLF001
        target=target,
        fair_value=0.415,
        pair_sum=1.0,
        delta_bps=3.0,
    )

    expected_net_edge = 0.415 - 0.40 - ((0.40 - 0.39) * 0.35) - fee_per_share(
        fee_rate_bps=2500.0,
        price=0.40,
        category="crypto",
    )
    assert round(net_edge * 10_000, 2) == round(expected_net_edge * 10_000, 2)
    db.close()


def test_shadow_cash_snapshot_uses_realized_execution_pnl(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    instruction_open = CopyInstruction(
        action=SignalAction.OPEN,
        side=TradeSide.BUY,
        asset="asset-up",
        condition_id="cond-1",
        size=10.0,
        price=0.40,
        notional=4.0,
        source_wallet="strategy:test",
        source_signal_id=1,
        title="Bitcoin Up or Down",
        slug="btc-updown-5m-test",
        outcome="Up",
        category="crypto",
        reason="unit-open",
    )
    apply_fill_to_database(
        db=db,
        instruction=instruction_open,
        mode="shadow",
        filled_size=10.0,
        fill_price=0.40,
        fill_notional=4.0,
        fee_paid=0.20,
        message="open",
        status="filled",
        notes="open",
    )
    instruction_close = instruction_open.model_copy(update={"action": SignalAction.CLOSE, "side": TradeSide.SELL, "price": 0.45, "notional": 4.5, "reason": "unit-close"})
    apply_fill_to_database(
        db=db,
        instruction=instruction_close,
        mode="shadow",
        filled_size=10.0,
        fill_price=0.45,
        fill_notional=4.5,
        fee_paid=0.10,
        message="close",
        status="filled",
        notes="close",
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72),
        logger=logging.getLogger("test-btc5m-shadow-cash"),
    )

    cash_balance, allowance = service._live_cash_snapshot(mode="shadow")  # noqa: SLF001

    assert round(cash_balance, 2) == 97.92
    assert allowance == 0.0
    db.close()


def test_live_user_trade_reconciliation_updates_live_position_from_pending_order(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={"asset-up": {"asks": [{"price": 0.42, "size": 50}], "bids": [{"price": 0.41, "size": 50}]}, "asset-down": {"asks": [{"price": 0.58, "size": 50}], "bids": [{"price": 0.57, "size": 50}]}}),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-live-reconcile"),
    )
    db.set_bot_state(
        "live_pending_order:order-1",
        json.dumps(
            {
                "order_id": "order-1",
                "action": "open",
                "side": "buy",
                "asset": "asset-up",
                "condition_id": "cond-1",
                "size": 10.0,
                "price": 0.42,
                "notional": 4.2,
                "source_wallet": "strategy:test",
                "source_signal_id": 1,
                "title": "Bitcoin Up or Down - Test",
                "slug": "btc-updown-5m-test",
                "outcome": "Up",
                "category": "crypto",
                "reason": "pending-live-order",
                "execution_profile": "maker_post_only_gtc",
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )

    service._handle_user_payload(  # noqa: SLF001
        {
            "event_type": "trade",
            "status": "MATCHED",
            "id": "trade-1",
            "maker_orders": [
                {
                    "order_id": "order-1",
                    "matched_amount": "10",
                    "price": "0.43",
                }
            ],
        }
    )

    position = db.get_copy_position("asset-up")
    executions = db.get_recent_executions(limit=5)

    assert position is not None
    assert float(position["size"]) == 10.0
    assert abs(float(position["avg_price"]) - 0.43) < 1e-9
    assert executions[0]["mode"] == "live"
    assert "live_user_feed_reconciled" in str(executions[0]["notes"])
    assert db.get_bot_state("live_pending_order:order-1") is None

    service._handle_user_payload(  # noqa: SLF001
        {
            "event_type": "trade",
            "status": "CONFIRMED",
            "id": "trade-1",
            "maker_orders": [
                {
                    "order_id": "order-1",
                    "matched_amount": "10",
                    "price": "0.43",
                }
            ],
        }
    )

    executions_after_repeat = db.get_recent_executions(limit=5)
    assert len(executions_after_repeat) == 1
    db.close()


def test_live_order_update_does_not_drop_pending_before_trade_reconciliation(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={"asset-up": {"asks": [{"price": 0.42, "size": 50}], "bids": [{"price": 0.41, "size": 50}]}, "asset-down": {"asks": [{"price": 0.58, "size": 50}], "bids": [{"price": 0.57, "size": 50}]}}),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-live-order-update"),
    )
    db.set_bot_state(
        "live_pending_order:order-1",
        json.dumps(
            {
                "order_id": "order-1",
                "action": "open",
                "side": "buy",
                "asset": "asset-up",
                "condition_id": "cond-1",
                "size": 10.0,
                "price": 0.42,
                "notional": 4.2,
                "source_wallet": "strategy:test",
                "source_signal_id": 1,
                "title": "Bitcoin Up or Down - Test",
                "slug": "btc-updown-5m-test",
                "outcome": "Up",
                "category": "crypto",
                "reason": "pending-live-order",
                "execution_profile": "maker_post_only_gtc",
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )

    service._handle_user_payload(  # noqa: SLF001
        {
            "event_type": "order",
            "status": "CONFIRMED",
            "order_id": "order-1",
        }
    )

    pending_after_order = db.get_bot_state("live_pending_order:order-1")
    assert pending_after_order is not None
    assert db.get_recent_executions(limit=5) == []

    service._handle_user_payload(  # noqa: SLF001
        {
            "event_type": "trade",
            "status": "MATCHED",
            "id": "trade-1",
            "maker_orders": [
                {
                    "order_id": "order-1",
                    "matched_amount": "10",
                    "price": "0.43",
                }
            ],
        }
    )

    position = db.get_copy_position("asset-up")
    executions = db.get_recent_executions(limit=5)

    assert position is not None
    assert float(position["size"]) == 10.0
    assert executions[0]["mode"] == "live"
    assert db.get_bot_state("live_pending_order:order-1") is None
    db.close()


def test_live_user_trade_failure_clears_pending_order_without_mutating_position(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={"asset-up": {"asks": [{"price": 0.42, "size": 50}], "bids": [{"price": 0.41, "size": 50}]}, "asset-down": {"asks": [{"price": 0.58, "size": 50}], "bids": [{"price": 0.57, "size": 50}]}}),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-live-failed-reconcile"),
    )
    db.set_bot_state(
        "live_pending_order:order-2",
        json.dumps(
            {
                "order_id": "order-2",
                "action": "open",
                "side": "buy",
                "asset": "asset-up",
                "condition_id": "cond-1",
                "size": 10.0,
                "price": 0.42,
                "notional": 4.2,
                "source_wallet": "strategy:test",
                "source_signal_id": 1,
                "title": "Bitcoin Up or Down - Test",
                "slug": "btc-updown-5m-test",
                "outcome": "Up",
                "category": "crypto",
                "reason": "pending-live-order",
                "execution_profile": "maker_post_only_gtc",
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )

    service._handle_user_payload(  # noqa: SLF001
        {
            "event_type": "trade",
            "status": "FAILED",
            "id": "trade-2",
            "maker_orders": [
                {
                    "order_id": "order-2",
                    "matched_amount": "10",
                    "price": "0.43",
                }
            ],
        }
    )

    assert db.get_copy_position("asset-up") is None
    assert db.get_recent_executions(limit=5) == []
    assert db.get_bot_state("live_pending_order:order-2") is None
    assert db.get_bot_state("live_last_failed_order_id") == "order-2"
    db.close()


def test_live_user_trade_without_pending_order_is_recorded_as_observed_activity(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    db.upsert_copy_position(
        asset="asset-down",
        condition_id="cond-1",
        size=5.0,
        avg_price=0.28,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Test",
        slug="btc-updown-5m-test",
        outcome="Down",
        category="crypto",
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={"asset-up": {"asks": [{"price": 0.42, "size": 50}], "bids": [{"price": 0.41, "size": 50}]}, "asset-down": {"asks": [{"price": 0.58, "size": 50}], "bids": [{"price": 0.57, "size": 50}]}}),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-live-observed-activity"),
    )

    service._handle_user_payload(  # noqa: SLF001
        {
            "event_type": "trade",
            "status": "CONFIRMED",
            "id": "trade-manual",
            "asset_id": "asset-down",
            "side": "SELL",
            "maker_orders": [
                {
                    "order_id": "manual-order",
                    "matched_amount": "5",
                    "price": "0.35",
                }
            ],
        }
    )

    observed_raw = db.get_bot_state("live_observed_activity:manual-order:trade-manual")
    position = db.get_copy_position("asset-down")

    assert observed_raw is not None
    assert position is not None
    assert float(position["size"]) == 5.0
    assert db.get_recent_executions(limit=5) == []
    assert db.get_bot_state("live_last_observed_trade_id") == "trade-manual"

    observed = json.loads(observed_raw)
    assert observed["action"] == "close"
    assert observed["side"] == "sell"
    assert observed["title"] == "Bitcoin Up or Down - Test"
    assert observed["outcome"] == "Down"
    assert "fuera del bot" in observed["notes"]
    db.close()


def test_live_position_ledger_preflight_blocks_on_observed_external_activity(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={"asset-up": {"asks": [{"price": 0.42, "size": 50}], "bids": [{"price": 0.41, "size": 50}]}, "asset-down": {"asks": [{"price": 0.58, "size": 50}], "bids": [{"price": 0.57, "size": 50}]}}),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_preflight_require_clean_ledger=True),
        logger=logging.getLogger("test-btc5m-live-ledger-preflight"),
    )
    db.set_bot_state(
        "live_observed_activity:manual-order:trade-manual",
        json.dumps(
            {
                "order_id": "manual-order",
                "trade_id": "trade-manual",
                "asset": "asset-up",
                "condition_id": "cond-1",
                "size": 5.0,
                "price": 0.44,
                "notional": 2.2,
                "title": "Bitcoin Up or Down - Test",
                "slug": "btc-updown-5m-test",
                "outcome": "Up",
                "category": "crypto",
                "status": "confirmed",
                "observed_live_activity": True,
                "notes": "movimiento live observado fuera del bot",
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )

    allowed, note = service._position_ledger_can_run(mode="live")  # noqa: SLF001

    assert allowed is False
    assert "outside bot ledger" in note
    assert db.get_bot_state("position_ledger_preflight") == "blocked"
    assert db.get_bot_state("position_ledger_mode") == "external"
    db.close()


def test_cleanup_stale_pending_live_orders_removes_expired_window_and_keeps_current(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    stale_slug = f"btc-updown-5m-{now_ts - 900}"
    fresh_slug = f"btc-updown-5m-{now_ts}"
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-cleanup-stale-pending"),
    )
    db.set_bot_state(
        "live_pending_order:stale-order",
        json.dumps(
            {
                "order_id": "stale-order",
                "action": "open",
                "side": "buy",
                "asset": "asset-stale",
                "condition_id": "cond-stale",
                "size": 5.0,
                "price": 0.30,
                "notional": 1.5,
                "slug": stale_slug,
                "outcome": "Down",
                "submitted_at": now_ts - 300,
                "response_status": "matched",
            },
            separators=(",", ":"),
        ),
    )
    db.set_bot_state(
        "live_pending_order:fresh-order",
        json.dumps(
            {
                "order_id": "fresh-order",
                "action": "open",
                "side": "buy",
                "asset": "asset-fresh",
                "condition_id": "cond-fresh",
                "size": 5.0,
                "price": 0.30,
                "notional": 1.5,
                "slug": fresh_slug,
                "outcome": "Down",
                "submitted_at": now_ts,
                "response_status": "matched",
            },
            separators=(",", ":"),
        ),
    )

    removed = service._cleanup_stale_pending_live_orders()  # noqa: SLF001

    assert removed == 1
    assert db.get_bot_state("live_pending_order:stale-order") is None
    assert db.get_bot_state("live_pending_order:fresh-order") is not None
    assert db.get_bot_state("live_last_stale_pending_order_id") == "stale-order"
    assert db.get_bot_state("live_last_stale_pending_reason") == "window_expired"
    db.close()


def test_live_blocks_flat_single_side_open_in_bracket_only_mode_even_if_second_leg_is_not_viable(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Flat Single Side",
        "slug": "btc-updown-5m-flat-single-side",
        "conditionId": "cond-flat-single-side",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {"asks": [{"price": 0.73, "size": 50}], "bids": [{"price": 0.72, "size": 50}]},
                "asset-down": {"asks": [{"price": 0.28, "size": 50}], "bids": [{"price": 0.27, "size": 50}]},
            },
            balance=97.72,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72),
        logger=logging.getLogger("test-btc5m-live-flat-single-side-block"),
    )

    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.73,
        best_bid=0.72,
        best_ask_size=50.0,
        ask_levels=(AskLevel(price=0.73, size=50.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.28,
        best_bid=0.27,
        best_ask_size=50.0,
        ask_levels=(AskLevel(price=0.28, size=50.0),),
    )
    signal = ArbSingleSideSignal(
        target=down_outcome,
        fair_value=0.40,
        raw_edge=0.12,
        net_edge=0.08,
        edge_source="spot",
    )

    blocked, reason = service._arb_should_block_flat_single_side_open(  # noqa: SLF001
        mode="live",
        bracket_phase="abrir",
        current_up_notional=0.0,
        current_down_notional=0.0,
        signal=signal,
        pair_sum=1.01,
        cycle_budget=25.0,
        cash_balance=97.72,
        single_budget=8.0,
        seconds_into_window=35,
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        fair_up=0.60,
        fair_down=0.40,
        delta_bps=-7.4,
    )

    assert blocked is True
    assert "live opera bracket-only" in reason
    db.close()


def test_live_biased_bracket_anchors_on_strong_edge_side_when_ratio_side_is_weak(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Live Strong Down",
        "slug": "btc-updown-5m-live-strong-down",
        "conditionId": "cond-live-strong-down",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": (datetime.now(timezone.utc) - timedelta(seconds=18)).isoformat().replace("+00:00", "Z")}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {"asks": [{"price": 0.51, "size": 120}], "bids": [{"price": 0.50, "size": 120}]},
                "asset-down": {"asks": [{"price": 0.50, "size": 120}], "bids": [{"price": 0.39, "size": 120}]},
            },
            balance=97.72,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72, live_btc5m_cycle_budget_usdc=25.0),
        logger=logging.getLogger("test-btc5m-live-strong-down-biased-bracket"),
    )

    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.51,
        best_bid=0.50,
        best_ask_size=120.0,
        ask_levels=(AskLevel(price=0.51, size=120.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.50,
        best_bid=0.39,
        best_ask_size=120.0,
        ask_levels=(AskLevel(price=0.50, size=120.0),),
    )
    spot_context = ArbSpotContext(
        current_price=66819.0,
        reference_price=66744.5386,
        lead_price=66819.0,
        anchor_price=66744.5386,
        local_anchor_price=66744.5386,
        official_price_to_beat=66744.5386,
        anchor_source="captured-chainlink",
        fair_up=0.51,
        fair_down=0.61,
        delta_bps=11.2,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=66819.0,
        chainlink_price=66819.0,
        captured_price_to_beat=66744.5386,
        effective_price_to_beat=66744.5386,
        effective_price_source="captured-chainlink",
    )

    plan = service._build_arb_biased_bracket_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        pair_sum=1.01,
        fair_up=0.51,
        fair_down=0.61,
        up_net_edge=-0.0167,
        down_net_edge=0.1003,
        desired_up_ratio=0.62,
        current_up_ratio=0.5,
        timing_regime="early",
        cycle_budget=25.0,
        cash_balance=97.72,
        remaining_instruction_capacity=6,
        current_up_notional=0.0,
        current_down_notional=0.0,
        spot_context=spot_context,
        bracket_phase="abrir",
    )

    assert plan is not None
    assert plan.price_mode == "biased-bracket"
    assert plan.trigger.label == "Down"
    assert {instruction.outcome for instruction in plan.instructions} == {"Up", "Down"}
    assert any(instruction.outcome == "Up" and abs(float(instruction.price) - 0.51) < 1e-9 for instruction in plan.instructions)
    assert "pata fuerte Down" in plan.note
    db.close()


def test_live_blocks_flat_cheap_side_open_in_bracket_only_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=97.72),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
        ),
        logger=logging.getLogger("test-btc5m-live-cheap-block"),
    )

    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.44,
        best_bid=0.43,
        best_ask_size=150.0,
        ask_levels=(AskLevel(price=0.44, size=150.0), AskLevel(price=0.45, size=150.0)),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.57,
        best_bid=0.56,
        best_ask_size=150.0,
        ask_levels=(AskLevel(price=0.57, size=150.0), AskLevel(price=0.58, size=150.0)),
    )
    signal = ArbSingleSideSignal(
        target=down_outcome,
        fair_value=0.67,
        raw_edge=0.10,
        net_edge=0.08,
        edge_source="spot",
    )

    blocked, reason = service._arb_should_block_flat_single_side_open(  # noqa: SLF001
        mode="live",
        bracket_phase="abrir",
        current_up_notional=0.0,
        current_down_notional=0.0,
        signal=signal,
        pair_sum=1.01,
        cycle_budget=25.0,
        cash_balance=97.72,
        single_budget=8.0,
        seconds_into_window=35,
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        fair_up=0.55,
        fair_down=0.67,
        delta_bps=-14.0,
    )

    assert blocked is True
    assert "live opera bracket-only" in reason
    db.close()


def test_live_flat_cheap_side_probe_is_blocked_in_bracket_only_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=35)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Probe",
        "slug": "btc-updown-5m-live-probe",
        "conditionId": "cond-live-probe",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.74"}],
                "asks": [{"price": "0.75", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.26"}],
                "asks": [{"price": "0.27", "size": "300"}, {"price": "0.28", "size": "300"}],
            },
        },
        balance=114.14,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-cheap-single-probe-blocked"),
    )
    spot_context = ArbSpotContext(
        current_price=66710.0,
        reference_price=66744.5386,
        lead_price=66710.0,
        anchor_price=66744.5386,
        local_anchor_price=66744.5386,
        official_price_to_beat=66744.5386,
        anchor_source="captured-chainlink",
        fair_up=0.55,
        fair_down=0.40,
        delta_bps=-8.0,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=66710.0,
        chainlink_price=66710.0,
        captured_price_to_beat=66744.5386,
        effective_price_to_beat=66744.5386,
        effective_price_source="captured-chainlink",
    )
    service._arb_spot_context = lambda **kwargs: spot_context  # type: ignore[method-assign]
    service._arb_reference_state = lambda **kwargs: ArbReferenceState(  # type: ignore[method-assign]
        comparable=True,
        quality="captured-chainlink",
        note="ok",
        budget_scale=1.0,
    )

    plan = service._build_arb_micro_plan(  # noqa: SLF001
        mode="live",
        market=market,
        cash_balance=114.14,
        effective_bankroll=97.72,
        live_total_capital=114.14,
        current_total_exposure=0.0,
        carry_exposure=0.0,
        carry_window_count=0,
    )

    assert plan is None
    assert "live opera bracket-only" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_live_single_leg_probe_exits_on_countertrend_before_repair(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=115)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Probe Countertrend Exit",
        "slug": "btc-updown-5m-live-probe-countertrend-exit",
        "conditionId": "cond-live-probe-countertrend-exit",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.78"}],
                "asks": [{"price": "0.79", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.20"}],
                "asks": [{"price": "0.21", "size": "300"}],
            },
        },
        balance=114.14,
    )
    db.upsert_copy_position(
        asset="asset-down",
        condition_id=market["conditionId"],
        size=24.0,
        avg_price=0.31,
        realized_pnl=0.0,
        title=market["question"],
        slug=market["slug"],
        outcome="Down",
        category="crypto",
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-single-leg-countertrend-exit"),
    )
    spot_context = ArbSpotContext(
        current_price=67520.0,
        reference_price=67440.0,
        lead_price=67520.0,
        anchor_price=67440.0,
        local_anchor_price=67440.0,
        official_price_to_beat=67440.0,
        anchor_source="captured-chainlink",
        fair_up=0.90,
        fair_down=0.10,
        delta_bps=11.9,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=67520.0,
        chainlink_price=67520.0,
        captured_price_to_beat=67440.0,
        effective_price_to_beat=67440.0,
        effective_price_source="captured-chainlink",
    )
    service._arb_spot_context = lambda **kwargs: spot_context  # type: ignore[method-assign]
    service._arb_reference_state = lambda **kwargs: ArbReferenceState(  # type: ignore[method-assign]
        comparable=True,
        quality="captured-chainlink",
        note="ok",
        budget_scale=1.0,
    )

    plan = service._build_arb_micro_plan(  # noqa: SLF001
        mode="live",
        market=market,
        cash_balance=114.14,
        effective_bankroll=97.72,
        live_total_capital=114.14,
        current_total_exposure=db.get_total_exposure(),
        carry_exposure=0.0,
        carry_window_count=0,
    )

    assert plan is not None
    assert plan.price_mode == "single-leg-defensive-exit"
    assert plan.primary_target.label == "Down"
    assert len(plan.instructions) == 1
    assert plan.instructions[0].side == TradeSide.SELL
    assert plan.instructions[0].action == SignalAction.CLOSE
    assert "contradiccion spot" in plan.note
    db.close()


def test_live_single_leg_probe_exits_late_when_still_uncovered(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=225)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Probe Late Exit",
        "slug": "btc-updown-5m-live-probe-late-exit",
        "conditionId": "cond-live-probe-late-exit",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.42"}],
                "asks": [{"price": "0.43", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.55"}],
                "asks": [{"price": "0.56", "size": "300"}],
            },
        },
        balance=114.14,
    )
    db.upsert_copy_position(
        asset="asset-down",
        condition_id=market["conditionId"],
        size=15.0,
        avg_price=0.58,
        realized_pnl=0.0,
        title=market["question"],
        slug=market["slug"],
        outcome="Down",
        category="crypto",
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-single-leg-late-exit"),
    )
    spot_context = ArbSpotContext(
        current_price=67438.0,
        reference_price=67440.0,
        lead_price=67438.0,
        anchor_price=67440.0,
        local_anchor_price=67440.0,
        official_price_to_beat=67440.0,
        anchor_source="captured-chainlink",
        fair_up=0.49,
        fair_down=0.51,
        delta_bps=-0.3,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=67438.0,
        chainlink_price=67438.0,
        captured_price_to_beat=67440.0,
        effective_price_to_beat=67440.0,
        effective_price_source="captured-chainlink",
    )
    service._arb_spot_context = lambda **kwargs: spot_context  # type: ignore[method-assign]
    service._arb_reference_state = lambda **kwargs: ArbReferenceState(  # type: ignore[method-assign]
        comparable=True,
        quality="captured-chainlink",
        note="ok",
        budget_scale=1.0,
    )

    plan = service._build_arb_micro_plan(  # noqa: SLF001
        mode="live",
        market=market,
        cash_balance=114.14,
        effective_bankroll=97.72,
        live_total_capital=114.14,
        current_total_exposure=db.get_total_exposure(),
        carry_exposure=0.0,
        carry_window_count=0,
    )

    assert plan is not None
    assert plan.price_mode == "single-leg-defensive-exit"
    assert plan.primary_target.label == "Down"
    assert len(plan.instructions) == 1
    assert plan.instructions[0].side == TradeSide.SELL
    assert plan.instructions[0].action == SignalAction.CLOSE
    assert "tramo final" in plan.note
    db.close()


def test_live_countertrend_cheap_side_is_blocked_when_spot_points_up(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=114.14),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72),
        logger=logging.getLogger("test-btc5m-live-countertrend-cheap-side"),
    )
    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.65,
        best_bid=0.55,
        best_ask_size=200.0,
        ask_levels=(AskLevel(price=0.65, size=200.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.25,
        best_bid=0.24,
        best_ask_size=200.0,
        ask_levels=(AskLevel(price=0.25, size=200.0),),
    )
    spot_context = ArbSpotContext(
        current_price=67615.88,
        reference_price=67285.61,
        lead_price=67615.88,
        anchor_price=67285.61,
        local_anchor_price=67285.61,
        official_price_to_beat=67285.61,
        anchor_source="captured-chainlink",
        fair_up=0.72,
        fair_down=0.28,
        delta_bps=49.0,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=67615.88,
        chainlink_price=67615.88,
        captured_price_to_beat=67285.61,
        effective_price_to_beat=67285.61,
        effective_price_source="captured-chainlink",
    )

    signal = service._select_cheap_side_target(  # noqa: SLF001
        mode="live",
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        pair_sum=0.90,
        spot_context=spot_context,
        desired_up_ratio=0.50,
        current_up_ratio=0.80,
    )

    assert signal is None
    db.close()


def test_live_does_not_expand_same_single_leg_inventory_with_cheap_side(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=35)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live One Leg",
        "slug": "btc-updown-5m-live-one-leg",
        "conditionId": "cond-live-one-leg",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {"bids": [{"price": "0.73"}], "asks": [{"price": "0.74", "size": "300"}]},
            "asset-down": {"bids": [{"price": "0.26"}], "asks": [{"price": "0.27", "size": "300"}]},
        },
        balance=114.14,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-one-leg-no-expand"),
    )
    db.upsert_copy_position(
        asset="asset-down",
        condition_id=market["conditionId"],
        size=30.0,
        avg_price=0.27,
        realized_pnl=0.0,
        title=market["question"],
        slug=market["slug"],
        outcome="Down",
        category="crypto",
    )
    spot_context = ArbSpotContext(
        current_price=66710.0,
        reference_price=66744.5386,
        lead_price=66710.0,
        anchor_price=66744.5386,
        local_anchor_price=66744.5386,
        official_price_to_beat=66744.5386,
        anchor_source="captured-chainlink",
        fair_up=0.55,
        fair_down=0.40,
        delta_bps=-8.0,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=66710.0,
        chainlink_price=66710.0,
        captured_price_to_beat=66744.5386,
        effective_price_to_beat=66744.5386,
        effective_price_source="captured-chainlink",
    )
    service._arb_spot_context = lambda **kwargs: spot_context  # type: ignore[method-assign]
    service._arb_reference_state = lambda **kwargs: ArbReferenceState(  # type: ignore[method-assign]
        comparable=True,
        quality="captured-chainlink",
        note="ok",
        budget_scale=1.0,
    )
    forced_down = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.27,
        best_bid=0.26,
        best_ask_size=300.0,
        ask_levels=(AskLevel(price=0.27, size=300.0),),
    )
    service._select_cheap_side_target = lambda **kwargs: ArbSingleSideSignal(  # type: ignore[method-assign]
        target=forced_down,
        fair_value=0.40,
        raw_edge=0.13,
        net_edge=0.1175,
        edge_source="spot",
    )
    service._build_arb_biased_bracket_plan = lambda **kwargs: None  # type: ignore[method-assign]
    service._build_arb_repair_plan = lambda **kwargs: None  # type: ignore[method-assign]
    service._build_arb_stabilize_plan = lambda **kwargs: None  # type: ignore[method-assign]
    service._build_arb_inventory_unwind_plan = lambda **kwargs: None  # type: ignore[method-assign]

    plan = service._build_arb_micro_plan(  # noqa: SLF001
        mode="live",
        market=market,
        cash_balance=114.14,
        effective_bankroll=97.72,
        live_total_capital=114.14,
        current_total_exposure=8.1,
        carry_exposure=0.0,
        carry_window_count=0,
    )

    assert plan is None
    assert "debe cubrirse antes de ampliar inventario" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_live_inventory_unwind_does_not_sell_single_leg_probe(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=95)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Single Leg",
        "slug": "btc-updown-5m-live-single-leg",
        "conditionId": "cond-live-single-leg",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=114.14),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72),
        logger=logging.getLogger("test-btc5m-live-single-leg-unwind"),
    )
    db.upsert_copy_position(
        asset="asset-up",
        condition_id=market["conditionId"],
        size=20.0,
        avg_price=0.31,
        realized_pnl=0.0,
        title=market["question"],
        slug=market["slug"],
        outcome="Up",
        category="crypto",
    )
    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.31,
        best_bid=0.30,
        best_ask_size=300.0,
        ask_levels=(AskLevel(price=0.31, size=300.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.70,
        best_bid=0.69,
        best_ask_size=300.0,
        ask_levels=(AskLevel(price=0.70, size=300.0),),
    )
    spot_context = ArbSpotContext(
        current_price=66710.0,
        reference_price=66744.5386,
        lead_price=66710.0,
        anchor_price=66744.5386,
        local_anchor_price=66744.5386,
        official_price_to_beat=66744.5386,
        anchor_source="captured-chainlink",
        fair_up=0.28,
        fair_down=0.72,
        delta_bps=-8.0,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=66710.0,
        chainlink_price=66710.0,
        captured_price_to_beat=66744.5386,
        effective_price_to_beat=66744.5386,
        effective_price_source="captured-chainlink",
    )

    live_plan = service._build_arb_inventory_unwind_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        pair_sum=1.01,
        fair_up=0.28,
        fair_down=0.72,
        desired_up_ratio=0.55,
        current_up_ratio=1.0,
        timing_regime="early-mid",
        cycle_budget=25.0,
        current_up_notional=6.0,
        current_down_notional=0.0,
        spot_context=spot_context,
        bracket_phase="redistribuir",
    )
    shadow_plan = service._build_arb_inventory_unwind_plan(  # noqa: SLF001
        mode="shadow",
        market=market,
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        pair_sum=1.01,
        fair_up=0.28,
        fair_down=0.72,
        desired_up_ratio=0.55,
        current_up_ratio=1.0,
        timing_regime="early-mid",
        cycle_budget=25.0,
        current_up_notional=6.0,
        current_down_notional=0.0,
        spot_context=spot_context,
        bracket_phase="redistribuir",
    )

    assert live_plan is None
    assert shadow_plan is not None
    assert shadow_plan.price_mode == "inventory-unwind"
    assert shadow_plan.instructions[0].side == TradeSide.SELL
    db.close()


def test_operability_state_prefers_waiting_bracket_over_waiting_edge_for_live_cheap_side_block(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=97.72),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72),
        logger=logging.getLogger("test-btc5m-operability-waiting-bracket"),
    )

    state = service._derive_strategy_operability_state(  # noqa: SLF001
        note=(
            "arb_micro no locked edge: pair sum 1.010 | Up edge 0.00% net -1.67% | "
            "Down edge 11.28% net 10.03% | cheap-side bloqueado en live-like: "
            "segunda pata Up sin tamano minimo operable en libro"
        ),
        extra_state={},
    )

    assert state.state == "waiting_bracket"
    assert state.label == "Esperando bracket"
    assert "cheap-side bloqueado en live-like" in state.reason
    db.close()


def test_record_strategy_snapshot_persists_local_window_audit_and_deduplicates(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=97.72),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72),
        logger=logging.getLogger("test-btc5m-window-audit"),
    )
    db.set_bot_state("strategy_runtime_mode", "live")
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-1775050500")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Audit Window")
    db.set_bot_state("live_cash_balance", "97.720000")
    db.set_bot_state("live_cash_allowance", "0.000000")

    snapshot_state = {
        "strategy_target_outcome": "Down",
        "strategy_trigger_outcome": "Down",
        "strategy_signal_side": "down",
        "strategy_selected_execution": "taker_fak",
        "strategy_bracket_phase": "abrir",
        "strategy_price_mode": "cheap-side",
        "strategy_reference_quality": "captured-chainlink",
        "strategy_reference_note": "captura local",
        "strategy_pair_sum": "1.010000",
        "strategy_expected_edge_bps": "27.4000",
        "strategy_terminal_ev_pct": "0.041200",
        "strategy_spot_delta_bps": "-8.5000",
        "strategy_cycle_budget": "25.000000",
        "strategy_budget_effective_ceiling": "25.000000",
        "strategy_current_market_exposure": "0.000000",
        "strategy_current_up_ratio": "0.500000",
        "strategy_desired_up_ratio": "0.430000",
    }

    service._record_strategy_snapshot(  # noqa: SLF001
        market=None,
        note="arb_micro no locked edge: pair sum 1.010",
        extra_state=snapshot_state,
    )
    service._record_strategy_snapshot(  # noqa: SLF001
        market=None,
        note="arb_micro no locked edge: pair sum 1.010",
        extra_state=snapshot_state,
    )

    rows = db.list_strategy_window_audit(limit=10)
    assert len(rows) == 1
    assert rows[0]["slug"] == "btc-updown-5m-1775050500"
    assert rows[0]["operability_state"] == "waiting_edge"
    payload = json.loads(rows[0]["payload_json"])
    assert payload["strategy_target_outcome"] == "Down"
    assert payload["strategy_reference_quality"] == "captured-chainlink"

    service._record_strategy_snapshot(  # noqa: SLF001
        market=None,
        note="arb_micro too late para abrir con cabeza",
        extra_state={**snapshot_state, "strategy_bracket_phase": "cerrar"},
    )

    rows = db.list_strategy_window_audit(limit=10)
    assert len(rows) == 2
    assert rows[0]["operability_state"] == "late_window"
    db.close()


def test_cheap_side_selector_accepts_strong_net_edge_even_when_raw_edge_is_below_strong_threshold(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-cheap-side-strong-net-edge"),
    )

    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.50,
        best_bid=0.40,
        best_ask_size=100.0,
        ask_levels=(AskLevel(price=0.50, size=100.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.50,
        best_bid=0.50,
        best_ask_size=100.0,
        ask_levels=(AskLevel(price=0.50, size=100.0),),
    )
    spot_context = ArbSpotContext(
        current_price=66819.0,
        reference_price=66744.5386,
        lead_price=66819.0,
        anchor_price=66744.5386,
        local_anchor_price=66744.5386,
        official_price_to_beat=66744.5386,
        anchor_source="captured-chainlink",
        fair_up=0.50,
        fair_down=0.60,
        delta_bps=10.5,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=66819.0,
        chainlink_price=66819.0,
        captured_price_to_beat=66744.5386,
        effective_price_to_beat=66744.5386,
        effective_price_source="captured-chainlink",
    )

    signal = service._select_cheap_side_target(  # noqa: SLF001
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        pair_sum=1.01,
        spot_context=spot_context,
        desired_up_ratio=0.62,
        current_up_ratio=0.5,
    )

    assert signal is not None
    assert signal.target.label == "Down"
    assert signal.net_edge > 0.08
    db.close()


def test_cheap_side_selector_accepts_micro_probe_signal_with_positive_net_edge(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=114.14),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72, live_btc5m_cycle_budget_usdc=25.0),
        logger=logging.getLogger("test-btc5m-cheap-side-micro-probe"),
    )

    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.73,
        best_bid=0.72,
        best_ask_size=100.0,
        ask_levels=(AskLevel(price=0.73, size=100.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.28,
        best_bid=0.25,
        best_ask_size=100.0,
        ask_levels=(AskLevel(price=0.28, size=100.0),),
    )
    spot_context = ArbSpotContext(
        current_price=66731.86,
        reference_price=66744.54,
        lead_price=66731.86,
        anchor_price=66744.54,
        local_anchor_price=66744.54,
        official_price_to_beat=66744.54,
        anchor_source="captured-chainlink",
        fair_up=0.70,
        fair_down=0.3023,
        delta_bps=-1.9,
        price_mode="captured-chainlink",
        source="polymarket-rtds+binance",
        age_ms=1,
        binance_price=66731.86,
        chainlink_price=66731.86,
        captured_price_to_beat=66744.54,
        effective_price_to_beat=66744.54,
        effective_price_source="captured-chainlink",
    )

    signal = service._select_cheap_side_target(  # noqa: SLF001
        mode="live",
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        pair_sum=1.01,
        spot_context=spot_context,
        desired_up_ratio=0.43,
        current_up_ratio=0.50,
    )

    assert signal is not None
    assert signal.target.label == "Down"
    assert signal.raw_edge > 0.02
    assert signal.net_edge > 0.0
    db.close()


def test_live_blocks_micro_probe_flat_cheap_side_open_in_bracket_only_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=114.14),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        shadow_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=97.72, live_btc5m_cycle_budget_usdc=25.0),
        logger=logging.getLogger("test-btc5m-live-cheap-micro-probe-block"),
    )

    up_outcome = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.73,
        best_bid=0.72,
        best_ask_size=100.0,
        ask_levels=(AskLevel(price=0.73, size=100.0),),
    )
    down_outcome = MarketOutcome(
        label="Down",
        asset_id="asset-down",
        best_ask=0.28,
        best_bid=0.25,
        best_ask_size=100.0,
        ask_levels=(AskLevel(price=0.28, size=100.0),),
    )
    signal = ArbSingleSideSignal(
        target=down_outcome,
        fair_value=0.3023,
        raw_edge=0.0223,
        net_edge=0.0013,
        edge_source="spot",
    )

    blocked, reason = service._arb_should_block_flat_single_side_open(  # noqa: SLF001
        mode="live",
        bracket_phase="abrir",
        current_up_notional=0.0,
        current_down_notional=0.0,
        signal=signal,
        pair_sum=1.01,
        cycle_budget=25.0,
        cash_balance=114.14,
        single_budget=1.05,
        seconds_into_window=40,
        up_outcome=up_outcome,
        down_outcome=down_outcome,
        fair_up=0.74,
        fair_down=0.3023,
        delta_bps=-1.9,
    )

    assert blocked is True
    assert "live opera bracket-only" in reason
    db.close()


def test_shadow_ignores_live_control_even_in_live_like_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("live_control_state", "paused")
    db.set_bot_state("live_control_reason", "dashboard pause")
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-shadow-ignore-live-control"),
    )

    allowed, note = service._live_control_can_execute(mode="shadow")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    db.close()


def test_strategy_uses_more_operable_trigger_profile(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.86"}],
                "asks": [{"price": "0.87", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.12"}],
                "asks": [{"price": "0.13", "size": "1000"}],
            },
        },
        balance=50.0,
    )
    broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_trigger_price=0.98,
            strategy_max_opposite_price=0.03,
            strategy_max_target_spread=0.02,
            strategy_max_seconds_into_window=210,
        ),
        logger=logging.getLogger("test-btc5m-strategy"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] == 1
    assert broker.instructions
    assert broker.instructions[0].asset == "asset-down"
    db.close()


def test_strategy_small_wallet_uses_minimum_viable_ticket(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.98"}],
                "asks": [{"price": "0.98", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.02"}],
                "asks": [{"price": "0.02", "size": "1000"}],
            },
        },
        balance=4.13,
    )
    broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_trigger_price=0.98,
            strategy_max_opposite_price=0.03,
            strategy_trade_allocation_pct=0.10,
            min_trade_amount=1.0,
            max_position_per_market=5.0,
            max_total_exposure=5.0,
            btc5m_reserve_enabled=True,
            btc5m_reserved_allocation_pct=1.0,
            live_btc5m_ticket_allocation_pct=1.0,
        ),
        logger=logging.getLogger("test-btc5m-strategy"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] == 1
    assert broker.instructions
    assert abs(broker.instructions[0].notional - 1.0) < 1e-9
    assert broker.instructions[0].price == 0.02
    db.close()


def test_strategy_skips_when_opposite_side_is_too_expensive(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.01"}],
                "asks": [{"price": "0.99", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.14"}],
                "asks": [{"price": "0.21", "size": "1000"}],
            },
        },
        balance=50.0,
    )
    broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-strategy"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] == 0
    assert stats["skipped"] == 1
    assert not broker.instructions
    assert "opposite too expensive" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_strategy_autonomous_exit_skips_missing_midpoint(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="stale-asset",
        condition_id="cond-stale",
        size=5.0,
        avg_price=0.4,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Old",
        slug="btc-updown-5m-old",
        outcome="Up",
        category="crypto",
    )
    db.set_bot_state("position_ledger_mode", "live")
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.98"}],
                "asks": [{"price": "0.99", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.01"}],
                "asks": [{"price": "0.01", "size": "1000"}],
            },
        },
        balance=50.0,
    )
    broker = _FakeBroker()
    exit_assets: list[str] = []

    def _build_exit_instruction(**kwargs):  # noqa: ANN003
        exit_assets.append(str(kwargs.get("asset") or ""))
        return None

    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=_build_exit_instruction),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_max_open_positions=2),
        logger=logging.getLogger("test-btc5m-strategy"),
    )

    stats = service.run(mode="live")

    assert stats["failed"] == 0
    assert broker.instructions
    assert broker.instructions[0].asset == "asset-down"
    assert "stale-asset" not in exit_assets
    db.close()


def test_strategy_logs_skip_reason_and_available_cash(tmp_path: Path, caplog) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-1",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.74"}],
                "asks": [{"price": "0.74", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.02"}],
                "asks": [{"price": "0.03", "size": "1000"}],
            },
        },
        balance=12.34,
    )
    logger = logging.getLogger("test-btc5m-strategy-log")
    caplog.set_level(logging.INFO, logger=logger.name)
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_trigger_price=0.98),
        logger=logger,
    )

    stats = service.run(mode="live")

    assert stats["skipped"] == 1
    assert "note=no trigger: richest ask 0.740 < 0.800" in caplog.text
    assert "cash_balance=12.3400" in caplog.text
    assert "available_to_trade=12.3400" in caplog.text
    db.close()


def test_vidarx_micro_builds_dual_leg_plan_in_paper(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=180)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Tilted",
        "slug": "btc-updown-5m-test",
        "conditionId": "cond-vidarx",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.72"}],
                "asks": [{"price": "0.74", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.26"}],
                "asks": [{"price": "0.28", "size": "1000"}],
            },
        },
        balance=30.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=30.0, max_position_per_market=10.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] == 2
    positions = db.list_copy_positions()
    assert len(positions) == 2
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert db.get_bot_state("strategy_plan_legs") == "2"
    assert "lidera" in str(db.get_bot_state("strategy_market_bias") or "")
    assert db.get_bot_state("strategy_price_mode") == "tilted"
    assert db.get_bot_state("strategy_timing_regime") == "mid-late"
    db.close()


def test_vidarx_micro_allows_tilted_early_mid_setup(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=70)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Tilted Early",
        "slug": "btc-updown-5m-tilted-early",
        "conditionId": "cond-tilted-early",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.72"}],
                "asks": [{"price": "0.74", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.26"}],
                "asks": [{"price": "0.28", "size": "1000"}],
            },
        },
        balance=40.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=40.0, max_position_per_market=10.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] >= 2
    assert db.get_bot_state("strategy_price_mode") == "tilted"
    assert db.get_bot_state("strategy_timing_regime") == "early-mid"
    db.close()


def test_vidarx_micro_blocks_extreme_bias_setup(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=170)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Extreme",
        "slug": "btc-updown-5m-extreme",
        "conditionId": "cond-extreme",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.79"}],
                "asks": [{"price": "0.81", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.17"}],
                "asks": [{"price": "0.19", "size": "1000"}],
            },
        },
        balance=40.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="vidarx_micro",
            bankroll=40.0,
            max_position_per_market=20.0,
            strategy_trade_allocation_pct=0.20,
        ),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert stats["skipped"] == 1
    assert "setup desactivado: extreme/mid-late" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_blocks_balanced_setup(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=75)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Balanced",
        "slug": "btc-updown-5m-balanced",
        "conditionId": "cond-balanced",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.58"}],
                "asks": [{"price": "0.60", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.38"}],
                "asks": [{"price": "0.40", "size": "1000"}],
            },
        },
        balance=40.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=40.0, max_position_per_market=10.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert stats["skipped"] == 1
    assert "setup desactivado: balanced/early-mid" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_uses_real_price_ladder_levels(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=175)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Ladder",
        "slug": "btc-updown-5m-ladder",
        "conditionId": "cond-ladder",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.70"}],
                "asks": [
                    {"price": "0.68", "size": "20"},
                    {"price": "0.70", "size": "20"},
                    {"price": "0.72", "size": "20"},
                ],
            },
            "asset-down": {
                "bids": [{"price": "0.26"}],
                "asks": [
                    {"price": "0.28", "size": "20"},
                    {"price": "0.30", "size": "20"},
                    {"price": "0.32", "size": "20"},
                ],
            },
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0, max_position_per_market=40.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    prices = {
        round(float(row["price"] or 0.0), 2)
        for row in db.get_recent_executions(limit=10)
        if str(row["asset"]) == "asset-up"
    }
    assert stats["filled"] >= 3
    assert prices >= {0.68, 0.70}
    db.close()


def test_vidarx_micro_does_not_reenter_same_market_after_initial_wave(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=170)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Repeat",
        "slug": "btc-updown-5m-repeat",
        "conditionId": "cond-repeat",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.72"}],
                "asks": [{"price": "0.74", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.26"}],
                "asks": [{"price": "0.28", "size": "1000"}],
            },
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0, max_position_per_market=60.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    first_stats = service.run(mode="paper")
    first_replenishment_count = int(db.get_bot_state("strategy_replenishment_count") or "0")
    first_exposure = db.get_total_exposure()
    second_stats = service.run(mode="paper")
    second_replenishment_count = int(db.get_bot_state("strategy_replenishment_count") or "0")
    second_exposure = db.get_total_exposure()

    assert first_stats["filled"] >= 2
    assert first_replenishment_count == 0
    assert second_stats["filled"] == 0
    assert second_replenishment_count == 0
    assert second_exposure == first_exposure
    assert "segunda oleada desactivada" in (db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_initial_ladder_does_not_count_as_replenishment(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=170)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Repeat",
        "slug": "btc-updown-5m-repeat",
        "conditionId": "cond-repeat",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.72"}],
                "asks": [{"price": "0.74", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.26"}],
                "asks": [{"price": "0.28", "size": "1000"}],
            },
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0, max_position_per_market=60.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    first_stats = service.run(mode="paper")
    first_replenishment_count = int(db.get_bot_state("strategy_replenishment_count") or "0")
    first_exposure = db.get_total_exposure()
    second_stats = service.run(mode="paper")
    second_replenishment_count = int(db.get_bot_state("strategy_replenishment_count") or "0")
    second_exposure = db.get_total_exposure()

    assert first_stats["filled"] >= 2
    assert first_replenishment_count == 0
    assert second_stats["filled"] == 0
    assert second_replenishment_count == 0
    assert second_exposure == first_exposure
    assert "segunda oleada desactivada" in (db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_cycle_budget_ignores_market_cap_setting(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="vidarx_micro",
            bankroll=100.0,
            strategy_trade_allocation_pct=0.20,
            max_position_per_market=1.0,
        ),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    budget = service._target_vidarx_cycle_budget(
        cash_balance=100.0,
        effective_bankroll=100.0,
        current_total_exposure=0.0,
        existing_market_notional=0.0,
        timing_regime="mid-late",
        price_mode="extreme",
    )

    assert budget > 1.0
    db.close()


def test_vidarx_micro_disables_second_wave_when_market_already_open(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=133)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Second Wave",
        "slug": "btc-updown-5m-wave",
        "conditionId": "cond-wave",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-wave",
        size=8.0,
        avg_price=0.8,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Second Wave",
        slug="btc-updown-5m-wave",
        outcome="Up",
        category="crypto",
    )
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.79"}],
                "asks": [
                    {"price": "0.77", "size": "20"},
                    {"price": "0.79", "size": "20"},
                    {"price": "0.81", "size": "20"},
                ],
            },
            "asset-down": {
                "bids": [{"price": "0.17"}],
                "asks": [
                    {"price": "0.17", "size": "20"},
                    {"price": "0.19", "size": "20"},
                    {"price": "0.21", "size": "20"},
                ],
            },
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    plan = service._build_vidarx_plan(
        market=market,
        cash_balance=92.0,
        effective_bankroll=100.0,
        current_total_exposure=db.get_total_exposure(),
    )

    assert plan is None
    assert "segunda oleada desactivada" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_records_window_results(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=170)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Recorded",
        "slug": "btc-updown-5m-recorded",
        "conditionId": "cond-recorded",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    closing_market = dict(market)
    closing_market["closed"] = True
    closing_market["outcomePrices"] = "[\"1\", \"0\"]"
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.79"}],
                "asks": [{"price": "0.81", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.17"}],
                "asks": [{"price": "0.19", "size": "1000"}],
            },
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(
            {
                "btc-updown-5m-recorded": closing_market,
                "btc-updown-5m-1770000000": market,
            }
        ),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    db.upsert_strategy_window(
        slug="btc-updown-5m-recorded",
        condition_id="cond-recorded",
        title="Bitcoin Up or Down - Recorded",
        price_mode="extreme",
        timing_regime="second-wave",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.8,
        planned_budget=10.0,
        current_exposure=10.0,
        notes="existing window",
    )
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-recorded",
        size=5.0,
        avg_price=0.8,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Recorded",
        slug="btc-updown-5m-recorded",
        outcome="Up",
        category="crypto",
    )

    service._settle_resolved_paper_positions({"pending": 0, "filled": 0, "blocked": 0, "failed": 0, "skipped": 0, "opportunities": 0})

    row = db.conn.execute("SELECT status, realized_pnl, winning_outcome FROM strategy_windows WHERE slug = ?", ("btc-updown-5m-recorded",)).fetchone()
    assert row is not None
    assert str(row["status"]) == "closed"
    assert abs(float(row["realized_pnl"]) - 1.0) < 1e-9
    assert str(row["winning_outcome"]) == "Up"
    db.close()


def test_settle_resolved_positions_closes_resolved_window_without_active_positions(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Empty",
        "slug": "btc-updown-5m-empty",
        "conditionId": "cond-empty",
        "closed": True,
        "acceptingOrders": False,
        "outcomes": "[\"Up\", \"Down\"]",
        "outcomePrices": "[\"1\", \"0\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-empty-window-cleanup"),
    )

    db.upsert_strategy_window(
        slug="btc-updown-5m-empty",
        condition_id="cond-empty",
        title="Bitcoin Up or Down - Empty",
        price_mode="extreme",
        timing_regime="second-wave",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.8,
        planned_budget=10.0,
        current_exposure=0.0,
        notes="opened but never filled",
    )

    stats = {"pending": 0, "filled": 0, "blocked": 0, "failed": 0, "skipped": 0, "opportunities": 0}
    service._settle_resolved_paper_positions(stats)

    row = db.conn.execute(
        "SELECT status, realized_pnl, winning_outcome, notes FROM strategy_windows WHERE slug = ?",
        ("btc-updown-5m-empty",),
    ).fetchone()
    assert row is not None
    assert str(row["status"]) == "closed"
    assert abs(float(row["realized_pnl"]) - 0.0) < 1e-9
    assert str(row["winning_outcome"]) == "Up"
    assert "no active positions" in str(row["notes"])
    assert stats["filled"] == 0
    assert stats["opportunities"] == 0
    db.close()


def test_settle_resolved_positions_keeps_open_window_without_active_positions_if_market_open(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    market = {
        "question": "Bitcoin Up or Down - Pending",
        "slug": "btc-updown-5m-pending",
        "conditionId": "cond-pending",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "outcomePrices": "[\"0.55\", \"0.45\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-open-window-preserved"),
    )

    db.upsert_strategy_window(
        slug="btc-updown-5m-pending",
        condition_id="cond-pending",
        title="Bitcoin Up or Down - Pending",
        price_mode="extreme",
        timing_regime="second-wave",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.8,
        planned_budget=10.0,
        current_exposure=0.0,
        notes="opened but waiting",
    )

    service._settle_resolved_paper_positions(
        {"pending": 0, "filled": 0, "blocked": 0, "failed": 0, "skipped": 0, "opportunities": 0}
    )

    row = db.conn.execute(
        "SELECT status, realized_pnl FROM strategy_windows WHERE slug = ?",
        ("btc-updown-5m-pending",),
    ).fetchone()
    assert row is not None
    assert str(row["status"]) == "open"
    assert abs(float(row["realized_pnl"]) - 0.0) < 1e-9
    db.close()


def test_vidarx_micro_stops_after_25pct_drawdown(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.add_daily_pnl(datetime.now(timezone.utc).date().isoformat(), -30.0)
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["blocked"] == 1
    assert "drawdown stop" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_drawdown_uses_mark_to_market_equity(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-open-loss",
        size=50.0,
        avg_price=1.0,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Open Loss",
        slug="btc-updown-5m-open-loss",
        outcome="Up",
        category="crypto",
    )
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "1000"}],
            }
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["blocked"] == 1
    assert db.get_bot_state("live_total_capital") == "70.00000000"
    assert db.get_bot_state("live_marked_exposure") == "20.00000000"
    assert db.get_bot_state("live_unrealized_pnl") == "-30.00000000"
    assert "drawdown stop" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_cycle_budget_is_paced_by_equity_and_regime(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="vidarx_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.50,
        ),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    budget = service._target_vidarx_cycle_budget(
        cash_balance=1000.0,
        effective_bankroll=1000.0,
        current_total_exposure=0.0,
        existing_market_notional=400.0,
        timing_regime="second-wave",
        price_mode="extreme",
    )

    assert budget <= 90.0
    db.close()


def test_vidarx_micro_blocks_setup_with_negative_history(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    for index in range(4):
        slug = f"btc-updown-5m-bad-{index}"
        db.upsert_strategy_window(
            slug=slug,
            condition_id=f"cond-bad-{index}",
            title=f"Bitcoin Up or Down - Bad {index}",
            price_mode="balanced",
            timing_regime="early-mid",
            primary_outcome="Up",
            hedge_outcome="Down",
            primary_ratio=0.55,
            planned_budget=20.0,
            current_exposure=20.0,
            notes="bad setup",
        )
        db.close_strategy_window(
            slug=slug,
            realized_pnl=-5.0,
            winning_outcome="Down",
            current_exposure=0.0,
            notes="resolved bad",
        )

    start_time = (datetime.now(timezone.utc) - timedelta(seconds=70)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Balanced",
        "slug": "btc-updown-5m-balanced-blocked",
        "conditionId": "cond-balanced-blocked",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.58"}],
                "asks": [{"price": "0.60", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.38"}],
                "asks": [{"price": "0.40", "size": "1000"}],
            },
        },
        balance=100.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert stats["skipped"] == 1
    assert "setup desactivado: balanced/early-mid" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_vidarx_micro_settles_closed_market_in_paper(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-old",
        size=10.0,
        avg_price=0.4,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Old",
        slug="btc-updown-5m-old",
        outcome="Up",
        category="crypto",
    )
    gamma = _FakeGammaClient(
        {
            "btc-updown-5m-old": {
                "question": "Bitcoin Up or Down - Old",
                "conditionId": "cond-old",
                "closed": True,
                "acceptingOrders": False,
                "outcomes": "[\"Up\", \"Down\"]",
                "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
                "outcomePrices": "[\"1\", \"0\"]",
                "events": [{"startTime": "2026-03-11T10:00:00Z"}],
            }
        }
    )
    clob = _FakeCLOBClient(books={}, balance=25.0)
    service = BTC5mStrategyService(
        db,
        gamma,
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=25.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] >= 1
    assert db.get_copy_position("asset-up") is None
    executions = db.get_recent_executions(limit=5)
    assert any(str(row["notes"]).startswith("strategy_resolution:") for row in executions)
    assert db.get_cumulative_pnl() == 6.0
    db.close()


def test_vidarx_micro_settlement_network_error_does_not_crash(tmp_path: Path, caplog) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-old",
        size=5.0,
        avg_price=0.8,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Old",
        slug="btc-updown-5m-old",
        outcome="Up",
        category="crypto",
    )
    service = BTC5mStrategyService(
        db,
        _FailingGammaClient(),
        _FakeCLOBClient(books={}, balance=25.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=25.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    caplog.set_level(logging.WARNING)
    service._settle_resolved_paper_positions({"pending": 0, "filled": 0, "blocked": 0, "failed": 0, "skipped": 0, "opportunities": 0})

    assert db.get_copy_position("asset-up") is not None
    assert "market lookup failed" in caplog.text
    db.close()


def test_vidarx_micro_refuses_live_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=20.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=20.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="live")

    assert stats["blocked"] == 1
    assert "paper-only" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_buys_both_sides_on_underround(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Test",
        "slug": "btc-updown-5m-arb",
        "conditionId": "cond-arb",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.38"}],
                "asks": [{"price": "0.40", "size": "200"}, {"price": "0.41", "size": "200"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "200"}, {"price": "0.56", "size": "200"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] >= 4
    positions = db.list_copy_positions()
    assert len(positions) == 2
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert db.get_bot_state("strategy_price_mode") == "underround"
    assert float(db.get_bot_state("strategy_terminal_ev_pct") or 0.0) > 0.0
    assert "underround" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_blocks_underround_when_terminal_ev_does_not_cover_fees(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Terminal EV Gate",
        "slug": "btc-updown-5m-terminal-ev-gate",
        "conditionId": "cond-arb-terminal-ev-gate",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FeeAwareCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.47"}],
                "asks": [{"price": "0.49", "size": "200"}, {"price": "0.495", "size": "200"}],
            },
            "asset-down": {
                "bids": [{"price": "0.49"}],
                "asks": [{"price": "0.499", "size": "200"}, {"price": "0.50", "size": "200"}],
            },
        },
        fee_bps=10000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-terminal-ev-gate"),
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert db.list_copy_positions() == []
    assert "EV terminal de pareja" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_relaxes_underround_gate_near_parity_when_net_edge_is_still_good(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Relaxed Underround",
        "slug": "btc-updown-5m-relaxed-underround",
        "conditionId": "cond-arb-relaxed-underround",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.47"}],
                "asks": [{"price": "0.49", "size": "200"}, {"price": "0.495", "size": "200"}],
            },
            "asset-down": {
                "bids": [{"price": "0.49"}],
                "asks": [{"price": "0.499", "size": "200"}, {"price": "0.50", "size": "200"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-relaxed-underround"),
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert len(positions) == 2
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert db.get_bot_state("strategy_price_mode") == "underround"
    assert "underround" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_does_not_buy_pair_above_one_without_edge(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - No Edge Pair",
        "slug": "btc-updown-5m-no-edge-pair",
        "conditionId": "cond-arb-no-edge",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.44"}],
                "asks": [{"price": "0.45", "size": "200"}],
            },
            "asset-down": {
                "bids": [{"price": "0.55"}],
                "asks": [{"price": "0.56", "size": "200"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-no-edge"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert db.list_copy_positions() == []
    assert "no locked edge" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_live_preflight_blocks_paper_ledger(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-preflight",
        size=5.0,
        avg_price=0.40,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Preflight",
        slug="btc-updown-5m-preflight",
        outcome="Up",
        category="crypto",
    )
    db.set_bot_state("position_ledger_mode", "paper")
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            live_small_target_capital=100.0,
        ),
        logger=logging.getLogger("test-btc5m-arb"),
    )

    stats = service.run(mode="live")

    assert stats["blocked"] == 1
    assert "live preflight" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_live_scales_to_live_small_target_capital(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Small",
        "slug": "btc-updown-5m-live-small",
        "conditionId": "cond-live-small",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.38"}],
                "asks": [{"price": "0.40", "size": "200"}, {"price": "0.41", "size": "200"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "200"}, {"price": "0.56", "size": "200"}],
            },
        },
        balance=100.0,
    )
    live_broker = _ApplyingLiveBroker(db)
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=live_broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            strategy_trade_allocation_pct=0.03,
            live_small_target_capital=100.0,
            live_btc5m_ticket_allocation_pct=0.25,
            profit_keep_ratio=0.0,
        ),
        logger=logging.getLogger("test-btc5m-arb-live-small"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] >= 6
    assert live_broker.instructions
    total_notional = sum(float(item.notional) for item in live_broker.instructions)
    assert total_notional <= 25.05
    assert total_notional >= 8.0
    assert db.get_bot_state("strategy_capital_target") == "100.00000000"
    assert round(float(db.get_bot_state("strategy_capital_scale_ratio") or 0.0), 4) == 0.01
    assert db.get_bot_state("strategy_price_mode") == "underround"
    assert "paper-only" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_live_cycle_budget_target_uses_absolute_usdc_cap_when_configured(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=111.26),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=111.26,
            live_btc5m_ticket_allocation_pct=0.25,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-cycle-budget"),
    )

    budget = service._live_cycle_budget_target(mode="live", live_total_capital=111.26)  # noqa: SLF001
    market_cap = service._arb_market_exposure_cap(  # noqa: SLF001
        mode="live",
        effective_bankroll=111.26,
        live_total_capital=111.26,
    )
    total_cap = service._arb_total_exposure_cap(  # noqa: SLF001
        mode="live",
        effective_bankroll=111.26,
        live_total_capital=111.26,
    )
    cap_mode = service._arb_exposure_cap_mode(  # noqa: SLF001
        mode="live",
        effective_bankroll=111.26,
        live_total_capital=111.26,
    )

    assert round(budget, 2) == 25.00
    assert round(market_cap, 2) == 25.00
    assert round(total_cap, 2) == 31.25
    assert cap_mode == "fixed-cycle"
    db.close()


def test_live_exposure_caps_switch_to_percent_after_compounding(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=111.26,
            live_btc5m_ticket_allocation_pct=0.25,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-percent-caps"),
    )

    budget = service._live_cycle_budget_target(mode="live", live_total_capital=1000.0)  # noqa: SLF001
    market_cap = service._arb_market_exposure_cap(  # noqa: SLF001
        mode="live",
        effective_bankroll=1000.0,
        live_total_capital=1000.0,
    )
    total_cap = service._arb_total_exposure_cap(  # noqa: SLF001
        mode="live",
        effective_bankroll=1000.0,
        live_total_capital=1000.0,
    )
    cap_mode = service._arb_exposure_cap_mode(  # noqa: SLF001
        mode="live",
        effective_bankroll=1000.0,
        live_total_capital=1000.0,
    )

    assert round(budget, 2) == 25.00
    assert round(market_cap, 2) == 50.00
    assert round(total_cap, 2) == 200.00
    assert cap_mode == "percent-after-compounding"
    db.close()


def test_live_exposure_caps_keep_fixed_cycle_for_tiny_profit_above_target(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_ticket_allocation_pct=0.25,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-percent-caps-tolerance"),
    )

    cap_mode = service._arb_exposure_cap_mode(  # noqa: SLF001
        mode="live",
        effective_bankroll=97.724329,
        live_total_capital=97.724329,
    )

    assert cap_mode == "fixed-cycle"
    db.close()


def test_live_exposure_caps_keep_fixed_cycle_until_one_cycle_budget_above_target(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_ticket_allocation_pct=0.25,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-fixed-cycle-buffer"),
    )

    market_cap = service._arb_market_exposure_cap(  # noqa: SLF001
        mode="live",
        effective_bankroll=99.9994,
        live_total_capital=99.9994,
    )
    cap_mode = service._arb_exposure_cap_mode(  # noqa: SLF001
        mode="live",
        effective_bankroll=99.9994,
        live_total_capital=99.9994,
    )

    assert cap_mode == "fixed-cycle"
    assert round(market_cap, 2) == 25.00
    db.close()


def test_arb_micro_paused_live_refreshes_fixed_cycle_caps_in_bot_state(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("live_control_state", "paused")
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=114.14),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_ticket_allocation_pct=0.25,
            live_btc5m_cycle_budget_usdc=25.0,
            live_control_default_state="paused",
        ),
        logger=logging.getLogger("test-btc5m-live-paused-cap-snapshot"),
    )

    stats = service.run(mode="live")

    assert stats["blocked"] == 1
    assert round(float(db.get_bot_state("strategy_market_exposure_cap") or 0.0), 2) == 25.00
    assert round(float(db.get_bot_state("strategy_total_exposure_cap") or 0.0), 2) == 31.25
    assert round(float(db.get_bot_state("strategy_budget_effective_ceiling") or 0.0), 2) == 25.00
    assert db.get_bot_state("strategy_exposure_cap_mode") == "fixed-cycle"
    db.close()


def test_live_small_drawdown_floor_uses_absolute_max_total_loss(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=100.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=100.0,
            live_small_max_total_loss=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-drawdown-floor"),
    )

    floor = service._mode_drawdown_floor(mode="live", live_total_capital=111.54)  # noqa: SLF001

    assert round(floor, 2) == 75.00
    db.close()


def test_live_small_drawdown_floor_uses_percent_of_capital_when_configured(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=111.26),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=111.26,
            live_small_max_total_loss=0.0,
            live_small_max_drawdown_pct=0.25,
        ),
        logger=logging.getLogger("test-btc5m-live-drawdown-pct"),
    )

    floor = service._mode_drawdown_floor(mode="live", live_total_capital=111.54)  # noqa: SLF001

    assert round(floor, 2) == 83.45
    db.close()


def test_arb_micro_opens_controlled_cheap_side_when_spot_confirms_bias(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-cheap"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Cheap Side",
        "slug": slug,
        "conditionId": "cond-arb-cheap",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.43"}],
                "asks": [{"price": "0.44", "size": "150"}, {"price": "0.45", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.56"}],
                "asks": [{"price": "0.57", "size": "150"}, {"price": "0.58", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=69750.0,
            lead_price=69750.0,
            binance_price=69750.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=5,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-cheap"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert len(positions) >= 1
    assert {str(row["outcome"]) for row in positions} == {"Down"}
    assert db.get_bot_state("strategy_price_mode") == "cheap-side"
    assert "cheap Down" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_opens_cheap_side_on_small_positive_delta_and_pair_sum_102(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-cheap-up"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=70)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Cheap Side Up",
        "slug": slug,
        "conditionId": "cond-arb-cheap-up",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.48"}],
                "asks": [{"price": "0.49", "size": "150"}, {"price": "0.50", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.52"}],
                "asks": [{"price": "0.53", "size": "150"}, {"price": "0.54", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70056.0,
            lead_price=70056.0,
            binance_price=70056.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=5,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-cheap-up"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert len(positions) >= 1
    assert {str(row["outcome"]) for row in positions} == {"Up"}
    assert db.get_bot_state("strategy_price_mode") == "cheap-side"
    assert "cheap Up" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_opens_cheap_side_on_soft_positive_delta_with_net_edge(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-soft-delta-edge"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=85)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Soft Delta Edge",
        "slug": slug,
        "conditionId": "cond-arb-soft-delta-edge",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.48"}],
                "asks": [{"price": "0.49", "size": "150"}, {"price": "0.50", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.45"}],
                "asks": [{"price": "0.52", "size": "150"}, {"price": "0.53", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70012.6,
            lead_price=70012.6,
            binance_price=70012.6,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=6,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-soft-delta-edge"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert positions
    assert {str(row["outcome"]) for row in positions} == {"Up"}
    assert db.get_bot_state("strategy_price_mode") == "cheap-side"
    assert "cheap Up" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_shadow_live_like_blocks_flat_cheap_side_open(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-shadow-cheap-block"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=85)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Shadow Cheap Block",
        "slug": slug,
        "conditionId": "cond-arb-shadow-cheap-block",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.48"}],
                "asks": [{"price": "0.49", "size": "150"}, {"price": "0.50", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.45"}],
                "asks": [{"price": "0.52", "size": "150"}, {"price": "0.53", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70012.6,
            lead_price=70012.6,
            binance_price=70012.6,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=6,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.05,
            shadow_live_like_mode=True,
        ),
        logger=logging.getLogger("test-btc5m-shadow-cheap-block"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="shadow")

    assert stats["filled"] == 0
    assert db.list_copy_positions() == []
    assert "cheap-side bloqueado en live-like" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_shadow_live_like_allows_strong_flat_cheap_side_open(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-shadow-cheap-allow"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Shadow Cheap Allow",
        "slug": slug,
        "conditionId": "cond-arb-shadow-cheap-allow",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.43"}],
                "asks": [{"price": "0.44", "size": "150"}, {"price": "0.45", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.56"}],
                "asks": [{"price": "0.57", "size": "150"}, {"price": "0.58", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=69750.0,
            lead_price=69750.0,
            binance_price=69750.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=5,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.05,
            shadow_live_like_mode=True,
        ),
        logger=logging.getLogger("test-btc5m-shadow-cheap-allow"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="shadow")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert positions
    assert {str(row["outcome"]) for row in positions} == {"Down"}
    assert db.get_bot_state("strategy_price_mode") == "cheap-side"
    db.close()


def test_shadow_live_like_blocks_cheap_side_when_hedge_leg_has_no_viable_book(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-shadow-cheap-hedge-book-block"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Shadow Cheap Hedge Book Block",
        "slug": slug,
        "conditionId": "cond-arb-shadow-cheap-hedge-book-block",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.43"}],
                "asks": [
                    {"price": "0.44", "size": "0.4"},
                    {"price": "0.49", "size": "150"},
                ],
            },
            "asset-down": {
                "bids": [{"price": "0.56"}],
                "asks": [{"price": "0.57", "size": "150"}, {"price": "0.58", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=69750.0,
            lead_price=69750.0,
            binance_price=69750.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=5,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.05,
            shadow_live_like_mode=True,
        ),
        logger=logging.getLogger("test-btc5m-shadow-cheap-hedge-book-block"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="shadow")

    assert stats["filled"] == 0
    assert db.list_copy_positions() == []
    assert "segunda pata Up sin tamano minimo operable en libro" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_skips_soft_delta_when_net_edge_is_too_thin(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-soft-delta-thin"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=85)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Soft Delta Thin Edge",
        "slug": slug,
        "conditionId": "cond-arb-soft-delta-thin",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.48"}],
                "asks": [{"price": "0.49", "size": "150"}, {"price": "0.50", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.46"}],
                "asks": [{"price": "0.52", "size": "150"}, {"price": "0.53", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70012.6,
            lead_price=70012.6,
            binance_price=70012.6,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=6,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-soft-delta-thin"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert db.list_copy_positions() == []
    assert "no locked edge" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_caps_market_exposure_and_cools_down_same_window(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=50)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Market Cap",
        "slug": "btc-updown-5m-cap",
        "conditionId": "cond-arb-cap",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "300"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.10),
        logger=logging.getLogger("test-btc5m-arb-cap"),
    )

    first = service.run(mode="paper")
    first_exposure = db.get_total_exposure()
    first_note = str(db.get_bot_state("strategy_last_note") or "")

    second = service.run(mode="paper")
    second_exposure = db.get_total_exposure()
    second_note = str(db.get_bot_state("strategy_last_note") or "")

    assert first["filled"] > 0
    assert first_exposure <= 50.0
    assert second["filled"] == 0
    assert second_exposure <= 50.0
    assert "underround" in first_note
    assert "cooldown" in second_note or "market cap exhausted" in second_note or "budget below minimum" in second_note
    db.close()


def test_arb_micro_live_pending_orders_reserve_market_cap_before_fill(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=50)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Pending Cap",
        "slug": "btc-updown-5m-pending-cap",
        "conditionId": "cond-arb-pending-cap",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    db.set_bot_state(
        "live_pending_order:pending-cap",
        json.dumps(
            {
                "order_id": "pending-cap",
                "action": "open",
                "side": "buy",
                "asset": "asset-down",
                "condition_id": market["conditionId"],
                "size": 45.4545,
                "price": 0.55,
                "notional": 24.999975,
                "source_wallet": "strategy-live",
                "source_signal_id": 0,
                "title": market["question"],
                "slug": market["slug"],
                "outcome": "Down",
                "reason": "fase abrir",
                "execution_profile": "taker_fak",
                "response_status": "live",
                "submitted_at": int(datetime.now(timezone.utc).timestamp()),
            },
            separators=(",", ":"),
        ),
    )
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "300"}],
            },
        },
        balance=100.0,
        feed_mode="websocket",
        feed_connected=True,
    )
    broker = _FakeBroker()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            live_small_target_capital=97.72,
            live_btc5m_ticket_allocation_pct=0.25,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-pending-cap"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] == 0
    assert broker.instructions == []
    assert any(
        fragment in str(db.get_bot_state("strategy_last_note") or "")
        for fragment in ("market cap exhausted", "budget below minimum after caps")
    )
    assert float(db.get_bot_state("strategy_total_exposure") or 0.0) >= 24.99
    db.close()


def test_arb_micro_does_not_stop_only_because_window_already_has_many_fills(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=50)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Fill Capacity",
        "slug": "btc-updown-5m-fill-cap",
        "conditionId": "cond-arb-fill-cap",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    db.upsert_strategy_window(
        slug=market["slug"],
        condition_id=market["conditionId"],
        title=market["question"],
        price_mode="underround",
        timing_regime="early-mid",
        primary_outcome="Up",
        hedge_outcome="Down",
        primary_ratio=0.5,
        planned_budget=20.0,
        current_exposure=0.0,
        notes="seed",
    )
    db.record_strategy_window_fills(
        slug=market["slug"],
        fill_count=24,
        added_notional=0.0,
        replenishment_count=0,
        notes="seed fills",
    )
    with db.conn:
        db.conn.execute(
            "UPDATE strategy_windows SET last_trade_at = 0, first_trade_at = 0 WHERE slug = ?",
            (market["slug"],),
        )
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "300"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.10),
        logger=logging.getLogger("test-btc5m-arb-fill-cap"),
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    row = db.get_strategy_window(market["slug"])
    assert row is not None
    assert int(row["filled_orders"] or 0) > 24
    db.close()


def test_arb_micro_keeps_trading_new_window_when_old_window_is_still_open(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    now = datetime.now(timezone.utc)
    current_start = now - timedelta(seconds=50)
    old_slug = f"btc-updown-5m-{int((current_start - timedelta(seconds=300)).timestamp())}"
    current_slug = f"btc-updown-5m-{int(current_start.timestamp())}"
    current_market = {
        "question": "Bitcoin Up or Down - Current Window",
        "slug": current_slug,
        "conditionId": "cond-arb-current-window",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up-current\", \"asset-down-current\"]",
        "events": [{"startTime": current_start.isoformat().replace("+00:00", "Z")}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up-old": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "300"}],
            },
            "asset-down-old": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "300"}],
            },
            "asset-up-current": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "300"}],
            },
            "asset-down-current": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "300"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(
            {
                old_slug: {
                    "question": "Bitcoin Up or Down - Previous Window",
                    "slug": old_slug,
                    "conditionId": "cond-arb-old-window",
                    "closed": False,
                    "acceptingOrders": True,
                    "outcomes": "[\"Up\", \"Down\"]",
                    "clobTokenIds": "[\"asset-up-old\", \"asset-down-old\"]",
                    "events": [{"startTime": (current_start - timedelta(seconds=300)).isoformat().replace("+00:00", "Z")}],
                },
                current_slug: current_market,
            }
        ),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.10,
            strategy_max_open_positions=1,
        ),
        logger=logging.getLogger("test-btc5m-arb-window-carry"),
    )
    seed = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-down-old",
            condition_id="cond-arb-old-window",
            size=90.0,
            price=0.55,
            notional=49.5,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title="Previous Window Carry",
            slug=old_slug,
            outcome="Down",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: current_market  # type: ignore[method-assign]
    second = service.run(mode="paper")

    assert seed.status == "filled"
    assert second["filled"] > 0
    positions = db.list_copy_positions()
    assert {str(row["condition_id"]) for row in positions} == {
        "cond-arb-old-window",
        "cond-arb-current-window",
    }
    assert "concurrent market limit reached" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_reduces_new_cycle_budget_when_previous_window_carry_exists(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=55)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Carry Budget",
        "slug": "btc-updown-5m-1773527700",
        "conditionId": "cond-arb-carry-budget",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "300"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-carry-budget"),
    )

    base_target_budget = service._target_arb_cycle_budget(  # noqa: SLF001
        mode="paper",
        cash_balance=1000.0,
        effective_bankroll=1000.0,
        current_total_exposure=0.0,
        timing_regime="early-mid",
        carry_exposure=0.0,
    )
    carry_target_budget = service._target_arb_cycle_budget(  # noqa: SLF001
        mode="paper",
        cash_balance=1000.0,
        effective_bankroll=1000.0,
        current_total_exposure=60.0,
        timing_regime="early-mid",
        carry_exposure=60.0,
    )

    base_plan = service._build_arb_micro_plan(  # noqa: SLF001
        mode="paper",
        market=market,
        cash_balance=1000.0,
        effective_bankroll=1000.0,
        current_total_exposure=0.0,
        carry_exposure=0.0,
        carry_window_count=0,
    )
    carry_plan = service._build_arb_micro_plan(  # noqa: SLF001
        mode="paper",
        market=market,
        cash_balance=1000.0,
        effective_bankroll=1000.0,
        current_total_exposure=60.0,
        carry_exposure=60.0,
        carry_window_count=1,
    )

    assert base_plan is not None
    assert carry_plan is not None
    assert carry_target_budget < base_target_budget
    assert "carry previo" in carry_plan.note
    db.close()


def test_arb_micro_does_not_keep_buying_same_cheap_side_when_bracket_is_far_off_target(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773529920"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=170)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Wrong Cheap Side",
        "slug": slug,
        "conditionId": "cond-arb-wrong-cheap-side",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.16"}],
                "asks": [{"price": "0.18", "size": "400"}, {"price": "0.19", "size": "400"}],
            },
            "asset-down": {
                "bids": [{"price": "0.82"}],
                "asks": [{"price": "0.84", "size": "400"}, {"price": "0.85", "size": "400"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70942.58,
            lead_price=70942.58,
            binance_price=70942.58,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=8,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-wrong-cheap-side"),
        spot_feed=spot_feed,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "71017.17000000")
    seeded = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id=market["conditionId"],
            size=111.1111,
            price=0.18,
            notional=19.999998,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title=market["question"],
            slug=slug,
            outcome="Up",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert seeded.status == "filled"
    assert stats["filled"] == 0
    positions = db.list_copy_positions()
    assert {str(row["outcome"]) for row in positions} == {"Up"}
    assert "repair Down" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_outcome_exposures_use_committed_cost_basis_for_rebalance(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-committed-basis"),
    )
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-committed-basis",
        size=4.8571,
        avg_price=0.35,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Committed Basis",
        slug="btc-updown-5m-committed-basis",
        outcome="Up",
        category="crypto",
    )
    db.upsert_copy_position(
        asset="asset-down",
        condition_id="cond-committed-basis",
        size=133.6,
        avg_price=0.13,
        realized_pnl=0.0,
        title="Bitcoin Up or Down - Committed Basis",
        slug="btc-updown-5m-committed-basis",
        outcome="Down",
        category="crypto",
    )

    committed_up, committed_down = service._get_condition_outcome_exposures(  # noqa: SLF001
        "cond-committed-basis",
        price_marks={"asset-up": 0.995, "asset-down": 0.005},
        basis="committed",
    )
    mark_up, mark_down = service._get_condition_outcome_exposures(  # noqa: SLF001
        "cond-committed-basis",
        price_marks={"asset-up": 0.995, "asset-down": 0.005},
        basis="mark",
    )

    assert round(committed_up, 2) == 1.70
    assert round(committed_down, 2) == 17.37
    assert round(mark_up, 2) == 4.83
    assert round(mark_down, 2) == 0.67
    assert committed_down > committed_up
    assert mark_up > mark_down
    db.close()


def test_arb_micro_late_directional_regime_does_not_repair_wrong_side(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-late-directional"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=240)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Late Directional",
        "slug": slug,
        "conditionId": "cond-arb-late-directional",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.92"}],
                "asks": [{"price": "0.94", "size": "300"}],
            },
            "asset-down": {
                "bids": [{"price": "0.05"}],
                "asks": [{"price": "0.06", "size": "300"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70120.0,
            lead_price=70120.0,
            binance_price=70120.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=10,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-late-directional"),
        spot_feed=spot_feed,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    seeded = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id=market["conditionId"],
            size=10.6382,
            price=0.94,
            notional=9.999908,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title=market["question"],
            slug=slug,
            outcome="Up",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert seeded.status == "filled"
    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert {str(row["outcome"]) for row in positions} == {"Up"}
    assert "repair Down" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_market_discovery_network_error_does_not_crash(tmp_path: Path, caplog) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FailingGammaClient(),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0),
        logger=logging.getLogger("test-btc5m-arb-discovery-dns"),
    )

    caplog.set_level(logging.WARNING)
    stats = service.run(mode="paper")

    assert stats["skipped"] == 1
    assert "no active btc5m market" in str(db.get_bot_state("strategy_last_note") or "")
    assert "market lookup failed" in caplog.text
    db.close()


def test_arb_micro_does_not_repair_underweight_leg_without_positive_edge(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-repair"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Repair Bracket",
        "slug": slug,
        "conditionId": "cond-arb-repair",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.16"}],
                "asks": [{"price": "0.18", "size": "500"}],
            },
            "asset-down": {
                "bids": [{"price": "0.83"}],
                "asks": [{"price": "0.84", "size": "500"}, {"price": "0.85", "size": "500"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70942.58,
            lead_price=70942.58,
            binance_price=70942.58,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=8,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-repair"),
        spot_feed=spot_feed,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "71017.17000000")
    seeded = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id=market["conditionId"],
            size=111.1111,
            price=0.18,
            notional=19.999998,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title=market["question"],
            slug=slug,
            outcome="Up",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert seeded.status == "filled"
    assert stats["filled"] == 0
    positions = db.list_copy_positions()
    assert {str(row["outcome"]) for row in positions} == {"Up"}
    assert "repair Down" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_live_repair_skips_when_terminal_ev_is_too_thin(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Repair EV Guard",
        "slug": "btc-updown-5m-repair-ev-guard",
        "conditionId": "cond-arb-repair-ev-guard",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.94"}],
                    "asks": [{"price": "0.95", "size": "500"}],
                },
                "asset-down": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.04"}],
                    "asks": [{"price": "0.05", "size": "500"}],
                },
            },
            balance=114.14,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-repair-ev-guard"),
    )

    plan = service._build_arb_repair_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=MarketOutcome(
            label="Up",
            asset_id="asset-up",
            best_ask=0.95,
            best_bid=0.94,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.95, size=500.0),),
        ),
        down_outcome=MarketOutcome(
            label="Down",
            asset_id="asset-down",
            best_ask=0.05,
            best_bid=0.04,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.05, size=500.0),),
        ),
        pair_sum=1.0,
        fair_up=0.952,
        fair_down=0.048,
        up_net_edge=0.012,
        down_net_edge=-0.02,
        desired_up_ratio=0.80,
        current_up_ratio=0.0,
        timing_regime="mid-late",
        cycle_budget=25.0,
        cash_balance=114.14,
        remaining_instruction_capacity=12,
        current_up_notional=0.0,
        current_down_notional=20.0,
        spot_context=ArbSpotContext(
            current_price=67600.0,
            reference_price=67600.0,
            lead_price=67600.0,
            anchor_price=67400.0,
            local_anchor_price=67400.0,
            official_price_to_beat=0.0,
            anchor_source="polymarket-rtds-anchor",
            fair_up=0.952,
            fair_down=0.048,
            delta_bps=12.0,
            price_mode="reference",
            source="polymarket-rtds+binance",
            age_ms=10,
            binance_price=67600.0,
            chainlink_price=67400.0,
        ),
        bracket_phase="redistribuir",
    )

    assert plan is None
    db.close()


def test_arb_micro_stabilizes_extreme_one_sided_inventory_when_spot_aligns(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-stabilize"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Stabilize Bracket",
        "slug": slug,
        "conditionId": "cond-arb-stabilize",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "500"}],
            },
            "asset-down": {
                "bids": [{"price": "0.60"}],
                "asks": [{"price": "0.61", "size": "500"}, {"price": "0.62", "size": "500"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70992.0,
            lead_price=70992.0,
            binance_price=70992.0,
            chainlink_price=71017.17,
            basis=0.0,
            source="polymarket-rtds+binance",
            age_ms=18,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-stabilize"),
        spot_feed=spot_feed,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "71017.17000000")
    seeded = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id=market["conditionId"],
            size=50.0,
            price=0.40,
            notional=20.0,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title=market["question"],
            slug=slug,
            outcome="Up",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert seeded.status == "filled"
    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert db.get_bot_state("strategy_price_mode") == "stabilize-bracket"
    assert "stabilize Down" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_live_stabilize_skips_when_terminal_ev_is_too_thin(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Stabilize EV Guard",
        "slug": "btc-updown-5m-stabilize-ev-guard",
        "conditionId": "cond-arb-stabilize-ev-guard",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.94"}],
                    "asks": [{"price": "0.95", "size": "500"}],
                },
                "asset-down": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.04"}],
                    "asks": [{"price": "0.05", "size": "500"}],
                },
            },
            balance=114.14,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-stabilize-ev-guard"),
    )

    plan = service._build_arb_stabilize_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=MarketOutcome(
            label="Up",
            asset_id="asset-up",
            best_ask=0.95,
            best_bid=0.94,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.95, size=500.0),),
        ),
        down_outcome=MarketOutcome(
            label="Down",
            asset_id="asset-down",
            best_ask=0.05,
            best_bid=0.04,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.05, size=500.0),),
        ),
        pair_sum=1.0,
        fair_up=0.952,
        fair_down=0.048,
        up_net_edge=0.012,
        down_net_edge=-0.02,
        desired_up_ratio=0.80,
        current_up_ratio=0.0,
        timing_regime="mid-late",
        cycle_budget=25.0,
        cash_balance=114.14,
        remaining_instruction_capacity=12,
        current_up_notional=0.0,
        current_down_notional=20.0,
        spot_context=ArbSpotContext(
            current_price=67600.0,
            reference_price=67600.0,
            lead_price=67600.0,
            anchor_price=67400.0,
            local_anchor_price=67400.0,
            official_price_to_beat=0.0,
            anchor_source="polymarket-rtds-anchor",
            fair_up=0.952,
            fair_down=0.048,
            delta_bps=12.0,
            price_mode="reference",
            source="polymarket-rtds+binance",
            age_ms=10,
            binance_price=67600.0,
            chainlink_price=67400.0,
        ),
        bracket_phase="redistribuir",
    )

    assert plan is None
    db.close()


def test_arb_micro_catchup_rebalances_extreme_one_sided_inventory_even_if_spot_still_favors_dominant_leg(
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-catchup"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Catch-up Bracket",
        "slug": slug,
        "conditionId": "cond-arb-catchup",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.60"}],
                "asks": [{"price": "0.61", "size": "500"}],
            },
            "asset-down": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "500"}, {"price": "0.41", "size": "500"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=71028.40,
            lead_price=71028.40,
            binance_price=71028.40,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=15,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-catchup"),
        spot_feed=spot_feed,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "71000.00000000")
    seeded = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id=market["conditionId"],
            size=32.7868,
            price=0.61,
            notional=19.999948,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title=market["question"],
            slug=slug,
            outcome="Up",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert seeded.status == "filled"
    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert db.get_bot_state("strategy_price_mode") == "stabilize-catchup"
    assert "catchup Down" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_live_stabilize_does_not_use_catchup_with_small_budget(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Small Catch-up",
        "slug": "btc-updown-5m-live-small-catchup",
        "conditionId": "cond-arb-live-small-catchup",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.57"}],
                    "asks": [{"price": "0.58", "size": "500"}],
                },
                "asset-down": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.43"}],
                    "asks": [{"price": "0.44", "size": "500"}],
                },
            },
            balance=111.26,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            live_small_target_capital=111.26,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-small-catchup"),
    )

    plan = service._build_arb_stabilize_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=MarketOutcome(
            label="Up",
            asset_id="asset-up",
            best_ask=0.58,
            best_bid=0.57,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.58, size=500.0),),
        ),
        down_outcome=MarketOutcome(
            label="Down",
            asset_id="asset-down",
            best_ask=0.44,
            best_bid=0.43,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.44, size=500.0),),
        ),
        pair_sum=1.02,
        fair_up=0.59,
        fair_down=0.41,
        up_net_edge=-0.0225,
        down_net_edge=0.1327,
        desired_up_ratio=0.58,
        current_up_ratio=0.0,
        timing_regime="mid-late",
        cycle_budget=5.0,
        cash_balance=111.26,
        remaining_instruction_capacity=12,
        current_up_notional=0.0,
        current_down_notional=20.0,
        spot_context=ArbSpotContext(
            current_price=71075.0,
            reference_price=71075.0,
            lead_price=71075.0,
            anchor_price=71000.0,
            local_anchor_price=71000.0,
            official_price_to_beat=0.0,
            anchor_source="polymarket-rtds-anchor",
            fair_up=0.59,
            fair_down=0.41,
            delta_bps=10.56,
            price_mode="reference",
            source="polymarket-rtds+binance",
            age_ms=15,
            binance_price=71075.0,
            chainlink_price=71000.0,
        ),
        bracket_phase="redistribuir",
    )

    assert plan is None
    db.close()


def test_live_repair_does_not_rebalance_when_market_leg_is_already_extreme(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=200)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Extreme Repair Guard",
        "slug": "btc-updown-5m-live-extreme-repair",
        "conditionId": "cond-arb-live-extreme-repair",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.90"}],
                    "asks": [{"price": "0.91", "size": "500"}],
                },
                "asset-down": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.08"}],
                    "asks": [{"price": "0.09", "size": "500"}],
                },
            },
            balance=114.14,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-extreme-repair"),
    )

    plan = service._build_arb_repair_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=MarketOutcome(
            label="Up",
            asset_id="asset-up",
            best_ask=0.91,
            best_bid=0.90,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.91, size=500.0),),
        ),
        down_outcome=MarketOutcome(
            label="Down",
            asset_id="asset-down",
            best_ask=0.09,
            best_bid=0.08,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.09, size=500.0),),
        ),
        pair_sum=1.00,
        fair_up=0.93,
        fair_down=0.07,
        up_net_edge=0.018,
        down_net_edge=-0.03,
        desired_up_ratio=0.80,
        current_up_ratio=0.40,
        timing_regime="late",
        cycle_budget=25.0,
        cash_balance=114.14,
        remaining_instruction_capacity=12,
        current_up_notional=8.0,
        current_down_notional=12.0,
        spot_context=ArbSpotContext(
            current_price=67285.0,
            reference_price=67285.0,
            lead_price=67285.0,
            anchor_price=67200.0,
            local_anchor_price=67200.0,
            official_price_to_beat=0.0,
            anchor_source="polymarket-rtds-anchor",
            fair_up=0.93,
            fair_down=0.07,
            delta_bps=12.0,
            price_mode="reference",
            source="polymarket-rtds+binance",
            age_ms=15,
            binance_price=67285.0,
            chainlink_price=67200.0,
        ),
        bracket_phase="redistribuir",
    )

    assert plan is None
    db.close()


def test_live_stabilize_catchup_does_not_buy_when_market_leg_is_already_extreme(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=200)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Extreme Catch-up Guard",
        "slug": "btc-updown-5m-live-extreme-catchup",
        "conditionId": "cond-arb-live-extreme-catchup",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.01"}],
                    "asks": [{"price": "0.02", "size": "500"}],
                },
                "asset-down": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.97"}],
                    "asks": [{"price": "0.98", "size": "500"}],
                },
            },
            balance=114.14,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            live_small_target_capital=97.72,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-extreme-catchup"),
    )

    plan = service._build_arb_stabilize_plan(  # noqa: SLF001
        mode="live",
        market=market,
        up_outcome=MarketOutcome(
            label="Up",
            asset_id="asset-up",
            best_ask=0.02,
            best_bid=0.01,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.02, size=500.0),),
        ),
        down_outcome=MarketOutcome(
            label="Down",
            asset_id="asset-down",
            best_ask=0.98,
            best_bid=0.97,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.98, size=500.0),),
        ),
        pair_sum=1.00,
        fair_up=0.02,
        fair_down=0.98,
        up_net_edge=-0.02,
        down_net_edge=0.01,
        desired_up_ratio=0.20,
        current_up_ratio=1.0,
        timing_regime="late",
        cycle_budget=25.0,
        cash_balance=114.14,
        remaining_instruction_capacity=12,
        current_up_notional=9.0,
        current_down_notional=0.0,
        spot_context=ArbSpotContext(
            current_price=67282.0,
            reference_price=67282.0,
            lead_price=67282.0,
            anchor_price=67446.06,
            local_anchor_price=67446.06,
            official_price_to_beat=0.0,
            anchor_source="polymarket-rtds-anchor",
            fair_up=0.02,
            fair_down=0.98,
            delta_bps=-24.0,
            price_mode="reference",
            source="polymarket-rtds+binance",
            age_ms=10,
            binance_price=67282.0,
            chainlink_price=67446.06,
        ),
        bracket_phase="redistribuir",
    )

    assert plan is None
    db.close()


def test_arb_stabilize_residual_catchup_completes_tiny_orphan_leg_in_shadow_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=145)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Residual Catch-up",
        "slug": "btc-updown-5m-residual-catchup",
        "conditionId": "cond-arb-residual-catchup",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.57"}],
                    "asks": [{"price": "0.58", "size": "500"}],
                },
                "asset-down": {
                    "min_order_size": "5.0",
                    "bids": [{"price": "0.41"}],
                    "asks": [{"price": "0.42", "size": "500"}],
                },
            },
            balance=178.73,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=3.0,
            live_small_target_capital=178.73,
            live_btc5m_cycle_budget_usdc=43.58,
        ),
        logger=logging.getLogger("test-btc5m-shadow-residual-catchup"),
    )

    plan = service._build_arb_stabilize_plan(  # noqa: SLF001
        mode="shadow",
        market=market,
        up_outcome=MarketOutcome(
            label="Up",
            asset_id="asset-up",
            best_ask=0.58,
            best_bid=0.57,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.58, size=500.0),),
        ),
        down_outcome=MarketOutcome(
            label="Down",
            asset_id="asset-down",
            best_ask=0.42,
            best_bid=0.41,
            best_ask_size=500.0,
            ask_levels=(AskLevel(price=0.42, size=500.0),),
        ),
        pair_sum=1.01,
        fair_up=0.6541,
        fair_down=0.4022,
        up_net_edge=0.0573,
        down_net_edge=-0.0178,
        desired_up_ratio=0.58,
        current_up_ratio=1.0,
        timing_regime="mid-late",
        cycle_budget=43.58,
        cash_balance=178.73,
        remaining_instruction_capacity=12,
        current_up_notional=1.5669,
        current_down_notional=0.0,
        spot_context=ArbSpotContext(
            current_price=71100.0,
            reference_price=71100.0,
            lead_price=71100.0,
            anchor_price=71000.0,
            local_anchor_price=71000.0,
            official_price_to_beat=0.0,
            anchor_source="polymarket-rtds-anchor",
            fair_up=0.6541,
            fair_down=0.4022,
            delta_bps=-0.8,
            price_mode="reference",
            source="polymarket-rtds+binance",
            age_ms=20,
            binance_price=71100.0,
            chainlink_price=71000.0,
        ),
        bracket_phase="redistribuir",
    )

    assert plan is not None
    assert plan.price_mode == "stabilize-catchup"
    assert plan.primary_target.label == "Down"
    assert "residual-catchup Down" in plan.note
    assert plan.primary_notional >= 2.1
    assert plan.primary_notional <= 2.12
    db.close()


def test_arb_micro_unwinds_extreme_wrong_side_inventory_when_repair_is_too_expensive(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-unwind"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=160)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Inventory Unwind",
        "slug": slug,
        "conditionId": "cond-arb-unwind",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.64"}],
                "asks": [{"price": "0.66", "size": "400"}],
            },
            "asset-down": {
                "bids": [{"price": "0.37"}],
                "asks": [{"price": "0.39", "size": "400"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70942.58,
            lead_price=70942.58,
            binance_price=70942.58,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=12,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-inventory-unwind"),
        spot_feed=spot_feed,
    )
    db.set_bot_state(f"arb_spot_anchor:{slug}", "71017.17000000")
    seeded = service.paper_broker.execute(
        CopyInstruction(
            action=SignalAction.OPEN,
            side=TradeSide.BUY,
            asset="asset-up",
            condition_id=market["conditionId"],
            size=50.0,
            price=0.66,
            notional=33.0,
            source_wallet="strategy:test-seed",
            source_signal_id=0,
            title=market["question"],
            slug=slug,
            outcome="Up",
            category="crypto",
            reason="test-seed",
        )
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert seeded.status == "filled"
    assert stats["filled"] > 0
    position = db.get_copy_position("asset-up")
    assert position is not None
    assert float(position["size"]) < 50.0
    assert db.get_bot_state("strategy_price_mode") == "inventory-unwind"
    assert "unwind Up" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_btc5m_operating_bankroll_keeps_reserved_profit_out_of_reinvestment(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, profit_keep_ratio=0.5),
        logger=logging.getLogger("test-btc5m-bankroll-vault"),
    )
    today = datetime.now(timezone.utc).date().isoformat()
    db.add_daily_pnl(today, 200.0)
    db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-up",
            size=10.0,
            price=0.60,
            notional=6.0,
            pnl_delta=200.0,
            message="paper fill",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-pnl",
        source_wallet="strategy:test",
        source_signal_id=0,
        notes="profit test",
    )

    operating_bankroll, reserved_profit = service._operating_bankroll_snapshot(live_total_capital=1200.0)  # noqa: SLF001

    assert reserved_profit == 100.0
    assert operating_bankroll == 1100.0
    db.close()


def test_btc5m_operating_bankroll_reserves_only_net_realized_profit(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, profit_keep_ratio=0.5),
        logger=logging.getLogger("test-btc5m-bankroll-vault-net"),
    )
    today = datetime.now(timezone.utc).date().isoformat()
    db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-win",
            size=10.0,
            price=0.60,
            notional=6.0,
            pnl_delta=200.0,
            message="paper win",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-win",
        source_wallet="strategy:test",
        source_signal_id=0,
        notes="profit test",
    )
    db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-loss",
            size=10.0,
            price=0.20,
            notional=2.0,
            pnl_delta=-150.0,
            message="paper loss",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-loss",
        source_wallet="strategy:test",
        source_signal_id=0,
        notes="loss test",
    )
    db.add_daily_pnl(today, 50.0)

    operating_bankroll, reserved_profit = service._operating_bankroll_snapshot(live_total_capital=1050.0)  # noqa: SLF001

    assert reserved_profit == 25.0
    assert operating_bankroll == 1025.0
    db.close()


def test_live_operating_bankroll_snapshot_uses_live_small_target_and_live_history_only(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=250.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            live_small_target_capital=100.0,
            profit_keep_ratio=0.0,
        ),
        logger=logging.getLogger("test-btc5m-live-bankroll-target"),
    )
    db.record_execution(
        result=ExecutionResult(
            mode="paper",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-paper",
            size=10.0,
            price=0.60,
            notional=6.0,
            pnl_delta=500.0,
            message="paper fill",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-paper",
        source_wallet="strategy:test",
        source_signal_id=0,
        notes="paper profit",
    )
    db.record_execution(
        result=ExecutionResult(
            mode="live",
            status="filled",
            action=SignalAction.CLOSE,
            asset="asset-live",
            size=10.0,
            price=0.39,
            notional=3.9,
            pnl_delta=-10.0,
            message="live fill",
        ),
        side=TradeSide.SELL.value,
        condition_id="cond-live",
        source_wallet="strategy:test",
        source_signal_id=0,
        notes="live loss",
    )

    operating_bankroll, reserved_profit = service._operating_bankroll_snapshot(  # noqa: SLF001
        mode="live",
        live_total_capital=250.0,
    )

    assert reserved_profit == 0.0
    assert operating_bankroll == 90.0
    db.close()


def test_arb_micro_skips_tiny_first_level_and_sweeps_deeper_levels(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Tiny Top Level",
        "slug": "btc-updown-5m-tiny-top",
        "conditionId": "cond-arb-tiny",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "2"}, {"price": "0.41", "size": "100"}],
            },
            "asset-down": {
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.55", "size": "2"}, {"price": "0.56", "size": "100"}],
            },
        },
        balance=1000.0,
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.05,
            min_trade_amount=5.0,
        ),
        logger=logging.getLogger("test-btc5m-arb-tiny"),
    )

    stats = service.run(mode="paper")

    assert stats["filled"] >= 4
    positions = db.list_copy_positions()
    assert len(positions) == 2
    assert float(positions[0]["avg_price"]) >= 0.4099 or float(positions[1]["avg_price"]) >= 0.5599
    db.close()


def test_arb_micro_uses_spot_context_to_open_controlled_single_side_trade(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-spot-cheap"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=75)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Spot Cheap",
        "slug": slug,
        "conditionId": "cond-arb-spot",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.43"}],
                "asks": [{"price": "0.45", "size": "150"}, {"price": "0.46", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.56"}],
                "asks": [{"price": "0.57", "size": "150"}, {"price": "0.58", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70150.0,
            lead_price=70150.0,
            binance_price=70150.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=12,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-spot"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert positions
    assert {str(row["outcome"]) for row in positions} == {"Up"}
    assert db.get_bot_state("strategy_price_mode") == "cheap-side"
    assert db.get_bot_state("strategy_spot_source") == "binance-direct"
    assert float(db.get_bot_state("strategy_spot_anchor") or 0.0) == 70000.0
    assert float(db.get_bot_state("strategy_spot_fair_up") or 0.0) > 0.45
    assert "cheap Up" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_spot_context_prefers_official_price_to_beat_over_local_anchor(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-official-beat"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=75)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Official Beat",
        "slug": slug,
        "conditionId": "cond-arb-official-beat",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": start_time,
                "eventMetadata": {"priceToBeat": 71775.07326019551},
            }
        ],
    }
    db.set_bot_state(f"arb_spot_anchor:{slug}", "71761.47000000")
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=71787.28,
            lead_price=71787.28,
            binance_price=71787.28,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=15,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0),
        logger=logging.getLogger("test-btc5m-official-beat"),
        spot_feed=spot_feed,
    )

    context = service._arb_spot_context(market=market, seconds_into_window=75)

    assert context is not None
    assert round(context.official_price_to_beat, 2) == 71775.07
    assert round(context.local_anchor_price, 2) == 71761.47
    assert round(context.anchor_price, 2) == 71775.07
    assert context.anchor_source == "polymarket-official"
    assert context.delta_bps > 0
    db.close()


def test_arb_spot_context_uses_lead_basis_for_lower_latency_current(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-lead-basis"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=75)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Lead Basis",
        "slug": slug,
        "conditionId": "cond-arb-lead-basis",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": start_time,
                "eventMetadata": {"priceToBeat": 70000.0},
            }
        ],
    }
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70010.0,
            lead_price=70020.0,
            binance_price=70020.0,
            chainlink_price=70010.0,
            basis=5.0,
            source="polymarket-rtds+binance",
            age_ms=9,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0),
        logger=logging.getLogger("test-btc5m-lead-basis"),
        spot_feed=spot_feed,
    )

    context = service._arb_spot_context(market=market, seconds_into_window=75)

    assert context is not None
    assert context.price_mode == "lead-basis"
    assert round(context.current_price, 2) == 70025.00
    assert round(context.reference_price, 2) == 70010.00
    assert round(context.lead_price, 2) == 70020.00
    assert round(context.delta_bps, 2) == round(((70025.0 / 70000.0) - 1.0) * 10000, 2)
    db.close()


def test_arb_live_spot_state_refetches_market_for_official_beat_from_slug(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773597300"
    db.set_bot_state("strategy_market_slug", slug)
    market = {
        "question": "Bitcoin Up or Down - Refetched Official Beat",
        "slug": slug,
        "conditionId": "cond-refetch-official",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": "2026-03-15T17:55:00Z",
                "eventMetadata": {"priceToBeat": 71982.15764705987},
            }
        ],
    }
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=71840.04,
            lead_price=71840.04,
            binance_price=71840.04,
            chainlink_price=71840.04,
            basis=0.0,
            source="polymarket-rtds-chainlink",
            age_ms=25,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({slug: market}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-refetch-official"),
        spot_feed=spot_feed,
    )

    state = service._arb_live_spot_state(market=None, seconds_into_window=20)

    assert round(float(state["strategy_official_price_to_beat"]), 2) == 71982.16
    assert state["strategy_anchor_source"] == "polymarket-official"
    assert state["strategy_reference_quality"] == "official"
    db.close()


def test_arb_live_spot_state_uses_lead_basis_for_current_price(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773597300"
    market = {
        "question": "Bitcoin Up or Down - Live Lead Basis",
        "slug": slug,
        "conditionId": "cond-live-lead-basis",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": "2026-03-15T17:55:00Z",
                "eventMetadata": {"priceToBeat": 70000.0},
            }
        ],
    }
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70010.0,
            lead_price=70020.0,
            binance_price=70020.0,
            chainlink_price=70010.0,
            basis=5.0,
            source="polymarket-rtds+binance",
            age_ms=11,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({slug: market}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-live-lead-basis"),
        spot_feed=spot_feed,
    )

    state = service._arb_live_spot_state(market=market, seconds_into_window=60)

    assert state["strategy_spot_price_mode"] == "lead-basis"
    assert round(float(state["strategy_spot_price"]), 2) == 70025.00
    assert round(float(state["strategy_spot_chainlink"]), 2) == 70010.00
    assert round(float(state["strategy_spot_binance"]), 2) == 70020.00
    db.close()


def test_record_strategy_snapshot_keeps_market_official_over_zero_snapshot(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773597300"
    market = {
        "question": "Bitcoin Up or Down - Snapshot Official Beat",
        "slug": slug,
        "conditionId": "cond-snapshot-official",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": "2026-03-15T17:55:00Z",
                "eventMetadata": {"priceToBeat": 71982.15764705987},
            }
        ],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({slug: market}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-snapshot-official"),
        spot_feed=None,
    )

    service._record_strategy_snapshot(
        market=market,
        note="arb_micro realism gate: fuente degradada: rest-coinbase",
        extra_state={"strategy_official_price_to_beat": "0.000000"},
    )

    assert round(float(db.get_bot_state("strategy_official_price_to_beat") or 0.0), 2) == 71982.16
    db.close()


def test_record_strategy_snapshot_preserves_market_context_when_market_is_missing(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-stale")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Stale")
    db.set_bot_state("strategy_target_outcome", "Up")
    db.set_bot_state("strategy_target_price", "0.810000")
    db.set_bot_state("strategy_trigger_outcome", "Down")
    db.set_bot_state("strategy_trigger_price_seen", "0.190000")
    db.set_bot_state("strategy_official_price_to_beat", "71234.560000")
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-clear-stale-snapshot"),
        spot_feed=None,
    )

    service._record_strategy_snapshot(note="no active btc5m market")

    assert db.get_bot_state("strategy_market_slug") == "btc-updown-5m-stale"
    assert db.get_bot_state("strategy_market_title") == "Bitcoin Up or Down - Stale"
    assert db.get_bot_state("strategy_target_outcome") == ""
    assert db.get_bot_state("strategy_target_price") == "0.000000"
    assert db.get_bot_state("strategy_trigger_outcome") == ""
    assert db.get_bot_state("strategy_trigger_price_seen") == "0.000000"
    assert db.get_bot_state("strategy_official_price_to_beat") == "71234.560000"
    db.close()


def test_record_strategy_snapshot_clears_stale_official_when_slug_changes_without_new_beat(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_market_slug", "btc-updown-5m-old")
    db.set_bot_state("strategy_market_title", "Bitcoin Up or Down - Old")
    db.set_bot_state("strategy_official_price_to_beat", "71234.560000")
    db.set_bot_state("strategy_official_price_slug", "btc-updown-5m-old")
    market = {
        "question": "Bitcoin Up or Down - New",
        "slug": "btc-updown-5m-new",
        "conditionId": "cond-new-window",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": "2026-03-15T17:55:00Z", "eventMetadata": {}}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({market["slug"]: market}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-clear-stale-official"),
        spot_feed=None,
    )

    service._record_strategy_snapshot(market=market, note="sin beat oficial en esta ventana")

    assert db.get_bot_state("strategy_market_slug") == "btc-updown-5m-new"
    assert db.get_bot_state("strategy_official_price_to_beat") == "0.000000"
    assert db.get_bot_state("strategy_official_price_slug") == "btc-updown-5m-new"
    db.close()


def test_market_official_price_to_beat_refetches_partial_market_by_slug(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773598800"
    partial_market = {
        "question": "Bitcoin Up or Down - Partial",
        "slug": slug,
        "conditionId": "cond-partial-market",
        "closed": False,
        "acceptingOrders": True,
        "events": [{"startTime": "2026-03-15T18:20:00Z", "eventMetadata": None}],
    }
    refreshed_market = {
        "question": "Bitcoin Up or Down - Refreshed",
        "slug": slug,
        "conditionId": "cond-refreshed-market",
        "closed": False,
        "acceptingOrders": True,
        "events": [{"startTime": "2026-03-15T18:20:00Z", "eventMetadata": {"priceToBeat": 71771.64821}}],
    }
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({slug: refreshed_market}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-refetch-partial-market"),
        spot_feed=None,
    )

    official = service._market_official_price_to_beat(partial_market)

    assert round(official, 2) == 71771.65
    db.close()


def test_runtime_guard_primes_feed_and_marks_operability_blocking(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Runtime Guard",
        "slug": "btc-updown-5m-runtime-guard",
        "conditionId": "cond-runtime-guard",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(books={}, balance=1000.0)
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-runtime-guard"),
        spot_feed=None,
    )
    service._runtime_guard_can_open = lambda mode="paper": (False, "runtime guard 35m: PnL reciente -60.79")  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["blocked"] == 1
    assert clob.tracked_assets == ("asset-up", "asset-down")
    assert str(db.get_bot_state("strategy_market_slug") or "").startswith("btc-updown-5m-")
    assert db.get_bot_state("strategy_market_title") == "Bitcoin Up or Down - Runtime Guard"
    assert db.get_bot_state("strategy_feed_tracked_assets") == "2"
    assert db.get_bot_state("strategy_data_source") == "websocket-warming"
    assert db.get_bot_state("strategy_operability_state") == "runtime_guard"
    assert db.get_bot_state("strategy_operability_label") == "Guardado por riesgo"
    assert db.get_bot_state("strategy_operability_blocking") == "1"
    assert "runtime guard" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_runtime_guard_uses_paper_profile_thresholds(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            paper_runtime_guard_enabled=True,
            paper_runtime_guard_lookback_minutes=90,
            paper_runtime_guard_loss_streak=5,
            paper_runtime_guard_max_recent_pnl=-120.0,
            paper_runtime_guard_cooldown_minutes=12,
        ),
        logger=logging.getLogger("test-btc5m-paper-runtime-guard"),
        spot_feed=None,
    )

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        evaluate_mock.return_value = {
            "blocked": False,
            "recent_close_count": 0,
            "recent_close_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": 0,
            "reason": "",
        }
        allowed, note = service._runtime_guard_can_open(mode="paper")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "paper"
    _, kwargs = evaluate_mock.call_args
    assert kwargs["lookback_minutes"] == 90
    assert kwargs["loss_streak_limit"] == 5
    assert kwargs["max_recent_close_pnl"] == -120.0
    assert kwargs["cooldown_minutes"] == 12
    db.close()


def test_runtime_guard_uses_shadow_profile_thresholds(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            shadow_runtime_guard_enabled=True,
            shadow_runtime_guard_lookback_minutes=120,
            shadow_runtime_guard_loss_streak=0,
            shadow_runtime_guard_max_recent_pnl=-55.0,
            shadow_runtime_guard_cooldown_minutes=18,
        ),
        logger=logging.getLogger("test-btc5m-shadow-runtime-guard"),
        spot_feed=None,
    )

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        evaluate_mock.return_value = {
            "blocked": False,
            "recent_close_count": 0,
            "recent_close_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": 0,
            "reason": "",
        }
        allowed, note = service._runtime_guard_can_open(mode="shadow")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "shadow"
    _, kwargs = evaluate_mock.call_args
    assert kwargs["lookback_minutes"] == 120
    assert kwargs["loss_streak_limit"] == 0
    assert kwargs["max_recent_close_pnl"] == -55.0
    assert kwargs["cooldown_minutes"] == 18
    db.close()


def test_runtime_guard_can_be_disabled_only_for_paper(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            runtime_guard_enabled=True,
            paper_runtime_guard_enabled=False,
        ),
        logger=logging.getLogger("test-btc5m-paper-runtime-guard-disabled"),
        spot_feed=None,
    )

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        allowed, note = service._runtime_guard_can_open(mode="paper")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "paper-disabled"
    assert db.get_bot_state("runtime_guard_state") == "disabled"
    evaluate_mock.assert_not_called()

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        evaluate_mock.return_value = {
            "blocked": False,
            "recent_close_count": 0,
            "recent_close_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": 0,
            "reason": "",
        }
        allowed, note = service._runtime_guard_can_open(mode="live")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "live"
    evaluate_mock.assert_called_once()
    db.close()


def test_runtime_guard_can_be_disabled_for_shadow_without_touching_live(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            runtime_guard_enabled=True,
            shadow_runtime_guard_enabled=False,
        ),
        logger=logging.getLogger("test-btc5m-shadow-runtime-guard-disabled"),
        spot_feed=None,
    )

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        allowed, note = service._runtime_guard_can_open(mode="shadow")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "shadow-disabled"
    assert db.get_bot_state("runtime_guard_state") == "disabled"
    evaluate_mock.assert_not_called()

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        evaluate_mock.return_value = {
            "blocked": False,
            "recent_close_count": 0,
            "recent_close_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": 0,
            "reason": "",
        }
        allowed, note = service._runtime_guard_can_open(mode="live")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "live"
    evaluate_mock.assert_called_once()
    db.close()


def test_runtime_guard_live_ignores_recent_paper_losses(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    for offset, pnl in enumerate((-25.0, -15.0, -10.0), start=1):
        db.conn.execute(
            """
            INSERT INTO executions (
                ts, mode, status, action, side, asset, condition_id, size, price, notional,
                source_wallet, source_signal_id, strategy_variant, notes, pnl_delta
            ) VALUES (?, 'paper', 'filled', 'close', 'sell', ?, 'cond-paper', 1, 0.5, 0.5, 'strategy:test', 0, '', 'paper loss', ?)
            """,
            (now_ts - offset, f"asset-paper-{offset}", pnl),
        )
    db.conn.commit()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            runtime_guard_enabled=True,
            paper_runtime_guard_enabled=True,
        ),
        logger=logging.getLogger("test-btc5m-live-runtime-guard-isolated"),
        spot_feed=None,
    )

    allowed, note = service._runtime_guard_can_open(mode="live")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    assert db.get_bot_state("runtime_guard_profile") == "live"
    assert db.get_bot_state("runtime_guard_recent_close_count") == "0"
    assert db.get_bot_state("runtime_guard_recent_close_pnl") == "0.000000"
    db.close()


def test_runtime_guard_live_can_disable_loss_streak_and_keep_pnl_guard(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            runtime_guard_enabled=True,
            runtime_guard_loss_streak=0,
            runtime_guard_max_recent_pnl=-25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-runtime-guard-no-streak"),
        spot_feed=None,
    )

    with patch("app.services.btc5m_strategy.evaluate_runtime_guard") as evaluate_mock:
        evaluate_mock.return_value = {
            "blocked": False,
            "recent_close_count": 3,
            "recent_close_pnl": -21.0,
            "consecutive_losses": 3,
            "cooldown_until": 0,
            "reason": "",
        }
        allowed, note = service._runtime_guard_can_open(mode="live")  # noqa: SLF001

    assert allowed is True
    assert note == ""
    _, kwargs = evaluate_mock.call_args
    assert kwargs["loss_streak_limit"] == 0
    assert kwargs["max_recent_close_pnl"] == -25.0
    db.close()


def test_market_official_price_to_beat_reads_refreshed_event_payload_top_level(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773600000"
    partial_market = {
        "question": "Bitcoin Up or Down - Event Fallback",
        "slug": slug,
        "conditionId": "cond-event-fallback",
        "closed": False,
        "acceptingOrders": True,
        "events": [{"id": "evt-top-level", "eventMetadata": {}}],
    }
    gamma = _FakeGammaClient(
        {slug: partial_market},
        events={"evt-top-level": {"id": "evt-top-level", "priceToBeat": 70984.43}},
    )
    service = BTC5mStrategyService(
        db,
        gamma,
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-event-fallback"),
    )

    official = service._market_official_price_to_beat(partial_market)

    assert round(official, 2) == 70984.43
    db.close()


def test_market_official_price_to_beat_falls_back_to_public_gamma_when_primary_client_is_incomplete(
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-1773600300"
    partial_market = {
        "question": "Bitcoin Up or Down - Public Gamma Fallback",
        "slug": slug,
        "conditionId": "cond-public-gamma-fallback",
        "closed": False,
        "acceptingOrders": True,
        "events": [{"id": "evt-public-fallback", "eventMetadata": {}}],
    }
    primary_gamma = _FakeGammaClient({slug: partial_market}, events={"evt-public-fallback": {"id": "evt-public-fallback"}})
    public_gamma = _FakeGammaClient(
        {
            slug: {
                "question": "Bitcoin Up or Down - Public Gamma Refreshed",
                "slug": slug,
                "conditionId": "cond-public-gamma-refreshed",
                "closed": False,
                "acceptingOrders": True,
                "events": [{"id": "evt-public-fallback", "eventMetadata": {"priceToBeat": 70888.12}}],
            }
        }
    )
    service = BTC5mStrategyService(
        db,
        primary_gamma,
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=SimpleNamespace(execute=lambda instruction: None),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-public-gamma-fallback"),
    )
    service._public_gamma_client = public_gamma  # type: ignore[assignment]

    official = service._market_official_price_to_beat(partial_market)

    assert round(official, 2) == 70888.12
    db.close()


def test_arb_micro_strict_realism_skips_degraded_reference(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-strict-realism"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=80)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Strict Realism",
        "slug": slug,
        "conditionId": "cond-arb-strict-realism",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": start_time,
                "eventMetadata": {"priceToBeat": 71775.07326019551},
            }
        ],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.70"}],
                "asks": [{"price": "0.74", "size": "200"}],
            },
            "asset-down": {
                "bids": [{"price": "0.24"}],
                "asks": [{"price": "0.26", "size": "200"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=71750.0,
            lead_price=71750.0,
            binance_price=71750.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=12,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.05,
            btc5m_strict_realism_mode=True,
        ),
        logger=logging.getLogger("test-btc5m-strict-realism"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] == 0
    assert not db.list_copy_positions()
    assert db.get_bot_state("strategy_reference_comparable") == "0"
    assert db.get_bot_state("strategy_reference_quality") == "degraded"
    assert db.get_bot_state("strategy_operability_state") == "degraded_reference"
    assert db.get_bot_state("strategy_operability_label") == "Referencia degradada"
    assert db.get_bot_state("strategy_operability_blocking") == "1"
    assert "realism gate" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_shadow_allows_degraded_reference_with_reduced_budget(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-shadow-fallback"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=80)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Shadow Fallback",
        "slug": slug,
        "conditionId": "cond-arb-shadow-fallback",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": start_time,
                "eventMetadata": {"priceToBeat": 71775.07326019551},
            }
        ],
    }
    shadow_broker = _FakeBroker()
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "min_order_size": "5.0",
                "bids": [{"price": "0.40"}],
                "asks": [{"price": "0.41", "size": "250"}, {"price": "0.42", "size": "250"}],
            },
            "asset-down": {
                "min_order_size": "5.0",
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.54", "size": "250"}, {"price": "0.55", "size": "250"}],
            },
        },
        balance=111.26,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=71750.0,
            lead_price=71750.0,
            binance_price=71750.0,
            chainlink_price=None,
            basis=0.0,
            source="rest-coinbase",
            age_ms=12,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=shadow_broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            arb_min_trade_amount=1.0,
            btc5m_strict_realism_mode=True,
            shadow_live_like_mode=False,
            live_small_target_capital=111.26,
            live_btc5m_cycle_budget_usdc=25.0,
            btc5m_reference_soft_budget_scale=0.55,
        ),
        logger=logging.getLogger("test-btc5m-shadow-fallback"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="shadow")

    assert stats["filled"] >= 2
    assert not shadow_broker.instructions
    assert db.get_bot_state("strategy_reference_comparable") == "1"
    assert db.get_bot_state("strategy_reference_quality") == "shadow-fallback"
    assert round(float(db.get_bot_state("strategy_cycle_budget") or 0.0), 2) >= 12.40
    assert "realism gate" not in str(db.get_bot_state("strategy_last_note") or "")
    assert db.list_copy_positions()
    db.close()


def test_arb_micro_strict_realism_allows_soft_stale_official_rtds(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-soft-stale-realism"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=95)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Soft Stale Realism",
        "slug": slug,
        "conditionId": "cond-arb-soft-stale-realism",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [
            {
                "startTime": start_time,
                "eventMetadata": {"priceToBeat": 71775.07326019551},
            }
        ],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.39"}],
                "asks": [{"price": "0.40", "size": "250"}],
            },
            "asset-down": {
                "bids": [{"price": "0.60"}],
                "asks": [{"price": "0.61", "size": "250"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=71750.0,
            lead_price=71750.0,
            binance_price=71750.0,
            chainlink_price=71775.07,
            basis=25.07,
            source="polymarket-rtds+binance",
            age_ms=1600,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=1000.0,
            strategy_trade_allocation_pct=0.05,
            btc5m_strict_realism_mode=True,
        ),
        logger=logging.getLogger("test-btc5m-soft-stale-realism"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["failed"] == 0
    assert db.get_bot_state("strategy_reference_comparable") == "1"
    assert db.get_bot_state("strategy_reference_quality") == "soft-stale-official"
    assert "realism gate" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_min_notional_uses_max_of_arb_strategy_and_exchange_minimums(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(
            books={
                "asset-up": {
                    "min_order_size": "2.5",
                    "bids": [{"price": "0.51", "size": "200"}],
                    "asks": [{"price": "0.52", "size": "200"}],
                }
            },
            balance=1000.0,
        ),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", min_trade_amount=5.0, arb_min_trade_amount=3.0),
        logger=logging.getLogger("test-btc5m-min-order-size"),
        spot_feed=None,
    )

    target = MarketOutcome(
        label="Up",
        asset_id="asset-up",
        best_ask=0.52,
        best_bid=0.51,
        best_ask_size=200.0,
        ask_levels=(AskLevel(price=0.52, size=200.0),),
    )

    effective_min_notional = service._arb_min_notional(target)  # noqa: SLF001

    assert round(effective_min_notional, 2) == 3.00
    assert db.get_bot_state("strategy_asset_min_order_size:asset-up") == "2.500000"
    assert db.get_bot_state("strategy_asset_min_notional:asset-up") == "1.300000"
    assert db.get_bot_state("strategy_strategy_min_notional") == "3.000000"
    assert db.get_bot_state("strategy_effective_min_notional") == "3.000000"
    db.close()


def test_arb_cycle_budget_floor_allows_near_minimum_redistribution(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-budget-floor-redistribution"),
        spot_feed=None,
    )

    budget, floored = service._arb_floor_cycle_budget_to_minimum(  # noqa: SLF001
        cycle_budget=2.55,
        effective_min_notional=3.20,
        market_cap_remaining=6.05,
        total_cap_remaining=84.99,
        cash_balance=12.34,
        bracket_phase="redistribuir",
    )

    assert floored is True
    assert budget == 3.2
    db.close()


def test_arb_cycle_budget_floor_keeps_new_open_restrictive(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-budget-floor-open"),
        spot_feed=None,
    )

    budget, floored = service._arb_floor_cycle_budget_to_minimum(  # noqa: SLF001
        cycle_budget=2.55,
        effective_min_notional=3.20,
        market_cap_remaining=6.05,
        total_cap_remaining=84.99,
        cash_balance=12.34,
        bracket_phase="abrir",
    )

    assert floored is False
    assert budget == 2.55
    db.close()


def test_arb_cycle_budget_floor_allows_live_like_open_when_close_to_minimum(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-budget-floor-open-live-like"),
        spot_feed=None,
    )

    budget, floored = service._arb_floor_cycle_budget_to_minimum(  # noqa: SLF001
        cycle_budget=2.95,
        effective_min_notional=3.20,
        market_cap_remaining=3.40,
        total_cap_remaining=84.99,
        cash_balance=12.34,
        bracket_phase="abrir",
        allow_opening_floor=True,
    )

    assert floored is True
    assert budget == 3.2
    db.close()


def test_arb_cycle_budget_floor_keeps_live_like_open_restrictive_when_gap_is_large(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro"),
        logger=logging.getLogger("test-btc5m-budget-floor-open-live-like-far"),
        spot_feed=None,
    )

    budget, floored = service._arb_floor_cycle_budget_to_minimum(  # noqa: SLF001
        cycle_budget=2.70,
        effective_min_notional=3.20,
        market_cap_remaining=3.40,
        total_cap_remaining=84.99,
        cash_balance=12.34,
        bracket_phase="abrir",
        allow_opening_floor=True,
    )

    assert floored is False
    assert budget == 2.7
    db.close()


def test_arb_micro_live_small_scales_underround_ladder_to_micro_tranches_under_25_usdc(tmp_path: Path) -> None:
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=70)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Live Small Underround",
        "slug": "btc-updown-5m-live-small-underround",
        "conditionId": "cond-arb-live-small-underround",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    books = {
        "asset-up": {
            "min_order_size": "5.0",
            "bids": [{"price": "0.40"}],
            "asks": [{"price": "0.41", "size": "500"}, {"price": "0.42", "size": "500"}, {"price": "0.43", "size": "500"}],
        },
        "asset-down": {
            "min_order_size": "5.0",
            "bids": [{"price": "0.54"}],
            "asks": [{"price": "0.54", "size": "500"}, {"price": "0.55", "size": "500"}, {"price": "0.56", "size": "500"}],
        },
    }

    paper_db = Database(tmp_path / "paper.db")
    paper_db.init_schema()
    paper_service = BTC5mStrategyService(
        paper_db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books=books, balance=10000.0),
        paper_broker=PaperBroker(paper_db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            arb_min_trade_amount=1.0,
            strategy_trade_allocation_pct=0.03,
        ),
        logger=logging.getLogger("test-btc5m-paper-underround"),
    )

    live_db = Database(tmp_path / "live.db")
    live_db.init_schema()
    live_broker = _FakeBroker()
    live_service = BTC5mStrategyService(
        live_db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books=books, balance=111.26),
        paper_broker=PaperBroker(live_db),
        live_broker=live_broker,
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            arb_min_trade_amount=1.0,
            live_small_target_capital=111.26,
            live_btc5m_cycle_budget_usdc=25.0,
        ),
        logger=logging.getLogger("test-btc5m-live-underround"),
    )

    paper_stats = paper_service.run(mode="paper")
    live_stats = live_service.run(mode="live")
    live_notionals = [float(instruction.notional) for instruction in live_broker.instructions]
    up_count = sum(1 for instruction in live_broker.instructions if instruction.outcome == "Up")
    down_count = sum(1 for instruction in live_broker.instructions if instruction.outcome == "Down")

    assert paper_stats["filled"] > live_stats["filled"] >= 6
    assert paper_db.get_bot_state("strategy_price_mode") == "underround"
    assert live_db.get_bot_state("strategy_price_mode") == "underround"
    assert round(float(live_db.get_bot_state("strategy_effective_min_notional") or 0.0), 2) == 2.70
    assert round(float(live_db.get_bot_state("strategy_cycle_budget") or 0.0), 2) >= 24.90
    assert sum(live_notionals) <= 25.00
    assert sum(live_notionals) >= 24.90
    assert up_count >= 3
    assert down_count >= 3
    assert max(live_notionals) < 6.25
    paper_db.close()
    live_db.close()


def test_arb_reference_state_relaxes_soft_stale_rtds_budget_in_live(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", btc5m_reference_soft_budget_scale=0.55),
        logger=logging.getLogger("test-btc5m-live-soft-stale-rtds"),
        spot_feed=None,
    )

    paper_state = service._arb_reference_state(  # noqa: SLF001
        mode="paper",
        source="polymarket-rtds+binance",
        age_ms=1600,
        chainlink_price=71775.07,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="polymarket-rtds-anchor",
    )
    live_state = service._arb_reference_state(  # noqa: SLF001
        mode="live",
        source="polymarket-rtds+binance",
        age_ms=1600,
        chainlink_price=71775.07,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="polymarket-rtds-anchor",
    )
    shadow_state = service._arb_reference_state(  # noqa: SLF001
        mode="shadow",
        source="polymarket-rtds+binance",
        age_ms=1600,
        chainlink_price=71775.07,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="polymarket-rtds-anchor",
    )

    assert paper_state.quality == "soft-stale-rtds"
    assert round(paper_state.budget_scale, 2) == 0.45
    assert live_state.comparable is False
    assert live_state.quality == "official-missing"
    assert "priceToBeat" in live_state.note
    assert shadow_state.comparable is False
    assert shadow_state.quality == "official-missing"
    assert "priceToBeat" in shadow_state.note
    db.close()


def test_arb_reference_state_live_like_can_allow_soft_stale_rtds_when_official_gate_is_disabled(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            btc5m_reference_soft_budget_scale=0.55,
            btc5m_require_official_price_to_beat_live_like=False,
        ),
        logger=logging.getLogger("test-btc5m-live-soft-stale-rtds-optout"),
        spot_feed=None,
    )

    live_state = service._arb_reference_state(  # noqa: SLF001
        mode="live",
        source="polymarket-rtds+binance",
        age_ms=1600,
        chainlink_price=71775.07,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="polymarket-rtds-anchor",
    )

    assert live_state.quality == "soft-stale-rtds"
    assert round(live_state.budget_scale, 2) == 0.80
    db.close()


def test_arb_reference_state_live_like_accepts_captured_chainlink_when_official_missing(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", btc5m_reference_soft_budget_scale=0.55),
        logger=logging.getLogger("test-btc5m-live-captured-chainlink"),
        spot_feed=None,
    )

    live_state = service._arb_reference_state(  # noqa: SLF001
        mode="live",
        source="polymarket-rtds+binance",
        age_ms=150,
        chainlink_price=71775.07,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="polymarket-chainlink",
    )
    shadow_state = service._arb_reference_state(  # noqa: SLF001
        mode="shadow",
        source="polymarket-rtds+binance",
        age_ms=150,
        chainlink_price=71775.07,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="polymarket-chainlink",
    )

    assert live_state.comparable is True
    assert live_state.quality == "captured-chainlink"
    assert "captura propia Chainlink" in live_state.note
    assert round(live_state.budget_scale, 2) == 0.90
    assert shadow_state.comparable is True
    assert shadow_state.quality == "captured-chainlink"
    db.close()


def test_arb_reference_state_live_like_accepts_soft_stale_captured_chainlink_when_official_missing(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", btc5m_reference_soft_budget_scale=0.55),
        logger=logging.getLogger("test-btc5m-live-soft-stale-captured-chainlink"),
        spot_feed=None,
    )

    shadow_state = service._arb_reference_state(  # noqa: SLF001
        mode="shadow",
        source="polymarket-rtds+binance",
        age_ms=1281,
        chainlink_price=70280.98,
        official_price_to_beat=0.0,
        local_anchor_price=70280.98,
        anchor_source="captured-chainlink",
    )

    assert shadow_state.comparable is True
    assert shadow_state.quality == "soft-stale-captured-chainlink"
    assert "captura Chainlink" in shadow_state.note
    assert round(shadow_state.budget_scale, 2) == 0.85
    db.close()


def test_arb_reference_state_live_like_accepts_rest_coinbase_when_public_official_exists(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", btc5m_reference_soft_budget_scale=0.55),
        logger=logging.getLogger("test-btc5m-live-rest-coinbase-official"),
        spot_feed=None,
    )

    shadow_state = service._arb_reference_state(  # noqa: SLF001
        mode="shadow",
        source="rest-coinbase",
        age_ms=150,
        chainlink_price=65804.65,
        official_price_to_beat=65804.65,
        local_anchor_price=65805.02,
        anchor_source="captured-chainlink",
    )

    assert shadow_state.comparable is True
    assert shadow_state.quality == "rest-coinbase-official"
    assert "beat oficial publico" in shadow_state.note
    assert round(shadow_state.budget_scale, 2) == 0.65
    db.close()


def test_arb_reference_state_allows_shadow_fallback_without_labeled_source(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            btc5m_reference_soft_budget_scale=0.55,
            shadow_live_like_mode=False,
        ),
        logger=logging.getLogger("test-btc5m-shadow-missing-source"),
        spot_feed=None,
    )

    shadow_state = service._arb_reference_state(  # noqa: SLF001
        mode="shadow",
        source="",
        age_ms=25,
        chainlink_price=0.0,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="local-anchor",
    )

    assert shadow_state.comparable is True
    assert shadow_state.quality == "shadow-fallback"
    assert round(shadow_state.budget_scale, 2) == 0.45
    db.close()


def test_arb_reference_state_blocks_shadow_fallback_in_live_like_mode(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", btc5m_reference_soft_budget_scale=0.55),
        logger=logging.getLogger("test-btc5m-shadow-live-like-reference"),
        spot_feed=None,
    )

    shadow_state = service._arb_reference_state(  # noqa: SLF001
        mode="shadow",
        source="",
        age_ms=25,
        chainlink_price=0.0,
        official_price_to_beat=0.0,
        local_anchor_price=71760.0,
        anchor_source="local-anchor",
    )

    assert shadow_state.comparable is False
    assert shadow_state.quality == "missing"
    assert shadow_state.note == "sin spot de referencia"
    db.close()


def test_arb_micro_shadow_bootstraps_anchor_from_current_spot_when_reference_missing(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_runtime_mode", "shadow")
    slug = "btc-updown-5m-shadow-bootstrap"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=80)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Shadow Bootstrap",
        "slug": slug,
        "conditionId": "cond-arb-shadow-bootstrap",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "min_order_size": "5.0",
                "bids": [{"price": "0.40"}],
                "asks": [{"price": "0.41", "size": "250"}, {"price": "0.42", "size": "250"}],
            },
            "asset-down": {
                "min_order_size": "5.0",
                "bids": [{"price": "0.54"}],
                "asks": [{"price": "0.54", "size": "250"}, {"price": "0.55", "size": "250"}],
            },
        },
        balance=111.26,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=None,
            lead_price=71750.0,
            binance_price=71750.0,
            chainlink_price=None,
            basis=0.0,
            source="",
            age_ms=12,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            bankroll=10000.0,
            min_trade_amount=5.0,
            arb_min_trade_amount=1.0,
            btc5m_strict_realism_mode=True,
            shadow_live_like_mode=False,
            live_small_target_capital=111.26,
            live_btc5m_cycle_budget_usdc=25.0,
            btc5m_reference_soft_budget_scale=0.55,
        ),
        logger=logging.getLogger("test-btc5m-shadow-bootstrap"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="shadow")
    live_spot_state = service._arb_live_spot_state(market=market, seconds_into_window=80)  # noqa: SLF001

    assert stats["filled"] > 0
    assert db.get_bot_state("strategy_reference_comparable") == "1"
    assert db.get_bot_state("strategy_reference_quality") == "shadow-fallback"
    assert float(live_spot_state["strategy_spot_local_anchor"]) > 0
    assert live_spot_state["strategy_anchor_source"] == "shadow-current-price"
    assert live_spot_state["strategy_reference_quality"] == "shadow-fallback"
    assert "realism gate" not in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_live_spot_state_exposes_captured_chainlink_as_effective_beat_when_gamma_missing(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_runtime_mode", "shadow")
    slug = "btc-updown-5m-captured-beat"
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70123.45000000")
    db.set_bot_state(f"arb_spot_anchor:{slug}:source", "polymarket-chainlink")
    market = {
        "question": "Bitcoin Up or Down - Captured Beat",
        "slug": slug,
        "conditionId": "cond-captured-beat",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [],
    }
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70140.0,
            lead_price=70140.0,
            binance_price=70138.0,
            chainlink_price=70140.0,
            basis=0.0,
            source="polymarket-rtds+binance",
            age_ms=12,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=111.26),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", live_small_target_capital=111.26),
        logger=logging.getLogger("test-btc5m-live-spot-captured-beat"),
        spot_feed=spot_feed,
    )

    state = service._arb_live_spot_state(market=market, seconds_into_window=42)  # noqa: SLF001

    assert state["strategy_official_price_to_beat"] == "0.000000"
    assert state["strategy_captured_price_to_beat"] == "70123.450000"
    assert state["strategy_captured_price_source"] == "captured-chainlink"
    assert state["strategy_effective_price_to_beat"] == "70123.450000"
    assert state["strategy_effective_price_source"] == "captured-chainlink"
    assert state["strategy_reference_quality"] == "captured-chainlink"
    db.close()


def test_arb_live_spot_state_labels_historical_chainlink_anchor_correctly(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("strategy_runtime_mode", "shadow")
    slug = "btc-updown-5m-1774643700"
    market = {
        "question": "Bitcoin Up or Down - Historical Anchor",
        "slug": slug,
        "conditionId": "cond-historical-anchor",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": "2026-03-29T18:35:00Z"}],
    }
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=65820.0,
            lead_price=65820.0,
            binance_price=65818.0,
            chainlink_price=None,
            basis=0.0,
            source="rest-coinbase",
            age_ms=120,
            connected=True,
        )
    )
    spot_feed.get_anchor_price = lambda symbol, target_ts: 65805.02  # type: ignore[attr-defined]
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        _FakeCLOBClient(books={}, balance=97.72),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(
            strategy_entry_mode="arb_micro",
            btc5m_strict_realism_mode=True,
            shadow_live_like_mode=True,
            btc5m_allow_captured_chainlink_price_to_beat_live_like=True,
            live_small_target_capital=97.72,
        ),
        logger=logging.getLogger("test-btc5m-live-rest-coinbase-historical-anchor"),
        spot_feed=spot_feed,
    )
    service._market_official_price_to_beat_with_source = lambda market: (65804.65, "public-web")  # type: ignore[method-assign]

    with patch("app.services.btc5m_strategy.time.time", return_value=1774643710):
        state = service._arb_live_spot_state(market=market, seconds_into_window=40)  # noqa: SLF001

    assert state["strategy_captured_price_to_beat"] == "65805.020000"
    assert state["strategy_captured_price_source"] == "captured-chainlink-confirmed"
    assert state["strategy_anchor_source"] == "polymarket-official"
    assert state["strategy_effective_price_source"] == "public-web"
    assert state["strategy_reference_quality"] == "rest-coinbase-official"
    assert state["strategy_reference_comparable"] == "1"
    db.close()


def test_arb_micro_opens_single_side_on_small_delta_with_strong_edge(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-small-delta"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=95)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Small Delta Strong Edge",
        "slug": slug,
        "conditionId": "cond-arb-small-delta",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.68"}],
                "asks": [{"price": "0.80", "size": "150"}],
            },
            "asset-down": {
                "bids": [{"price": "0.18"}],
                "asks": [{"price": "0.21", "size": "150"}, {"price": "0.22", "size": "150"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70020.0,
            lead_price=70020.0,
            binance_price=70020.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=8,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-small-delta"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert positions
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert db.get_bot_state("strategy_price_mode") == "biased-bracket"
    assert "biased bracket" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()


def test_arb_micro_does_not_overlay_pair_when_overlay_flag_is_disabled(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    slug = "btc-updown-5m-overlay-off"
    start_time = (datetime.now(timezone.utc) - timedelta(seconds=180)).isoformat().replace("+00:00", "Z")
    market = {
        "question": "Bitcoin Up or Down - Overlay Disabled",
        "slug": slug,
        "conditionId": "cond-arb-overlay-off",
        "closed": False,
        "acceptingOrders": True,
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"asset-up\", \"asset-down\"]",
        "events": [{"startTime": start_time}],
    }
    db.set_bot_state(f"arb_spot_anchor:{slug}", "70000.00000000")
    clob = _FakeCLOBClient(
        books={
            "asset-up": {
                "bids": [{"price": "0.29"}],
                "asks": [{"price": "0.30", "size": "400"}],
            },
            "asset-down": {
                "bids": [{"price": "0.63"}],
                "asks": [{"price": "0.64", "size": "400"}],
            },
        },
        balance=1000.0,
    )
    spot_feed = _FakeSpotFeed(
        SpotSnapshot(
            reference_price=70120.0,
            lead_price=70120.0,
            binance_price=70120.0,
            chainlink_price=None,
            basis=0.0,
            source="binance-direct",
            age_ms=6,
            connected=True,
        )
    )
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient(market),
        clob,
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0, strategy_trade_allocation_pct=0.05),
        logger=logging.getLogger("test-btc5m-arb-overlay-off"),
        spot_feed=spot_feed,
    )
    service._discover_market = lambda: market  # type: ignore[method-assign]

    stats = service.run(mode="paper")

    assert stats["filled"] > 0
    positions = db.list_copy_positions()
    assert len(positions) == 2
    assert {str(row["outcome"]) for row in positions} == {"Up", "Down"}
    assert "cheap" not in str(db.get_bot_state("strategy_last_note") or "").lower()
    db.close()


def test_arb_micro_primes_anchor_from_spot_feed_near_window_open(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    service = BTC5mStrategyService(
        db,
        _FakeGammaClient({}),
        _FakeCLOBClient(books={}, balance=1000.0),
        paper_broker=PaperBroker(db),
        live_broker=_FakeBroker(),
        autonomous_decider=SimpleNamespace(build_exit_instruction=lambda **kwargs: None),
        daily_summary=SimpleNamespace(send_if_due=lambda: False),
        trade_notifier=SimpleNamespace(send_realized_result=lambda **kwargs: False),
        settings=_settings(strategy_entry_mode="arb_micro", bankroll=1000.0),
        logger=logging.getLogger("test-btc5m-arb-anchor"),
        spot_feed=_FakeSpotFeed(
            SpotSnapshot(
                reference_price=70200.0,
                lead_price=70200.0,
                binance_price=70200.0,
                chainlink_price=None,
                basis=0.0,
                source="binance-direct",
                age_ms=4,
                connected=True,
            )
        ),
    )

    with patch("app.services.btc5m_strategy.time.time", return_value=1_773_340_500.75):
        service._maybe_prime_arb_spot_anchor()

    assert float(db.get_bot_state("arb_spot_anchor:btc-updown-5m-1773340500") or 0.0) == 70200.0
    assert db.get_bot_state("arb_spot_anchor:btc-updown-5m-1773340500:source") == "binance-direct"
    db.close()
