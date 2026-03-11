from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

from app.core.paper_broker import PaperBroker
from app.db import Database
from app.models import ExecutionResult, SignalAction
from app.services.btc5m_strategy import BTC5mStrategyService
from app.settings import AppPaths, AppSettings, BotConfig, EnvSettings


class _FakeGammaClient:
    def __init__(self, market: dict) -> None:
        self.market = market

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


class _FakeCLOBClient:
    def __init__(self, books: dict[str, dict], balance: float = 50.0) -> None:
        self.books = books
        self.balance = balance

    def get_collateral_balance(self) -> dict[str, float]:
        return {"balance": self.balance, "allowance": self.balance}

    def get_book(self, token_id: str) -> dict:
        return self.books[token_id]

    def get_midpoint(self, token_id: str) -> float | None:
        book = self.books.get(token_id) or {}
        asks = book.get("asks") or []
        if not asks:
            return None
        return float(asks[0]["price"])


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
        "question": "Bitcoin Up or Down - Test",
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
                "bids": [{"price": "0.58"}],
                "asks": [{"price": "0.60", "size": "1000"}],
            },
            "asset-down": {
                "bids": [{"price": "0.38"}],
                "asks": [{"price": "0.40", "size": "1000"}],
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
    db.close()


def test_vidarx_micro_uses_extreme_80_20_bias(tmp_path: Path) -> None:
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

    assert stats["filled"] >= 2
    assert float(db.get_bot_state("strategy_primary_ratio") or "0") >= 0.79
    assert db.get_bot_state("strategy_price_mode") == "extreme"
    assert db.get_bot_state("strategy_primary_outcome") == "Up"
    db.close()


def test_vidarx_micro_uses_balanced_55_45_bias(tmp_path: Path) -> None:
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

    assert stats["filled"] >= 2
    assert 0.54 <= float(db.get_bot_state("strategy_primary_ratio") or "0") <= 0.56
    assert db.get_bot_state("strategy_price_mode") == "balanced"
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
        settings=_settings(strategy_entry_mode="vidarx_micro", bankroll=100.0, max_position_per_market=40.0),
        logger=logging.getLogger("test-btc5m-vidarx"),
    )

    stats = service.run(mode="paper")

    prices = {
        round(float(row["price"] or 0.0), 2)
        for row in db.get_recent_executions(limit=10)
        if str(row["asset"]) == "asset-up"
    }
    assert stats["filled"] >= 4
    assert prices >= {0.77, 0.79}
    db.close()


def test_vidarx_micro_replenishes_same_price_bucket(tmp_path: Path) -> None:
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
    assert second_stats["filled"] >= 1
    assert second_replenishment_count >= 1
    assert second_exposure > first_exposure
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
    assert second_stats["filled"] >= 1
    assert second_replenishment_count >= 1
    assert second_exposure > first_exposure
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


def test_vidarx_micro_uses_second_wave_when_market_already_open(tmp_path: Path) -> None:
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

    assert plan is not None
    assert plan.timing_regime == "second-wave"
    assert len(plan.instructions) >= 1
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
    assert any(str(row["notes"]).startswith("vidarx_resolution:") for row in executions)
    assert db.get_cumulative_pnl() == 6.0
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
