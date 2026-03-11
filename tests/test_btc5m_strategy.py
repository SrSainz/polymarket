from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

from app.db import Database
from app.models import ExecutionResult, SignalAction
from app.services.btc5m_strategy import BTC5mStrategyService
from app.settings import AppPaths, AppSettings, BotConfig, EnvSettings


class _FakeGammaClient:
    def __init__(self, market: dict) -> None:
        self.market = market

    def get_market_by_slug(self, slug: str) -> dict | None:
        payload = dict(self.market)
        payload["slug"] = slug
        return payload


class _FakeCLOBClient:
    def __init__(self, books: dict[str, dict], balance: float = 50.0) -> None:
        self.books = books
        self.balance = balance

    def get_collateral_balance(self) -> dict[str, float]:
        return {"balance": self.balance, "allowance": self.balance}

    def get_book(self, token_id: str) -> dict:
        return self.books[token_id]


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
        watched_wallets=["0xabc"],
        strategy_mode="btc5m_orderbook",
        strategy_entry_mode="buy_opposite",
        strategy_trigger_price=0.98,
        strategy_trade_allocation_pct=0.10,
        strategy_max_opposite_price=0.20,
        bankroll=100.0,
        max_position_per_market=10.0,
        max_total_exposure=100.0,
        min_trade_amount=1.0,
        **overrides,
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
        settings=_settings(),
        logger=logging.getLogger("test-btc5m-strategy"),
    )

    stats = service.run(mode="live")

    assert stats["filled"] == 0
    assert stats["skipped"] == 1
    assert not broker.instructions
    assert "opposite too expensive" in str(db.get_bot_state("strategy_last_note") or "")
    db.close()
