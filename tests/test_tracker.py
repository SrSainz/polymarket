from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.core.tracker import SourceTracker
from app.settings import BotConfig


class _FakeActivityClient:
    def __init__(
        self,
        rows: list[dict[str, object]],
        trades: list[dict[str, object]] | None = None,
    ) -> None:
        self.rows = rows
        self.trades = trades or []

    def get_positions(self, wallet: str) -> list[dict[str, object]]:  # noqa: ARG002
        return self.rows

    def get_trades(self, wallet: str | None = None, limit: int = 200, offset: int = 0) -> list[dict[str, object]]:  # noqa: ARG002
        return self.trades


class _FakeGammaClient:
    def get_category(self, slug: str) -> str:  # noqa: ARG002
        return "sports"


def _config() -> BotConfig:
    return BotConfig(
        watched_wallets=["0xabc"],
        skip_expired_source_positions=True,
        expired_market_grace_hours=0,
    )


def test_tracker_skips_expired_positions() -> None:
    old_date = (datetime.now(timezone.utc) - timedelta(days=3)).date().isoformat()
    rows = [
        {
            "asset": "a1",
            "conditionId": "c1",
            "size": 100,
            "avgPrice": 0.55,
            "curPrice": 0.4,
            "title": "old market",
            "slug": "old-market",
            "outcome": "Yes",
            "endDate": old_date,
        }
    ]
    tracker = SourceTracker(_FakeActivityClient(rows), _FakeGammaClient(), _config(), logging.getLogger("test"))
    positions = tracker.fetch_wallet_positions("0xabc")
    assert positions == []


def test_tracker_keeps_zero_current_price_when_present() -> None:
    future_date = (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat()
    rows = [
        {
            "asset": "a2",
            "conditionId": "c2",
            "size": 50,
            "avgPrice": 0.62,
            "curPrice": 0,
            "title": "future market",
            "slug": "future-market",
            "outcome": "No",
            "endDate": future_date,
        }
    ]
    tracker = SourceTracker(_FakeActivityClient(rows), _FakeGammaClient(), _config(), logging.getLogger("test"))
    positions = tracker.fetch_wallet_positions("0xabc")
    assert len(positions) == 1
    assert positions[0].current_price == 0.0


def test_tracker_requires_recent_trade_when_enabled() -> None:
    future_date = (datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    rows = [
        {
            "asset": "a-live",
            "conditionId": "c-live",
            "size": 10,
            "avgPrice": 0.5,
            "curPrice": 0.52,
            "title": "live market",
            "slug": "live-market",
            "outcome": "Yes",
            "endDate": future_date,
        },
        {
            "asset": "a-stale",
            "conditionId": "c-stale",
            "size": 12,
            "avgPrice": 0.5,
            "curPrice": 0.51,
            "title": "stale market",
            "slug": "stale-market",
            "outcome": "No",
            "endDate": future_date,
        },
    ]
    trades = [
        {"asset": "a-live", "conditionId": "c-live", "timestamp": now_ts},
    ]
    cfg = BotConfig(
        watched_wallets=["0xabc"],
        skip_expired_source_positions=True,
        expired_market_grace_hours=0,
        require_recent_trade_for_position=True,
        position_recent_trade_lookback_hours=48,
        position_recent_trades_limit=50,
    )
    tracker = SourceTracker(_FakeActivityClient(rows, trades), _FakeGammaClient(), cfg, logging.getLogger("test"))
    positions = tracker.fetch_wallet_positions("0xabc")
    assert [position.asset for position in positions] == ["a-live"]


def test_tracker_skips_redeemable_positions() -> None:
    future_date = (datetime.now(timezone.utc) + timedelta(days=2)).date().isoformat()
    rows = [
        {
            "asset": "a-redeemable",
            "conditionId": "c-redeemable",
            "size": 10,
            "avgPrice": 0.5,
            "curPrice": 0.51,
            "title": "redeemable market",
            "slug": "redeemable-market",
            "outcome": "Yes",
            "endDate": future_date,
            "redeemable": True,
        }
    ]
    tracker = SourceTracker(_FakeActivityClient(rows), _FakeGammaClient(), _config(), logging.getLogger("test"))
    positions = tracker.fetch_wallet_positions("0xabc")
    assert positions == []
