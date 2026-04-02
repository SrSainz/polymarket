from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.db import Database
from app.services.live_wallet_sync import LiveWalletSyncService


class _FakeActivityClient:
    def __init__(
        self,
        *,
        activity: list[dict],
        positions: list[dict],
        closed_positions: list[dict] | None = None,
    ) -> None:
        self._activity = activity
        self._positions = positions
        self._closed_positions = list(closed_positions or [])

    def get_activity(self, wallet: str, limit: int = 200, offset: int = 0) -> list[dict]:
        return self._activity[offset : offset + limit]

    def get_positions(self, wallet: str, limit: int = 500, offset: int = 0) -> list[dict]:
        return self._positions[offset : offset + limit]

    def get_closed_positions(self, wallet: str, limit: int = 500, offset: int = 0) -> list[dict]:
        return self._closed_positions[offset : offset + limit]


def test_live_wallet_sync_imports_activity_into_executions_and_daily_pnl(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot_live.db")
    db.init_schema()
    service = LiveWalletSyncService(
        db,
        _FakeActivityClient(
            activity=[
                {
                    "timestamp": 1775000000,
                    "transactionHash": "tx-buy",
                    "conditionId": "cond-1",
                    "type": "TRADE",
                    "size": 10.0,
                    "usdcSize": 4.0,
                    "price": 0.4,
                    "asset": "asset-1",
                    "side": "BUY",
                    "title": "Bitcoin Up or Down - Import",
                    "slug": "btc-updown-5m-import",
                    "outcome": "Down",
                },
                {
                    "timestamp": 1775000030,
                    "transactionHash": "tx-sell",
                    "conditionId": "cond-1",
                    "type": "TRADE",
                    "size": 10.0,
                    "usdcSize": 6.0,
                    "price": 0.6,
                    "asset": "asset-1",
                    "side": "SELL",
                    "title": "Bitcoin Up or Down - Import",
                    "slug": "btc-updown-5m-import",
                    "outcome": "Down",
                },
            ],
            positions=[],
            closed_positions=[],
        ),
    )

    result = service.sync(wallet="0xabc", mode="live", page_limit=50, max_pages=2)

    executions = db.get_recent_executions(limit=10)
    assert result["ok"] is True
    assert result["imported"] == 2
    assert len(executions) == 2
    day = datetime.fromtimestamp(1775000030, timezone.utc).date().isoformat()
    assert abs(db.get_daily_execution_pnl(day, mode="live") - 2.0) < 1e-9
    assert db.list_copy_positions() == []
    assert db.get_bot_state("live_wallet_sync_status") == "ok"
    db.close()


def test_live_wallet_sync_is_idempotent_with_import_keys(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot_live.db")
    db.init_schema()
    activity = [
        {
            "timestamp": 1775000100,
            "transactionHash": "tx-buy",
            "conditionId": "cond-2",
            "type": "TRADE",
            "size": 5.0,
            "usdcSize": 2.0,
            "price": 0.4,
            "asset": "asset-2",
            "side": "BUY",
            "title": "Bitcoin Up or Down - Idempotent",
            "slug": "btc-updown-5m-idempotent",
            "outcome": "Up",
        }
    ]
    positions = [{"asset": "asset-2", "size": 5.0}]
    service = LiveWalletSyncService(db, _FakeActivityClient(activity=activity, positions=positions))

    first = service.sync(wallet="0xabc", mode="live")
    second = service.sync(wallet="0xabc", mode="live")

    assert first["imported"] == 1
    assert second["duplicates"] == 1
    assert len(db.get_recent_executions(limit=10)) == 1
    db.close()


def test_live_wallet_sync_blocks_when_wallet_positions_do_not_match_ledger(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot_live.db")
    db.init_schema()
    service = LiveWalletSyncService(
        db,
        _FakeActivityClient(
            activity=[
                {
                    "timestamp": 1775000200,
                    "transactionHash": "tx-buy",
                    "conditionId": "cond-3",
                    "type": "TRADE",
                    "size": 5.0,
                    "usdcSize": 2.0,
                    "price": 0.4,
                    "asset": "asset-3",
                    "side": "BUY",
                    "title": "Bitcoin Up or Down - Mismatch",
                    "slug": "btc-updown-5m-mismatch",
                    "outcome": "Up",
                }
            ],
            positions=[{"asset": "asset-3", "size": 7.0}],
            closed_positions=[],
        ),
    )

    result = service.sync(wallet="0xabc", mode="live")

    assert result["ok"] is False
    assert "size distinta" in str(result["mismatch_reason"])
    assert db.get_bot_state("position_ledger_preflight") == "blocked"
    assert db.get_bot_state("position_ledger_mode") == "external"
    db.close()


def test_live_wallet_sync_closes_resolved_position_from_closed_positions(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot_live.db")
    db.init_schema()
    service = LiveWalletSyncService(
        db,
        _FakeActivityClient(
            activity=[
                {
                    "timestamp": 1775001000,
                    "transactionHash": "tx-buy",
                    "conditionId": "cond-4",
                    "type": "TRADE",
                    "size": 10.0,
                    "usdcSize": 4.0,
                    "price": 0.4,
                    "asset": "asset-4",
                    "side": "BUY",
                    "title": "Bitcoin Up or Down - Closed",
                    "slug": "btc-updown-5m-closed",
                    "outcome": "Up",
                }
            ],
            positions=[],
            closed_positions=[
                {
                    "timestamp": 1775001060,
                    "conditionId": "cond-4",
                    "asset": "asset-4",
                    "slug": "btc-updown-5m-closed",
                    "title": "Bitcoin Up or Down - Closed",
                    "outcome": "Up",
                    "curPrice": 1.0,
                    "realizedPnl": 6.0,
                }
            ],
        ),
    )

    result = service.sync(wallet="0xabc", mode="live", page_limit=50, max_pages=2)

    executions = db.get_recent_executions(limit=10)
    day = datetime.fromtimestamp(1775001060, timezone.utc).date().isoformat()
    assert result["ok"] is True
    assert result["imported"] == 1
    assert result["closed_imported"] == 1
    assert len(executions) == 2
    assert executions[0]["side"] == "sell"
    assert executions[0]["price"] == 1.0
    assert abs(db.get_daily_execution_pnl(day, mode="live") - 6.0) < 1e-9
    assert db.list_copy_positions() == []
    assert db.get_bot_state("live_wallet_sync_closed_imported") == "1"
    db.close()


def test_live_wallet_sync_closed_positions_is_idempotent(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot_live.db")
    db.init_schema()
    service = LiveWalletSyncService(
        db,
        _FakeActivityClient(
            activity=[
                {
                    "timestamp": 1775001100,
                    "transactionHash": "tx-buy",
                    "conditionId": "cond-5",
                    "type": "TRADE",
                    "size": 5.0,
                    "usdcSize": 2.5,
                    "price": 0.5,
                    "asset": "asset-5",
                    "side": "BUY",
                    "title": "Bitcoin Up or Down - Idempotent Closed",
                    "slug": "btc-updown-5m-idempotent-closed",
                    "outcome": "Down",
                }
            ],
            positions=[],
            closed_positions=[
                {
                    "timestamp": 1775001160,
                    "conditionId": "cond-5",
                    "asset": "asset-5",
                    "slug": "btc-updown-5m-idempotent-closed",
                    "title": "Bitcoin Up or Down - Idempotent Closed",
                    "outcome": "Down",
                    "curPrice": 0.0,
                    "realizedPnl": -2.5,
                }
            ],
        ),
    )

    first = service.sync(wallet="0xabc", mode="live")
    second = service.sync(wallet="0xabc", mode="live")

    assert first["closed_imported"] == 1
    assert second["closed_imported"] == 0
    assert len(db.get_recent_executions(limit=10)) == 2
    assert db.list_copy_positions() == []
    db.close()
