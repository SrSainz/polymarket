from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.db import Database
from app.services.live_wallet_sync import LiveWalletSyncService


class _FakeActivityClient:
    def __init__(self, *, activity: list[dict], positions: list[dict]) -> None:
        self._activity = activity
        self._positions = positions

    def get_activity(self, wallet: str, limit: int = 200, offset: int = 0) -> list[dict]:
        return self._activity[offset : offset + limit]

    def get_positions(self, wallet: str, limit: int = 500, offset: int = 0) -> list[dict]:
        return self._positions[offset : offset + limit]


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
        ),
    )

    result = service.sync(wallet="0xabc", mode="live")

    assert result["ok"] is False
    assert "size distinta" in str(result["mismatch_reason"])
    assert db.get_bot_state("position_ledger_preflight") == "blocked"
    assert db.get_bot_state("position_ledger_mode") == "external"
    db.close()
