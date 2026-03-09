from __future__ import annotations

import logging

from app.db import Database
from app.models import SourcePosition
from app.services.detect_changes import DetectChangesService
from app.services.sync_wallets import SyncWalletsService
from app.settings import BotConfig


class FakeTracker:
    def __init__(self, positions_map: dict[str, list[SourcePosition]]) -> None:
        self.positions_map = positions_map

    def fetch_wallet_positions(self, wallet: str) -> list[SourcePosition]:
        return self.positions_map.get(wallet, [])


class FakeWalletSelector:
    def __init__(self, wallets: list[str]) -> None:
        self.wallets = wallets

    def resolve_wallets(self) -> list[str]:
        return self.wallets

    def get_last_selection_rows(self) -> list[dict[str, float | int | str]]:
        rows: list[dict[str, float | int | str]] = []
        for rank, wallet in enumerate(self.wallets, start=1):
            rows.append(
                {
                    "wallet": wallet,
                    "score": float(1.0 - (rank * 0.1)),
                    "win_rate": 0.6,
                    "recent_trades": 12,
                    "pnl": 1000.0,
                }
            )
        return rows


def _source_position(wallet: str, asset: str, size: float, condition_id: str) -> SourcePosition:
    return SourcePosition(
        wallet=wallet,
        asset=asset,
        condition_id=condition_id,
        size=size,
        avg_price=0.5,
        current_price=0.5,
        title=f"Market {asset}",
        slug=f"market-{asset}",
        outcome="Yes",
        category="sports",
        observed_at=1,
    )


def test_rebalance_generates_reduce_and_close_for_removed_wallet(tmp_path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()

    wallet_a = "0xaaa"
    wallet_b = "0xbbb"
    asset_shared = "asset-shared"
    asset_only_removed = "asset-only-removed"

    db.replace_source_positions(
        wallet_a,
        [
            _source_position(wallet_a, asset_shared, 100.0, "cond-shared"),
            _source_position(wallet_a, asset_only_removed, 50.0, "cond-only"),
        ],
        run_id="seed",
    )
    db.replace_source_positions(
        wallet_b,
        [_source_position(wallet_b, asset_shared, 100.0, "cond-shared")],
        run_id="seed",
    )

    tracker = FakeTracker(
        {
            wallet_b: [_source_position(wallet_b, asset_shared, 100.0, "cond-shared")],
        }
    )
    selector = FakeWalletSelector([wallet_b])
    service = SyncWalletsService(
        db=db,
        tracker=tracker,
        wallet_selector=selector,
        detect_changes_service=DetectChangesService(noise_threshold=0.5),
        config=BotConfig(watched_wallets=[wallet_b], auto_select_wallets=False),
        logger=logging.getLogger("rebalance_test"),
    )

    stats = service.run()
    assert stats["dropped_wallets"] == 1
    assert stats["rebalance_signals"] == 2

    pending = db.list_pending_signals(limit=50)
    by_asset = {signal.asset: signal for signal in pending}

    assert by_asset[asset_shared].action.value == "reduce"
    assert by_asset[asset_shared].prev_size == 200.0
    assert by_asset[asset_shared].new_size == 100.0

    assert by_asset[asset_only_removed].action.value == "close"
    assert by_asset[asset_only_removed].prev_size == 50.0
    assert by_asset[asset_only_removed].new_size == 0.0

    assert db.get_source_positions(wallet_a) == {}
    db.close()
