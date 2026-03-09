from __future__ import annotations

import logging

from app.core.wallet_selector import WalletSelector
from app.settings import BotConfig


class FakeActivityClient:
    def __init__(self) -> None:
        self.leaderboard = [
            {"proxyWallet": "0xaaa", "pnl": 1000},
            {"proxyWallet": "0xbbb", "pnl": 400},
            {"proxyWallet": "0xccc", "pnl": 800},
        ]
        self.closed_map = {
            "0xaaa": [{"realizedPnl": 2}] * 7 + [{"realizedPnl": -1}] * 3,  # 70%
            "0xbbb": [{"realizedPnl": 2}] * 4 + [{"realizedPnl": -1}] * 6,  # 40%
            "0xccc": [{"realizedPnl": 2}] * 6 + [{"realizedPnl": -1}] * 4,  # 60%
        }
        now = 2_000_000_000
        self.trades_map = {
            "0xaaa": [{"timestamp": now - 60}] * 20,
            "0xbbb": [{"timestamp": now - 60}] * 20,
            "0xccc": [{"timestamp": now - 60}] * 5,
        }

    def get_leaderboard(self, *, category: str, time_period: str, limit: int) -> list[dict]:
        return self.leaderboard[:limit]

    def get_closed_positions(self, wallet: str, limit: int) -> list[dict]:
        return self.closed_map.get(wallet, [])[:limit]

    def get_trades(self, wallet: str | None = None, limit: int = 200, offset: int = 0) -> list[dict]:
        if wallet is None:
            return []
        return self.trades_map.get(wallet, [])[:limit]


def test_selects_best_winrate_and_active_wallets(monkeypatch) -> None:
    fake_client = FakeActivityClient()
    config = BotConfig(
        watched_wallets=["0xfallback"],
        auto_select_wallets=True,
        top_wallets_to_copy=2,
        min_wallet_win_rate=0.55,
        min_closed_positions_for_scoring=10,
        min_recent_trades=8,
        recent_trade_lookback_hours=24,
    )
    logger = logging.getLogger("wallet_selector_test")
    selector = WalletSelector(fake_client, config, logger)

    monkeypatch.setattr("app.core.wallet_selector.time.time", lambda: 2_000_000_000)
    selected = selector.resolve_wallets()

    assert selected == ["0xaaa"]


def test_falls_back_to_configured_wallets_when_no_candidate_passes(monkeypatch) -> None:
    fake_client = FakeActivityClient()
    config = BotConfig(
        watched_wallets=["0xfallback"],
        auto_select_wallets=True,
        top_wallets_to_copy=2,
        min_wallet_win_rate=0.95,
        min_closed_positions_for_scoring=10,
        min_recent_trades=50,
        recent_trade_lookback_hours=24,
    )
    logger = logging.getLogger("wallet_selector_test")
    selector = WalletSelector(fake_client, config, logger)

    monkeypatch.setattr("app.core.wallet_selector.time.time", lambda: 2_000_000_000)
    selected = selector.resolve_wallets()

    assert selected == ["0xfallback"]
