from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from app.polymarket.activity_client import ActivityClient
from app.settings import BotConfig


@dataclass(frozen=True)
class WalletScore:
    wallet: str
    win_rate: float
    recent_trades: int
    pnl: float
    score: float


class WalletSelector:
    """
    Selects source wallets to copy from leaderboard performance and trading activity.
    """

    def __init__(self, activity_client: ActivityClient, config: BotConfig, logger: logging.Logger) -> None:
        self.activity_client = activity_client
        self.config = config
        self.logger = logger
        self._cached_wallets: list[str] = []
        self._next_refresh_ts: float = 0.0

    def resolve_wallets(self) -> list[str]:
        if not self.config.auto_select_wallets:
            return self.config.watched_wallets

        now = time.time()
        if self._cached_wallets and now < self._next_refresh_ts:
            return self._cached_wallets

        selected = self._select_wallets_from_market_data()
        self._cached_wallets = selected
        self._next_refresh_ts = now + (self.config.wallet_selection_refresh_minutes * 60)
        return selected

    def _select_wallets_from_market_data(self) -> list[str]:
        leaderboard = self.activity_client.get_leaderboard(
            category=self.config.leaderboard_category,
            time_period=self.config.leaderboard_time_period,
            limit=self.config.leaderboard_candidate_limit,
        )
        if not leaderboard:
            self.logger.warning("wallet-selector: leaderboard returned no results, falling back to configured wallets")
            return self.config.watched_wallets

        max_positive_pnl = max(
            [max(_to_float(item.get("pnl")), 0.0) for item in leaderboard],
            default=1.0,
        )
        max_positive_pnl = max(max_positive_pnl, 1.0)

        scored: list[WalletScore] = []
        for item in leaderboard:
            wallet = str(item.get("proxyWallet") or "").strip().lower()
            if not wallet:
                continue

            closed_positions = self.activity_client.get_closed_positions(
                wallet=wallet,
                limit=self.config.closed_positions_limit,
            )
            wins, losses = _wins_losses_from_closed_positions(closed_positions)
            resolved = wins + losses
            if resolved < self.config.min_closed_positions_for_scoring:
                continue

            win_rate = wins / resolved
            if win_rate < self.config.min_wallet_win_rate:
                continue

            recent_trades = self._count_recent_trades(wallet)
            if recent_trades < self.config.min_recent_trades:
                continue

            pnl = _to_float(item.get("pnl"))
            pnl_score = min(max(pnl, 0.0) / max_positive_pnl, 1.0)
            freq_denominator = max(self.config.min_recent_trades * 3, 1)
            frequency_score = min(recent_trades / freq_denominator, 1.0)

            # Prioritize winrate first, then recency/activity, then pnl rank.
            total_score = (0.60 * win_rate) + (0.30 * frequency_score) + (0.10 * pnl_score)
            scored.append(
                WalletScore(
                    wallet=wallet,
                    win_rate=win_rate,
                    recent_trades=recent_trades,
                    pnl=pnl,
                    score=total_score,
                )
            )

        scored.sort(key=lambda row: (row.score, row.win_rate, row.recent_trades, row.pnl), reverse=True)
        selected = [row.wallet for row in scored[: self.config.top_wallets_to_copy]]

        if not selected:
            self.logger.warning(
                "wallet-selector: no wallet passed filters (winrate/activity), falling back to configured wallets"
            )
            return self.config.watched_wallets

        for row in scored[: self.config.top_wallets_to_copy]:
            self.logger.info(
                "wallet-selector: wallet=%s score=%.4f win_rate=%.2f recent_trades=%s pnl=%.2f",
                row.wallet,
                row.score,
                row.win_rate,
                row.recent_trades,
                row.pnl,
            )
        return selected

    def _count_recent_trades(self, wallet: str) -> int:
        trades = self.activity_client.get_trades(wallet=wallet, limit=self.config.recent_trades_limit_per_wallet)
        cutoff = int(time.time()) - (self.config.recent_trade_lookback_hours * 3600)
        return sum(1 for trade in trades if int(_to_float(trade.get("timestamp"))) >= cutoff)


def _wins_losses_from_closed_positions(closed_positions: list[dict]) -> tuple[int, int]:
    wins = 0
    losses = 0
    for position in closed_positions:
        realized = _to_float(position.get("realizedPnl"))
        if realized > 0:
            wins += 1
        elif realized < 0:
            losses += 1
    return wins, losses


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
