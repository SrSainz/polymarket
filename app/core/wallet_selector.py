from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from app.core.market_classifier import is_dynamic_market
from app.polymarket.activity_client import ActivityClient
from app.settings import BotConfig


@dataclass(frozen=True)
class WalletScore:
    wallet: str
    win_rate: float
    recent_trades: int
    dynamic_recent_trades: int
    dynamic_share: float
    recent_notional: float
    pnl: float
    score: float
    dynamic_score: float


class WalletSelector:
    """
    Selects source wallets to copy from leaderboard performance and trading activity.
    """

    def __init__(self, activity_client: ActivityClient, config: BotConfig, logger: logging.Logger) -> None:
        self.activity_client = activity_client
        self.config = config
        self.logger = logger
        self._cached_wallets: list[str] = []
        self._cached_scores: list[WalletScore] = []
        self._next_refresh_ts: float = 0.0

    def resolve_wallets(self) -> list[str]:
        if not self.config.auto_select_wallets:
            self._cached_wallets = list(self.config.watched_wallets)
            self._cached_scores = self._fallback_scores(self._cached_wallets)
            return self._cached_wallets

        now = time.time()
        if self._cached_wallets and now < self._next_refresh_ts:
            return self._cached_wallets

        selected, selected_scores = self._select_wallets_from_market_data()
        self._cached_wallets = selected
        self._cached_scores = selected_scores
        self._next_refresh_ts = now + (self.config.wallet_selection_refresh_minutes * 60)
        return selected

    def get_last_selection_rows(self) -> list[dict[str, float | int | str]]:
        return [
            {
                "wallet": row.wallet,
                "score": row.score,
                "win_rate": row.win_rate,
                "recent_trades": row.recent_trades,
                "pnl": row.pnl,
            }
            for row in self._cached_scores
        ]

    def _select_wallets_from_market_data(self) -> tuple[list[str], list[WalletScore]]:
        leaderboard = self.activity_client.get_leaderboard(
            category=self.config.leaderboard_category,
            time_period=self.config.leaderboard_time_period,
            limit=self.config.leaderboard_candidate_limit,
        )
        if not leaderboard:
            self.logger.warning("wallet-selector: leaderboard returned no results, falling back to configured wallets")
            fallback = list(self.config.watched_wallets)
            return fallback, self._fallback_scores(fallback)

        max_positive_pnl = max(
            [max(_to_float(item.get("pnl")), 0.0) for item in leaderboard],
            default=1.0,
        )
        max_positive_pnl = max(max_positive_pnl, 1.0)

        candidates: list[dict[str, float | int | str]] = []
        max_recent_notional = 1.0
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

            recent_trades, dynamic_recent_trades, recent_notional = self._recent_trade_metrics(wallet)
            if recent_trades < self.config.min_recent_trades:
                continue

            dynamic_share = dynamic_recent_trades / max(recent_trades, 1)
            pnl = _to_float(item.get("pnl"))
            max_recent_notional = max(max_recent_notional, recent_notional)
            candidates.append(
                {
                    "wallet": wallet,
                    "win_rate": win_rate,
                    "recent_trades": recent_trades,
                    "dynamic_recent_trades": dynamic_recent_trades,
                    "recent_notional": recent_notional,
                    "pnl": pnl,
                    "dynamic_share": dynamic_share,
                }
            )

        for candidate in candidates:
            pnl = float(candidate["pnl"])
            recent_trades = int(candidate["recent_trades"])
            dynamic_recent_trades = int(candidate["dynamic_recent_trades"])
            recent_notional = float(candidate["recent_notional"])
            win_rate = float(candidate["win_rate"])
            dynamic_share = float(candidate["dynamic_share"])

            pnl_score = min(max(pnl, 0.0) / max_positive_pnl, 1.0)
            freq_denominator = max(self.config.min_recent_trades * 3, 1)
            frequency_score = min(recent_trades / freq_denominator, 1.0)
            notional_score = min(recent_notional / max_recent_notional, 1.0)
            dyn_denominator = max(self.config.min_dynamic_recent_trades * 3, 1)
            dynamic_activity_score = min(dynamic_recent_trades / dyn_denominator, 1.0)

            # Base ranking: winrate first, then activity, then notional, then pnl rank.
            base_score = (0.55 * win_rate) + (0.20 * frequency_score) + (0.15 * notional_score) + (0.10 * pnl_score)
            # Dynamic ranking for reserved dynamic slots.
            dynamic_score = (
                (0.40 * win_rate)
                + (0.20 * dynamic_activity_score)
                + (0.20 * dynamic_share)
                + (0.15 * notional_score)
                + (0.05 * frequency_score)
            )

            scored.append(
                WalletScore(
                    wallet=str(candidate["wallet"]),
                    win_rate=win_rate,
                    recent_trades=recent_trades,
                    dynamic_recent_trades=dynamic_recent_trades,
                    dynamic_share=dynamic_share,
                    recent_notional=recent_notional,
                    pnl=pnl,
                    score=base_score,
                    dynamic_score=dynamic_score,
                )
            )

        base_ranked = sorted(scored, key=lambda row: (row.score, row.win_rate, row.recent_trades, row.pnl), reverse=True)
        selected_scores: list[WalletScore]
        if self.config.prioritize_dynamic_wallets and self.config.dynamic_wallet_slots > 0:
            dynamic_eligible = [
                row
                for row in scored
                if row.dynamic_recent_trades >= self.config.min_dynamic_recent_trades
                and row.dynamic_share >= self.config.min_dynamic_trade_share
            ]
            dynamic_target = min(self.config.dynamic_wallet_slots, self.config.top_wallets_to_copy)
            if len(dynamic_eligible) < dynamic_target:
                fallback_dynamic = self._dynamic_fallback_candidates(
                    exclude_wallets={row.wallet for row in scored}
                )
                existing_dynamic_wallets = {row.wallet for row in dynamic_eligible}
                for row in fallback_dynamic:
                    if row.wallet in existing_dynamic_wallets:
                        continue
                    dynamic_eligible.append(row)
                    existing_dynamic_wallets.add(row.wallet)
                    if len(dynamic_eligible) >= dynamic_target:
                        break
            dynamic_ranked = sorted(
                dynamic_eligible,
                key=lambda row: (
                    row.dynamic_score,
                    row.win_rate,
                    row.dynamic_recent_trades,
                    row.recent_notional,
                ),
                reverse=True,
            )
            selected_scores = []
            for row in dynamic_ranked:
                if len(selected_scores) >= dynamic_target:
                    break
                selected_scores.append(row)
            selected_wallets = {item.wallet for item in selected_scores}
            for row in base_ranked:
                if row.wallet in selected_wallets:
                    continue
                selected_scores.append(row)
                selected_wallets.add(row.wallet)
                if len(selected_scores) >= self.config.top_wallets_to_copy:
                    break
        else:
            selected_scores = base_ranked[: self.config.top_wallets_to_copy]

        selected = [row.wallet for row in selected_scores]

        if not selected:
            self.logger.warning(
                "wallet-selector: no wallet passed filters (winrate/activity), falling back to configured wallets"
            )
            fallback = list(self.config.watched_wallets)
            return fallback, self._fallback_scores(fallback)

        for row in selected_scores:
            self.logger.info(
                "wallet-selector: wallet=%s score=%.4f dyn_score=%.4f win_rate=%.2f recent=%s dynamic=%s dynamic_share=%.2f notional=%.2f pnl=%.2f",
                row.wallet,
                row.score,
                row.dynamic_score,
                row.win_rate,
                row.recent_trades,
                row.dynamic_recent_trades,
                row.dynamic_share,
                row.recent_notional,
                row.pnl,
            )
        return selected, selected_scores

    def _recent_trade_metrics(self, wallet: str) -> tuple[int, int, float]:
        trades = self.activity_client.get_trades(wallet=wallet, limit=self.config.recent_trades_limit_per_wallet)
        cutoff = int(time.time()) - (self.config.recent_trade_lookback_hours * 3600)
        recent = 0
        dynamic_recent = 0
        recent_notional = 0.0
        for trade in trades:
            timestamp = int(_to_float(trade.get("timestamp")))
            if timestamp < cutoff:
                continue
            recent += 1
            size = abs(_to_float(trade.get("size")))
            price = abs(_to_float(trade.get("price")))
            recent_notional += size * price
            if is_dynamic_market(
                title=str(trade.get("title") or ""),
                slug=str(trade.get("slug") or trade.get("eventSlug") or ""),
                category=str(trade.get("category") or ""),
                keywords=self.config.dynamic_keywords,
            ):
                dynamic_recent += 1
        return recent, dynamic_recent, recent_notional

    def _dynamic_fallback_candidates(self, *, exclude_wallets: set[str]) -> list[WalletScore]:
        leaderboard = self.activity_client.get_leaderboard(
            category=self.config.dynamic_leaderboard_category,
            time_period=self.config.dynamic_leaderboard_time_period,
            limit=self.config.leaderboard_candidate_limit,
        )
        if not leaderboard:
            return []

        raw_rows: list[dict[str, float | int | str]] = []
        max_positive_pnl = 1.0
        max_recent_notional = 1.0
        for item in leaderboard:
            wallet = str(item.get("proxyWallet") or "").strip().lower()
            if not wallet or wallet in exclude_wallets:
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

            recent_trades, dynamic_recent_trades, recent_notional = self._recent_trade_metrics(wallet)
            if recent_trades < self.config.min_recent_trades:
                continue
            dynamic_share = dynamic_recent_trades / max(recent_trades, 1)
            if dynamic_recent_trades < self.config.min_dynamic_recent_trades:
                continue
            if dynamic_share < self.config.min_dynamic_trade_share:
                continue

            pnl = _to_float(item.get("pnl"))
            max_positive_pnl = max(max_positive_pnl, max(pnl, 0.0))
            max_recent_notional = max(max_recent_notional, recent_notional)
            raw_rows.append(
                {
                    "wallet": wallet,
                    "win_rate": win_rate,
                    "recent_trades": recent_trades,
                    "dynamic_recent_trades": dynamic_recent_trades,
                    "dynamic_share": dynamic_share,
                    "recent_notional": recent_notional,
                    "pnl": pnl,
                }
            )

        candidates: list[WalletScore] = []
        for row in raw_rows:
            win_rate = float(row["win_rate"])
            recent_trades = int(row["recent_trades"])
            dynamic_recent_trades = int(row["dynamic_recent_trades"])
            dynamic_share = float(row["dynamic_share"])
            recent_notional = float(row["recent_notional"])
            pnl = float(row["pnl"])

            pnl_score = min(max(pnl, 0.0) / max_positive_pnl, 1.0)
            freq_denominator = max(self.config.min_recent_trades * 3, 1)
            frequency_score = min(recent_trades / freq_denominator, 1.0)
            notional_score = min(recent_notional / max_recent_notional, 1.0)
            dyn_denominator = max(self.config.min_dynamic_recent_trades * 3, 1)
            dynamic_activity_score = min(dynamic_recent_trades / dyn_denominator, 1.0)
            base_score = (0.55 * win_rate) + (0.20 * frequency_score) + (0.15 * notional_score) + (0.10 * pnl_score)
            dynamic_score = (
                (0.40 * win_rate)
                + (0.20 * dynamic_activity_score)
                + (0.20 * dynamic_share)
                + (0.15 * notional_score)
                + (0.05 * frequency_score)
            )
            candidates.append(
                WalletScore(
                    wallet=str(row["wallet"]),
                    win_rate=win_rate,
                    recent_trades=recent_trades,
                    dynamic_recent_trades=dynamic_recent_trades,
                    dynamic_share=dynamic_share,
                    recent_notional=recent_notional,
                    pnl=pnl,
                    score=base_score,
                    dynamic_score=dynamic_score,
                )
            )
        candidates.sort(
            key=lambda row: (
                row.dynamic_score,
                row.win_rate,
                row.dynamic_recent_trades,
                row.recent_notional,
            ),
            reverse=True,
        )
        return candidates

    def _fallback_scores(self, wallets: list[str]) -> list[WalletScore]:
        return [
            WalletScore(
                wallet=wallet,
                win_rate=0.0,
                recent_trades=0,
                dynamic_recent_trades=0,
                dynamic_share=0.0,
                recent_notional=0.0,
                pnl=0.0,
                score=0.0,
                dynamic_score=0.0,
            )
            for wallet in wallets
        ]


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
