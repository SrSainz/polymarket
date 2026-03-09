from __future__ import annotations

import logging
import time

from app.models import NormalizedSignal, SignalAction, SourcePosition
from app.core.tracker import SourceTracker
from app.core.wallet_selector import WalletSelector
from app.db import Database
from app.services.detect_changes import DetectChangesService
from app.settings import BotConfig


class SyncWalletsService:
    def __init__(
        self,
        db: Database,
        tracker: SourceTracker,
        wallet_selector: WalletSelector,
        detect_changes_service: DetectChangesService,
        config: BotConfig,
        logger: logging.Logger,
    ) -> None:
        self.db = db
        self.tracker = tracker
        self.wallet_selector = wallet_selector
        self.detect_changes_service = detect_changes_service
        self.config = config
        self.logger = logger

    def run(self) -> dict[str, int]:
        inserted_signals = 0
        snapshots = 0

        run_id = str(int(time.time()))
        previous_wallets = set(self.db.list_source_wallets())
        active_wallets = self.wallet_selector.resolve_wallets()
        self.db.replace_selected_wallets(self.wallet_selector.get_last_selection_rows())

        for wallet in active_wallets:
            previous = self.db.get_source_positions(wallet)
            current_positions = self.tracker.fetch_wallet_positions(wallet)
            current_map = {position.asset: position for position in current_positions}

            signals = self.detect_changes_service.run(
                wallet=wallet,
                previous=previous,
                current=current_map,
            )

            self.db.replace_source_positions(wallet, current_positions, run_id)
            snapshots += len(current_positions)

            for signal in signals:
                if self.db.insert_signal(signal):
                    inserted_signals += 1

            self.logger.info(
                "wallet=%s previous_positions=%s current_positions=%s signals=%s",
                wallet,
                len(previous),
                len(current_positions),
                len(signals),
            )

        rebalance_signals, dropped_wallets = self._rebalance_removed_wallets(
            previous_wallets=previous_wallets,
            active_wallets=active_wallets,
            run_id=run_id,
        )
        inserted_signals += rebalance_signals

        return {
            "signals": inserted_signals,
            "snapshots": snapshots,
            "wallets": len(active_wallets),
            "dropped_wallets": dropped_wallets,
            "rebalance_signals": rebalance_signals,
        }

    def _rebalance_removed_wallets(
        self,
        *,
        previous_wallets: set[str],
        active_wallets: list[str],
        run_id: str,
    ) -> tuple[int, int]:
        dropped_wallets = sorted(previous_wallets - set(active_wallets))
        if not dropped_wallets:
            return 0, 0

        active_asset_sizes: dict[str, float] = {}
        for wallet in active_wallets:
            for position in self.db.get_source_positions(wallet).values():
                active_asset_sizes[position.asset] = active_asset_sizes.get(position.asset, 0.0) + position.size

        dropped_asset_sizes: dict[str, float] = {}
        dropped_asset_meta: dict[str, SourcePosition] = {}
        for wallet in dropped_wallets:
            dropped_positions = self.db.get_source_positions(wallet)
            for asset, position in dropped_positions.items():
                dropped_asset_sizes[asset] = dropped_asset_sizes.get(asset, 0.0) + position.size
                dropped_asset_meta.setdefault(asset, position)

        now_ts = int(time.time())
        inserted_signals = 0
        for asset, dropped_size in dropped_asset_sizes.items():
            if dropped_size <= self.config.noise_threshold_shares:
                continue

            active_size = active_asset_sizes.get(asset, 0.0)
            prev_size = active_size + dropped_size
            if prev_size <= self.config.noise_threshold_shares:
                continue

            if active_size <= self.config.noise_threshold_shares:
                action = SignalAction.CLOSE
                new_size = 0.0
            else:
                action = SignalAction.REDUCE
                new_size = active_size

            meta = dropped_asset_meta[asset]
            signal = NormalizedSignal(
                event_key=f"rebalance:{run_id}:{asset}:{action.value}:{prev_size:.6f}:{new_size:.6f}",
                wallet="rebalance",
                asset=asset,
                condition_id=meta.condition_id,
                action=action,
                prev_size=prev_size,
                new_size=new_size,
                delta_size=new_size - prev_size,
                reference_price=meta.current_price or meta.avg_price or 0.5,
                title=meta.title,
                slug=meta.slug,
                outcome=meta.outcome,
                category=meta.category,
                detected_at=now_ts,
            )
            if self.db.insert_signal(signal):
                inserted_signals += 1

        for wallet in dropped_wallets:
            self.db.delete_source_wallet_positions(wallet)

        self.logger.info(
            "rebalance dropped_wallets=%s rebalance_signals=%s",
            len(dropped_wallets),
            inserted_signals,
        )
        return inserted_signals, len(dropped_wallets)
