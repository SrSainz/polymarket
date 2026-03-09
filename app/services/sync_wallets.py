from __future__ import annotations

import logging
import time

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
        active_wallets = self.wallet_selector.resolve_wallets()
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

        return {"signals": inserted_signals, "snapshots": snapshots, "wallets": len(active_wallets)}
