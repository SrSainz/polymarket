from __future__ import annotations

import logging
import time

from app.models import SourcePosition
from app.polymarket.activity_client import ActivityClient
from app.polymarket.gamma_client import GammaClient


class SourceTracker:
    def __init__(
        self,
        activity_client: ActivityClient,
        gamma_client: GammaClient,
        logger: logging.Logger,
    ) -> None:
        self.activity_client = activity_client
        self.gamma_client = gamma_client
        self.logger = logger

    def fetch_wallet_positions(self, wallet: str) -> list[SourcePosition]:
        raw_positions = self.activity_client.get_positions(wallet)
        observed_at = int(time.time())

        normalized: list[SourcePosition] = []
        for item in raw_positions:
            size = _to_float(item.get("size"))
            if size <= 0:
                continue

            slug = str(item.get("slug") or "")
            category = self.gamma_client.get_category(slug) if slug else ""

            avg_price = _to_float(item.get("avgPrice"))
            current_price = _to_float(item.get("curPrice"))
            if current_price <= 0:
                current_price = avg_price if avg_price > 0 else 0.5
            if avg_price <= 0:
                avg_price = current_price

            normalized.append(
                SourcePosition(
                    wallet=wallet,
                    asset=str(item.get("asset") or ""),
                    condition_id=str(item.get("conditionId") or ""),
                    size=size,
                    avg_price=avg_price,
                    current_price=current_price,
                    title=str(item.get("title") or ""),
                    slug=slug,
                    outcome=str(item.get("outcome") or ""),
                    category=category,
                    observed_at=observed_at,
                )
            )

        self.logger.info("wallet=%s positions_fetched=%s", wallet, len(normalized))
        return normalized


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
