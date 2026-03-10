from __future__ import annotations

import logging
import time

from app.core.market_expiry import is_market_expired, is_market_within_horizon
from app.models import SourcePosition
from app.polymarket.activity_client import ActivityClient
from app.polymarket.gamma_client import GammaClient
from app.settings import BotConfig


class SourceTracker:
    def __init__(
        self,
        activity_client: ActivityClient,
        gamma_client: GammaClient,
        config: BotConfig,
        logger: logging.Logger,
    ) -> None:
        self.activity_client = activity_client
        self.gamma_client = gamma_client
        self.config = config
        self.logger = logger

    def fetch_wallet_positions(self, wallet: str) -> list[SourcePosition]:
        raw_positions = self.activity_client.get_positions(wallet)
        observed_at = int(time.time())

        normalized: list[SourcePosition] = []
        skipped_expired = 0
        skipped_long_horizon = 0
        for item in raw_positions:
            size = _to_float(item.get("size"))
            if size <= 0:
                continue

            end_date = str(item.get("endDate") or "")
            if self.config.skip_expired_source_positions and is_market_expired(
                end_date,
                grace_hours=self.config.expired_market_grace_hours,
            ):
                skipped_expired += 1
                continue

            slug = str(item.get("slug") or "")
            event_slug = str(item.get("eventSlug") or "")
            title = str(item.get("title") or "")
            if self.config.short_horizon_only and not _matches_forced_keywords(
                title=title,
                slug=slug,
                event_slug=event_slug,
                keywords=self.config.forced_include_market_keywords,
            ):
                if not is_market_within_horizon(end_date, max_horizon_days=self.config.max_market_horizon_days):
                    skipped_long_horizon += 1
                    continue

            category = self.gamma_client.get_category(slug) if slug else ""

            avg_price = _to_float(item.get("avgPrice"))
            raw_current_price = item.get("curPrice")
            current_price = _to_float(raw_current_price)
            if _is_missing(raw_current_price):
                current_price = avg_price if avg_price > 0 else 0.5
            elif current_price < 0:
                current_price = avg_price if avg_price > 0 else 0.5
            if avg_price <= 0:
                avg_price = current_price if current_price > 0 else 0.5

            normalized.append(
                SourcePosition(
                    wallet=wallet,
                    asset=str(item.get("asset") or ""),
                    condition_id=str(item.get("conditionId") or ""),
                    size=size,
                    avg_price=avg_price,
                    current_price=current_price,
                    title=title,
                    slug=slug,
                    outcome=str(item.get("outcome") or ""),
                    category=category,
                    observed_at=observed_at,
                )
            )

        self.logger.info(
            "wallet=%s positions_fetched=%s skipped_expired=%s skipped_long_horizon=%s",
            wallet,
            len(normalized),
            skipped_expired,
            skipped_long_horizon,
        )
        return normalized


def _to_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _matches_forced_keywords(*, title: str, slug: str, event_slug: str, keywords: list[str]) -> bool:
    if not keywords:
        return False
    haystack = " ".join([title or "", slug or "", event_slug or ""]).strip().lower()
    if not haystack:
        return False
    for raw_keyword in keywords:
        keyword = (raw_keyword or "").strip().lower()
        if keyword and keyword in haystack:
            return True
    return False
