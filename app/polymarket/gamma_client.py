from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class GammaClient:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self._category_cache: dict[str, str] = {}

        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get_market_by_slug(self, slug: str) -> dict[str, Any] | None:
        if not slug:
            return None
        response = self.session.get(
            f"{self.base_url}/markets",
            params={"slug": slug, "limit": 1},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            return None
        return payload[0]

    def get_category(self, slug: str) -> str:
        if not slug:
            return ""
        if slug in self._category_cache:
            return self._category_cache[slug]

        market = self.get_market_by_slug(slug)
        if not market:
            self._category_cache[slug] = ""
            return ""

        category = (market.get("category") or "").strip().lower()
        if not category:
            events = market.get("events") or []
            if events:
                category = (events[0].get("category") or "").strip().lower()

        self._category_cache[slug] = category
        return category

    def get_tags(self, slug: str) -> list[str]:
        category = self.get_category(slug)
        if not category:
            return []
        parts = [item for item in category.replace("_", "-").split("-") if item]
        return list(dict.fromkeys([category, *parts]))
