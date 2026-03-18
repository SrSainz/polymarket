from __future__ import annotations

import json
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_EVENT_CACHE_TTL_SECONDS = 300.0
_INCOMPLETE_EVENT_CACHE_TTL_SECONDS = 2.0


class GammaClient:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self._category_cache: dict[str, str] = {}
        self._event_cache: dict[str, tuple[dict[str, Any], float]] = {}

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
        market = self._get_market_by_slug_direct(slug)
        if market is None:
            market = self._get_market_by_slug_list(slug)
        if market is None:
            return None
        return self._hydrate_market_event(market)

    def get_event_by_id(self, event_id: str) -> dict[str, Any] | None:
        event_key = str(event_id or "").strip()
        if not event_key:
            return None
        cached_entry = self._event_cache.get(event_key)
        if cached_entry is not None:
            cached_payload, cached_expires_at = cached_entry
            if cached_expires_at > time.time():
                return dict(cached_payload)
        response = self.session.get(
            f"{self.base_url}/events/{event_key}",
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        event_payload = dict(payload)
        ttl_seconds = (
            _EVENT_CACHE_TTL_SECONDS
            if self._event_has_official_metadata(event_payload)
            else _INCOMPLETE_EVENT_CACHE_TTL_SECONDS
        )
        self._event_cache[event_key] = (event_payload, time.time() + ttl_seconds)
        return dict(event_payload)

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

    def _get_market_by_slug_direct(self, slug: str) -> dict[str, Any] | None:
        try:
            response = self.session.get(
                f"{self.base_url}/markets/slug/{slug}",
                timeout=self.timeout,
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
        except requests.HTTPError:
            raise
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        return payload

    def _get_market_by_slug_list(self, slug: str) -> dict[str, Any] | None:
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

    def _hydrate_market_event(self, market: dict[str, Any]) -> dict[str, Any]:
        hydrated_market = dict(market)
        raw_events = hydrated_market.get("events") or []
        if not isinstance(raw_events, list) or not raw_events:
            return hydrated_market

        hydrated_events: list[Any] = []
        changed = False
        for raw_event in raw_events:
            if not isinstance(raw_event, dict):
                hydrated_events.append(raw_event)
                continue
            event_copy = dict(raw_event)
            if self._event_has_official_metadata(event_copy):
                hydrated_events.append(event_copy)
                continue
            event_id = str(event_copy.get("id") or "").strip()
            if not event_id:
                hydrated_events.append(event_copy)
                continue
            try:
                event_payload = self.get_event_by_id(event_id)
            except requests.HTTPError:
                hydrated_events.append(event_copy)
                continue
            if not isinstance(event_payload, dict):
                hydrated_events.append(event_copy)
                continue
            if event_payload.get("eventMetadata") not in (None, "", {}):
                event_copy["eventMetadata"] = event_payload.get("eventMetadata")
                changed = True
            if not event_copy.get("resolutionSource") and event_payload.get("resolutionSource"):
                event_copy["resolutionSource"] = event_payload.get("resolutionSource")
                changed = True
            hydrated_events.append(event_copy)

        if changed:
            hydrated_market["events"] = hydrated_events
        return hydrated_market

    def _event_has_official_metadata(self, event_payload: dict[str, Any]) -> bool:
        metadata = event_payload.get("eventMetadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        if not isinstance(metadata, dict):
            return False
        try:
            return float(metadata.get("priceToBeat") or 0.0) > 0
        except (TypeError, ValueError):
            return False
