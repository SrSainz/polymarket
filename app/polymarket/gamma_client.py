from __future__ import annotations

import json
import re
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_EVENT_CACHE_TTL_SECONDS = 300.0
_INCOMPLETE_EVENT_CACHE_TTL_SECONDS = 2.0
_PUBLIC_WEB_PRICE_CACHE_TTL_SECONDS = 20.0


class GammaClient:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self._category_cache: dict[str, str] = {}
        self._event_cache: dict[str, tuple[dict[str, Any], float]] = {}
        self._public_price_cache: dict[str, tuple[float, str, float]] = {}

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

    def get_public_price_to_beat(self, slug: str) -> tuple[float, str]:
        safe_slug = str(slug or "").strip()
        if not safe_slug:
            return 0.0, "public-gamma-missing"
        cached_entry = self._public_price_cache.get(safe_slug)
        if cached_entry is not None:
            cached_price, cached_source, cached_expires_at = cached_entry
            if cached_expires_at > time.time():
                return cached_price, cached_source

        market = self.get_market_by_slug(safe_slug)
        official = self._extract_price_to_beat(market)
        if official > 0:
            source = "public-gamma"
            self._public_price_cache[safe_slug] = (official, source, time.time() + _PUBLIC_WEB_PRICE_CACHE_TTL_SECONDS)
            return official, source

        official = self._get_event_page_open_price(safe_slug)
        if official > 0:
            source = "public-web"
            self._public_price_cache[safe_slug] = (official, source, time.time() + _PUBLIC_WEB_PRICE_CACHE_TTL_SECONDS)
            return official, source

        return 0.0, "public-gamma-missing"

    def get_tags(self, slug: str) -> list[str]:
        category = self.get_category(slug)
        if not category:
            return []
        parts = [item for item in category.replace("_", "-").split("-") if item]
        return list(dict.fromkeys([category, *parts]))

    def prefetch_next_btc5m_window(self, reference_slug: str) -> dict[str, Any] | None:
        safe_slug = str(reference_slug or "").strip()
        if not safe_slug.startswith("btc-updown-5m-"):
            return None
        suffix = safe_slug.rsplit("-", 1)[-1]
        if not suffix.isdigit():
            return None
        next_slug = f"btc-updown-5m-{int(suffix) + 300}"
        try:
            return self.get_market_by_slug(next_slug)
        except requests.RequestException:
            return None

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

    def _extract_price_to_beat(self, payload: object) -> float:
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return 0.0
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return 0.0
        if isinstance(payload, dict):
            for key in ("priceToBeat", "price_to_beat"):
                try:
                    value = float(payload.get(key) or 0.0)
                except (TypeError, ValueError):
                    value = 0.0
                if value > 0:
                    return value
            for nested_key in ("eventMetadata", "metadata", "marketMetadata", "event"):
                value = self._extract_price_to_beat(payload.get(nested_key))
                if value > 0:
                    return value
            for list_key in ("events", "markets"):
                raw_items = payload.get(list_key)
                if not isinstance(raw_items, list):
                    continue
                for item in raw_items:
                    value = self._extract_price_to_beat(item)
                    if value > 0:
                        return value
        return 0.0

    def _get_event_page_open_price(self, slug: str) -> float:
        safe_slug = str(slug or "").strip()
        if not safe_slug:
            return 0.0
        try:
            response = self.session.get(
                f"https://polymarket.com/event/{safe_slug}",
                timeout=self.timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
        except requests.RequestException:
            return 0.0
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text)
        if match is None:
            return 0.0
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            return 0.0
        queries = (((payload.get("props") or {}).get("pageProps") or {}).get("dehydratedState") or {}).get("queries") or []
        for query in queries:
            if not isinstance(query, dict):
                continue
            query_key = query.get("queryKey")
            if not isinstance(query_key, list) or len(query_key) < 6:
                continue
            if query_key[:3] != ["crypto-prices", "price", "BTC"]:
                continue
            if str(query_key[4] or "").strip().lower() != "fiveminute":
                continue
            data = ((query.get("state") or {}).get("data") or {})
            if not isinstance(data, dict):
                continue
            try:
                open_price = float(data.get("openPrice") or 0.0)
            except (TypeError, ValueError):
                open_price = 0.0
            if open_price > 0:
                return open_price
        return 0.0
