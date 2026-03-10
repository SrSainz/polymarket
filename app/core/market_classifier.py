from __future__ import annotations

import re


def normalize_market_text(*parts: str) -> str:
    raw = " ".join(part or "" for part in parts).strip().lower()
    if not raw:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", raw).strip()


def matches_market_keywords(*, title: str, slug: str, category: str = "", event_slug: str = "", keywords: list[str]) -> bool:
    if not keywords:
        return False

    haystack = normalize_market_text(title, slug, category, event_slug)
    if not haystack:
        return False

    for raw_keyword in keywords:
        keyword = normalize_market_text(raw_keyword or "")
        if keyword and keyword in haystack:
            return True
    return False


def is_btc5m_market(*, title: str, slug: str, category: str = "", event_slug: str = "") -> bool:
    haystack = normalize_market_text(title, slug, category, event_slug)
    if not haystack:
        return False

    has_btc = "btc" in haystack or "bitcoin" in haystack
    has_five_minute_window = any(
        marker in haystack
        for marker in (
            "5m",
            "5 min",
            "5 mins",
            "5 minute",
            "5 minutes",
            "next 5 minute",
            "next 5 minutes",
        )
    )
    has_direction = any(
        marker in haystack
        for marker in (
            "up or down",
            "updown",
            "up down",
        )
    )
    return has_btc and has_five_minute_window and has_direction


def is_dynamic_market(
    *,
    title: str,
    slug: str,
    category: str,
    keywords: list[str],
    event_slug: str = "",
) -> bool:
    return matches_market_keywords(
        title=title,
        slug=slug,
        category=category,
        event_slug=event_slug,
        keywords=keywords,
    )
