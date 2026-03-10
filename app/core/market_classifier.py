from __future__ import annotations


def is_dynamic_market(
    *,
    title: str,
    slug: str,
    category: str,
    keywords: list[str],
) -> bool:
    if not keywords:
        return False

    haystack = " ".join([title or "", slug or "", category or ""]).strip().lower()
    if not haystack:
        return False

    for raw_keyword in keywords:
        keyword = (raw_keyword or "").strip().lower()
        if keyword and keyword in haystack:
            return True
    return False
