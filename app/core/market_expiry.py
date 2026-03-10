from __future__ import annotations

from datetime import datetime, timedelta, timezone


def is_market_expired(end_date: str, *, grace_hours: int = 0) -> bool:
    cutoff = _parse_end_date_to_cutoff(end_date)
    if cutoff is None:
        return False
    cutoff = cutoff + timedelta(hours=max(grace_hours, 0))
    return datetime.now(timezone.utc) >= cutoff


def _parse_end_date_to_cutoff(raw: str) -> datetime | None:
    value = (raw or "").strip()
    if not value:
        return None

    # Date-only values are treated as valid through the full UTC day.
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        try:
            date_point = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return date_point + timedelta(days=1)
        except ValueError:
            return None

    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
