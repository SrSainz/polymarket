from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.market_expiry import is_market_expired


def test_market_expired_for_old_date_only() -> None:
    old_date = (datetime.now(timezone.utc) - timedelta(days=2)).date().isoformat()
    assert is_market_expired(old_date, grace_hours=0)


def test_market_not_expired_for_today_date_only() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    assert not is_market_expired(today, grace_hours=0)


def test_market_not_expired_for_future_datetime() -> None:
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    assert not is_market_expired(future, grace_hours=0)
