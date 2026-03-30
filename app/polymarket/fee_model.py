from __future__ import annotations

from datetime import date, datetime, timezone

_CRYPTO_FEE_SCHEDULE_SWITCH = date(2026, 3, 30)
_CRYPTO_FEE_RATE_V2_BPS = 720.0
_CRYPTO_FEE_RATE_V2_ENDPOINT_BASE_BPS = 1000.0


def _schedule_date(as_of: date | datetime | None = None) -> date:
    if isinstance(as_of, datetime):
        if as_of.tzinfo is None:
            return as_of.date()
        return as_of.astimezone(timezone.utc).date()
    if isinstance(as_of, date):
        return as_of
    return datetime.now(timezone.utc).date()


def normalize_market_category(category: str) -> str:
    normalized = str(category or "").strip().lower().replace("_", "-")
    if not normalized:
        return ""
    if normalized.startswith("crypto"):
        return "crypto"
    if normalized.startswith("sports"):
        return "sports"
    if normalized.startswith("politic"):
        return "politics"
    if normalized.startswith("finance"):
        return "finance"
    if normalized.startswith("economic"):
        return "economics"
    if normalized.startswith("weather"):
        return "weather"
    if normalized.startswith("culture"):
        return "culture"
    if normalized.startswith("tech"):
        return "tech"
    if normalized.startswith("other"):
        return "other"
    return normalized


def taker_fee_exponent(*, category: str, as_of: date | datetime | None = None) -> float:
    normalized = normalize_market_category(category)
    schedule_date = _schedule_date(as_of)
    if schedule_date < _CRYPTO_FEE_SCHEDULE_SWITCH:
        if normalized == "crypto":
            return 2.0
        if normalized == "sports":
            return 1.0
        return 2.0

    upcoming = {
        "crypto": 1.0,
        "sports": 1.0,
        "politics": 1.0,
        "finance": 1.0,
        "tech": 1.0,
        "culture": 1.0,
        "economics": 0.5,
        "weather": 0.5,
        "other": 2.0,
    }
    return float(upcoming.get(normalized, 1.0))


def effective_taker_fee_rate(
    *,
    fee_rate_bps: float,
    price: float,
    category: str,
    as_of: date | datetime | None = None,
) -> float:
    normalized_category = normalize_market_category(category)
    raw_fee_rate_bps = max(float(fee_rate_bps or 0.0), 0.0)
    if (
        normalized_category == "crypto"
        and _schedule_date(as_of) >= _CRYPTO_FEE_SCHEDULE_SWITCH
        and abs(raw_fee_rate_bps - _CRYPTO_FEE_RATE_V2_ENDPOINT_BASE_BPS) <= 1e-9
    ):
        raw_fee_rate_bps = _CRYPTO_FEE_RATE_V2_BPS
    fee_rate = raw_fee_rate_bps / 10_000
    best_ask = max(min(float(price or 0.0), 1.0), 0.0)
    if fee_rate <= 0 or best_ask <= 0 or best_ask >= 1:
        return 0.0
    exponent = taker_fee_exponent(category=normalized_category, as_of=as_of)
    return fee_rate * max(best_ask * (1.0 - best_ask), 0.0) ** exponent


def fee_per_share(
    *,
    fee_rate_bps: float,
    price: float,
    category: str,
    as_of: date | datetime | None = None,
) -> float:
    best_ask = max(min(float(price or 0.0), 1.0), 0.0)
    effective_rate = effective_taker_fee_rate(
        fee_rate_bps=fee_rate_bps,
        price=best_ask,
        category=category,
        as_of=as_of,
    )
    if effective_rate <= 0 or best_ask <= 0:
        return 0.0
    return best_ask * effective_rate


def fee_cost_usdc(
    *,
    size: float,
    price: float,
    fee_rate_bps: float,
    category: str,
    as_of: date | datetime | None = None,
) -> float:
    shares = max(float(size or 0.0), 0.0)
    best_ask = max(min(float(price or 0.0), 1.0), 0.0)
    if shares <= 0 or best_ask <= 0:
        return 0.0
    effective_rate = effective_taker_fee_rate(
        fee_rate_bps=fee_rate_bps,
        price=best_ask,
        category=category,
        as_of=as_of,
    )
    return shares * best_ask * effective_rate
