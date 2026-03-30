from __future__ import annotations

from datetime import date

from app.polymarket.fee_model import effective_taker_fee_rate, fee_cost_usdc, fee_per_share, taker_fee_exponent


def test_crypto_fee_model_before_switch_matches_current_curve() -> None:
    assert taker_fee_exponent(category="crypto", as_of=date(2026, 3, 29)) == 2.0
    effective_rate = effective_taker_fee_rate(
        fee_rate_bps=2500.0,
        price=0.40,
        category="crypto",
        as_of=date(2026, 3, 29),
    )
    assert round(effective_rate * 10_000, 2) == 144.0
    assert round(fee_per_share(
        fee_rate_bps=2500.0,
        price=0.40,
        category="crypto",
        as_of=date(2026, 3, 29),
    ), 6) == 0.00576


def test_crypto_fee_model_after_switch_matches_public_curve() -> None:
    assert taker_fee_exponent(category="crypto", as_of=date(2026, 3, 30)) == 1.0
    effective_rate = effective_taker_fee_rate(
        fee_rate_bps=720.0,
        price=0.40,
        category="crypto",
        as_of=date(2026, 3, 30),
    )
    assert round(effective_rate * 10_000, 2) == 172.8
    assert round(fee_per_share(
        fee_rate_bps=720.0,
        price=0.40,
        category="crypto",
        as_of=date(2026, 3, 30),
    ), 6) == 0.006912
    assert round(fee_cost_usdc(
        size=100.0,
        price=0.40,
        fee_rate_bps=720.0,
        category="crypto",
        as_of=date(2026, 3, 30),
    ), 4) == 0.6912


def test_crypto_fee_model_after_switch_normalizes_fee_rate_endpoint_base() -> None:
    explicit_rate = effective_taker_fee_rate(
        fee_rate_bps=720.0,
        price=0.50,
        category="crypto",
        as_of=date(2026, 3, 30),
    )
    endpoint_rate = effective_taker_fee_rate(
        fee_rate_bps=1000.0,
        price=0.50,
        category="crypto",
        as_of=date(2026, 3, 30),
    )

    assert round(explicit_rate * 10_000, 2) == 180.0
    assert endpoint_rate == explicit_rate
