from __future__ import annotations

from app.polymarket.clob_client import _extract_balance_value


def test_extract_balance_value_converts_micro_usdc_integer_strings() -> None:
    payload = {
        "balance": "4130218",
        "allowance": "4000000",
    }

    assert _extract_balance_value(payload, "balance") == 4.130218
    assert _extract_balance_value(payload, "allowance") == 4.0


def test_extract_balance_value_keeps_decimal_usdc_strings() -> None:
    payload = {
        "balance": "4.130218",
        "allowance": "4.000000",
    }

    assert _extract_balance_value(payload, "balance") == 4.130218
    assert _extract_balance_value(payload, "allowance") == 4.0


def test_extract_balance_value_converts_integer_payloads() -> None:
    payload = {
        "balance": 4130218,
        "allowance": 0,
    }

    assert _extract_balance_value(payload, "balance") == 4.130218
    assert _extract_balance_value(payload, "allowance") == 0.0
