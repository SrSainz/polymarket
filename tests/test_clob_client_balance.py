from __future__ import annotations

import sys
import types

import pytest

import app.polymarket.clob_client as clob_client_module
from app.polymarket.clob_client import _extract_balance_value
from app.polymarket.clob_client import CLOBClient
from app.settings import EnvSettings


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


class _FakeAuthenticatedClient:
    def __init__(self) -> None:
        self.limit_orders: list[dict] = []
        self.cancelled: list[str] = []

    def create_and_post_order(self, order_args, **kwargs):  # noqa: ANN001
        self.limit_orders.append(
            {
                "token_id": getattr(order_args, "token_id", ""),
                "price": getattr(order_args, "price", 0.0),
                "size": getattr(order_args, "size", 0.0),
                "side": getattr(order_args, "side", ""),
                **kwargs,
            }
        )
        return {"orderID": "limit-1", "status": "live"}

    def cancel_order(self, order_id: str) -> dict[str, list[str]]:
        self.cancelled.append(order_id)
        return {"canceled": [order_id]}

    def get_open_orders(self, params=None, include_fills=True):  # noqa: ANN001, ARG002
        return [
            {
                "id": "order-1",
                "status": "live",
                "market": str((params or {}).get("market") or ""),
                "asset_id": str((params or {}).get("asset_id") or ""),
                "side": "BUY",
                "size": "10",
                "matched_amount": "2",
                "price": "0.42",
                "type": "GTC",
            }
        ]


@pytest.fixture
def _patch_py_clob(monkeypatch: pytest.MonkeyPatch) -> _FakeAuthenticatedClient:
    client = _FakeAuthenticatedClient()
    monkeypatch.setattr(clob_client_module, "build_authenticated_clob_client", lambda _env: client)
    py_clob_package = types.ModuleType("py_clob_client")
    clob_types_module = types.ModuleType("py_clob_client.clob_types")
    clob_types_module.OrderArgs = lambda **kwargs: types.SimpleNamespace(**kwargs)
    clob_types_module.OrderType = types.SimpleNamespace(GTC="GTC")
    order_builder_package = types.ModuleType("py_clob_client.order_builder")
    constants_module = types.ModuleType("py_clob_client.order_builder.constants")
    constants_module.BUY = "BUY"
    constants_module.SELL = "SELL"
    monkeypatch.setitem(sys.modules, "py_clob_client", py_clob_package)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types_module)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder", order_builder_package)
    monkeypatch.setitem(sys.modules, "py_clob_client.order_builder.constants", constants_module)
    return client


def test_place_limit_order_uses_authenticated_client(_patch_py_clob: _FakeAuthenticatedClient) -> None:
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(live_trading=True))

    response = client.place_limit_order(token_id="asset-1", side="BUY", price=0.42, size=10.0, order_type="GTC", post_only=True)

    assert response["orderID"] == "limit-1"
    assert _patch_py_clob.limit_orders[0]["token_id"] == "asset-1"
    assert _patch_py_clob.limit_orders[0]["postOnly"] is True


def test_cancel_order_and_list_open_orders_normalize_payload(_patch_py_clob: _FakeAuthenticatedClient) -> None:
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(live_trading=True))

    canceled = client.cancel_order("order-1")
    rows = client.list_open_orders(market="btc-updown", asset_id="asset-1")

    assert canceled == {"canceled": ["order-1"]}
    assert rows == [
        {
            "id": "order-1",
            "status": "live",
            "market": "btc-updown",
            "asset_id": "asset-1",
            "side": "BUY",
            "original_size": 10.0,
            "size_matched": 2.0,
            "price": 0.42,
            "created_at": 0,
            "order_type": "GTC",
        }
    ]


def test_get_fee_rate_bps_uses_public_fee_rate_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(live_trading=False))

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, int]:
            return {"base_fee": 2500}

    captured: list[dict[str, object]] = []

    def _fake_get(url: str, *, params=None, timeout=None):  # noqa: ANN001
        captured.append({"url": url, "params": params, "timeout": timeout})
        return _Response()

    monkeypatch.setattr(client.session, "get", _fake_get)

    fee_bps = client.get_fee_rate_bps("asset-1")

    assert fee_bps == 2500.0
    assert captured == [
        {
            "url": "https://clob.polymarket.com/fee-rate",
            "params": {"token_id": "asset-1"},
            "timeout": 15,
        }
    ]
