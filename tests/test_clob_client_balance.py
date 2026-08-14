from __future__ import annotations

from inspect import signature
import sys
import types

import pytest

import app.polymarket.clob_client as clob_client_module
import app.polymarket.auth as auth_module
from app.polymarket.clob_client import _extract_balance_value
from app.polymarket.clob_client import CLOBClient
from app.settings import EnvSettings


def test_clob_v2_sdk_surface_matches_adapter_contract() -> None:
    from py_clob_client_v2 import (  # noqa: PLC0415
        ApiCreds,
        ClobClient,
        MarketOrderArgs,
        OrderArgs,
        OrderPayload,
        OrderType,
        Side,
    )

    assert hasattr(OrderType, "FOK")
    assert hasattr(OrderType, "GTC")
    assert hasattr(Side, "BUY")
    assert hasattr(Side, "SELL")
    assert ApiCreds(api_key="k", api_secret="s", api_passphrase="p").api_key == "k"
    market_parameters = signature(ClobClient.create_and_post_market_order).parameters
    limit_parameters = signature(ClobClient.create_and_post_order).parameters
    open_order_parameters = signature(ClobClient.get_open_orders).parameters
    assert {"order_args", "order_type"}.issubset(market_parameters)
    assert {"order_args", "order_type", "post_only"}.issubset(limit_parameters)
    assert "only_first_page" in open_order_parameters
    assert "orderID" in signature(OrderPayload).parameters
    assert "token_id" in signature(MarketOrderArgs).parameters
    assert "token_id" in signature(OrderArgs).parameters


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
        self.market_orders: list[dict] = []
        self.cancelled: list[str] = []

    def create_and_post_market_order(self, order_args, *, order_type, defer_exec=False):  # noqa: ANN001
        self.market_orders.append(
            {
                "token_id": getattr(order_args, "token_id", ""),
                "amount": getattr(order_args, "amount", 0.0),
                "price": getattr(order_args, "price", 0.0),
                "side": getattr(order_args, "side", ""),
                "order_type": getattr(order_args, "order_type", ""),
                "order_type": order_type,
                "defer_exec": defer_exec,
            }
        )
        return {"orderID": "market-1", "status": "live"}

    def create_and_post_order(self, order_args, *, order_type, post_only=False):  # noqa: ANN001
        self.limit_orders.append(
            {
                "token_id": getattr(order_args, "token_id", ""),
                "price": getattr(order_args, "price", 0.0),
                "size": getattr(order_args, "size", 0.0),
                "side": getattr(order_args, "side", ""),
                "order_type": order_type,
                "post_only": post_only,
            }
        )
        return {"orderID": "limit-1", "status": "live"}

    def cancel_order(self, payload) -> dict[str, list[str]]:  # noqa: ANN001
        order_id = str(getattr(payload, "orderID", ""))
        self.cancelled.append(order_id)
        return {"canceled": [order_id]}

    def get_open_orders(self, params=None, *, only_first_page=False):  # noqa: ANN001, ARG002
        market = getattr(params, "market", "") if params is not None else ""
        asset_id = getattr(params, "asset_id", "") if params is not None else ""
        return [
            {
                "id": "order-1",
                "status": "live",
                "market": str(market or ""),
                "asset_id": str(asset_id or ""),
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
    v2_package = types.ModuleType("py_clob_client_v2")
    v2_package.OrderArgs = lambda **kwargs: types.SimpleNamespace(**kwargs)
    v2_package.MarketOrderArgs = lambda **kwargs: types.SimpleNamespace(**kwargs)
    v2_package.OrderType = types.SimpleNamespace(GTC="GTC", FOK="FOK")
    v2_package.Side = types.SimpleNamespace(BUY="BUY", SELL="SELL")
    v2_package.OrderPayload = lambda **kwargs: types.SimpleNamespace(**kwargs)
    v2_package.OpenOrderParams = lambda **kwargs: types.SimpleNamespace(**kwargs)
    v2_package.OrderMarketCancelParams = lambda **kwargs: types.SimpleNamespace(**kwargs)
    v2_package.AssetType = types.SimpleNamespace(COLLATERAL="COLLATERAL")
    v2_package.BalanceAllowanceParams = lambda **kwargs: types.SimpleNamespace(**kwargs)
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", v2_package)
    return client


def test_place_limit_order_uses_authenticated_client(_patch_py_clob: _FakeAuthenticatedClient) -> None:
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(live_trading=True))

    response = client.place_limit_order(token_id="asset-1", side="BUY", price=0.42, size=10.0, order_type="GTC", post_only=True)

    assert response["orderID"] == "limit-1"
    assert _patch_py_clob.limit_orders[0]["token_id"] == "asset-1"
    assert _patch_py_clob.limit_orders[0]["post_only"] is True


def test_place_market_order_uses_v2_market_order_contract(_patch_py_clob: _FakeAuthenticatedClient) -> None:
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(live_trading=True))

    response = client.place_market_order(
        "asset-1",
        "BUY",
        size=10.0,
        notional=2.5,
        limit_price=0.42,
        order_type="FOK",
    )

    assert response["orderID"] == "market-1"
    assert _patch_py_clob.market_orders[0]["token_id"] == "asset-1"
    assert _patch_py_clob.market_orders[0]["amount"] == 2.5
    assert _patch_py_clob.market_orders[0]["order_type"] == "FOK"


def test_auth_v2_derives_and_sets_api_credentials_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeClient:
        def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.args = args
            self.kwargs = kwargs
            self.set_values = []

        def create_or_derive_api_key(self):
            return "derived-creds"

        def set_api_creds(self, value):  # noqa: ANN001
            self.set_values.append(value)

    fake_package = types.ModuleType("py_clob_client_v2")
    fake_package.ApiCreds = lambda **kwargs: types.SimpleNamespace(**kwargs)
    fake_package.ClobClient = FakeClient
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_package)

    env = EnvSettings(live_trading=True, polymarket_private_key="private-key")
    client = auth_module.build_authenticated_clob_client(env)

    assert client.set_values == ["derived-creds"]
    assert client.kwargs["chain_id"] == 137


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


def test_get_fee_rate_bps_preserves_a_valid_zero_fee(monkeypatch: pytest.MonkeyPatch) -> None:
    client = CLOBClient("https://clob.polymarket.com", EnvSettings(live_trading=False))

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, int]:
            return {"base_fee": 0}

    monkeypatch.setattr(client.session, "get", lambda *_args, **_kwargs: _Response())

    assert client.get_fee_rate_bps("asset-free") == 0.0
