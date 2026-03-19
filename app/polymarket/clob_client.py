from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.polymarket.auth import build_authenticated_clob_client
from app.polymarket.market_feed import FeedStatus, MarketFeed
from app.settings import EnvSettings


class CLOBClient:
    def __init__(self, base_url: str, env: EnvSettings, timeout: int = 15, market_feed: MarketFeed | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.env = env
        self.market_feed = market_feed
        self.session = requests.Session()

        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def track_assets(self, token_ids: list[str] | tuple[str, ...]) -> None:
        if self.market_feed is None:
            return
        self.market_feed.ensure_assets(token_ids)

    def market_feed_status(self) -> FeedStatus:
        if self.market_feed is None:
            return FeedStatus(mode="rest-fallback", connected=False, tracked_assets=0, age_ms=0)
        return self.market_feed.status()

    def close(self) -> None:
        if self.market_feed is not None:
            self.market_feed.close()

    def get_midpoint(self, token_id: str) -> float | None:
        if self.market_feed is not None:
            midpoint = self.market_feed.get_midpoint(token_id)
            if midpoint is not None:
                return midpoint
        try:
            response = self.session.get(
                f"{self.base_url}/midpoint",
                params={"token_id": token_id},
                timeout=self.timeout,
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            payload = response.json()
            raw_mid = payload.get("mid")
            if raw_mid is None:
                return None
            return float(raw_mid)
        except requests.RequestException:
            return None

    def get_book(self, token_id: str) -> dict[str, Any]:
        if self.market_feed is not None:
            book = self.market_feed.get_book(token_id)
            if book:
                return book
        response = self.session.get(
            f"{self.base_url}/book",
            params={"token_id": token_id},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def get_collateral_balance(self) -> dict[str, float]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to fetch balances.")

        client = build_authenticated_clob_client(self.env)

        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
        except ImportError as error:
            raise RuntimeError("py-clob-client install is incomplete for balance queries.") from error

        asset_type = getattr(AssetType, "COLLATERAL", "COLLATERAL")
        params = _build_balance_params(BalanceAllowanceParams, asset_type)
        if hasattr(client, "update_balance_allowance"):
            client.update_balance_allowance(params)
        response = client.get_balance_allowance(params)
        return {
            "balance": _extract_balance_value(response, "balance"),
            "allowance": _extract_balance_value(response, "allowance"),
        }

    def place_market_order(
        self,
        token_id: str,
        side: str,
        size: float,
        *,
        notional: float | None = None,
        limit_price: float | None = None,
        order_type: str = "FOK",
    ) -> dict[str, Any]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to enable order placement.")

        client = build_authenticated_clob_client(self.env)

        if hasattr(client, "create_market_order") and hasattr(client, "post_order"):
            try:
                from py_clob_client.clob_types import MarketOrderArgs, OrderType
                from py_clob_client.order_builder.constants import BUY, SELL
            except ImportError as error:
                raise RuntimeError("py-clob-client install is incomplete for market order types.") from error

            side_upper = side.upper().strip()
            if side_upper not in {"BUY", "SELL"}:
                raise RuntimeError(f"Unsupported side: {side}")
            side_const = BUY if side_upper == "BUY" else SELL
            order_type_name = str(order_type or "FOK").upper().strip()
            order_type_value = getattr(OrderType, order_type_name, None)
            if order_type_value is None:
                raise RuntimeError(f"Unsupported order type: {order_type}")

            # py-clob-client expects amount in USDC for BUY market orders.
            amount = float(notional) if side_upper == "BUY" and notional and notional > 0 else float(size)
            if amount <= 0:
                raise RuntimeError("Order amount must be > 0.")

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=side_const,
                price=float(limit_price) if limit_price and limit_price > 0 else 0.0,
                order_type=order_type_value,
            )
            try:
                signed_order = client.create_market_order(order_args)
                return client.post_order(signed_order, orderType=order_type_value)
            except Exception as error:  # noqa: BLE001
                message = str(error or "")
                lower_message = message.lower()
                if "invalid signature" in lower_message:
                    raise RuntimeError(
                        "invalid signature from CLOB. Verify POLYMARKET_SIGNATURE_TYPE and POLYMARKET_FUNDER match the wallet account type."
                    ) from error
                if "unauthorized/invalid api key" in lower_message:
                    raise RuntimeError(
                        "invalid api key credentials. Clear POLYMARKET_API_KEY/SECRET/PASSPHRASE to derive fresh creds or set valid values."
                    ) from error
                raise

        raise RuntimeError(
            "py-clob-client API mismatch. Expected create_market_order/post_order methods are unavailable."
        )

    def place_limit_order(
        self,
        *,
        token_id: str,
        side: str,
        price: float,
        size: float,
        order_type: str = "GTC",
        post_only: bool = False,
    ) -> dict[str, Any]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to enable order placement.")

        client = build_authenticated_clob_client(self.env)
        side_upper = str(side or "").upper().strip()
        if side_upper not in {"BUY", "SELL"}:
            raise RuntimeError(f"Unsupported side: {side}")
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
        except ImportError as error:
            raise RuntimeError("py-clob-client install is incomplete for limit order types.") from error

        side_const = BUY if side_upper == "BUY" else SELL
        order_type_name = str(order_type or "GTC").upper().strip()
        order_type_value = getattr(OrderType, order_type_name, None)
        if order_type_value is None:
            raise RuntimeError(f"Unsupported order type: {order_type}")

        order_args = OrderArgs(
            token_id=token_id,
            price=float(price),
            size=float(size),
            side=side_const,
        )
        if hasattr(client, "create_and_post_order"):
            return client.create_and_post_order(order_args, orderType=order_type_value, postOnly=bool(post_only))
        if hasattr(client, "create_order") and hasattr(client, "post_order"):
            signed_order = client.create_order(order_args)
            return client.post_order(signed_order, orderType=order_type_value, postOnly=bool(post_only))
        raise RuntimeError("py-clob-client API mismatch. Expected create limit order methods are unavailable.")

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to manage orders.")
        client = build_authenticated_clob_client(self.env)
        if hasattr(client, "cancel_order"):
            response = client.cancel_order(order_id)
            return response if isinstance(response, dict) else {"canceled": [order_id]}
        raise RuntimeError("py-clob-client API mismatch. Expected cancel_order().")

    def cancel_all(self, *, market: str = "", asset_id: str = "") -> dict[str, Any]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to manage orders.")
        client = build_authenticated_clob_client(self.env)
        if market or asset_id:
            if hasattr(client, "cancel_market_orders"):
                payload = {}
                if market:
                    payload["market"] = market
                if asset_id:
                    payload["asset_id"] = asset_id
                response = client.cancel_market_orders(payload)
                return response if isinstance(response, dict) else {"canceled": []}
            raise RuntimeError("py-clob-client API mismatch. Expected cancel_market_orders().")
        if hasattr(client, "cancel_all"):
            response = client.cancel_all()
            return response if isinstance(response, dict) else {"canceled": []}
        raise RuntimeError("py-clob-client API mismatch. Expected cancel_all().")

    def list_open_orders(self, *, market: str = "", asset_id: str = "") -> list[dict[str, Any]]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to query orders.")
        client = build_authenticated_clob_client(self.env)
        if not hasattr(client, "get_open_orders"):
            raise RuntimeError("py-clob-client API mismatch. Expected get_open_orders().")
        params = {}
        if market:
            params["market"] = market
        if asset_id:
            params["asset_id"] = asset_id
        rows = client.get_open_orders(params or None, True)
        return [_normalize_order_status(row) for row in (rows or [])]


def _build_balance_params(balance_params_cls, asset_type):  # noqa: ANN001
    try:
        return balance_params_cls(asset_type=asset_type)
    except TypeError:
        return {"asset_type": asset_type}


def _extract_balance_value(payload: object, key: str) -> float:
    if isinstance(payload, dict):
        raw_value = payload.get(key)
    else:
        raw_value = getattr(payload, key, None)
    return _normalize_usdc_balance(raw_value)


def _normalize_usdc_balance(raw_value: object) -> float:
    if raw_value is None:
        return 0.0

    if isinstance(raw_value, bool):
        return 0.0

    if isinstance(raw_value, int):
        return raw_value / 1_000_000

    if isinstance(raw_value, float):
        if raw_value >= 100_000 and raw_value.is_integer():
            return raw_value / 1_000_000
        return raw_value

    if isinstance(raw_value, str):
        cleaned = raw_value.strip()
        if not cleaned:
            return 0.0
        if cleaned.isdigit():
            return int(cleaned) / 1_000_000
        try:
            parsed = float(cleaned)
        except ValueError:
            return 0.0
        if parsed >= 100_000 and "." not in cleaned and "e" not in cleaned.lower():
            return parsed / 1_000_000
        return parsed

    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    if parsed >= 100_000 and parsed.is_integer():
        return parsed / 1_000_000
    return parsed


def _normalize_order_status(payload: object) -> dict[str, Any]:
    if isinstance(payload, dict):
        row = dict(payload)
    else:
        row = {
            "id": getattr(payload, "id", ""),
            "status": getattr(payload, "status", ""),
            "market": getattr(payload, "market", ""),
            "asset_id": getattr(payload, "asset_id", ""),
            "side": getattr(payload, "side", ""),
            "original_size": getattr(payload, "original_size", 0.0),
            "size_matched": getattr(payload, "size_matched", 0.0),
            "price": getattr(payload, "price", 0.0),
            "created_at": getattr(payload, "created_at", 0),
            "order_type": getattr(payload, "order_type", ""),
        }
    return {
        "id": str(row.get("id") or row.get("orderID") or ""),
        "status": str(row.get("status") or ""),
        "market": str(row.get("market") or ""),
        "asset_id": str(row.get("asset_id") or ""),
        "side": str(row.get("side") or ""),
        "original_size": _safe_float(row.get("original_size") or row.get("size") or 0.0),
        "size_matched": _safe_float(row.get("size_matched") or row.get("matched_amount") or 0.0),
        "price": _safe_float(row.get("price") or 0.0),
        "created_at": int(_safe_float(row.get("created_at") or 0)),
        "order_type": str(row.get("order_type") or row.get("type") or ""),
    }


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
