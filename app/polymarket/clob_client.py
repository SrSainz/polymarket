from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.polymarket.auth import build_authenticated_clob_client
from app.settings import EnvSettings


class CLOBClient:
    def __init__(self, base_url: str, env: EnvSettings, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.env = env
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

    def get_midpoint(self, token_id: str) -> float | None:
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
        response = self.session.get(
            f"{self.base_url}/book",
            params={"token_id": token_id},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def place_market_order(self, token_id: str, side: str, size: float) -> dict[str, Any]:
        if not self.env.live_trading:
            raise RuntimeError("Live trading is disabled. Set LIVE_TRADING=true to enable order placement.")

        client = build_authenticated_clob_client(self.env)

        if hasattr(client, "create_market_order") and hasattr(client, "post_order"):
            order = client.create_market_order(
                token_id=token_id,
                side=side.upper(),
                amount=str(size),
            )
            return client.post_order(order, orderType="GTC")

        raise RuntimeError(
            "py-clob-client API mismatch. Expected create_market_order/post_order methods are unavailable."
        )
