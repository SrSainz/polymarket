from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ActivityClient:
    def __init__(self, base_url: str, timeout: int = 15) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
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

    def get_positions(self, wallet: str, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._get("/positions", {"user": wallet, "limit": limit, "offset": offset})
        return payload if isinstance(payload, list) else []

    def get_activity(self, wallet: str, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._get("/activity", {"user": wallet, "limit": limit, "offset": offset})
        return payload if isinstance(payload, list) else []

    def get_trades(self, wallet: str, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._get("/trades", {"user": wallet, "limit": limit, "offset": offset})
        return payload if isinstance(payload, list) else []

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
