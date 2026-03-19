from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Callable

try:
    import websocket
except ImportError:  # pragma: no cover - optional dependency at runtime
    websocket = None


LiquidationListener = Callable[[dict[str, Any]], None]


class LiquidationFeed:
    def __init__(
        self,
        logger: logging.Logger,
        *,
        enabled: bool = False,
        binance_ws_url: str = "",
        bybit_ws_url: str = "",
        bybit_symbol: str = "BTCUSDT",
        reconnect_delay_seconds: float = 1.0,
    ) -> None:
        self.logger = logger
        self.enabled = bool(enabled)
        self.binance_ws_url = str(binance_ws_url or "").strip()
        self.bybit_ws_url = str(bybit_ws_url or "").strip()
        self.bybit_symbol = str(bybit_symbol or "BTCUSDT").strip().upper()
        self.reconnect_delay_seconds = max(float(reconnect_delay_seconds), 0.25)
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._listeners: list[LiquidationListener] = []
        self._update_event = threading.Event()

    def start(self) -> bool:
        if not self.enabled or websocket is None:
            return False
        if self._threads:
            return True
        self._stop_event.clear()
        if self.binance_ws_url:
            self._threads.append(
                threading.Thread(
                    target=self._run_forever,
                    args=("binance", self.binance_ws_url),
                    name="binance-liquidation-feed",
                    daemon=True,
                )
            )
        if self.bybit_ws_url:
            self._threads.append(
                threading.Thread(
                    target=self._run_forever,
                    args=("bybit", self.bybit_ws_url),
                    name="bybit-liquidation-feed",
                    daemon=True,
                )
            )
        for thread in self._threads:
            thread.start()
        return bool(self._threads)

    def close(self) -> None:
        self._stop_event.set()
        self._update_event.set()
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        self._threads.clear()

    def wait_for_update(self, timeout_seconds: float) -> bool:
        timeout = max(float(timeout_seconds), 0.0)
        signaled = self._update_event.wait(timeout)
        if signaled:
            self._update_event.clear()
        return signaled

    def register_listener(self, listener: LiquidationListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def _run_forever(self, source: str, ws_url: str) -> None:
        while not self._stop_event.is_set():
            def on_open(ws_app) -> None:  # noqa: ANN001
                if source != "bybit":
                    return
                try:
                    ws_app.send(json.dumps({"op": "subscribe", "args": [f"allLiquidation.{self.bybit_symbol}"]}))
                except Exception as error:  # noqa: BLE001
                    self.logger.warning("bybit liquidation subscribe failed: %s", error)

            def on_message(_ws_app, message: str) -> None:
                self._handle_message(source, message)

            def on_error(_ws_app, error: object) -> None:
                if self._stop_event.is_set():
                    return
                self.logger.debug("%s liquidation websocket error: %s", source, error)

            ws_app = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error)
            try:
                ws_app.run_forever()
            except Exception as error:  # noqa: BLE001
                if not self._stop_event.is_set():
                    self.logger.debug("%s liquidation websocket failed: %s", source, error)
            if not self._stop_event.is_set():
                time.sleep(self.reconnect_delay_seconds)

    def _handle_message(self, source: str, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        items = _parse_liquidations(source=source, payload=payload)
        if not items:
            return
        self._update_event.set()
        for item in items:
            for listener in list(self._listeners):
                listener(dict(item))


def _parse_liquidations(*, source: str, payload: Any) -> list[dict[str, Any]]:
    if source == "binance":
        return _parse_binance_liquidations(payload)
    if source == "bybit":
        return _parse_bybit_liquidations(payload)
    return []


def _parse_binance_liquidations(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        order = item.get("o") if isinstance(item.get("o"), dict) else item
        symbol = str(order.get("s") or "").strip().upper()
        side = str(order.get("S") or "").strip().lower()
        price = _coerce_float(order.get("p"))
        quantity = _coerce_float(order.get("q") or order.get("l"))
        timestamp = int(_coerce_float(order.get("T") or item.get("E") or 0))
        if not symbol or price <= 0 or quantity <= 0:
            continue
        rows.append(
            {
                "exchange": "binance",
                "symbol": symbol,
                "side": side if side in {"buy", "sell"} else "unknown",
                "price": price,
                "quantity": quantity,
                "notional": price * quantity,
                "timestamp": timestamp,
            }
        )
    return rows


def _parse_bybit_liquidations(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items = payload.get("data") or []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("s") or "").strip().upper()
        side = str(item.get("S") or "").strip().lower()
        price = _coerce_float(item.get("p"))
        quantity = _coerce_float(item.get("v"))
        timestamp = int(_coerce_float(item.get("T") or payload.get("ts") or 0))
        if not symbol or price <= 0 or quantity <= 0:
            continue
        rows.append(
            {
                "exchange": "bybit",
                "symbol": symbol,
                "side": side if side in {"buy", "sell"} else "unknown",
                "price": price,
                "quantity": quantity,
                "notional": price * quantity,
                "timestamp": timestamp,
            }
        )
    return rows


def _coerce_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
