from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from app.core.event_bus import BaseEvent

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import websocket
except ImportError:  # pragma: no cover - optional dependency at runtime
    websocket = None


@dataclass(frozen=True)
class SpotSnapshot:
    reference_price: float | None
    lead_price: float | None
    binance_price: float | None
    chainlink_price: float | None
    basis: float
    source: str
    age_ms: int
    connected: bool


class SpotFeed:
    def __init__(
        self,
        ws_url: str,
        logger: logging.Logger,
        *,
        enabled: bool = True,
        stale_after_seconds: float = 1.5,
        chainlink_stale_after_seconds: float = 8.0,
        reconnect_delay_seconds: float = 1.0,
        rest_cache_seconds: float = 0.5,
    ) -> None:
        self.ws_url = ws_url.strip()
        self.logger = logger
        self.enabled = enabled and bool(self.ws_url)
        self.stale_after_seconds = max(float(stale_after_seconds), 0.25)
        self.chainlink_stale_after_seconds = max(float(chainlink_stale_after_seconds), self.stale_after_seconds)
        self.reconnect_delay_seconds = max(float(reconnect_delay_seconds), 0.25)
        self.rest_cache_seconds = max(float(rest_cache_seconds), 0.1)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._update_event = threading.Event()
        self._ws_app = None
        self._ws_kind = self._detect_ws_kind(self.ws_url)
        self._warned_unavailable = False
        self._prices: dict[str, tuple[float, float]] = {}
        self._history: dict[str, deque[tuple[float, float]]] = {
            "btcusdt": deque(maxlen=512),
            "btc/usd": deque(maxlen=512),
        }
        self._basis = 0.0
        self._basis_updated_at = 0.0
        self._rest_snapshot: SpotSnapshot | None = None
        self._rest_snapshot_at = 0.0
        self._listeners: list[callable] = []

        self.session = requests.Session()
        retries = Retry(
            total=2,
            backoff_factor=0.2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def start(self) -> bool:
        if not self._ws_supported():
            return False
        if self._thread is not None and self._thread.is_alive():
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever, name="polymarket-spot-feed", daemon=True)
        self._thread.start()
        return True

    def close(self) -> None:
        self._stop_event.set()
        self._update_event.set()
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:  # noqa: BLE001
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def wait_for_update(self, timeout_seconds: float) -> bool:
        timeout = max(float(timeout_seconds), 0.0)
        signaled = self._update_event.wait(timeout)
        if signaled:
            self._update_event.clear()
        return signaled

    def register_listener(self, listener) -> None:  # noqa: ANN001
        if listener not in self._listeners:
            self._listeners.append(listener)

    def get_snapshot(self) -> SpotSnapshot:
        now = time.time()
        with self._lock:
            binance = self._fresh_price_locked("btcusdt", now)
            chainlink = self._fresh_price_locked("btc/usd", now)
            basis = self._basis if now - self._basis_updated_at <= 15 else 0.0

        if chainlink is not None:
            age_ms = self._age_ms_locked("btc/usd", now)
            return SpotSnapshot(
                reference_price=chainlink,
                lead_price=binance or chainlink,
                binance_price=binance,
                chainlink_price=chainlink,
                basis=basis,
                source="polymarket-rtds+binance" if binance is not None and self._ws_kind == "polymarket" else (
                    "polymarket-rtds-chainlink" if self._ws_kind == "polymarket" else "chainlink"
                ),
                age_ms=age_ms,
                connected=self._connected_event.is_set(),
            )

        if binance is not None:
            age_ms = self._age_ms_locked("btcusdt", now)
            return SpotSnapshot(
                reference_price=binance,
                lead_price=binance,
                binance_price=binance,
                chainlink_price=None,
                basis=basis,
                source="binance-direct" if self._ws_kind == "binance" else "binance-only",
                age_ms=age_ms,
                connected=self._connected_event.is_set(),
            )

        if self._rest_snapshot is not None and now - self._rest_snapshot_at <= self.rest_cache_seconds:
            return self._rest_snapshot

        snapshot = self._fetch_rest_snapshot()
        self._rest_snapshot = snapshot
        self._rest_snapshot_at = now
        return snapshot

    def _ws_supported(self) -> bool:
        if not self.enabled:
            return False
        if websocket is not None:
            return True
        if not self._warned_unavailable:
            self.logger.warning("spot feed websocket disabled: websocket-client is not installed")
            self._warned_unavailable = True
        return False

    def _run_forever(self) -> None:
        if not self._ws_supported():
            return

        while not self._stop_event.is_set():
            ping_stop = threading.Event()
            ping_thread: threading.Thread | None = None

            def on_open(ws_app) -> None:  # noqa: ANN001
                self._connected_event.set()
                if self._ws_kind != "polymarket":
                    return
                try:
                    ws_app.send(_polymarket_prices_subscription_message())
                    ws_app.send(_polymarket_chainlink_subscription_message())
                except Exception as error:  # noqa: BLE001
                    self.logger.warning("spot feed subscribe failed: %s", error)
                    return

                def ping_loop() -> None:
                    refresh_counter = 0
                    while not ping_stop.wait(2.0):
                        try:
                            if self._ws_kind == "polymarket":
                                ws_app.send(_polymarket_chainlink_subscription_message())
                            refresh_counter += 2
                            if refresh_counter >= 6:
                                ws_app.send("PING")
                                refresh_counter = 0
                        except Exception:  # noqa: BLE001
                            return

                nonlocal ping_thread
                ping_thread = threading.Thread(target=ping_loop, name="polymarket-spot-ping", daemon=True)
                ping_thread.start()

            def on_message(_ws_app, message: str) -> None:
                self._handle_message(message)

            def on_error(_ws_app, error: object) -> None:
                if self._stop_event.is_set():
                    return
                self.logger.debug("spot feed websocket error: %s", error)

            def on_close(_ws_app, _status_code, _msg) -> None:
                ping_stop.set()
                self._connected_event.clear()

            self._ws_app = websocket.WebSocketApp(
                self.ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            try:
                self._ws_app.run_forever()
            except Exception as error:  # noqa: BLE001
                if not self._stop_event.is_set():
                    self.logger.debug("spot feed websocket loop failed: %s", error)
            finally:
                ping_stop.set()
                if ping_thread is not None and ping_thread.is_alive():
                    ping_thread.join(timeout=0.2)
                self._connected_event.clear()
                self._ws_app = None

            if not self._stop_event.is_set():
                time.sleep(self.reconnect_delay_seconds)

    def _detect_ws_kind(self, ws_url: str) -> str:
        lowered = ws_url.lower()
        if "stream.binance.com" in lowered:
            return "binance"
        return "polymarket"

    def _handle_message(self, message: str) -> None:
        if message in {"PING", "PONG"}:
            return
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        updated = False
        for item in _iter_spot_events(payload):
            symbol = str(item.get("symbol") or "").strip().lower()
            price = _coerce_price(item.get("price"))
            if not symbol or price is None:
                continue
            now = time.time()
            with self._lock:
                self._prices[symbol] = (price, now)
                self._history.setdefault(symbol, deque(maxlen=512)).append((price, now))
                if symbol == "btcusdt":
                    chainlink = self._fresh_price_locked("btc/usd", now)
                    if chainlink is not None:
                        self._basis = chainlink - price
                        self._basis_updated_at = now
                elif symbol == "btc/usd":
                    binance = self._fresh_price_locked("btcusdt", now)
                    if binance is not None:
                        self._basis = price - binance
                        self._basis_updated_at = now
            updated = True
            self._emit_event(
                symbol=symbol,
                price=price,
                side=str(item.get("side") or ""),
                quantity=float(item.get("quantity") or 0.0),
                timestamp_ms=int(item.get("timestamp_ms") or 0),
                source_kind=str(item.get("source_kind") or ""),
            )
        if updated:
            self._update_event.set()

    def _fresh_price_locked(self, symbol: str, now: float) -> float | None:
        price_entry = self._prices.get(symbol)
        if price_entry is None:
            return None
        price, received_at = price_entry
        max_age_seconds = self.chainlink_stale_after_seconds if symbol == "btc/usd" else self.stale_after_seconds
        if now - received_at > max_age_seconds:
            return None
        return price

    def _age_ms_locked(self, symbol: str, now: float) -> int:
        price_entry = self._prices.get(symbol)
        if price_entry is None:
            return 0
        _, received_at = price_entry
        return int(max((now - received_at) * 1000, 0))

    def get_anchor_price(self, *, symbol: str, target_ts: float, tolerance_seconds: float = 3.0) -> float | None:
        with self._lock:
            history = list(self._history.get(symbol, ()))
            latest = self._prices.get(symbol)

        if history:
            candidates = [
                (price, ts)
                for price, ts in history
                if abs(ts - target_ts) <= tolerance_seconds
            ]
            if candidates:
                later = sorted((item for item in candidates if item[1] >= target_ts), key=lambda item: (item[1] - target_ts))
                if later:
                    return later[0][0]
                earlier = sorted((item for item in candidates if item[1] < target_ts), key=lambda item: (target_ts - item[1]))
                if earlier:
                    return earlier[0][0]

        if latest is None:
            return None
        price, ts = latest
        if abs(ts - target_ts) <= tolerance_seconds:
            return price
        return None

    def _emit_event(
        self,
        *,
        symbol: str,
        price: float,
        side: str,
        quantity: float,
        timestamp_ms: int,
        source_kind: str,
    ) -> None:
        if not self._listeners:
            return
        recv_ns = time.time_ns()
        payload = {
            "symbol": symbol,
            "price": price,
            "side": str(side or "").strip().lower(),
            "quantity": float(quantity or 0.0),
            "source_kind": source_kind,
            "timestamp": int(timestamp_ms or 0),
        }
        event = BaseEvent(
            kind="spot_price",
            source=f"spot-feed:{self._ws_kind}",
            payload=payload,
            asset_id=symbol,
            ts_exchange_ms=int(timestamp_ms or 0),
            ts_recv_ns=recv_ns,
            ts_process_ns=recv_ns,
        )
        for listener in list(self._listeners):
            listener(event)

    def _fetch_rest_snapshot(self) -> SpotSnapshot:
        prices: dict[str, float | None] = {"btcusdt": None, "btc/usd": None}
        try:
            response = self.session.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
                timeout=3,
            )
            response.raise_for_status()
            payload = response.json()
            prices["btcusdt"] = _coerce_price(payload.get("price"))
        except Exception:  # noqa: BLE001
            prices["btcusdt"] = None

        try:
            response = self.session.get(
                "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
                timeout=3,
            )
            response.raise_for_status()
            payload = response.json()
            prices["btc/usd"] = _coerce_price(payload.get("price"))
        except Exception:  # noqa: BLE001
            prices["btc/usd"] = None

        chainlink = prices["btc/usd"]
        binance = prices["btcusdt"]
        if chainlink is not None:
            return SpotSnapshot(
                reference_price=chainlink,
                lead_price=binance or chainlink,
                binance_price=binance,
                chainlink_price=chainlink,
                basis=(chainlink - binance) if binance is not None else 0.0,
                source="rest-coinbase",
                age_ms=0,
                connected=False,
            )
        if binance is not None:
            return SpotSnapshot(
                reference_price=binance,
                lead_price=binance,
                binance_price=binance,
                chainlink_price=None,
                basis=0.0,
                source="rest-binance",
                age_ms=0,
                connected=False,
            )
        return SpotSnapshot(
            reference_price=None,
            lead_price=None,
            binance_price=None,
            chainlink_price=None,
            basis=0.0,
            source="unavailable",
            age_ms=0,
            connected=False,
        )


def _iter_price_points(payload: Any) -> list[tuple[str, float]]:
    points = [(str(item.get("symbol") or ""), float(item["price"])) for item in _iter_spot_events(payload) if item.get("price") is not None]
    deduped: dict[str, float] = {}
    for symbol, price in points:
        deduped[symbol] = price
    return list(deduped.items())


def _iter_spot_events(payload: Any) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    _walk_spot_events(payload, events)
    deduped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for item in events:
        key = (
            str(item.get("symbol") or ""),
            int(item.get("timestamp_ms") or 0),
            str(item.get("source_kind") or ""),
        )
        deduped[key] = item
    return list(deduped.values())


def _polymarket_prices_subscription_message() -> str:
    return json.dumps({"action": "subscribe", "subscriptions": [{"topic": "crypto_prices", "type": "update"}]})


def _polymarket_chainlink_subscription_message() -> str:
    return json.dumps(
        {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": json.dumps({"symbol": "btc/usd"})}
            ],
        }
    )


def _walk_spot_events(payload: Any, sink: list[dict[str, Any]]) -> None:
    if isinstance(payload, list):
        for item in payload:
            _walk_spot_events(item, sink)
        return

    if not isinstance(payload, dict):
        return

    symbol = _normalize_symbol(payload.get("symbol") or payload.get("pair") or payload.get("ticker") or payload.get("s"))
    if symbol:
        historical_points = payload.get("data")
        if isinstance(historical_points, list):
            for item in reversed(historical_points):
                if not isinstance(item, dict):
                    continue
                historical_price = _extract_price(item)
                if historical_price is not None:
                    sink.append(
                        {
                            "symbol": symbol,
                            "price": historical_price,
                            "quantity": _coerce_quantity(item.get("q") or item.get("size") or item.get("v")),
                            "side": _extract_side(item),
                            "timestamp_ms": int(_coerce_quantity(item.get("timestamp") or item.get("T") or item.get("E") or 0)),
                            "source_kind": _extract_source_kind(item),
                        }
                    )
                    break
    price = _extract_price(payload)
    if symbol and price is not None:
        sink.append(
            {
                "symbol": symbol,
                "price": price,
                "quantity": _coerce_quantity(payload.get("q") or payload.get("size") or payload.get("v")),
                "side": _extract_side(payload),
                "timestamp_ms": int(_coerce_quantity(payload.get("timestamp") or payload.get("T") or payload.get("E") or 0)),
                "source_kind": _extract_source_kind(payload),
            }
        )

    for value in payload.values():
        _walk_spot_events(value, sink)


def _normalize_symbol(raw_symbol: object) -> str:
    value = str(raw_symbol or "").strip().lower()
    if not value:
        return ""
    if value in {"btcusdt", "btc/usdt", "btc-usdt"}:
        return "btcusdt"
    if value in {"btc/usd", "btcusd", "btc-usd"}:
        return "btc/usd"
    return ""


def _extract_price(payload: dict[str, Any]) -> float | None:
    for key in ("price", "value", "last", "last_price", "close", "mark_price", "mid", "p", "c"):
        price = _coerce_price(payload.get(key))
        if price is not None:
            return price
    best_bid = _coerce_price(payload.get("best_bid"))
    best_ask = _coerce_price(payload.get("best_ask"))
    if best_bid is not None and best_ask is not None:
        return (best_bid + best_ask) / 2
    return None


def _extract_side(payload: dict[str, Any]) -> str:
    side = str(payload.get("side") or "").strip().lower()
    if side in {"buy", "sell"}:
        return side
    buyer_is_maker = payload.get("m")
    if buyer_is_maker is True:
        return "sell"
    if buyer_is_maker is False:
        return "buy"
    return "unknown"


def _extract_source_kind(payload: dict[str, Any]) -> str:
    event_type = str(payload.get("e") or payload.get("type") or payload.get("topic") or "").strip().lower()
    if event_type:
        return event_type
    if "best_bid" in payload or "best_ask" in payload:
        return "book_ticker"
    return "price"


def _coerce_quantity(raw_value: object) -> float:
    if raw_value is None:
        return 0.0
    try:
        quantity = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    if quantity <= 0:
        return 0.0
    return quantity


def _coerce_price(raw_value: object) -> float | None:
    if raw_value is None:
        return None
    try:
        price = float(raw_value)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price
