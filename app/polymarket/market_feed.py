from __future__ import annotations

import json
import logging
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

try:
    import websocket
except ImportError:  # pragma: no cover - optional dependency at runtime
    websocket = None


@dataclass(frozen=True)
class FeedStatus:
    mode: str
    connected: bool
    tracked_assets: int
    age_ms: int


class MarketFeed:
    def __init__(
        self,
        ws_url: str,
        logger: logging.Logger,
        *,
        enabled: bool = True,
        stale_after_seconds: float = 2.5,
        reconnect_delay_seconds: float = 1.0,
        heartbeat_interval_seconds: float = 5.0,
    ) -> None:
        self.ws_url = ws_url.strip()
        self.logger = logger
        self.enabled = enabled and bool(self.ws_url)
        self.stale_after_seconds = max(float(stale_after_seconds), 0.25)
        self.reconnect_delay_seconds = max(float(reconnect_delay_seconds), 0.25)
        self.heartbeat_interval_seconds = max(float(heartbeat_interval_seconds), 1.0)
        self._lock = threading.Lock()
        self._books: dict[str, dict[str, list[dict[str, str]]]] = {}
        self._updated_at: dict[str, float] = {}
        self._desired_assets: tuple[str, ...] = ()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._update_event = threading.Event()
        self._ws_app = None
        self._warned_unavailable = False

    def ensure_assets(self, asset_ids: list[str] | tuple[str, ...]) -> bool:
        normalized = tuple(sorted({str(asset).strip() for asset in asset_ids if str(asset).strip()}))
        if not normalized:
            return False

        if not self._ws_supported():
            return False

        restart_required = False
        with self._lock:
            if normalized != self._desired_assets:
                self._desired_assets = normalized
                restart_required = True

        if self._thread is None or not self._thread.is_alive():
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_forever,
                name="polymarket-market-feed",
                daemon=True,
            )
            self._thread.start()
            return True

        if restart_required and self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:  # noqa: BLE001
                pass
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

    def get_book(self, token_id: str) -> dict[str, Any] | None:
        with self._lock:
            book = self._books.get(token_id)
            updated_at = self._updated_at.get(token_id, 0.0)
            if not book:
                return None
            if time.time() - updated_at > self.stale_after_seconds:
                return None
            return deepcopy(book)

    def get_midpoint(self, token_id: str) -> float | None:
        book = self.get_book(token_id)
        if not book:
            return None
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            return None
        try:
            best_bid = float(bids[0].get("price"))
            best_ask = float(asks[0].get("price"))
        except (AttributeError, TypeError, ValueError):
            return None
        if best_bid <= 0 or best_ask <= 0:
            return None
        return (best_bid + best_ask) / 2

    def status(self) -> FeedStatus:
        now = time.time()
        with self._lock:
            tracked_assets = len(self._desired_assets)
            fresh_timestamps = [ts for ts in self._updated_at.values() if now - ts <= self.stale_after_seconds]
        if not self._ws_supported():
            return FeedStatus(mode="rest-fallback", connected=False, tracked_assets=tracked_assets, age_ms=0)
        if not fresh_timestamps:
            return FeedStatus(
                mode="websocket-warming",
                connected=self._connected_event.is_set(),
                tracked_assets=tracked_assets,
                age_ms=0,
            )
        age_ms = int(max((now - max(fresh_timestamps)) * 1000, 0))
        return FeedStatus(
            mode="websocket",
            connected=self._connected_event.is_set(),
            tracked_assets=tracked_assets,
            age_ms=age_ms,
        )

    def wait_for_update(self, timeout_seconds: float) -> bool:
        timeout = max(float(timeout_seconds), 0.0)
        signaled = self._update_event.wait(timeout)
        if signaled:
            self._update_event.clear()
        return signaled

    def _ws_supported(self) -> bool:
        if not self.enabled:
            return False
        if websocket is not None:
            return True
        if not self._warned_unavailable:
            self.logger.warning("market feed websocket disabled: websocket-client is not installed")
            self._warned_unavailable = True
        return False

    def _run_forever(self) -> None:
        if not self._ws_supported():
            return

        while not self._stop_event.is_set():
            with self._lock:
                asset_ids = self._desired_assets
            if not asset_ids:
                time.sleep(0.2)
                continue

            heartbeat_stop = threading.Event()

            def on_open(ws_app) -> None:  # noqa: ANN001
                self._connected_event.set()
                try:
                    ws_app.send(json.dumps({"type": "market", "assets_ids": list(asset_ids)}))
                except Exception as error:  # noqa: BLE001
                    self.logger.warning("market feed subscribe failed: %s", error)
                    return

                def heartbeat_loop() -> None:
                    while not heartbeat_stop.is_set() and not self._stop_event.is_set():
                        try:
                            ws_app.send("PING")
                        except Exception:  # noqa: BLE001
                            return
                        heartbeat_stop.wait(self.heartbeat_interval_seconds)

                threading.Thread(target=heartbeat_loop, name="polymarket-market-feed-heartbeat", daemon=True).start()

            def on_message(ws_app, message: str) -> None:  # noqa: ANN001
                self._handle_message(ws_app, message)

            def on_error(_ws_app, error: object) -> None:
                if self._stop_event.is_set():
                    return
                self.logger.warning("market feed websocket error: %s", error)

            def on_close(_ws_app, _status_code, _msg) -> None:
                heartbeat_stop.set()
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
                    self.logger.warning("market feed websocket loop failed: %s", error)
            finally:
                heartbeat_stop.set()
                self._connected_event.clear()
                self._ws_app = None

            if not self._stop_event.is_set():
                time.sleep(self.reconnect_delay_seconds)

    def _handle_message(self, ws_app, message: str) -> None:  # noqa: ANN001
        if message == "PONG":
            return
        if message == "PING":
            try:
                ws_app.send("PONG")
            except Exception:  # noqa: BLE001
                pass
            return

        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            event_type = str(item.get("event_type") or item.get("type") or "").strip().lower()
            if event_type == "book" or ("asset_id" in item and ("bids" in item or "asks" in item or "buys" in item or "sells" in item)):
                self._store_book_from_payload(item)
                continue
            if event_type == "price_change":
                self._apply_price_change_payload(item)
                continue
            if event_type == "best_bid_ask":
                self._apply_best_bid_ask_payload(item)

    def _store_book_from_payload(self, payload: dict[str, Any]) -> None:
        asset_id = _payload_asset_id(payload)
        book = _canonical_book(payload)
        if not asset_id or book is None:
            return
        with self._lock:
            self._books[asset_id] = book
            self._updated_at[asset_id] = time.time()
        self._update_event.set()

    def _apply_price_change_payload(self, payload: dict[str, Any]) -> None:
        asset_id = _payload_asset_id(payload)
        if not asset_id:
            return
        with self._lock:
            current = deepcopy(self._books.get(asset_id) or {"bids": [], "asks": []})
            updated = _apply_price_changes(current, payload)
            self._books[asset_id] = updated
            self._updated_at[asset_id] = time.time()
        self._update_event.set()

    def _apply_best_bid_ask_payload(self, payload: dict[str, Any]) -> None:
        asset_id = _payload_asset_id(payload)
        if not asset_id:
            return
        with self._lock:
            current = deepcopy(self._books.get(asset_id) or {"bids": [], "asks": []})
            updated = _apply_best_bid_ask(current, payload)
            self._books[asset_id] = updated
            self._updated_at[asset_id] = time.time()
        self._update_event.set()


def _payload_asset_id(payload: dict[str, Any]) -> str:
    for key in ("asset_id", "asset", "token_id", "market"):
        raw_value = payload.get(key)
        if raw_value:
            return str(raw_value)
    return ""


def _canonical_book(payload: dict[str, Any]) -> dict[str, list[dict[str, str]]] | None:
    bids = _normalized_levels(payload.get("bids") or payload.get("buys") or [], descending=True)
    asks = _normalized_levels(payload.get("asks") or payload.get("sells") or [], descending=False)
    if not bids and not asks:
        return None
    return {"bids": bids, "asks": asks}


def _apply_price_changes(book: dict[str, list[dict[str, str]]], payload: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    bids = {str(level["price"]): str(level["size"]) for level in book.get("bids") or []}
    asks = {str(level["price"]): str(level["size"]) for level in book.get("asks") or []}
    changes = payload.get("changes") or payload.get("price_changes") or []
    for change in changes:
        if not isinstance(change, dict):
            continue
        side = str(change.get("side") or "").strip().upper()
        price = str(change.get("price") or "").strip()
        if not price:
            continue
        size = _coerce_size_string(change.get("size") or change.get("new_size") or change.get("remaining_size") or "0")
        target = asks if side == "SELL" else bids if side == "BUY" else None
        if target is None:
            continue
        if _coerce_float(size) <= 0:
            target.pop(price, None)
        else:
            target[price] = size
    return {
        "bids": _book_side_from_map(bids, descending=True),
        "asks": _book_side_from_map(asks, descending=False),
    }


def _apply_best_bid_ask(book: dict[str, list[dict[str, str]]], payload: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    bids = {str(level["price"]): str(level["size"]) for level in book.get("bids") or []}
    asks = {str(level["price"]): str(level["size"]) for level in book.get("asks") or []}
    current_best_bid = max(bids, key=_coerce_float, default="")
    current_best_ask = min(asks, key=_coerce_float, default="")

    best_bid_price = _coerce_price_string(payload.get("best_bid_price") or payload.get("best_bid"))
    best_bid_size = _coerce_size_string(payload.get("best_bid_size") or payload.get("bid_size") or "0")
    best_ask_price = _coerce_price_string(payload.get("best_ask_price") or payload.get("best_ask"))
    best_ask_size = _coerce_size_string(payload.get("best_ask_size") or payload.get("ask_size") or "0")

    if best_bid_price:
        if current_best_bid and current_best_bid != best_bid_price:
            bids.pop(current_best_bid, None)
        bids[best_bid_price] = best_bid_size if _coerce_float(best_bid_size) > 0 else "0"
    if best_ask_price:
        if current_best_ask and current_best_ask != best_ask_price:
            asks.pop(current_best_ask, None)
        asks[best_ask_price] = best_ask_size if _coerce_float(best_ask_size) > 0 else "0"

    bids = {price: size for price, size in bids.items() if _coerce_float(size) > 0}
    asks = {price: size for price, size in asks.items() if _coerce_float(size) > 0}

    return {
        "bids": _book_side_from_map(bids, descending=True),
        "asks": _book_side_from_map(asks, descending=False),
    }


def _normalized_levels(raw_levels: list[Any], *, descending: bool) -> list[dict[str, str]]:
    levels: dict[str, float] = {}
    for raw_level in raw_levels:
        if isinstance(raw_level, dict):
            price = _coerce_price_string(raw_level.get("price"))
            size = _coerce_float(raw_level.get("size"))
        elif isinstance(raw_level, (list, tuple)) and len(raw_level) >= 2:
            price = _coerce_price_string(raw_level[0])
            size = _coerce_float(raw_level[1])
        else:
            continue
        if not price or size <= 0:
            continue
        levels[price] = levels.get(price, 0.0) + size
    return _book_side_from_map({price: _coerce_size_string(size) for price, size in levels.items()}, descending=descending)


def _book_side_from_map(levels: dict[str, str], *, descending: bool) -> list[dict[str, str]]:
    ordered = sorted(levels.items(), key=lambda item: _coerce_float(item[0]), reverse=descending)
    return [{"price": price, "size": size} for price, size in ordered if _coerce_float(size) > 0]


def _coerce_price_string(value: object) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return ""
    if parsed <= 0:
        return ""
    return f"{parsed:.6f}".rstrip("0").rstrip(".")


def _coerce_size_string(value: object) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "0"
    if parsed <= 0:
        return "0"
    return f"{parsed:.8f}".rstrip("0").rstrip(".")


def _coerce_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
