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


UserEventListener = Callable[[dict[str, Any]], None]


class UserFeed:
    def __init__(
        self,
        ws_url: str,
        logger: logging.Logger,
        *,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        markets: list[str] | None = None,
        enabled: bool = False,
        reconnect_delay_seconds: float = 1.0,
    ) -> None:
        self.ws_url = str(ws_url or "").strip()
        self.logger = logger
        self.api_key = str(api_key or "").strip()
        self.api_secret = str(api_secret or "").strip()
        self.api_passphrase = str(api_passphrase or "").strip()
        self.markets = [str(item).strip() for item in (markets or []) if str(item).strip()]
        self.enabled = bool(enabled and self.ws_url and self.api_key and self.api_secret and self.api_passphrase)
        self.reconnect_delay_seconds = max(float(reconnect_delay_seconds), 0.25)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._listeners: list[UserEventListener] = []

    def start(self) -> bool:
        if not self.enabled or websocket is None:
            return False
        if self._thread is not None and self._thread.is_alive():
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_forever, name="polymarket-user-feed", daemon=True)
        self._thread.start()
        return True

    def close(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)

    def register_listener(self, listener: UserEventListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def _run_forever(self) -> None:
        while not self._stop_event.is_set():
            def on_open(ws_app) -> None:  # noqa: ANN001
                try:
                    ws_app.send(
                        json.dumps(
                            {
                                "auth": {
                                    "apiKey": self.api_key,
                                    "secret": self.api_secret,
                                    "passphrase": self.api_passphrase,
                                },
                                "markets": self.markets,
                                "type": "user",
                            }
                        )
                    )
                except Exception as error:  # noqa: BLE001
                    self.logger.warning("user feed subscribe failed: %s", error)

            def on_message(_ws_app, message: str) -> None:
                self._handle_message(message)

            def on_error(_ws_app, error: object) -> None:
                if self._stop_event.is_set():
                    return
                self.logger.debug("user feed websocket error: %s", error)

            ws_app = websocket.WebSocketApp(self.ws_url, on_open=on_open, on_message=on_message, on_error=on_error)
            try:
                ws_app.run_forever()
            except Exception as error:  # noqa: BLE001
                if not self._stop_event.is_set():
                    self.logger.debug("user feed websocket loop failed: %s", error)
            if not self._stop_event.is_set():
                time.sleep(self.reconnect_delay_seconds)

    def _handle_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            event_type = str(item.get("event_type") or item.get("type") or "").strip().lower()
            if event_type not in {"trade", "order", "matched", "placement", "update", "cancellation"}:
                continue
            for listener in list(self._listeners):
                listener(dict(item))
