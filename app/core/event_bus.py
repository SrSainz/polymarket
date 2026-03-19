from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class BaseEvent:
    kind: str
    source: str
    payload: dict[str, Any]
    market_id: str | None = None
    asset_id: str | None = None
    window_id: str | None = None
    ts_exchange_ms: int = 0
    ts_recv_ns: int = 0
    ts_process_ns: int = 0
    seq: int | None = None
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


EventHandler = Callable[[BaseEvent], None]


class EventBus:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)

    def subscribe(self, kind: str, handler: EventHandler) -> None:
        topic = str(kind or "*").strip() or "*"
        with self._lock:
            if handler not in self._handlers[topic]:
                self._handlers[topic].append(handler)

    def unsubscribe(self, kind: str, handler: EventHandler) -> None:
        topic = str(kind or "*").strip() or "*"
        with self._lock:
            handlers = self._handlers.get(topic)
            if not handlers:
                return
            try:
                handlers.remove(handler)
            except ValueError:
                return
            if not handlers:
                self._handlers.pop(topic, None)

    def publish(self, event: BaseEvent) -> None:
        with self._lock:
            handlers = list(self._handlers.get("*", ())) + list(self._handlers.get(event.kind, ()))
        for handler in handlers:
            handler(event)

    def emit(
        self,
        *,
        kind: str,
        source: str,
        payload: dict[str, Any],
        market_id: str | None = None,
        asset_id: str | None = None,
        window_id: str | None = None,
        ts_exchange_ms: int = 0,
        seq: int | None = None,
    ) -> BaseEvent:
        recv_ns = time.time_ns()
        event = BaseEvent(
            kind=str(kind or "").strip() or "unknown",
            source=str(source or "").strip() or "runtime",
            payload=dict(payload or {}),
            market_id=market_id,
            asset_id=asset_id,
            window_id=window_id,
            ts_exchange_ms=int(ts_exchange_ms or 0),
            ts_recv_ns=recv_ns,
            ts_process_ns=recv_ns,
            seq=seq,
        )
        self.publish(event)
        return event
