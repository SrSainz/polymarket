from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any

from app.core.event_bus import BaseEvent


@dataclass(frozen=True, slots=True)
class Level:
    price: float
    size: float


@dataclass(frozen=True, slots=True)
class BookState:
    asset_id: str
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    updated_ns: int
    tick_size: float


@dataclass(frozen=True, slots=True)
class TradePrint:
    source: str
    asset_id: str
    side: str
    price: float
    size: float
    notional: float
    ts_ns: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SpotPrint:
    source: str
    symbol: str
    side: str
    price: float
    quantity: float
    notional: float
    ts_ns: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LiquidationPrint:
    source: str
    exchange: str
    symbol: str
    side: str
    price: float
    quantity: float
    notional: float
    ts_ns: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class StateStore:
    def __init__(
        self,
        *,
        trade_retention_seconds: float = 900.0,
        spot_retention_seconds: float = 900.0,
        liquidation_retention_seconds: float = 1800.0,
    ) -> None:
        self.trade_retention_seconds = max(float(trade_retention_seconds), 30.0)
        self.spot_retention_seconds = max(float(spot_retention_seconds), 30.0)
        self.liquidation_retention_seconds = max(float(liquidation_retention_seconds), 60.0)
        self._lock = threading.Lock()
        self._books: dict[str, BookState] = {}
        self._trades: dict[str, deque[TradePrint]] = {}
        self._spot: dict[str, deque[SpotPrint]] = {}
        self._liquidations: deque[LiquidationPrint] = deque()
        self._last_event_lag_ms = 0.0
        self._last_event_kind = ""
        self._last_event_received_ns = 0

    def apply(self, event: BaseEvent) -> None:
        now_ns = int(event.ts_process_ns or time.time_ns())
        lag_ms = 0.0
        if event.ts_exchange_ms > 0:
            lag_ms = max((now_ns // 1_000_000) - int(event.ts_exchange_ms), 0)
        with self._lock:
            self._last_event_lag_ms = float(lag_ms)
            self._last_event_kind = event.kind
            self._last_event_received_ns = int(event.ts_recv_ns or now_ns)
            if event.kind in {"book", "book_snapshot", "book_delta", "best_bid_ask"}:
                self._apply_book(event, now_ns)
            elif event.kind == "market_trade":
                self._apply_trade(event, now_ns)
            elif event.kind == "spot_price":
                self._apply_spot(event, now_ns)
            elif event.kind == "liquidation":
                self._apply_liquidation(event, now_ns)
            elif event.kind == "tick_size_change":
                self._apply_tick_size(event)
            self._trim_locked(now_ns)

    def latest_event_lag_ms(self) -> float:
        with self._lock:
            return float(self._last_event_lag_ms)

    def latest_event_kind(self) -> str:
        with self._lock:
            return self._last_event_kind

    def get_book(self, asset_id: str) -> BookState | None:
        with self._lock:
            book = self._books.get(str(asset_id or ""))
            return book

    def recent_trades(self, asset_id: str, *, lookback_seconds: float) -> list[TradePrint]:
        cutoff_ns = time.time_ns() - int(max(float(lookback_seconds), 0.0) * 1_000_000_000)
        with self._lock:
            return [row for row in list(self._trades.get(str(asset_id or ""), ())) if row.ts_ns >= cutoff_ns]

    def recent_spot_points(self, symbol: str | None = None, *, lookback_seconds: float) -> list[SpotPrint]:
        cutoff_ns = time.time_ns() - int(max(float(lookback_seconds), 0.0) * 1_000_000_000)
        with self._lock:
            if symbol:
                return [row for row in list(self._spot.get(str(symbol or "").lower(), ())) if row.ts_ns >= cutoff_ns]
            rows: list[SpotPrint] = []
            for queue in self._spot.values():
                rows.extend(row for row in queue if row.ts_ns >= cutoff_ns)
            return rows

    def recent_liquidations(
        self,
        *,
        lookback_seconds: float,
        exchange: str | None = None,
        symbol: str | None = None,
    ) -> list[LiquidationPrint]:
        cutoff_ns = time.time_ns() - int(max(float(lookback_seconds), 0.0) * 1_000_000_000)
        exchange_lower = str(exchange or "").strip().lower()
        symbol_upper = str(symbol or "").strip().upper()
        with self._lock:
            rows = [row for row in self._liquidations if row.ts_ns >= cutoff_ns]
        if exchange_lower:
            rows = [row for row in rows if row.exchange.lower() == exchange_lower]
        if symbol_upper:
            rows = [row for row in rows if row.symbol.upper() == symbol_upper]
        return rows

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "books": {
                    asset_id: {
                        "bids": [asdict(level) for level in book.bids],
                        "asks": [asdict(level) for level in book.asks],
                        "tick_size": book.tick_size,
                        "updated_ns": book.updated_ns,
                    }
                    for asset_id, book in self._books.items()
                },
                "last_event_lag_ms": self._last_event_lag_ms,
                "last_event_kind": self._last_event_kind,
            }

    def _apply_book(self, event: BaseEvent, now_ns: int) -> None:
        payload = dict(event.payload or {})
        asset_id = str(event.asset_id or payload.get("asset_id") or payload.get("token_id") or "").strip()
        if not asset_id:
            return
        raw_book = payload.get("book")
        if not isinstance(raw_book, dict):
            raw_book = {
                "bids": payload.get("bids") or payload.get("buys") or [],
                "asks": payload.get("asks") or payload.get("sells") or [],
            }
        bids = tuple(_normalized_levels(raw_book.get("bids") or [], descending=True))
        asks = tuple(_normalized_levels(raw_book.get("asks") or [], descending=False))
        current = self._books.get(asset_id)
        tick_size = _coerce_float(payload.get("tick_size")) or (current.tick_size if current is not None else 0.01)
        self._books[asset_id] = BookState(
            asset_id=asset_id,
            bids=bids,
            asks=asks,
            updated_ns=now_ns,
            tick_size=tick_size if tick_size > 0 else 0.01,
        )

    def _apply_trade(self, event: BaseEvent, now_ns: int) -> None:
        payload = dict(event.payload or {})
        asset_id = str(event.asset_id or payload.get("asset_id") or payload.get("token_id") or "").strip()
        if not asset_id:
            return
        price = _coerce_float(payload.get("price"))
        size = _coerce_float(payload.get("size"))
        if price <= 0 or size <= 0:
            return
        side = str(payload.get("side") or payload.get("aggressor_side") or payload.get("taker_side") or "").strip().lower()
        if side not in {"buy", "sell"}:
            side = "unknown"
        notional = _coerce_float(payload.get("notional")) or (price * size)
        queue = self._trades.setdefault(asset_id, deque())
        queue.append(
            TradePrint(
                source=str(event.source or ""),
                asset_id=asset_id,
                side=side,
                price=price,
                size=size,
                notional=notional,
                ts_ns=now_ns,
            )
        )

    def _apply_spot(self, event: BaseEvent, now_ns: int) -> None:
        payload = dict(event.payload or {})
        symbol = str(payload.get("symbol") or payload.get("pair") or "").strip().lower()
        if not symbol:
            return
        price = _coerce_float(payload.get("price"))
        if price <= 0:
            return
        quantity = _coerce_float(payload.get("quantity") or payload.get("size") or payload.get("volume"))
        side = str(payload.get("side") or "").strip().lower()
        if side not in {"buy", "sell"}:
            side = "unknown"
        queue = self._spot.setdefault(symbol, deque())
        queue.append(
            SpotPrint(
                source=str(event.source or ""),
                symbol=symbol,
                side=side,
                price=price,
                quantity=quantity,
                notional=(price * quantity) if quantity > 0 else 0.0,
                ts_ns=now_ns,
            )
        )

    def _apply_liquidation(self, event: BaseEvent, now_ns: int) -> None:
        payload = dict(event.payload or {})
        exchange = str(payload.get("exchange") or event.source or "").strip().lower()
        symbol = str(payload.get("symbol") or "").strip().upper()
        price = _coerce_float(payload.get("price"))
        quantity = _coerce_float(payload.get("quantity") or payload.get("size") or payload.get("volume"))
        notional = _coerce_float(payload.get("notional")) or (price * quantity)
        if not exchange or not symbol or price <= 0 or quantity <= 0 or notional <= 0:
            return
        side = str(payload.get("side") or "").strip().lower()
        if side not in {"buy", "sell"}:
            side = "unknown"
        self._liquidations.append(
            LiquidationPrint(
                source=str(event.source or ""),
                exchange=exchange,
                symbol=symbol,
                side=side,
                price=price,
                quantity=quantity,
                notional=notional,
                ts_ns=now_ns,
            )
        )

    def _apply_tick_size(self, event: BaseEvent) -> None:
        payload = dict(event.payload or {})
        asset_id = str(event.asset_id or payload.get("asset_id") or payload.get("token_id") or "").strip()
        tick_size = _coerce_float(payload.get("tick_size") or payload.get("min_tick_size"))
        if not asset_id or tick_size <= 0:
            return
        current = self._books.get(asset_id)
        if current is None:
            self._books[asset_id] = BookState(
                asset_id=asset_id,
                bids=(),
                asks=(),
                updated_ns=time.time_ns(),
                tick_size=tick_size,
            )
            return
        self._books[asset_id] = BookState(
            asset_id=asset_id,
            bids=current.bids,
            asks=current.asks,
            updated_ns=current.updated_ns,
            tick_size=tick_size,
        )

    def _trim_locked(self, now_ns: int) -> None:
        trade_cutoff = now_ns - int(self.trade_retention_seconds * 1_000_000_000)
        for asset_id, queue in list(self._trades.items()):
            while queue and queue[0].ts_ns < trade_cutoff:
                queue.popleft()
            if not queue:
                self._trades.pop(asset_id, None)

        spot_cutoff = now_ns - int(self.spot_retention_seconds * 1_000_000_000)
        for symbol, queue in list(self._spot.items()):
            while queue and queue[0].ts_ns < spot_cutoff:
                queue.popleft()
            if not queue:
                self._spot.pop(symbol, None)

        liq_cutoff = now_ns - int(self.liquidation_retention_seconds * 1_000_000_000)
        while self._liquidations and self._liquidations[0].ts_ns < liq_cutoff:
            self._liquidations.popleft()


def _normalized_levels(raw_levels: list[Any], *, descending: bool) -> list[Level]:
    aggregated: dict[float, float] = {}
    for raw_level in raw_levels:
        if isinstance(raw_level, dict):
            price = _coerce_float(raw_level.get("price"))
            size = _coerce_float(raw_level.get("size"))
        elif isinstance(raw_level, (list, tuple)) and len(raw_level) >= 2:
            price = _coerce_float(raw_level[0])
            size = _coerce_float(raw_level[1])
        else:
            continue
        if price <= 0 or size <= 0:
            continue
        aggregated[price] = aggregated.get(price, 0.0) + size
    ordered = sorted(aggregated.items(), key=lambda item: item[0], reverse=descending)
    return [Level(price=price, size=size) for price, size in ordered]


def _coerce_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(parsed) or math.isinf(parsed):
        return 0.0
    return parsed
