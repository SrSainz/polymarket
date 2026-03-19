from __future__ import annotations

from app.core.event_bus import BaseEvent, EventBus


def test_event_bus_dispatches_specific_and_wildcard_handlers() -> None:
    bus = EventBus()
    seen: list[tuple[str, str]] = []

    def on_all(event: BaseEvent) -> None:
        seen.append(("all", event.kind))

    def on_trade(event: BaseEvent) -> None:
        seen.append(("trade", event.kind))

    bus.subscribe("*", on_all)
    bus.subscribe("market_trade", on_trade)
    bus.emit(kind="market_trade", source="test", payload={"price": 0.51})

    assert seen == [("all", "market_trade"), ("trade", "market_trade")]


def test_event_bus_unsubscribe_removes_handler() -> None:
    bus = EventBus()
    seen: list[str] = []

    def on_event(event: BaseEvent) -> None:
        seen.append(event.kind)

    bus.subscribe("book_snapshot", on_event)
    bus.unsubscribe("book_snapshot", on_event)
    bus.emit(kind="book_snapshot", source="test", payload={})

    assert seen == []
