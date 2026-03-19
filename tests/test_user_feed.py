from __future__ import annotations

import json
import logging

from app.polymarket.user_feed import UserFeed


def test_user_feed_filters_supported_event_types() -> None:
    feed = UserFeed(
        "wss://example.test/user",
        logging.getLogger("test-user-feed"),
        api_key="key",
        api_secret="secret",
        api_passphrase="pass",
        enabled=True,
    )
    received: list[dict] = []
    feed.register_listener(received.append)

    feed._handle_message(
        json.dumps(
            [
                {"event_type": "trade", "id": "trade-1"},
                {"event_type": "order", "id": "order-1"},
                {"event_type": "heartbeat", "id": "ignore-1"},
            ]
        )
    )

    assert [row["id"] for row in received] == ["trade-1", "order-1"]


def test_user_feed_ignores_invalid_json_payload() -> None:
    feed = UserFeed(
        "wss://example.test/user",
        logging.getLogger("test-user-feed"),
        api_key="key",
        api_secret="secret",
        api_passphrase="pass",
        enabled=True,
    )
    received: list[dict] = []
    feed.register_listener(received.append)

    feed._handle_message("{not-json}")

    assert received == []
