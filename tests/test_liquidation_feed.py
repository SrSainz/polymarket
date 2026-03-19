from __future__ import annotations

from app.services.liquidation_feed import _parse_binance_liquidations, _parse_bybit_liquidations


def test_parse_binance_liquidations_accepts_force_order_shape() -> None:
    payload = {
        "e": "forceOrder",
        "E": 1773341700123,
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "p": "70123.4",
            "q": "0.25",
            "T": 1773341700123,
        },
    }

    rows = _parse_binance_liquidations(payload)

    assert rows == [
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "side": "sell",
            "price": 70123.4,
            "quantity": 0.25,
            "notional": 17530.85,
            "timestamp": 1773341700123,
        }
    ]


def test_parse_bybit_liquidations_accepts_public_stream_shape() -> None:
    payload = {
        "topic": "allLiquidation.BTCUSDT",
        "ts": 1773341700999,
        "data": [
            {"s": "BTCUSDT", "S": "Buy", "p": "70200", "v": "0.4", "T": 1773341700000},
        ],
    }

    rows = _parse_bybit_liquidations(payload)

    assert rows == [
        {
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "side": "buy",
            "price": 70200.0,
            "quantity": 0.4,
            "notional": 28080.0,
            "timestamp": 1773341700000,
        }
    ]
