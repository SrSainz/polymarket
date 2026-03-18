from __future__ import annotations

from pathlib import Path

from app.core.live_broker import LiveBroker
from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.settings import EnvSettings


class _FakeCLOBClient:
    def __init__(self, response: dict | None = None) -> None:
        self.response = response or {"orderID": "live-123", "status": "matched"}
        self.calls: list[dict] = []

    def place_market_order(
        self,
        token_id: str,
        side: str,
        size: float,
        *,
        notional: float | None = None,
        limit_price: float | None = None,
        order_type: str = "FOK",
    ) -> dict:
        self.calls.append(
            {
                "token_id": token_id,
                "side": side,
                "size": size,
                "notional": notional,
                "limit_price": limit_price,
                "order_type": order_type,
            }
        )
        return dict(self.response)


class _MissingOrderbookCLOBClient:
    def place_market_order(
        self,
        token_id: str,
        side: str,
        size: float,
        *,
        notional: float | None = None,
        limit_price: float | None = None,
        order_type: str = "FOK",
    ) -> dict:
        raise RuntimeError("PolyApiException[status_code=404, error_message={'error': 'No orderbook exists for the requested token id'}]")


def _instruction(*, side: TradeSide, action: SignalAction, size: float, price: float) -> CopyInstruction:
    return CopyInstruction(
        action=action,
        side=side,
        asset="asset-1",
        condition_id="cond-1",
        size=size,
        price=price,
        notional=size * price,
        source_wallet="0xsrc",
        source_signal_id=1,
        title="BTC 5 Minute Up or Down",
        slug="btc-updown-5m",
        outcome="Yes",
        category="crypto",
        reason="copy",
    )


def test_live_broker_buy_updates_position_and_execution_log(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    clob = _FakeCLOBClient(response={"orderID": "live-123", "status": "matched", "makingAmount": "4.20", "takingAmount": "10.0"})
    broker = LiveBroker(db, clob, EnvSettings(live_trading=True))

    result = broker.execute(
        _instruction(side=TradeSide.BUY, action=SignalAction.OPEN, size=10.0, price=0.40)
    )

    position = db.get_copy_position("asset-1")
    execution = db.get_recent_executions(limit=1)[0]

    assert result.status == "filled"
    assert position is not None
    assert float(position["size"]) == 10.0
    assert abs(float(position["avg_price"]) - 0.42) < 1e-9
    assert execution["mode"] == "live"
    assert execution["status"] == "filled"
    assert "order_id=live-123" in str(execution["notes"])
    assert clob.calls[0]["order_type"] == "FOK"
    assert abs(float(clob.calls[0]["limit_price"]) - 0.4121) < 1e-9
    db.close()


def test_live_broker_sell_updates_realized_pnl(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-1",
        condition_id="cond-1",
        size=10.0,
        avg_price=0.40,
        realized_pnl=0.0,
        title="BTC 5 Minute Up or Down",
        slug="btc-updown-5m",
        outcome="Yes",
        category="crypto",
    )
    broker = LiveBroker(
        db,
        _FakeCLOBClient(response={"orderID": "live-123", "status": "matched", "makingAmount": "4.0", "takingAmount": "2.20"}),
        EnvSettings(live_trading=True),
    )

    result = broker.execute(
        _instruction(side=TradeSide.SELL, action=SignalAction.REDUCE, size=4.0, price=0.55)
    )

    position = db.get_copy_position("asset-1")

    assert result.status == "filled"
    assert abs(result.pnl_delta - 0.6) < 1e-9
    assert position is not None
    assert float(position["size"]) == 6.0
    assert abs(float(position["realized_pnl"]) - 0.6) < 1e-9
    db.close()


def test_live_broker_skips_unmatched_marketable_order(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    broker = LiveBroker(
        db,
        _FakeCLOBClient(response={"orderID": "live-456", "status": "unmatched", "tradeIDs": []}),
        EnvSettings(live_trading=True),
    )

    result = broker.execute(
        _instruction(side=TradeSide.BUY, action=SignalAction.OPEN, size=10.0, price=0.45)
    )

    assert result.status == "skipped"
    assert db.get_copy_position("asset-1") is None
    assert db.get_recent_executions(limit=5) == []
    db.close()


def test_live_broker_skips_when_orderbook_is_missing(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-1",
        condition_id="cond-1",
        size=10.0,
        avg_price=0.40,
        realized_pnl=0.0,
        title="BTC 5 Minute Up or Down",
        slug="btc-updown-5m",
        outcome="Yes",
        category="crypto",
    )
    broker = LiveBroker(db, _MissingOrderbookCLOBClient(), EnvSettings(live_trading=True))

    result = broker.execute(
        _instruction(side=TradeSide.SELL, action=SignalAction.REDUCE, size=4.0, price=0.55)
    )

    position = db.get_copy_position("asset-1")
    executions = db.get_recent_executions(limit=5)

    assert result.status == "skipped"
    assert result.message == "missing_orderbook"
    assert position is not None
    assert float(position["size"]) == 10.0
    assert executions == []
    db.close()
