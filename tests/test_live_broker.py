from __future__ import annotations

from pathlib import Path

from app.core.live_broker import LiveBroker
from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.settings import EnvSettings


class _FakeCLOBClient:
    def place_market_order(self, token_id: str, side: str, size: float, *, notional: float | None = None) -> dict:
        return {"orderID": "live-123", "status": "matched"}


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
    broker = LiveBroker(db, _FakeCLOBClient(), EnvSettings(live_trading=True))

    result = broker.execute(
        _instruction(side=TradeSide.BUY, action=SignalAction.OPEN, size=10.0, price=0.45)
    )

    position = db.get_copy_position("asset-1")
    execution = db.get_recent_executions(limit=1)[0]

    assert result.status == "filled"
    assert position is not None
    assert float(position["size"]) == 10.0
    assert float(position["avg_price"]) == 0.45
    assert execution["mode"] == "live"
    assert execution["status"] == "filled"
    assert "order_id=live-123" in str(execution["notes"])
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
    broker = LiveBroker(db, _FakeCLOBClient(), EnvSettings(live_trading=True))

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
