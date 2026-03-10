from __future__ import annotations

from app.core.autonomous_decider import AutonomousDecider
from app.settings import BotConfig


class FakeDB:
    def __init__(self, last_sell_ts: int | None = None, mark_before: float | None = None) -> None:
        self.last_sell_ts = last_sell_ts
        self.mark_before = mark_before

    def get_last_autonomous_sell_ts(self, asset: str) -> int | None:
        return self.last_sell_ts

    def get_position_mark_before(self, asset: str, cutoff_ts: int) -> float | None:
        return self.mark_before


def test_take_profit_generates_close() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        autonomous_decisions_enabled=True,
        autonomous_take_profit_pct=0.10,
        autonomous_stop_loss_pct=0.10,
        autonomous_depreciation_threshold_pct=0.05,
    )
    decider = AutonomousDecider(config, FakeDB())
    instruction = decider.build_exit_instruction(
        asset="asset",
        condition_id="cond",
        size=20.0,
        avg_price=0.5,
        mark_price=0.56,
        title="M",
        slug="m",
        outcome="Yes",
        category="sports",
    )
    assert instruction is not None
    assert instruction.action.value == "close"
    assert instruction.side.value == "sell"


def test_stop_loss_generates_close() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        autonomous_decisions_enabled=True,
        autonomous_take_profit_pct=0.20,
        autonomous_stop_loss_pct=0.08,
        autonomous_depreciation_threshold_pct=0.05,
    )
    decider = AutonomousDecider(config, FakeDB())
    instruction = decider.build_exit_instruction(
        asset="asset",
        condition_id="cond",
        size=20.0,
        avg_price=0.5,
        mark_price=0.45,
        title="M",
        slug="m",
        outcome="Yes",
        category="sports",
    )
    assert instruction is not None
    assert instruction.action.value == "close"


def test_depreciation_generates_reduce() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        autonomous_decisions_enabled=True,
        autonomous_take_profit_pct=0.50,
        autonomous_stop_loss_pct=0.50,
        autonomous_depreciation_window_minutes=30,
        autonomous_depreciation_threshold_pct=0.03,
        autonomous_reduce_fraction=0.5,
    )
    decider = AutonomousDecider(config, FakeDB(mark_before=0.50))
    instruction = decider.build_exit_instruction(
        asset="asset",
        condition_id="cond",
        size=10.0,
        avg_price=0.50,
        mark_price=0.47,
        title="M",
        slug="m",
        outcome="Yes",
        category="sports",
    )
    assert instruction is not None
    assert instruction.action.value == "reduce"
    assert instruction.size == 5.0
