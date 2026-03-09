from app.core.risk import RiskManager
from app.models import CopyInstruction, SignalAction, TradeSide
from app.settings import BotConfig


def _instruction(notional: float = 20.0, side: TradeSide = TradeSide.BUY, category: str = "crypto") -> CopyInstruction:
    return CopyInstruction(
        action=SignalAction.OPEN,
        side=side,
        asset="asset",
        condition_id="cond",
        size=40.0,
        price=0.5,
        notional=notional,
        source_wallet="0xabc",
        source_signal_id=1,
        title="Market",
        slug="market",
        outcome="Yes",
        category=category,
        reason="",
    )


def test_blocks_max_exposure() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        max_total_exposure=100.0,
        max_position_per_market=100.0,
        max_daily_loss=50.0,
        slippage_limit=0.1,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=30.0)

    allowed, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=10.0,
        current_total_exposure=80.0,
        daily_pnl=0.0,
        reference_price=0.5,
    )
    assert not allowed
    assert "max_total_exposure" in reason


def test_blocks_daily_loss_for_buys() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        max_daily_loss=25.0,
        max_position_per_market=200.0,
        max_total_exposure=300.0,
        slippage_limit=0.1,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=10.0)

    allowed, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=0.0,
        daily_pnl=-30.0,
        reference_price=0.5,
    )
    assert not allowed
    assert "max_daily_loss" in reason


def test_allows_sell_even_if_daily_loss_hit() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        max_daily_loss=25.0,
        max_position_per_market=20.0,
        max_total_exposure=20.0,
        slippage_limit=0.1,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=100.0, side=TradeSide.SELL)

    allowed, _ = risk.evaluate_instruction(
        instruction,
        current_market_notional=15.0,
        current_total_exposure=18.0,
        daily_pnl=-30.0,
        reference_price=0.5,
    )
    assert allowed


def test_blocks_buy_below_min_price() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        min_price=0.2,
        max_price=0.9,
        max_daily_loss=25.0,
        max_position_per_market=200.0,
        max_total_exposure=300.0,
        slippage_limit=0.2,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=10.0)
    instruction.price = 0.1

    allowed, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=0.0,
        daily_pnl=0.0,
        reference_price=0.1,
    )
    assert not allowed
    assert "min_price" in reason


def test_blocks_buy_above_max_price() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        min_price=0.2,
        max_price=0.8,
        max_daily_loss=25.0,
        max_position_per_market=200.0,
        max_total_exposure=300.0,
        slippage_limit=0.3,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=10.0)
    instruction.price = 0.9

    allowed, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=0.0,
        daily_pnl=0.0,
        reference_price=0.9,
    )
    assert not allowed
    assert "max_price" in reason
