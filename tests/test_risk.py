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
        bankroll=100.0,
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
        daily_profit_gross=0.0,
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
        daily_profit_gross=0.0,
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
        daily_profit_gross=0.0,
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
        daily_profit_gross=0.0,
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
        daily_profit_gross=0.0,
        reference_price=0.9,
    )
    assert not allowed
    assert "max_price" in reason


def test_daily_loss_uses_10_percent_of_bankroll_cap() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        bankroll=1000.0,
        max_daily_loss=500.0,
        max_daily_loss_pct=0.10,
        max_position_per_market=2000.0,
        max_total_exposure=2000.0,
        slippage_limit=0.3,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=10.0)

    allowed, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=0.0,
        daily_pnl=-105.0,
        daily_profit_gross=0.0,
        reference_price=0.5,
    )
    assert not allowed
    assert "max_daily_loss" in reason


def test_daily_loss_limit_expands_with_realized_daily_gains() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        bankroll=1000.0,
        max_daily_loss=500.0,
        max_daily_loss_pct=0.10,
        max_position_per_market=2000.0,
        max_total_exposure=2000.0,
        slippage_limit=0.3,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=10.0)

    allowed, _ = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=0.0,
        daily_pnl=-130.0,
        daily_profit_gross=40.0,
        reference_price=0.5,
    )
    assert allowed

    allowed2, reason2 = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=0.0,
        daily_pnl=-145.0,
        daily_profit_gross=40.0,
        reference_price=0.5,
    )
    assert not allowed2
    assert "max_daily_loss" in reason2


def test_exposure_limit_shrinks_when_effective_bankroll_drops() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        bankroll=1000.0,
        max_position_per_market=1000.0,
        max_total_exposure=1000.0,
        max_daily_loss=500.0,
        max_daily_loss_pct=0.10,
        slippage_limit=0.3,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=40.0)

    allowed, _ = risk.evaluate_instruction(
        instruction,
        current_market_notional=850.0,
        current_total_exposure=850.0,
        daily_pnl=0.0,
        daily_profit_gross=0.0,
        effective_bankroll=900.0,
        reference_price=0.5,
    )
    assert allowed

    blocked, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=880.0,
        current_total_exposure=880.0,
        daily_pnl=0.0,
        daily_profit_gross=0.0,
        effective_bankroll=900.0,
        reference_price=0.5,
    )
    assert not blocked
    assert "max_position_per_market" in reason or "max_total_exposure" in reason


def test_blocks_dynamic_market_above_dynamic_allocation_cap() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        bankroll=1000.0,
        max_position_per_market=1000.0,
        max_total_exposure=1000.0,
        max_daily_loss=200.0,
        max_daily_loss_pct=0.10,
        slippage_limit=0.3,
        dynamic_keywords=["bitcoin", "5m"],
        dynamic_max_allocation_pct=0.20,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=30.0)
    instruction.title = "Bitcoin up or down in 5m?"

    allowed, reason = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=150.0,
        current_dynamic_exposure=180.0,
        daily_pnl=0.0,
        daily_profit_gross=0.0,
        reference_price=0.5,
    )
    assert not allowed
    assert "dynamic_allocation_cap" in reason


def test_allows_dynamic_market_under_dynamic_allocation_cap() -> None:
    config = BotConfig(
        watched_wallets=["0xabc"],
        bankroll=1000.0,
        max_position_per_market=1000.0,
        max_total_exposure=1000.0,
        max_daily_loss=200.0,
        max_daily_loss_pct=0.10,
        slippage_limit=0.3,
        dynamic_keywords=["bitcoin", "5m"],
        dynamic_max_allocation_pct=0.20,
    )
    risk = RiskManager(config)
    instruction = _instruction(notional=20.0)
    instruction.title = "Bitcoin up or down in 5m?"

    allowed, _ = risk.evaluate_instruction(
        instruction,
        current_market_notional=0.0,
        current_total_exposure=100.0,
        current_dynamic_exposure=170.0,
        daily_pnl=0.0,
        daily_profit_gross=0.0,
        reference_price=0.5,
    )
    assert allowed
