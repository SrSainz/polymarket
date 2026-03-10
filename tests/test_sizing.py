from app.core.sizing import SizingEngine
from app.models import NormalizedSignal, SignalAction
from app.settings import BotConfig


def _signal(action: SignalAction, prev_size: float, new_size: float, delta: float) -> NormalizedSignal:
    return NormalizedSignal(
        event_key="evt",
        wallet="0xabc",
        asset="asset",
        condition_id="cond",
        action=action,
        prev_size=prev_size,
        new_size=new_size,
        delta_size=delta,
        reference_price=0.5,
        title="M",
        slug="m",
        outcome="Yes",
        category="crypto",
        detected_at=1,
    )


def test_fixed_amount_sizing() -> None:
    cfg = BotConfig(
        watched_wallets=["0xabc"],
        sizing_mode="fixed_amount_per_trade",
        fixed_amount_per_trade=20.0,
        min_trade_amount=5.0,
        bankroll=100.0,
    )
    sizing = SizingEngine(cfg)
    signal = _signal(SignalAction.OPEN, 0, 100, 100)

    size = sizing.calculate_buy_size(signal, execution_price=0.5, current_total_exposure=0.0)
    assert size == 40.0


def test_proportional_sizing_respects_bankroll() -> None:
    cfg = BotConfig(
        watched_wallets=["0xabc"],
        sizing_mode="proportional_to_source",
        proportional_scale=0.5,
        bankroll=50.0,
        min_trade_amount=5.0,
    )
    sizing = SizingEngine(cfg)
    signal = _signal(SignalAction.ADD, 100, 200, 100)

    size = sizing.calculate_buy_size(signal, execution_price=0.5, current_total_exposure=45.0)
    assert size > 0
    assert size <= 10.0


def test_reduce_size_fractional() -> None:
    cfg = BotConfig(watched_wallets=["0xabc"])
    sizing = SizingEngine(cfg)
    signal = _signal(SignalAction.REDUCE, 100, 60, -40)

    size = sizing.calculate_reduce_size(signal, copy_position_size=20)
    assert size == 8.0


def test_proportional_sizing_uses_effective_bankroll_override() -> None:
    cfg = BotConfig(
        watched_wallets=["0xabc"],
        sizing_mode="proportional_to_source",
        proportional_scale=0.10,
        bankroll=1000.0,
        min_trade_amount=5.0,
    )
    sizing = SizingEngine(cfg)
    signal = _signal(SignalAction.OPEN, 0, 1000, 1000)

    size_base = sizing.calculate_buy_size(
        signal,
        execution_price=0.5,
        current_total_exposure=0.0,
        effective_bankroll=1000.0,
    )
    size_compounded = sizing.calculate_buy_size(
        signal,
        execution_price=0.5,
        current_total_exposure=0.0,
        effective_bankroll=1200.0,
    )

    assert size_compounded > size_base
