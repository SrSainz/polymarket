import pytest

from app.services.execute_copy import calculate_effective_bankroll


def test_profit_policy_reinvests_half_of_profits() -> None:
    bankroll = calculate_effective_bankroll(
        base_bankroll=16.87,
        prior_realized_pnl=20.0,
        prior_profit_gross=20.0,
        profit_keep_ratio=0.50,
    )
    assert bankroll == pytest.approx(26.87)


def test_profit_policy_keeps_losses_fully_in_trading_bankroll() -> None:
    bankroll = calculate_effective_bankroll(
        base_bankroll=16.87,
        prior_realized_pnl=-6.0,
        prior_profit_gross=0.0,
        profit_keep_ratio=0.50,
    )
    assert bankroll == pytest.approx(10.87)


def test_profit_policy_handles_profit_then_loss() -> None:
    bankroll = calculate_effective_bankroll(
        base_bankroll=16.87,
        prior_realized_pnl=15.0,
        prior_profit_gross=20.0,
        profit_keep_ratio=0.50,
    )
    assert bankroll == pytest.approx(21.87)
