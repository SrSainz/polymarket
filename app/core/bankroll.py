from __future__ import annotations


def calculate_reserved_profit(*, profit_gross: float, profit_keep_ratio: float) -> float:
    normalized_profit = max(float(profit_gross), 0.0)
    normalized_ratio = max(min(float(profit_keep_ratio), 1.0), 0.0)
    return normalized_profit * normalized_ratio


def calculate_effective_bankroll(
    *,
    base_bankroll: float,
    realized_pnl: float,
    profit_gross: float,
    profit_keep_ratio: float,
) -> float:
    reserved_profit = calculate_reserved_profit(
        profit_gross=profit_gross,
        profit_keep_ratio=profit_keep_ratio,
    )
    return max(float(base_bankroll) + float(realized_pnl) - reserved_profit, 0.0)
