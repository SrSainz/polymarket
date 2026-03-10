from __future__ import annotations

from app.core.reconciler import Reconciler
from app.core.risk import RiskManager
from app.core.sizing import SizingEngine
from app.models import CopyInstruction, NormalizedSignal, SignalAction, TradeSide


class Copier:
    def __init__(self, sizing: SizingEngine, risk: RiskManager, reconciler: Reconciler) -> None:
        self.sizing = sizing
        self.risk = risk
        self.reconciler = reconciler

    def build_instruction(
        self,
        *,
        signal: NormalizedSignal,
        copy_position_size: float,
        copy_position_avg_price: float,
        execution_price: float,
        current_total_exposure: float,
        current_dynamic_exposure: float = 0.0,
        daily_pnl: float,
        daily_profit_gross: float,
        effective_bankroll: float | None = None,
    ) -> tuple[CopyInstruction | None, str]:
        action, _ = self.reconciler.decide(signal, copy_position_size)

        if action in (SignalAction.OPEN, SignalAction.ADD):
            size = self.sizing.calculate_buy_size(
                signal,
                execution_price=execution_price,
                current_total_exposure=current_total_exposure,
                effective_bankroll=effective_bankroll,
            )
            side = TradeSide.BUY
        else:
            size = self.sizing.calculate_reduce_size(signal, copy_position_size)
            side = TradeSide.SELL

        if size <= 0:
            return None, "size below minimum"

        notional = size * execution_price
        instruction = CopyInstruction(
            action=action,
            side=side,
            asset=signal.asset,
            condition_id=signal.condition_id,
            size=size,
            price=execution_price,
            notional=notional,
            source_wallet=signal.wallet,
            source_signal_id=signal.id or 0,
            title=signal.title,
            slug=signal.slug,
            outcome=signal.outcome,
            category=signal.category,
            reason="",
        )

        market_notional = abs(copy_position_size * (copy_position_avg_price or execution_price))
        allowed, reason = self.risk.evaluate_instruction(
            instruction,
            current_market_notional=market_notional,
            current_total_exposure=current_total_exposure,
            current_dynamic_exposure=current_dynamic_exposure,
            daily_pnl=daily_pnl,
            daily_profit_gross=daily_profit_gross,
            effective_bankroll=effective_bankroll,
            reference_price=signal.reference_price,
        )
        if not allowed:
            return None, reason
        return instruction, "ok"
