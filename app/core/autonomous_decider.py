from __future__ import annotations

import time
from decimal import Decimal, ROUND_DOWN

from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.settings import BotConfig


class AutonomousDecider:
    """
    Optional strategy overlay: generate sell/reduce instructions based on
    local risk/profit heuristics, independent from source-wallet actions.
    """

    def __init__(self, config: BotConfig, db: Database) -> None:
        self.config = config
        self.db = db

    def build_exit_instruction(
        self,
        *,
        asset: str,
        condition_id: str,
        size: float,
        avg_price: float,
        mark_price: float,
        title: str,
        slug: str,
        outcome: str,
        category: str,
    ) -> CopyInstruction | None:
        if not self.config.autonomous_decisions_enabled:
            return None

        if size <= 0 or avg_price <= 0 or mark_price <= 0:
            return None

        now_ts = int(time.time())
        cooldown_seconds = self.config.autonomous_cooldown_minutes * 60
        last_action_ts = self.db.get_last_autonomous_sell_ts(asset)
        if last_action_ts is not None and (now_ts - last_action_ts) < cooldown_seconds:
            return None

        pnl_pct = (mark_price - avg_price) / avg_price

        # 1) Immediate hard exits on take-profit / stop-loss.
        if pnl_pct >= self.config.autonomous_take_profit_pct:
            return _build_sell_instruction(
                action=SignalAction.CLOSE,
                asset=asset,
                condition_id=condition_id,
                size=size,
                price=mark_price,
                title=title,
                slug=slug,
                outcome=outcome,
                category=category,
                reason=f"autonomous take_profit {pnl_pct * 100:.2f}%",
            )

        if pnl_pct <= -abs(self.config.autonomous_stop_loss_pct):
            return _build_sell_instruction(
                action=SignalAction.CLOSE,
                asset=asset,
                condition_id=condition_id,
                size=size,
                price=mark_price,
                title=title,
                slug=slug,
                outcome=outcome,
                category=category,
                reason=f"autonomous stop_loss {pnl_pct * 100:.2f}%",
            )

        # 2) Depreciation signal: if short-window mark is falling fast, de-risk.
        window_seconds = self.config.autonomous_depreciation_window_minutes * 60
        reference_mark = self.db.get_position_mark_before(asset, now_ts - window_seconds)
        if reference_mark is None or reference_mark <= 0:
            return None

        depreciation_pct = (mark_price - reference_mark) / reference_mark
        if depreciation_pct > -abs(self.config.autonomous_depreciation_threshold_pct):
            return None

        reduce_size = _round_down(size * self.config.autonomous_reduce_fraction, precision="0.0001")
        if reduce_size <= 0:
            return None

        action = SignalAction.REDUCE
        if reduce_size >= size:
            action = SignalAction.CLOSE
            reduce_size = _round_down(size, precision="0.0001")

        return _build_sell_instruction(
            action=action,
            asset=asset,
            condition_id=condition_id,
            size=reduce_size,
            price=mark_price,
            title=title,
            slug=slug,
            outcome=outcome,
            category=category,
            reason=f"autonomous depreciation {depreciation_pct * 100:.2f}%/{self.config.autonomous_depreciation_window_minutes}m",
        )


def _build_sell_instruction(
    *,
    action: SignalAction,
    asset: str,
    condition_id: str,
    size: float,
    price: float,
    title: str,
    slug: str,
    outcome: str,
    category: str,
    reason: str,
) -> CopyInstruction:
    notional = size * price
    return CopyInstruction(
        action=action,
        side=TradeSide.SELL,
        asset=asset,
        condition_id=condition_id,
        size=size,
        price=price,
        notional=notional,
        source_wallet="autonomous",
        source_signal_id=0,
        title=title,
        slug=slug,
        outcome=outcome,
        category=category,
        reason=reason,
    )


def _round_down(value: float, precision: str) -> float:
    quant = Decimal(precision)
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_DOWN))
