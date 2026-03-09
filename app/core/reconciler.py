from __future__ import annotations

from app.models import NormalizedSignal, SignalAction


class Reconciler:
    """
    Compares source wallet signal vs replica wallet state and determines execution intent.
    """

    def decide(self, signal: NormalizedSignal, copy_position_size: float) -> tuple[SignalAction, float]:
        if signal.action in (SignalAction.OPEN, SignalAction.ADD):
            return signal.action, 0.0

        if signal.action == SignalAction.CLOSE:
            return SignalAction.CLOSE, max(copy_position_size, 0.0)

        if signal.action == SignalAction.REDUCE:
            if signal.prev_size <= 0:
                return SignalAction.REDUCE, 0.0
            fraction = min(abs(signal.delta_size) / signal.prev_size, 1.0)
            return SignalAction.REDUCE, copy_position_size * fraction

        return signal.action, 0.0
