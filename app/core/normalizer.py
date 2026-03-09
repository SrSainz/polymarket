from __future__ import annotations

import time

from app.models import NormalizedSignal, SignalAction, SourcePosition


def detect_position_changes(
    *,
    wallet: str,
    previous: dict[str, SourcePosition],
    current: dict[str, SourcePosition],
    noise_threshold: float,
) -> list[NormalizedSignal]:
    now_ts = int(time.time())
    output: list[NormalizedSignal] = []

    for asset in sorted(set(previous.keys()) | set(current.keys())):
        prev = previous.get(asset)
        curr = current.get(asset)

        prev_size = prev.size if prev else 0.0
        curr_size = curr.size if curr else 0.0
        delta = curr_size - prev_size

        if abs(delta) < noise_threshold:
            continue

        if prev_size <= 0 < curr_size:
            action = SignalAction.OPEN
        elif curr_size <= 0 < prev_size:
            action = SignalAction.CLOSE
        elif delta > 0:
            action = SignalAction.ADD
        else:
            action = SignalAction.REDUCE

        reference_position = curr or prev
        if reference_position is None:
            continue

        reference_price = reference_position.current_price or reference_position.avg_price or 0.5
        signal = NormalizedSignal(
            event_key=_event_key(wallet, asset, action, prev_size, curr_size),
            wallet=wallet,
            asset=asset,
            condition_id=reference_position.condition_id,
            action=action,
            prev_size=prev_size,
            new_size=curr_size,
            delta_size=delta,
            reference_price=reference_price,
            title=reference_position.title,
            slug=reference_position.slug,
            outcome=reference_position.outcome,
            category=reference_position.category,
            detected_at=now_ts,
        )
        output.append(signal)

    return output


def _event_key(wallet: str, asset: str, action: SignalAction, prev_size: float, curr_size: float) -> str:
    prev_fmt = f"{prev_size:.6f}"
    curr_fmt = f"{curr_size:.6f}"
    return f"{wallet}:{asset}:{action.value}:{prev_fmt}:{curr_fmt}"
