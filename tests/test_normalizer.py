from app.core.normalizer import detect_position_changes
from app.models import SignalAction, SourcePosition


def _pos(wallet: str, asset: str, size: float) -> SourcePosition:
    return SourcePosition(
        wallet=wallet,
        asset=asset,
        condition_id="cond",
        size=size,
        avg_price=0.55,
        current_price=0.56,
        title="Market",
        slug="market",
        outcome="Yes",
        category="crypto",
        observed_at=1,
    )


def test_detect_open_add_reduce_close() -> None:
    wallet = "0xabc"

    previous = {"asset-a": _pos(wallet, "asset-a", 10)}
    current = {
        "asset-a": _pos(wallet, "asset-a", 15),
        "asset-b": _pos(wallet, "asset-b", 5),
    }

    changes = detect_position_changes(wallet=wallet, previous=previous, current=current, noise_threshold=0.5)
    actions = {change.asset: change.action for change in changes}

    assert actions["asset-a"] == SignalAction.ADD
    assert actions["asset-b"] == SignalAction.OPEN

    previous_2 = current
    current_2 = {"asset-a": _pos(wallet, "asset-a", 4)}
    changes_2 = detect_position_changes(wallet=wallet, previous=previous_2, current=current_2, noise_threshold=0.5)
    actions_2 = {change.asset: change.action for change in changes_2}

    assert actions_2["asset-a"] == SignalAction.REDUCE
    assert actions_2["asset-b"] == SignalAction.CLOSE


def test_ignore_small_noise() -> None:
    wallet = "0xabc"
    previous = {"asset": _pos(wallet, "asset", 10.0)}
    current = {"asset": _pos(wallet, "asset", 10.3)}

    changes = detect_position_changes(wallet=wallet, previous=previous, current=current, noise_threshold=0.5)
    assert changes == []
