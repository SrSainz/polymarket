from __future__ import annotations

from types import SimpleNamespace

from app.models import NormalizedSignal, SignalAction
from app.services.execute_copy import ExecuteCopyService
from app.settings import BotConfig


def _signal(title: str, slug: str) -> NormalizedSignal:
    return NormalizedSignal(
        event_key="evt",
        wallet="0xabc",
        asset="asset",
        condition_id="cond",
        action=SignalAction.OPEN,
        prev_size=0.0,
        new_size=10.0,
        delta_size=10.0,
        reference_price=0.5,
        title=title,
        slug=slug,
        outcome="Yes",
        category="crypto",
        detected_at=1,
    )


def test_live_scope_skips_non_btc5m_entries() -> None:
    fake_service = SimpleNamespace(
        settings=SimpleNamespace(config=BotConfig(watched_wallets=["0xabc"], live_only_btc5m=True))
    )
    signal = _signal("Bitcoin Up or Down - March 10", "bitcoin-up-or-down-march-10")

    should_skip = ExecuteCopyService._should_skip_signal_for_mode(fake_service, signal=signal, mode="live")

    assert should_skip is True


def test_live_scope_keeps_btc5m_entries() -> None:
    fake_service = SimpleNamespace(
        settings=SimpleNamespace(config=BotConfig(watched_wallets=["0xabc"], live_only_btc5m=True))
    )
    signal = _signal("BTC 5 Minute Up or Down", "btc-updown-5m")

    should_skip = ExecuteCopyService._should_skip_signal_for_mode(fake_service, signal=signal, mode="live")

    assert should_skip is False
