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
        settings=SimpleNamespace(config=BotConfig(watched_wallets=["0xabc"], live_only_btc5m=True)),
        db=SimpleNamespace(get_copy_position=lambda asset: None, list_copy_positions=lambda: []),
    )
    signal = _signal("Bitcoin Up or Down - March 10", "bitcoin-up-or-down-march-10")

    skip_reason = ExecuteCopyService._skip_reason_for_mode(fake_service, signal=signal, mode="live")

    assert skip_reason == "live_only_btc5m"


def test_live_scope_keeps_btc5m_entries() -> None:
    fake_service = SimpleNamespace(
        settings=SimpleNamespace(config=BotConfig(watched_wallets=["0xabc"], live_only_btc5m=True)),
        db=SimpleNamespace(get_copy_position=lambda asset: None, list_copy_positions=lambda: []),
        _get_open_btc5m_positions_count=lambda: 0,
    )
    signal = _signal("BTC 5 Minute Up or Down", "btc-updown-5m")

    skip_reason = ExecuteCopyService._skip_reason_for_mode(fake_service, signal=signal, mode="live")

    assert skip_reason == ""


def test_live_scope_blocks_when_btc5m_open_positions_cap_is_reached() -> None:
    fake_service = SimpleNamespace(
        settings=SimpleNamespace(
            config=BotConfig(
                watched_wallets=["0xabc"],
                live_only_btc5m=True,
                live_btc5m_max_open_positions=3,
            )
        ),
        db=SimpleNamespace(get_copy_position=lambda asset: None, list_copy_positions=lambda: []),
        _get_open_btc5m_positions_count=lambda: 3,
    )
    signal = _signal("BTC 5 Minute Up or Down", "btc-updown-5m")

    skip_reason = ExecuteCopyService._skip_reason_for_mode(fake_service, signal=signal, mode="live")

    assert skip_reason == "live_btc5m_max_open_positions"
