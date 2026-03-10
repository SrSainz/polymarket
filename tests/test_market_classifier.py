from __future__ import annotations

from app.core.market_classifier import is_btc5m_market, matches_market_keywords


def test_btc5m_market_matches_actual_five_minute_market() -> None:
    assert is_btc5m_market(
        title="BTC 5 Minute Up or Down",
        slug="btc-updown-5m-20260310-2015",
        event_slug="btc-updown-5m",
    )


def test_btc5m_market_rejects_daily_btc_market() -> None:
    assert not is_btc5m_market(
        title="Bitcoin Up or Down - March 10",
        slug="bitcoin-up-or-down-march-10",
        event_slug="bitcoin-up-or-down-march-10",
    )


def test_keyword_matching_is_normalized() -> None:
    assert matches_market_keywords(
        title="BTC 5 Minute Up or Down",
        slug="btc-updown-5m",
        event_slug="btc-updown-5m",
        keywords=["bitcoin 5 minute up or down", "btc-updown-5m"],
    )
