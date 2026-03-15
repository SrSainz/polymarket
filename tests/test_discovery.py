import json
from pathlib import Path

from strategy import OfficialApiClient, ResearchConfig, discovery


def test_discovery_extracts_yes_no_tokens(monkeypatch) -> None:
    fixture = json.loads(Path("tests/fixtures/gamma_market_sample.json").read_text(encoding="utf-8"))

    def fake_get_json(self, base_url, path, params, rate_key):  # noqa: ANN001
        assert "markets" in path or path == "/markets"
        return fixture

    monkeypatch.setattr(OfficialApiClient, "get_json", fake_get_json)
    monkeypatch.setattr(OfficialApiClient, "get_fee_rate_bps", lambda self, token_id, fallback_bps=None: 12.5)

    market = discovery(ResearchConfig())

    assert market.slug == "btc-updown-5m-1710500100"
    assert market.token_id_yes == "TOKEN_YES"
    assert market.token_id_no == "TOKEN_NO"
    assert market.fee_rate_bps_yes == 12.5
