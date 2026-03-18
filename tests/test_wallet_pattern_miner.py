from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.services.wallet_pattern_miner import WalletPatternMiner


class _FakeActivityClient:
    def get_closed_positions(self, wallet: str, limit: int = 500, offset: int = 0):  # noqa: ARG002
        return [
            {"category": "crypto", "avgPrice": 0.82, "pnl": 5.0, "slug": "btc-updown-5m-a"},
            {"category": "crypto", "avgPrice": 0.79, "pnl": 4.0, "slug": "btc-updown-5m-a"},
            {"category": "crypto", "avgPrice": 0.18, "pnl": -1.0, "slug": "btc-updown-5m-b"},
        ]

    def get_trades(self, wallet: str | None = None, limit: int = 200, offset: int = 0):  # noqa: ARG002
        return [
            {"side": "BUY"},
            {"side": "BUY"},
            {"side": "SELL"},
        ]


class _FakeGammaClient:
    def get_category(self, slug: str) -> str:  # noqa: ARG002
        return "crypto"


def test_wallet_pattern_miner_builds_hypotheses_from_selected_wallets(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.replace_selected_wallets(
        [{"wallet": "0xabc", "score": 10.0, "win_rate": 0.7, "recent_trades": 12, "pnl": 20.0}]
    )
    research_root = tmp_path / "research"
    research_root.mkdir(parents=True, exist_ok=True)

    payload = WalletPatternMiner(
        db,
        _FakeActivityClient(),
        _FakeGammaClient(),
        research_root,
    ).run(wallet_limit=1)

    assert payload["wallets"] == ["0xabc"]
    assert payload["hypotheses"]
    assert payload["patterns"]
    assert (research_root / "hypotheses" / "top_wallet_patterns.json").exists()
    db.close()
