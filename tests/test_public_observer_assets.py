from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_FILES = (
    ROOT / "web" / "index.html",
    ROOT / "web" / "assets" / "app.js",
    ROOT / "web" / "assets" / "styles.css",
)


def test_public_observer_has_no_live_controls_or_credentials() -> None:
    content = "\n".join(path.read_text(encoding="utf-8") for path in PUBLIC_FILES)
    forbidden = (
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "TELEGRAM_BOT_TOKEN",
        "LiveBroker",
        "LIVE_TRADING",
        "execute_copy",
        "armLive",
        "pauseLive",
        "summary_now",
    )
    assert not [token for token in forbidden if token in content]


def test_public_observer_only_uses_public_market_hosts() -> None:
    content = "\n".join(path.read_text(encoding="utf-8") for path in PUBLIC_FILES)
    assert "https://gamma-api.polymarket.com" in content
    assert "https://clob.polymarket.com" in content
    assert "wss://sports-api.polymarket.com/ws" in content
    assert "nas.polysainz.com" not in content
