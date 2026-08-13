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


def test_public_observer_preserves_missing_numbers_and_escapes_invalid_dates() -> None:
    content = (ROOT / "web" / "assets" / "app.js").read_text(encoding="utf-8")
    assert 'value === null || value === undefined' in content
    assert 'value.trim() === ""' in content
    assert 'return escapeHtml(String(value));' in content


def test_public_observer_guards_paper_storage_and_refresh_races() -> None:
    content = (ROOT / "web" / "assets" / "app.js").read_text(encoding="utf-8")
    assert "parsed.filter(isPaperEntry).slice(0, 50)" in content
    assert 'entry.side === "YES" || entry.side === "NO"' in content
    assert 'escapeHtml(entry.side)' in content
    assert "state.refreshPending = true" in content
    assert "state.requestId += 1" in content
