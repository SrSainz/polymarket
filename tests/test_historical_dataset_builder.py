from __future__ import annotations

import json
from pathlib import Path

from app.services.historical_dataset_builder import HistoricalDatasetBuilder


def test_historical_dataset_builder_compacts_capture_logs(tmp_path: Path) -> None:
    research_root = tmp_path / "research"
    research_root.mkdir(parents=True, exist_ok=True)
    events_path = research_root / "paper_events.jsonl"
    snapshots_path = research_root / "paper_snapshots.jsonl"
    events_path.write_text(
        "\n".join(
            [
                json.dumps({"ts_ms": 1710500000000, "event": "book", "token_id": "TOKEN_UP", "bids": [[0.49, 10]], "asks": [[0.51, 10]]}),
                json.dumps({"ts_ms": 1710500000000, "event": "book", "token_id": "TOKEN_DOWN", "bids": [[0.47, 10]], "asks": [[0.49, 10]]}),
                json.dumps({"ts_ms": 1710500000200, "event": "trade", "token_id": "TOKEN_UP", "bids": [], "asks": [], "extra": {"price": 0.51, "size": 4}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    snapshots_path.write_text(
        json.dumps({"ts_ms": 1710500000000, "market_slug": "btc-updown-5m-test"}) + "\n",
        encoding="utf-8",
    )

    summary = HistoricalDatasetBuilder(research_root).build_from_capture_logs()

    assert summary["windows"] == 1
    assert summary["events"] == 3
    bundle_path = Path(summary["bundles"][0]["path"])
    assert bundle_path.exists()
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert payload["meta"]["slug"] == "btc-updown-5m-test"
    assert payload["meta"]["token_yes"] == "TOKEN_UP"
    assert payload["meta"]["token_no"] == "TOKEN_DOWN"
    assert len(payload["trades"]) == 1
