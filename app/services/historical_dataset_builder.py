from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.lab_artifacts import dataset_summary_path, dataset_windows_dir, dump_json


WINDOW_MS = 5 * 60 * 1000


class HistoricalDatasetBuilder:
    def __init__(self, research_root: Path) -> None:
        self.research_root = research_root

    def build_from_capture_logs(
        self,
        *,
        events_path: Path | None = None,
        snapshots_path: Path | None = None,
    ) -> dict[str, Any]:
        events_file = events_path or (self.research_root / "paper_events.jsonl")
        snapshots_file = snapshots_path or (self.research_root / "paper_snapshots.jsonl")
        slug_hints = self._slug_hints(snapshots_file)
        grouped_events: dict[int, list[dict[str, Any]]] = defaultdict(list)
        token_counts: dict[int, Counter[str]] = defaultdict(Counter)

        for row in _read_jsonl(events_file):
            ts_ms = _safe_int(row.get("ts_ms"))
            token_id = str(row.get("token_id") or "").strip()
            if ts_ms <= 0 or not token_id:
                continue
            bucket = ts_ms // WINDOW_MS
            grouped_events[bucket].append(dict(row))
            token_counts[bucket][token_id] += 1

        windows_dir = dataset_windows_dir(self.research_root)
        windows_dir.mkdir(parents=True, exist_ok=True)

        bundles: list[dict[str, Any]] = []
        total_events = 0
        total_trades = 0
        for bucket in sorted(grouped_events):
            events = sorted(grouped_events[bucket], key=lambda item: _safe_int(item.get("ts_ms")))
            top_tokens = [token for token, _ in token_counts[bucket].most_common(2)]
            if len(top_tokens) < 2:
                continue
            filtered_events = [row for row in events if str(row.get("token_id") or "").strip() in top_tokens]
            if len(filtered_events) < 2:
                continue
            slug = slug_hints.get(bucket) or f"btc-updown-5m-{bucket * 300}"
            title = f"BTC 5m window {bucket}"
            bundle = {
                "meta": {
                    "market_id": slug,
                    "slug": slug,
                    "title": title,
                    "token_yes": top_tokens[0],
                    "token_no": top_tokens[1],
                    "condition_id": slug,
                    "window_start_ts_ms": bucket * WINDOW_MS,
                    "window_end_ts_ms": (bucket + 1) * WINDOW_MS,
                    "source": "polymarket-capture",
                },
                "events": filtered_events,
                "trades": [_trade_row(row) for row in filtered_events if str(row.get("event") or "") == "trade"],
            }
            output_path = windows_dir / f"{_safe_filename(slug)}.json"
            dump_json(output_path, bundle)
            trade_count = len(bundle["trades"])
            bundles.append(
                {
                    "slug": slug,
                    "path": str(output_path),
                    "events": len(filtered_events),
                    "trades": trade_count,
                    "token_yes": top_tokens[0],
                    "token_no": top_tokens[1],
                    "window_start_ts_ms": bucket * WINDOW_MS,
                }
            )
            total_events += len(filtered_events)
            total_trades += trade_count

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": str(events_file),
            "snapshot_source": str(snapshots_file),
            "windows": len(bundles),
            "events": total_events,
            "trades": total_trades,
            "bundles": bundles,
        }
        dump_json(dataset_summary_path(self.research_root), summary)
        return summary

    def _slug_hints(self, snapshots_file: Path) -> dict[int, str]:
        hints: dict[int, Counter[str]] = defaultdict(Counter)
        for row in _read_jsonl(snapshots_file):
            ts_ms = _safe_int(row.get("ts_ms"))
            slug = str(row.get("market_slug") or "").strip()
            if ts_ms <= 0 or not slug:
                continue
            hints[ts_ms // WINDOW_MS][slug] += 1
        return {
            bucket: counter.most_common(1)[0][0]
            for bucket, counter in hints.items()
            if counter
        }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _trade_row(row: dict[str, Any]) -> dict[str, Any]:
    extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
    return {
        "ts_ms": _safe_int(row.get("ts_ms")),
        "token_id": str(row.get("token_id") or "").strip(),
        "price": _safe_float(extra.get("price")),
        "size": _safe_float(extra.get("size")),
    }


def _safe_filename(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value).strip("_") or "window"


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
