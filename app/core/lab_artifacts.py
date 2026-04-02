from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_RUNTIME_JSONL_MAX_BYTES = 256 * 1024 * 1024
_RUNTIME_JSONL_BACKUP_COUNT = 4


def research_root_from_db(db_path: Path) -> Path:
    return db_path.parent / "research"


def experiment_leaderboard_path(research_root: Path) -> Path:
    return research_root / "experiments" / "variant_leaderboard.json"


def wallet_hypotheses_path(research_root: Path) -> Path:
    return research_root / "hypotheses" / "top_wallet_patterns.json"


def dataset_summary_path(research_root: Path) -> Path:
    return research_root / "datasets" / "btc5m" / "dataset_summary.json"


def dataset_windows_dir(research_root: Path) -> Path:
    return research_root / "datasets" / "btc5m" / "windows"


def runtime_diagnostics_path(research_root: Path) -> Path:
    return research_root / "runtime" / "diagnostics_latest.json"


def microstructure_snapshot_path(research_root: Path) -> Path:
    return research_root / "runtime" / "microstructure_latest.json"


def liquidation_snapshot_path(research_root: Path) -> Path:
    return research_root / "runtime" / "liquidations_latest.json"


def latency_snapshot_path(research_root: Path) -> Path:
    return research_root / "runtime" / "latency_latest.json"


def events_log_path(research_root: Path, name: str) -> Path:
    safe_name = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in str(name or "")).strip("_")
    safe_name = safe_name or "events"
    return research_root / "runtime" / f"{safe_name}.jsonl"


def load_experiment_leaderboard(research_root: Path) -> dict[str, Any]:
    return _load_json(experiment_leaderboard_path(research_root), default={"generated_at": "", "variants": []})


def load_wallet_hypotheses(research_root: Path) -> dict[str, Any]:
    return _load_json(wallet_hypotheses_path(research_root), default={"generated_at": "", "hypotheses": [], "patterns": []})


def load_dataset_summary(research_root: Path) -> dict[str, Any]:
    return _load_json(dataset_summary_path(research_root), default={"generated_at": "", "windows": 0, "events": 0, "bundles": []})


def load_runtime_diagnostics(research_root: Path) -> dict[str, Any]:
    return _load_json(
        runtime_diagnostics_path(research_root),
        default={"generated_at": "", "status": "unknown", "summary": "", "findings": []},
    )


def load_microstructure_snapshot(research_root: Path) -> dict[str, Any]:
    return _load_json(
        microstructure_snapshot_path(research_root),
        default={"generated_at": "", "market_slug": "", "frame": {}, "decision": {}},
    )


def load_liquidation_snapshot(research_root: Path) -> dict[str, Any]:
    return _load_json(
        liquidation_snapshot_path(research_root),
        default={"generated_at": "", "totals": {}, "recent": []},
    )


def load_latency_snapshot(research_root: Path) -> dict[str, Any]:
    return _load_json(
        latency_snapshot_path(research_root),
        default={"generated_at": "", "latencies": {}},
    )


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def append_jsonl(
    path: Path,
    payload: dict[str, Any],
    *,
    max_bytes: int | None = None,
    backup_count: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    resolved_max_bytes, resolved_backup_count = _resolve_jsonl_rotation_limits(
        path,
        max_bytes=max_bytes,
        backup_count=backup_count,
    )
    if resolved_max_bytes > 0 and path.exists():
        try:
            current_size = path.stat().st_size
        except OSError:
            current_size = 0
        if current_size >= resolved_max_bytes:
            _rotate_jsonl_file(path, backup_count=resolved_backup_count)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _resolve_jsonl_rotation_limits(path: Path, *, max_bytes: int | None, backup_count: int | None) -> tuple[int, int]:
    if max_bytes is not None:
        return max(int(max_bytes), 0), max(int(backup_count or 0), 0)
    if path.suffix == ".jsonl" and "runtime" in path.parts:
        return _RUNTIME_JSONL_MAX_BYTES, _RUNTIME_JSONL_BACKUP_COUNT
    return 0, 0


def _rotate_jsonl_file(path: Path, *, backup_count: int) -> None:
    safe_backup_count = max(int(backup_count), 0)
    if safe_backup_count <= 0:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            return
        return
    oldest = path.with_name(f"{path.name}.{safe_backup_count}")
    try:
        oldest.unlink(missing_ok=True)
    except OSError:
        return
    for index in range(safe_backup_count - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        target = path.with_name(f"{path.name}.{index + 1}")
        if not source.exists():
            continue
        try:
            source.replace(target)
        except OSError:
            continue
    try:
        path.replace(path.with_name(f"{path.name}.1"))
    except OSError:
        return


def _load_json(path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    return payload if isinstance(payload, dict) else dict(default)
