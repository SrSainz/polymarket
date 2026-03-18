from __future__ import annotations

import json
from pathlib import Path
from typing import Any


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


def load_experiment_leaderboard(research_root: Path) -> dict[str, Any]:
    return _load_json(experiment_leaderboard_path(research_root), default={"generated_at": "", "variants": []})


def load_wallet_hypotheses(research_root: Path) -> dict[str, Any]:
    return _load_json(wallet_hypotheses_path(research_root), default={"generated_at": "", "hypotheses": [], "patterns": []})


def load_dataset_summary(research_root: Path) -> dict[str, Any]:
    return _load_json(dataset_summary_path(research_root), default={"generated_at": "", "windows": 0, "events": 0, "bundles": []})


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _load_json(path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    return payload if isinstance(payload, dict) else dict(default)
