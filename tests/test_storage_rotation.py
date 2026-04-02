from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.core.lab_artifacts import append_jsonl
from app.logger import setup_logger


def test_append_jsonl_rotates_runtime_logs_when_cap_is_reached(tmp_path: Path) -> None:
    path = tmp_path / "research" / "runtime" / "market_events.jsonl"
    payload = {"kind": "book_snapshot", "value": "x" * 80}

    append_jsonl(path, payload, max_bytes=120, backup_count=2)
    append_jsonl(path, payload, max_bytes=120, backup_count=2)
    append_jsonl(path, payload, max_bytes=120, backup_count=2)

    assert path.exists()
    rotated = path.with_name(f"{path.name}.1")
    assert rotated.exists()
    assert json.loads(rotated.read_text(encoding="utf-8").splitlines()[0])["kind"] == "book_snapshot"


def test_setup_logger_uses_rotating_file_handler(tmp_path: Path) -> None:
    logger_name = "polymarket_copy_bot"
    logger = logging.getLogger(logger_name)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # noqa: BLE001
            pass

    configured = setup_logger(tmp_path, "INFO")

    file_handlers = [handler for handler in configured.handlers if isinstance(handler, RotatingFileHandler)]
    assert file_handlers
    assert Path(file_handlers[0].baseFilename).name == "bot.log"
