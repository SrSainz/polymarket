from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

_BOT_LOG_MAX_BYTES = 50 * 1024 * 1024
_BOT_LOG_BACKUP_COUNT = 5


def setup_logger(log_dir: Path, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("polymarket_copy_bot")
    if logger.handlers:
        return logger

    log_dir.mkdir(parents=True, exist_ok=True)
    logger.setLevel(level.upper())

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        log_dir / "bot.log",
        maxBytes=_BOT_LOG_MAX_BYTES,
        backupCount=_BOT_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
