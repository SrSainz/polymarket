from __future__ import annotations

import time
from pathlib import Path

import pytest

from app.db import Database
from run import _acquire_runtime_session, _clear_runtime_ledger


def test_acquire_runtime_session_blocks_other_active_pid(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("runtime_session_mode", "paper")
    db.set_bot_state("runtime_session_pid", "999999")
    db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))

    with pytest.raises(RuntimeError, match="runtime session active"):
        _acquire_runtime_session(db, mode="live")

    db.close()


def test_clear_runtime_ledger_deletes_positions_and_arms_live(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.upsert_copy_position(
        asset="asset-up",
        condition_id="cond-1",
        size=5.0,
        avg_price=0.4,
        realized_pnl=0.0,
        title="BTC 5m",
        slug="btc-updown-5m-1",
        outcome="Up",
        category="crypto",
    )

    _clear_runtime_ledger(db)

    assert db.list_copy_positions() == []
    assert db.get_bot_state("position_ledger_mode") == ""
    assert db.get_bot_state("position_ledger_preflight") == "ready"
    assert db.get_bot_state("live_control_state") == "armed"
    db.close()
