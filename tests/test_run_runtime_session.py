from __future__ import annotations

import time
from pathlib import Path

import pytest

import run as run_module
from app.db import Database
from app.settings import AppPaths, AppSettings, BotConfig, EnvSettings
from run import (
    _acquire_runtime_session,
    _clear_runtime_ledger,
    _clone_runtime_state,
    _runtime_db_path,
    _settings_for_runtime_mode,
)


def test_acquire_runtime_session_blocks_other_active_pid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("runtime_session_mode", "paper")
    db.set_bot_state("runtime_session_pid", "999999")
    db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))
    monkeypatch.setattr(run_module, "_runtime_pid_alive", lambda pid: True)

    with pytest.raises(RuntimeError, match="runtime session active"):
        _acquire_runtime_session(db, mode="live")

    db.close()


def test_acquire_runtime_session_ignores_dead_pid_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("runtime_session_mode", "paper")
    db.set_bot_state("runtime_session_pid", "999999")
    db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))
    monkeypatch.setattr(run_module, "_runtime_pid_alive", lambda pid: False)

    _acquire_runtime_session(db, mode="live")

    assert db.get_bot_state("runtime_session_mode") == "live"
    assert int(str(db.get_bot_state("runtime_session_pid") or "0")) > 0
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


def test_clear_runtime_ledger_preserves_trade_dedupe_markers(tmp_path: Path) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("live_processed_trade:order-1:trade-1", "1")
    db.set_bot_state("live_imported_activity:tx-1:asset-up:buy:1:1.00000000:0.25000000", "1")
    db.set_bot_state("live_imported_closed_position:slug-1:asset-up:1:0.00000000:1.00000000", "1")
    db.set_bot_state("live_observed_activity:order-1:trade-1", "{\"asset\":\"asset-up\"}")

    _clear_runtime_ledger(db)

    assert db.get_bot_state("live_processed_trade:order-1:trade-1") == "1"
    assert db.get_bot_state("live_imported_activity:tx-1:asset-up:buy:1:1.00000000:0.25000000") == "1"
    assert db.get_bot_state("live_imported_closed_position:slug-1:asset-up:1:0.00000000:1.00000000") == "1"
    assert db.get_bot_state("live_observed_activity:order-1:trade-1") is None
    db.close()


def test_clear_runtime_ledger_ignores_dead_pid_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = Database(tmp_path / "bot.db")
    db.init_schema()
    db.set_bot_state("runtime_session_mode", "shadow")
    db.set_bot_state("runtime_session_pid", "31337")
    db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))
    monkeypatch.setattr(run_module, "_runtime_pid_alive", lambda pid: False)

    _clear_runtime_ledger(db)

    assert db.get_bot_state("runtime_session_mode") == ""
    assert db.get_bot_state("runtime_session_pid") == "0"
    db.close()


def test_settings_for_runtime_mode_uses_live_db(tmp_path: Path) -> None:
    paths = AppPaths(
        root=tmp_path,
        db_path=tmp_path / "data" / "bot.db",
        logs_dir=tmp_path / "data" / "logs",
        reports_dir=tmp_path / "data" / "reports",
    )
    settings = AppSettings(
        config=BotConfig(watched_wallets=["0xabc"]),
        env=EnvSettings(),
        paths=paths,
        strategy_registry=None,
    )

    live_settings = _settings_for_runtime_mode(settings, runtime_mode="live")
    shadow_settings = _settings_for_runtime_mode(settings, runtime_mode="shadow")

    assert live_settings.paths.db_path.name == "bot_live.db"
    assert shadow_settings.paths.db_path.name == "bot_shadow.db"
    assert settings.paths.db_path.name == "bot.db"


def test_clone_runtime_state_copies_positions_and_clears_runtime_lock(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    source_db = Database(_runtime_db_path(tmp_path, runtime_mode="live"))
    source_db.init_schema()
    source_db.upsert_copy_position(
        asset="asset-down",
        condition_id="cond-live",
        size=12.0,
        avg_price=0.18,
        realized_pnl=-1.5,
        title="BTC 5m",
        slug="btc-updown-live",
        outcome="Down",
        category="crypto",
    )
    source_db.set_bot_state("runtime_session_mode", "live")
    source_db.set_bot_state("runtime_session_pid", "4242")
    source_db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))
    source_db.set_bot_state("live_control_state", "armed")
    source_db.set_bot_state("strategy_runtime_mode", "live")
    source_db.close()

    cloned_path = _clone_runtime_state(tmp_path, source_mode="live", target_mode="shadow")

    assert cloned_path.name == "bot_shadow.db"
    shadow_db = Database(cloned_path)
    shadow_db.init_schema()
    positions = shadow_db.list_copy_positions()
    assert len(positions) == 1
    assert str(positions[0]["asset"]) == "asset-down"
    assert shadow_db.get_bot_state("runtime_session_mode") == ""
    assert shadow_db.get_bot_state("runtime_session_pid") == "0"
    assert shadow_db.get_bot_state("runtime_session_heartbeat") == "0"
    assert shadow_db.get_bot_state("position_ledger_mode") == "shadow"
    assert shadow_db.get_bot_state("position_ledger_preflight") == "disabled"
    assert shadow_db.get_bot_state("runtime_clone_source_mode") == "live"
    assert shadow_db.get_bot_state("runtime_clone_target_mode") == "shadow"
    assert shadow_db.get_bot_state("runtime_clone_source_db") == "bot_live.db"
    shadow_db.close()


def test_clone_runtime_state_blocks_active_target_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    source_db = Database(_runtime_db_path(tmp_path, runtime_mode="live"))
    source_db.init_schema()
    source_db.close()

    target_db = Database(_runtime_db_path(tmp_path, runtime_mode="shadow"))
    target_db.init_schema()
    target_db.set_bot_state("runtime_session_mode", "shadow")
    target_db.set_bot_state("runtime_session_pid", "31337")
    target_db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))
    target_db.close()
    monkeypatch.setattr(run_module, "_runtime_pid_alive", lambda pid: True)

    with pytest.raises(RuntimeError, match="target runtime session active"):
        _clone_runtime_state(tmp_path, source_mode="live", target_mode="shadow")


def test_clone_runtime_state_ignores_dead_target_runtime_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    source_db = Database(_runtime_db_path(tmp_path, runtime_mode="live"))
    source_db.init_schema()
    source_db.close()

    target_db = Database(_runtime_db_path(tmp_path, runtime_mode="shadow"))
    target_db.init_schema()
    target_db.set_bot_state("runtime_session_mode", "shadow")
    target_db.set_bot_state("runtime_session_pid", "31337")
    target_db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))
    target_db.close()
    monkeypatch.setattr(run_module, "_runtime_pid_alive", lambda pid: False)

    cloned_path = _clone_runtime_state(tmp_path, source_mode="live", target_mode="shadow")

    assert cloned_path.name == "bot_shadow.db"
