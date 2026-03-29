from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from dataclasses import replace
from pathlib import Path

from app.core.strategy_registry import active_variant_metadata
from app.core.autonomous_decider import AutonomousDecider
from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.core.shadow_broker import ShadowBroker
from app.db import Database
from app.logger import setup_logger
from app.polymarket.activity_client import ActivityClient
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.polymarket.market_feed import MarketFeed
from app.polymarket.spot_feed import SpotFeed
from app.polymarket.user_feed import UserFeed
from app.services.btc5m_strategy import BTC5mStrategyService
from app.services.dashboard_server import run_dashboard_server
from app.services.experiment_runner import ExperimentRunner
from app.services.historical_dataset_builder import HistoricalDatasetBuilder
from app.services.liquidation_feed import LiquidationFeed
from app.services.report import ReportService
from app.services.runtime_diagnostics import RuntimeDiagnosticsService
from app.services.telegram_daily_summary import TelegramDailySummaryService
from app.services.telegram_trade_notifier import TelegramTradeNotifierService
from app.services.wallet_pattern_miner import WalletPatternMiner
from app.settings import AppPaths, AppSettings, load_settings
from app.core.replay_engine import ReplayEngine
from app.core.lab_artifacts import load_microstructure_snapshot

_RUNTIME_SESSION_TTL_SECONDS = 20
_RUNTIME_DB_FILENAMES = {
    "paper": "bot.db",
    "live": "bot_live.db",
    "shadow": "bot_shadow.db",
}


def _runtime_db_path(root_dir: Path, *, runtime_mode: str) -> Path:
    safe_mode = str(runtime_mode or "").strip().lower() or "paper"
    db_name = _RUNTIME_DB_FILENAMES.get(safe_mode, "bot.db")
    return root_dir / "data" / db_name


def _settings_for_runtime_mode(settings: AppSettings, *, runtime_mode: str) -> AppSettings:
    safe_mode = str(runtime_mode or "").strip().lower() or "paper"
    db_path = _runtime_db_path(settings.paths.root, runtime_mode=safe_mode)
    if db_path == settings.paths.db_path:
        return settings
    paths = AppPaths(
        root=settings.paths.root,
        db_path=db_path,
        logs_dir=settings.paths.logs_dir,
        reports_dir=settings.paths.reports_dir,
    )
    paths.ensure()
    return replace(settings, paths=paths)


def build_core_context(root_dir: Path, *, runtime_mode: str = "paper") -> tuple[AppSettings, Database]:
    settings = load_settings(root_dir)
    settings = _settings_for_runtime_mode(settings, runtime_mode=runtime_mode)
    db = Database(settings.paths.db_path)
    db.init_schema()
    _record_runtime_metadata(db, settings, runtime_mode=runtime_mode)
    return settings, db


def build_context(root_dir: Path, *, runtime_mode: str = "paper") -> tuple[AppSettings, Database, BTC5mStrategyService, ReportService]:
    settings, db = build_core_context(root_dir, runtime_mode=runtime_mode)
    logger = setup_logger(settings.paths.logs_dir, settings.env.log_level)

    gamma_client = GammaClient(settings.env.gamma_api_host)
    market_feed = MarketFeed(
        settings.env.clob_ws_host,
        logger,
        enabled=settings.config.market_feed_enabled,
        stale_after_seconds=settings.config.market_feed_stale_seconds,
    )
    spot_feed = SpotFeed(
        settings.env.spot_ws_host,
        logger,
        enabled=settings.config.spot_feed_enabled,
        stale_after_seconds=settings.config.spot_feed_stale_seconds,
    )
    liquidation_feed = LiquidationFeed(
        logger,
        enabled=settings.config.liquidation_feed_enabled,
        binance_ws_url=settings.env.binance_liquidation_ws_host,
        bybit_ws_url=settings.env.bybit_liquidation_ws_host,
        bybit_symbol=settings.config.liquidation_bybit_symbol,
    )
    user_feed = UserFeed(
        settings.env.polymarket_user_ws_host,
        logger,
        api_key=settings.env.polymarket_api_key,
        api_secret=settings.env.polymarket_api_secret,
        api_passphrase=settings.env.polymarket_api_passphrase,
        enabled=settings.env.live_trading or settings.config.shadow_mode_enabled,
    )
    spot_feed.start()
    liquidation_feed.start()
    user_feed.start()
    clob_client = CLOBClient(settings.env.clob_host, settings.env, market_feed=market_feed)

    paper_broker = PaperBroker(db, clob_client=clob_client)
    shadow_broker = ShadowBroker(
        db,
        clob_client,
        slippage_limit=settings.config.slippage_limit,
        execution_profile=settings.config.live_execution_profile,
    )
    live_broker = LiveBroker(
        db,
        clob_client,
        settings.env,
        slippage_limit=settings.config.slippage_limit,
        execution_profile=settings.config.live_execution_profile,
        dry_run=settings.config.dry_run,
    )
    autonomous_decider = AutonomousDecider(settings.config, db)
    daily_summary = TelegramDailySummaryService(db, settings.config, settings.env, logger)
    trade_notifier = TelegramTradeNotifierService(settings.config, settings.env, logger)
    runtime_diagnostics = RuntimeDiagnosticsService(db, settings.paths.research_dir, settings, logger)

    strategy_service = BTC5mStrategyService(
        db,
        gamma_client,
        clob_client,
        paper_broker=paper_broker,
        live_broker=live_broker,
        shadow_broker=shadow_broker,
        autonomous_decider=autonomous_decider,
        daily_summary=daily_summary,
        trade_notifier=trade_notifier,
        settings=settings,
        logger=logger,
        runtime_diagnostics=runtime_diagnostics,
        spot_feed=spot_feed,
        liquidation_feed=liquidation_feed,
        user_feed=user_feed,
    )
    report_service = ReportService(db, settings.paths.reports_dir)
    return settings, db, strategy_service, report_service


def _record_runtime_metadata(db: Database, settings: AppSettings, *, runtime_mode: str) -> None:
    db.set_bot_state("strategy_variant", settings.config.strategy_variant)
    db.set_bot_state("strategy_notes", settings.config.strategy_notes)
    db.set_bot_state("strategy_incubation_stage", settings.config.incubation_stage)
    db.set_bot_state("strategy_runtime_mode", str(runtime_mode or "paper").strip().lower() or "paper")
    db.set_bot_state("live_control_default_state", settings.config.live_control_default_state)
    db.set_bot_state(
        "telegram_status_summary_enabled",
        "1" if settings.config.telegram_status_summary_enabled else "0",
    )
    db.set_bot_state(
        "telegram_status_summary_interval_minutes",
        str(settings.config.telegram_status_summary_interval_minutes),
    )
    db.set_bot_state(
        "telegram_status_summary_recent_limit",
        str(settings.config.telegram_status_summary_recent_limit),
    )
    db.set_bot_state("strategy_incubation_auto_promote", "1" if settings.config.incubation_auto_promote else "0")
    db.set_bot_state("strategy_incubation_min_days", str(settings.config.incubation_min_days))
    db.set_bot_state("strategy_incubation_min_resolutions", str(settings.config.incubation_min_resolutions))
    db.set_bot_state("strategy_incubation_max_drawdown", f"{settings.config.incubation_max_drawdown:.4f}")
    db.set_bot_state(
        "strategy_incubation_min_backtest_pnl",
        f"{settings.config.incubation_min_backtest_pnl:.4f}",
    )
    db.set_bot_state(
        "strategy_incubation_min_backtest_fill_rate",
        f"{settings.config.incubation_min_backtest_fill_rate:.6f}",
    )
    db.set_bot_state(
        "strategy_incubation_min_backtest_hit_rate",
        f"{settings.config.incubation_min_backtest_hit_rate:.6f}",
    )
    db.set_bot_state(
        "strategy_incubation_min_backtest_edge_bps",
        f"{settings.config.incubation_min_backtest_edge_bps:.4f}",
    )
    registry = settings.strategy_registry
    if registry is not None:
        for key, value in active_variant_metadata(
            registry,
            variant_name=settings.config.strategy_variant,
            entry_mode=settings.config.strategy_entry_mode,
        ).items():
            db.set_bot_state(key, value)
    if db.get_bot_state("live_control_state") is None:
        db.set_bot_state("live_control_state", settings.config.live_control_default_state)
        if settings.config.live_control_default_state == "armed":
            reason = "armado por defecto desde configuracion"
        else:
            reason = "pendiente de armar desde live control center"
        db.set_bot_state("live_control_reason", reason)
        db.set_bot_state("live_control_updated_at", str(int(time.time())))


def run_strategy_once(strategy_service: BTC5mStrategyService, mode: str) -> None:
    exec_stats = strategy_service.run(mode=mode)
    print(
        "strategy => "
        f"pending={exec_stats['pending']} filled={exec_stats['filled']} "
        f"blocked={exec_stats['blocked']} skipped={exec_stats['skipped']} failed={exec_stats['failed']} "
        f"opportunities={exec_stats.get('opportunities', 0)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BTC 5m microstructure lab for Polymarket")
    parser.add_argument(
        "command",
        choices=[
            "paper",
            "live",
            "shadow",
            "once",
            "dashboard",
            "report",
            "dataset",
            "experiments",
            "hypotheses",
            "diagnostics",
            "replay",
            "clear-ledger",
            "clone-runtime",
        ],
        help="Command to run",
    )
    parser.add_argument("--replay-market-slug", default="", help="Slug de mercado a usar para replay")
    parser.add_argument("--replay-output-dir", default="data/research/replay", help="Directorio de salida para replay")
    parser.add_argument(
        "--runtime-mode",
        choices=["paper", "live", "shadow"],
        default="",
        help="DB de runtime a usar para dashboard/report/diagnostics/clear-ledger",
    )
    parser.add_argument(
        "--from-runtime-mode",
        choices=["paper", "live", "shadow"],
        default="live",
        help="Runtime origen para clonar estado",
    )
    parser.add_argument(
        "--to-runtime-mode",
        choices=["paper", "live", "shadow"],
        default="shadow",
        help="Runtime destino para clonar estado",
    )
    return parser.parse_args()


def _command_runtime_mode(args: argparse.Namespace) -> str:
    if args.command in {"paper", "live", "shadow"}:
        return args.command
    if args.command == "once":
        return "paper"
    raw = str(args.runtime_mode or "").strip().lower()
    if raw in {"paper", "live", "shadow"}:
        return raw
    return "paper"


def _runtime_session_state(db: Database) -> dict[str, int | str]:
    heartbeat = 0
    try:
        heartbeat = int(str(db.get_bot_state("runtime_session_heartbeat") or "0"))
    except ValueError:
        heartbeat = 0
    pid = 0
    try:
        pid = int(str(db.get_bot_state("runtime_session_pid") or "0"))
    except ValueError:
        pid = 0
    return {
        "mode": str(db.get_bot_state("runtime_session_mode") or "").strip().lower(),
        "pid": pid,
        "heartbeat": heartbeat,
    }


def _runtime_pid_alive(pid: int) -> bool:
    if int(pid) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _runtime_session_is_active(
    state: dict[str, int | str],
    *,
    now_ts: int,
    current_pid: int | None = None,
) -> tuple[bool, int]:
    heartbeat_age = now_ts - int(state["heartbeat"])
    pid = int(state["pid"])
    if not str(state["mode"]) or heartbeat_age > _RUNTIME_SESSION_TTL_SECONDS or pid <= 0:
        return False, heartbeat_age
    if current_pid is not None and pid == int(current_pid):
        return False, heartbeat_age
    if not _runtime_pid_alive(pid):
        return False, heartbeat_age
    return True, heartbeat_age


def _clear_stale_runtime_session(db: Database, state: dict[str, int | str], *, now_ts: int) -> None:
    heartbeat_age = now_ts - int(state["heartbeat"])
    pid = int(state["pid"])
    if not str(state["mode"]):
        return
    if heartbeat_age <= _RUNTIME_SESSION_TTL_SECONDS and pid > 0 and _runtime_pid_alive(pid):
        return
    db.set_bot_state("runtime_session_mode", "")
    db.set_bot_state("runtime_session_pid", "0")
    db.set_bot_state("runtime_session_heartbeat", "0")


def _acquire_runtime_session(db: Database, *, mode: str) -> None:
    now_ts = int(time.time())
    current_pid = os.getpid()
    state = _runtime_session_state(db)
    active, heartbeat_age = _runtime_session_is_active(state, now_ts=now_ts, current_pid=current_pid)
    if active:
        raise RuntimeError(
            f"runtime session active: mode={state['mode']} pid={state['pid']} heartbeat_age={heartbeat_age}s"
        )
    _clear_stale_runtime_session(db, state, now_ts=now_ts)
    db.set_bot_state("runtime_session_mode", mode)
    db.set_bot_state("runtime_session_pid", str(current_pid))
    db.set_bot_state("runtime_session_heartbeat", str(now_ts))


def _heartbeat_runtime_session(db: Database, *, mode: str) -> None:
    state = _runtime_session_state(db)
    current_pid = os.getpid()
    if str(state["mode"]) != mode or int(state["pid"]) != current_pid:
        return
    db.set_bot_state("runtime_session_heartbeat", str(int(time.time())))


def _release_runtime_session(db: Database, *, mode: str) -> None:
    state = _runtime_session_state(db)
    current_pid = os.getpid()
    if str(state["mode"]) != mode or int(state["pid"]) != current_pid:
        return
    db.set_bot_state("runtime_session_mode", "")
    db.set_bot_state("runtime_session_pid", "0")
    db.set_bot_state("runtime_session_heartbeat", "0")


def _clear_runtime_ledger(db: Database) -> None:
    state = _runtime_session_state(db)
    now_ts = int(time.time())
    active, heartbeat_age = _runtime_session_is_active(state, now_ts=now_ts)
    if active:
        raise RuntimeError(
            f"runtime session active: mode={state['mode']} pid={state['pid']} heartbeat_age={heartbeat_age}s"
        )
    _clear_stale_runtime_session(db, state, now_ts=now_ts)
    db.clear_copy_positions()
    db.set_bot_state("position_ledger_mode", "")
    db.set_bot_state("position_ledger_preflight", "ready")
    db.set_bot_state("live_control_state", "armed")
    db.set_bot_state("live_control_reason", "ledger limpiado y live armado")
    db.set_bot_state("live_control_updated_at", str(now_ts))


def _clone_runtime_state(root_dir: Path, *, source_mode: str, target_mode: str) -> Path:
    safe_source_mode = str(source_mode or "").strip().lower() or "live"
    safe_target_mode = str(target_mode or "").strip().lower() or "shadow"
    if safe_source_mode not in _RUNTIME_DB_FILENAMES:
        raise RuntimeError(f"unsupported source runtime mode: {safe_source_mode}")
    if safe_target_mode not in _RUNTIME_DB_FILENAMES:
        raise RuntimeError(f"unsupported target runtime mode: {safe_target_mode}")
    if safe_source_mode == safe_target_mode:
        raise RuntimeError("source and target runtime modes must be different")

    source_path = _runtime_db_path(root_dir, runtime_mode=safe_source_mode)
    target_path = _runtime_db_path(root_dir, runtime_mode=safe_target_mode)
    if not source_path.exists():
        raise RuntimeError(f"source runtime db not found: {source_path}")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_db = Database(target_path)
    target_db.init_schema()
    try:
        state = _runtime_session_state(target_db)
        now_ts = int(time.time())
        active, heartbeat_age = _runtime_session_is_active(state, now_ts=now_ts)
        if active:
            raise RuntimeError(
                f"target runtime session active: mode={state['mode']} pid={state['pid']} heartbeat_age={heartbeat_age}s"
            )
        _clear_stale_runtime_session(target_db, state, now_ts=now_ts)
    finally:
        target_db.close()

    source_conn = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True, timeout=30.0)
    target_conn = sqlite3.connect(str(target_path), timeout=30.0)
    try:
        source_conn.execute("PRAGMA busy_timeout=30000")
        target_conn.execute("PRAGMA busy_timeout=30000")
        source_conn.backup(target_conn)
        target_conn.commit()
    finally:
        target_conn.close()
        source_conn.close()

    cloned_db = Database(target_path)
    cloned_db.init_schema()
    try:
        now_ts = int(time.time())
        cloned_db.set_bot_state("runtime_session_mode", "")
        cloned_db.set_bot_state("runtime_session_pid", "0")
        cloned_db.set_bot_state("runtime_session_heartbeat", "0")
        cloned_db.set_bot_state("position_ledger_mode", safe_target_mode)
        cloned_db.set_bot_state(
            "position_ledger_preflight",
            "disabled" if safe_target_mode != "live" else "ready",
        )
        cloned_db.set_bot_state("runtime_clone_source_mode", safe_source_mode)
        cloned_db.set_bot_state("runtime_clone_target_mode", safe_target_mode)
        cloned_db.set_bot_state("runtime_clone_source_db", source_path.name)
        cloned_db.set_bot_state("runtime_clone_target_db", target_path.name)
        cloned_db.set_bot_state("runtime_clone_at", str(now_ts))
        cloned_db.set_bot_state(
            "runtime_clone_note",
            f"clonado desde {safe_source_mode} hacia {safe_target_mode}",
        )
    finally:
        cloned_db.close()
    return target_path


def _wait_for_next_cycle(strategy_service: BTC5mStrategyService, *, mode: str, sleep_seconds: float) -> None:
    feed = getattr(strategy_service.clob_client, "market_feed", None)
    spot_feed = getattr(strategy_service, "spot_feed", None)
    entry_mode = str(strategy_service.settings.config.strategy_entry_mode or "")
    if (
        mode in {"paper", "shadow"}
        and entry_mode == "arb_micro"
        and (feed is not None or spot_feed is not None)
    ):
        deadline = time.monotonic() + sleep_seconds
        while time.monotonic() < deadline:
            remaining = max(deadline - time.monotonic(), 0.0)
            slice_seconds = min(0.05, remaining)
            if feed is not None and getattr(strategy_service.settings.config, "market_feed_enabled", False):
                if feed.wait_for_update(timeout_seconds=slice_seconds):
                    return
            elif slice_seconds > 0:
                time.sleep(slice_seconds)
            if spot_feed is not None and getattr(strategy_service.settings.config, "spot_feed_enabled", False):
                if spot_feed.wait_for_update(timeout_seconds=0):
                    return
        return
    time.sleep(sleep_seconds)


def _loop_strategy(strategy_service: BTC5mStrategyService, db: Database, *, mode: str, sleep_seconds: float) -> int:
    while True:
        _heartbeat_runtime_session(db, mode=mode)
        run_strategy_once(strategy_service, mode)
        _heartbeat_runtime_session(db, mode=mode)
        _wait_for_next_cycle(strategy_service, mode=mode, sleep_seconds=sleep_seconds)


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parent
    if args.command == "clone-runtime":
        try:
            target_path = _clone_runtime_state(
                root_dir,
                source_mode=args.from_runtime_mode,
                target_mode=args.to_runtime_mode,
            )
            print(
                "runtime clone => "
                f"{args.from_runtime_mode} -> {args.to_runtime_mode} "
                f"db={target_path}"
            )
            return 0
        except RuntimeError as error:
            print(error)
            return 1
    runtime_mode = _command_runtime_mode(args)
    strategy_service = None
    if args.command in {"paper", "live", "shadow", "once"}:
        settings, db, strategy_service, report_service = build_context(root_dir, runtime_mode=runtime_mode)
    else:
        settings, db = build_core_context(root_dir, runtime_mode=runtime_mode)
        report_service = ReportService(db, settings.paths.reports_dir)

    try:
        if args.command == "report":
            _, report_path = report_service.generate()
            print(f"report => {report_path}")
            return 0

        if args.command == "clear-ledger":
            _clear_runtime_ledger(db)
            print("ledger => cleared and live armed")
            return 0

        if args.command == "dashboard":
            run_dashboard_server(
                db_path=settings.paths.db_path,
                static_dir=settings.paths.root / "web",
                clob_host=settings.env.clob_host,
                execution_mode=settings.config.execution_mode,
                live_trading_enabled=settings.env.live_trading,
                host=settings.env.dashboard_host,
                port=settings.env.dashboard_port,
            )
            return 0

        if args.command == "dataset":
            summary = HistoricalDatasetBuilder(settings.paths.research_dir).build_from_capture_logs()
            print(
                "dataset => "
                f"windows={summary['windows']} events={summary['events']} trades={summary['trades']}"
            )
            return 0

        if args.command == "experiments":
            dataset_builder = HistoricalDatasetBuilder(settings.paths.research_dir)
            dataset_summary = dataset_builder.build_from_capture_logs()
            runner = ExperimentRunner(
                settings.paths.research_dir,
                settings.strategy_registry if settings.strategy_registry is not None else load_settings(root_dir).strategy_registry,  # pragma: no cover
            )
            results = runner.run(fallback_inputs=[settings.paths.root / "sample.json"])
            print(
                "experiments => "
                f"variants={len(results.get('variants', []))} datasets={len(results.get('datasets', []))} "
                f"windows={dataset_summary['windows']}"
            )
            return 0

        if args.command == "hypotheses":
            miner = WalletPatternMiner(
                db,
                ActivityClient(settings.env.data_api_host),
                GammaClient(settings.env.gamma_api_host),
                settings.paths.research_dir,
                watched_wallets=settings.config.watched_wallets,
            )
            payload = miner.run(wallet_limit=max(settings.config.top_wallets_to_copy, 1))
            print(
                "hypotheses => "
                f"wallets={len(payload.get('wallets', []))} hypotheses={len(payload.get('hypotheses', []))}"
            )
            return 0

        if args.command == "diagnostics":
            diagnostics = RuntimeDiagnosticsService(db, settings.paths.research_dir, settings)
            payload = diagnostics.generate()
            print(
                "diagnostics => "
                f"status={payload['status']} findings={len(payload.get('findings', []))} "
                f"summary={payload['summary']}"
            )
            return 0

        if args.command == "replay":
            market_slug = str(args.replay_market_slug or "").strip()
            latest = load_microstructure_snapshot(settings.paths.research_dir)
            if not market_slug:
                market_slug = str(latest.get("market_slug") or "")
            if not market_slug:
                print("replay => no market slug available. Use --replay-market-slug.")
                return 1
            gamma_client = GammaClient(settings.env.gamma_api_host)
            market = gamma_client.get_market_by_slug(market_slug)
            if market is None:
                print(f"replay => market not found for slug {market_slug}")
                return 1
            replay = ReplayEngine(
                market=market,
                research_dir=settings.paths.research_dir,
                output_dir=(settings.paths.root / args.replay_output_dir),
                official_price_to_beat=float(latest.get("frame", {}).get("spot_anchor") or 0.0),
            )
            summary = replay.run()
            print(
                "replay => "
                f"market={summary.market_slug} events={summary.events} features={summary.feature_frames} "
                f"decisions={summary.decision_traces} readiness={summary.latest_readiness_score:.2f} "
                f"edge={summary.latest_expected_edge_bps:.2f}bps"
            )
            return 0

        if args.command == "once":
            mode = "paper" if settings.config.execution_mode == "paper" else "live"
            if mode == "live" and not settings.env.live_trading:
                print("execution_mode=live but LIVE_TRADING=false, falling back to paper")
                mode = "paper"
            _acquire_runtime_session(db, mode=mode)
            try:
                run_strategy_once(strategy_service, mode)
                return 0
            finally:
                _release_runtime_session(db, mode=mode)

        if args.command == "live":
            if not settings.env.live_trading:
                print("LIVE_TRADING=false in environment. Refusing to run live mode.")
                return 1
            _acquire_runtime_session(db, mode="live")
            return _loop_strategy(
                strategy_service,
                db,
                mode="live",
                sleep_seconds=settings.config.polling_interval_seconds,
            )

        if args.command == "paper":
            _acquire_runtime_session(db, mode="paper")
            return _loop_strategy(
                strategy_service,
                db,
                mode="paper",
                sleep_seconds=settings.config.polling_interval_seconds,
            )

        if args.command == "shadow":
            if not settings.config.shadow_mode_enabled:
                print("shadow_mode_enabled=false in config. Enable it before running shadow mode.")
                return 1
            _acquire_runtime_session(db, mode="shadow")
            return _loop_strategy(
                strategy_service,
                db,
                mode="shadow",
                sleep_seconds=settings.config.polling_interval_seconds,
            )

    except RuntimeError as error:
        print(error)
        return 1
    except KeyboardInterrupt:
        print("stopped by user")
        return 0
    finally:
        if args.command in {"paper", "live", "shadow", "once"}:
            try:
                command_mode = "paper" if args.command == "paper" else "live" if args.command == "live" else "shadow" if args.command == "shadow" else ""
                if args.command == "once":
                    command_mode = "paper" if settings.config.execution_mode == "paper" else "live"
                    if command_mode == "live" and not settings.env.live_trading:
                        command_mode = "paper"
                if command_mode:
                    _release_runtime_session(db, mode=command_mode)
            except Exception:  # noqa: BLE001
                pass
        try:
            if strategy_service is not None:
                strategy_service.clob_client.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if strategy_service is not None and getattr(strategy_service, "spot_feed", None) is not None:
                strategy_service.spot_feed.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if strategy_service is not None and getattr(strategy_service, "liquidation_feed", None) is not None:
                strategy_service.liquidation_feed.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if strategy_service is not None and getattr(strategy_service, "user_feed", None) is not None:
                strategy_service.user_feed.close()
        except Exception:  # noqa: BLE001
            pass
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
