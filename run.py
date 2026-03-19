from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from app.core.strategy_registry import active_variant_metadata
from app.core.autonomous_decider import AutonomousDecider
from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.db import Database
from app.logger import setup_logger
from app.polymarket.activity_client import ActivityClient
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.polymarket.market_feed import MarketFeed
from app.polymarket.spot_feed import SpotFeed
from app.services.btc5m_strategy import BTC5mStrategyService
from app.services.dashboard_server import run_dashboard_server
from app.services.experiment_runner import ExperimentRunner
from app.services.historical_dataset_builder import HistoricalDatasetBuilder
from app.services.report import ReportService
from app.services.runtime_diagnostics import RuntimeDiagnosticsService
from app.services.telegram_daily_summary import TelegramDailySummaryService
from app.services.telegram_trade_notifier import TelegramTradeNotifierService
from app.services.wallet_pattern_miner import WalletPatternMiner
from app.settings import AppSettings, load_settings


def build_core_context(root_dir: Path) -> tuple[AppSettings, Database]:
    settings = load_settings(root_dir)
    db = Database(settings.paths.db_path)
    db.init_schema()
    _record_runtime_metadata(db, settings)
    return settings, db


def build_context(root_dir: Path) -> tuple[AppSettings, Database, BTC5mStrategyService, ReportService]:
    settings, db = build_core_context(root_dir)
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
    spot_feed.start()
    clob_client = CLOBClient(settings.env.clob_host, settings.env, market_feed=market_feed)

    paper_broker = PaperBroker(db)
    live_broker = LiveBroker(db, clob_client, settings.env, slippage_limit=settings.config.slippage_limit)
    autonomous_decider = AutonomousDecider(settings.config, db)
    daily_summary = TelegramDailySummaryService(db, settings.config, settings.env, logger)
    trade_notifier = TelegramTradeNotifierService(settings.config, settings.env, logger)
    runtime_diagnostics = RuntimeDiagnosticsService(db, settings.paths.research_dir, settings, logger)

    strategy_service = BTC5mStrategyService(
        db,
        gamma_client,
        clob_client,
        paper_broker,
        live_broker,
        autonomous_decider,
        daily_summary,
        trade_notifier,
        settings,
        logger,
        runtime_diagnostics=runtime_diagnostics,
        spot_feed=spot_feed,
    )
    report_service = ReportService(db, settings.paths.reports_dir)
    return settings, db, strategy_service, report_service


def _record_runtime_metadata(db: Database, settings: AppSettings) -> None:
    db.set_bot_state("strategy_variant", settings.config.strategy_variant)
    db.set_bot_state("strategy_notes", settings.config.strategy_notes)
    db.set_bot_state("strategy_incubation_stage", settings.config.incubation_stage)
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
        choices=["paper", "live", "once", "dashboard", "report", "dataset", "experiments", "hypotheses", "diagnostics"],
        help="Command to run",
    )
    return parser.parse_args()


def _wait_for_next_cycle(strategy_service: BTC5mStrategyService, *, mode: str, sleep_seconds: float) -> None:
    feed = getattr(strategy_service.clob_client, "market_feed", None)
    spot_feed = getattr(strategy_service, "spot_feed", None)
    entry_mode = str(strategy_service.settings.config.strategy_entry_mode or "")
    if (
        mode == "paper"
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


def _loop_strategy(strategy_service: BTC5mStrategyService, *, mode: str, sleep_seconds: float) -> int:
    while True:
        run_strategy_once(strategy_service, mode)
        _wait_for_next_cycle(strategy_service, mode=mode, sleep_seconds=sleep_seconds)


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parent
    strategy_service = None
    if args.command in {"paper", "live", "once"}:
        settings, db, strategy_service, report_service = build_context(root_dir)
    else:
        settings, db = build_core_context(root_dir)
        report_service = ReportService(db, settings.paths.reports_dir)

    try:
        if args.command == "report":
            _, report_path = report_service.generate()
            print(f"report => {report_path}")
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

        if args.command == "once":
            mode = "paper" if settings.config.execution_mode == "paper" else "live"
            if mode == "live" and not settings.env.live_trading:
                print("execution_mode=live but LIVE_TRADING=false, falling back to paper")
                mode = "paper"
            run_strategy_once(strategy_service, mode)
            return 0

        if args.command == "live":
            if not settings.env.live_trading:
                print("LIVE_TRADING=false in environment. Refusing to run live mode.")
                return 1
            return _loop_strategy(
                strategy_service,
                mode="live",
                sleep_seconds=settings.config.polling_interval_seconds,
            )

        if args.command == "paper":
            return _loop_strategy(
                strategy_service,
                mode="paper",
                sleep_seconds=settings.config.polling_interval_seconds,
            )

    except KeyboardInterrupt:
        print("stopped by user")
        return 0
    finally:
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
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
