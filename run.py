from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from app.core.autonomous_decider import AutonomousDecider
from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.db import Database
from app.logger import setup_logger
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.polymarket.market_feed import MarketFeed
from app.services.btc5m_strategy import BTC5mStrategyService
from app.services.dashboard_server import run_dashboard_server
from app.services.report import ReportService
from app.services.telegram_daily_summary import TelegramDailySummaryService
from app.services.telegram_trade_notifier import TelegramTradeNotifierService
from app.settings import AppSettings, load_settings


def build_context(root_dir: Path) -> tuple[AppSettings, Database, BTC5mStrategyService, ReportService]:
    settings = load_settings(root_dir)
    logger = setup_logger(settings.paths.logs_dir, settings.env.log_level)

    db = Database(settings.paths.db_path)
    db.init_schema()

    gamma_client = GammaClient(settings.env.gamma_api_host)
    market_feed = MarketFeed(
        settings.env.clob_ws_host,
        logger,
        enabled=settings.config.market_feed_enabled,
        stale_after_seconds=settings.config.market_feed_stale_seconds,
    )
    clob_client = CLOBClient(settings.env.clob_host, settings.env, market_feed=market_feed)

    paper_broker = PaperBroker(db)
    live_broker = LiveBroker(db, clob_client, settings.env)
    autonomous_decider = AutonomousDecider(settings.config, db)
    daily_summary = TelegramDailySummaryService(db, settings.config, settings.env, logger)
    trade_notifier = TelegramTradeNotifierService(settings.config, settings.env, logger)

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
    )
    report_service = ReportService(db, settings.paths.reports_dir)
    return settings, db, strategy_service, report_service


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
        choices=["paper", "live", "once", "dashboard", "report"],
        help="Command to run",
    )
    return parser.parse_args()


def _loop_strategy(strategy_service: BTC5mStrategyService, *, mode: str, sleep_seconds: int) -> int:
    while True:
        run_strategy_once(strategy_service, mode)
        time.sleep(sleep_seconds)


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parent
    settings, db, strategy_service, report_service = build_context(root_dir)

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
            strategy_service.clob_client.close()
        except Exception:  # noqa: BLE001
            pass
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
