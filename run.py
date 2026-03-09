from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from app.core.copier import Copier
from app.core.live_broker import LiveBroker
from app.core.paper_broker import PaperBroker
from app.core.reconciler import Reconciler
from app.core.risk import RiskManager
from app.core.sizing import SizingEngine
from app.core.tracker import SourceTracker
from app.db import Database
from app.logger import setup_logger
from app.polymarket.activity_client import ActivityClient
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient
from app.services.detect_changes import DetectChangesService
from app.services.dashboard_server import run_dashboard_server
from app.services.execute_copy import ExecuteCopyService
from app.services.report import ReportService
from app.services.sync_wallets import SyncWalletsService
from app.settings import AppSettings, load_settings


def build_context(root_dir: Path) -> tuple[AppSettings, Database, SyncWalletsService, ExecuteCopyService, ReportService]:
    settings = load_settings(root_dir)
    logger = setup_logger(settings.paths.logs_dir, settings.env.log_level)

    db = Database(settings.paths.db_path)
    db.init_schema()

    activity_client = ActivityClient(settings.env.data_api_host)
    gamma_client = GammaClient(settings.env.gamma_api_host)
    clob_client = CLOBClient(settings.env.clob_host, settings.env)

    tracker = SourceTracker(activity_client, gamma_client, logger)
    detect_changes = DetectChangesService(settings.config.noise_threshold_shares)
    sync_service = SyncWalletsService(db, tracker, detect_changes, settings.config, logger)

    risk = RiskManager(settings.config)
    sizing = SizingEngine(settings.config)
    reconciler = Reconciler()
    copier = Copier(sizing, risk, reconciler)

    paper_broker = PaperBroker(db)
    live_broker = LiveBroker(db, clob_client, settings.env)
    execute_service = ExecuteCopyService(
        db,
        copier,
        paper_broker,
        live_broker,
        clob_client,
        settings,
        logger,
    )

    report_service = ReportService(db, settings.paths.reports_dir)
    return settings, db, sync_service, execute_service, report_service


def run_once(sync_service: SyncWalletsService, execute_service: ExecuteCopyService, mode: str) -> None:
    sync_stats = sync_service.run()
    print(f"sync => snapshots={sync_stats['snapshots']} new_signals={sync_stats['signals']}")
    exec_stats = execute_service.run(mode=mode)
    print(
        "execute => "
        f"pending={exec_stats['pending']} filled={exec_stats['filled']} "
        f"blocked={exec_stats['blocked']} skipped={exec_stats['skipped']} failed={exec_stats['failed']}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Polymarket copy trading bot")
    parser.add_argument(
        "command",
        choices=["sync", "paper", "live", "report", "once", "dashboard"],
        help="Command to run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root_dir = Path(__file__).resolve().parent

    settings, db, sync_service, execute_service, report_service = build_context(root_dir)

    try:
        if args.command == "sync":
            stats = sync_service.run()
            print(f"sync => snapshots={stats['snapshots']} new_signals={stats['signals']}")
            return 0

        if args.command == "report":
            _, report_path = report_service.generate()
            print(f"report => {report_path}")
            return 0

        if args.command == "dashboard":
            run_dashboard_server(
                db_path=settings.paths.db_path,
                static_dir=settings.paths.root / "web",
                host=settings.env.dashboard_host,
                port=settings.env.dashboard_port,
            )
            return 0

        if args.command == "once":
            mode = "paper" if settings.config.execution_mode == "paper" else "live"
            if mode == "live" and not settings.env.live_trading:
                print("execution_mode=live but LIVE_TRADING=false, falling back to paper")
                mode = "paper"
            run_once(sync_service, execute_service, mode)
            return 0

        if args.command == "live":
            if not settings.env.live_trading:
                print("LIVE_TRADING=false in environment. Refusing to run live mode.")
                return 1
            while True:
                run_once(sync_service, execute_service, "live")
                time.sleep(settings.config.polling_interval_seconds)

        if args.command == "paper":
            while True:
                run_once(sync_service, execute_service, "paper")
                time.sleep(settings.config.polling_interval_seconds)

    except KeyboardInterrupt:
        print("stopped by user")
        return 0
    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
