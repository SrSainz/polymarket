from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv

from app.advisor.runtime import build_advisor_runtime
from app.advisor.webhook import run_webhook_server


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    parser = _build_parser()
    args = parser.parse_args(argv)
    db_path = Path(args.db_path or os.getenv("ADVISOR_DB_PATH", "data/advisor.db")).expanduser()
    bankroll = _read_bankroll(os.getenv("ADVISOR_BANKROLL_USDC", "0"))
    runtime = build_advisor_runtime(
        db_path=db_path,
        bankroll_usdc=bankroll,
    )
    try:
        if args.command == "status":
            _print_status(runtime, db_path)
            return 0
        if args.command == "daily":
            if bankroll <= 0:
                raise SystemExit("ADVISOR_BANKROLL_USDC must be > 0 for the daily command")
            result = runtime.runner.run_once()
            _print_daily_result(result)
            return 0
        if args.command == "scheduler":
            if bankroll <= 0:
                raise SystemExit("ADVISOR_BANKROLL_USDC must be > 0 for the scheduler command")
            from app.advisor.runner import DailyAdvisorScheduler

            scheduler = DailyAdvisorScheduler(runtime.runner, poll_seconds=args.poll_seconds)
            try:
                scheduler.run_forever(on_result=_print_daily_result)
            except KeyboardInterrupt:
                scheduler.stop()
            return 0
        if args.command == "webhook":
            run_webhook_server(
                runtime.approval,
                runtime.config.whatsapp,
                host=args.host,
                port=args.port,
            )
            return 0
        parser.error(f"unknown command: {args.command}")
    finally:
        runtime.close()
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket sports advisor runtime")
    parser.add_argument("--db-path", default="", help="SQLite path; defaults to ADVISOR_DB_PATH")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="Show safe runtime gates without secrets")
    subparsers.add_parser("daily", help="Run one daily discovery/evaluation cycle")
    scheduler = subparsers.add_parser("scheduler", help="Run at most one cycle per UTC day")
    scheduler.add_argument("--poll-seconds", type=float, default=60.0)
    webhook = subparsers.add_parser("webhook", help="Serve the private WhatsApp webhook")
    webhook.add_argument("--host", default=os.getenv("ADVISOR_WEBHOOK_HOST", "127.0.0.1"))
    webhook.add_argument("--port", type=int, default=_read_int_env("ADVISOR_WEBHOOK_PORT", 8787))
    return parser


def _read_bankroll(raw: str) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError) as error:
        raise SystemExit("ADVISOR_BANKROLL_USDC must be numeric") from error
    if not math.isfinite(value) or value < 0:
        raise SystemExit("ADVISOR_BANKROLL_USDC must be finite and >= 0")
    return value


def _read_int_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise SystemExit(f"{name} must be an integer") from error
    if not 1 <= value <= 65_535:
        raise SystemExit(f"{name} must be between 1 and 65535")
    return value


def _print_status(runtime, db_path: Path) -> None:  # noqa: ANN001
    payload = {
        "db_path": str(db_path.resolve()),
        "advisor_enabled": runtime.config.enabled,
        "paper_ready": runtime.config.paper_ready,
        "live_ready": runtime.config.live_ready,
        "control_state": runtime.config.control_state,
        "global_execution_mode": runtime.config.global_execution_mode,
        "global_dry_run": runtime.config.global_dry_run,
        "whatsapp_ready": runtime.config.whatsapp.ready,
        "runner_enabled": runtime.runner.enabled,
        "evidence_path": runtime.config.evidence_path,
    }
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True))


def _print_daily_result(result) -> None:  # noqa: ANN001
    evaluation = result.run.evaluation
    proposal = result.run.proposal
    payload = {
        "discovered_quotes": result.discovered_quotes,
        "modelled_opportunities": result.modelled_opportunities,
        "notifications": result.notifications,
        "evaluation": asdict(evaluation),
        "proposal": (
            {
                "proposal_id": proposal.proposal_id,
                "market_id": proposal.market_id,
                "condition_id": proposal.condition_id,
                "token_id": proposal.token_id,
                "title": proposal.title,
                "outcome": proposal.outcome,
                "max_price": proposal.max_price,
                "max_notional_usdc": proposal.max_notional_usdc,
                "net_edge_bps": proposal.net_edge_bps,
                "expires_at": proposal.expires_at,
                "quote_expires_at": proposal.quote_expires_at,
            }
            if proposal is not None
            else None
        ),
    }
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
