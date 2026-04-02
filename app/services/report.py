from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.core.incubation_policy import evaluate_incubation_progress
from app.core.lab_artifacts import (
    load_dataset_summary,
    load_experiment_leaderboard,
    load_runtime_diagnostics,
    load_wallet_hypotheses,
)
from app.core.strategy_monitoring import build_incubation_summary, build_recent_resolution_windows
from app.db import Database


class ReportService:
    def __init__(self, db: Database, reports_dir: Path) -> None:
        self.db = db
        self.reports_dir = reports_dir

    def generate(self) -> tuple[str, Path]:
        now = datetime.now(timezone.utc)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        positions = self.db.list_copy_positions()
        executions = self.db.get_recent_executions(limit=25)

        total_exposure = self.db.get_total_exposure()
        cumulative_pnl = self.db.get_cumulative_pnl()
        strategy_variant = (self.db.get_bot_state("strategy_variant") or "default").strip() or "default"
        strategy_notes = (self.db.get_bot_state("strategy_notes") or "").strip()
        incubation_stage = self.db.get_bot_state("strategy_incubation_stage") or "disabled"
        incubation_min_days = _to_int(self.db.get_bot_state("strategy_incubation_min_days"), default=14)
        incubation_min_resolutions = _to_int(
            self.db.get_bot_state("strategy_incubation_min_resolutions"),
            default=20,
        )
        incubation_max_drawdown = _to_float(
            self.db.get_bot_state("strategy_incubation_max_drawdown"),
            default=50.0,
        )
        incubation = build_incubation_summary(
            self.db.conn,
            variant=strategy_variant,
            stage=incubation_stage,
            min_days=max(incubation_min_days, 0),
            min_resolutions=max(incubation_min_resolutions, 1),
            max_drawdown=max(incubation_max_drawdown, 0.0),
        )
        recent_resolutions = build_recent_resolution_windows(
            self.db.conn,
            variant=strategy_variant,
            limit=5,
            runtime_mode=str(self.db.get_bot_state("strategy_runtime_mode") or ""),
        )
        research_root = self.reports_dir.parent / "research"
        experiment_payload = load_experiment_leaderboard(research_root)
        dataset_payload = load_dataset_summary(research_root)
        diagnostics_payload = load_runtime_diagnostics(research_root)
        wallet_payload = load_wallet_hypotheses(research_root)
        active_experiment = _active_experiment_row(experiment_payload, variant=strategy_variant)
        incubation_transition = evaluate_incubation_progress(
            stage=incubation_stage,
            live_metrics=incubation,
            backtest_metrics=active_experiment,
            auto_promote=_to_bool(self.db.get_bot_state("strategy_incubation_auto_promote")),
        )

        lines: list[str] = []
        lines.append(f"# Polymarket Copy Bot Report ({now.isoformat()} UTC)")
        lines.append("")
        lines.append("## Summary")
        lines.append(f"- Open copied positions: {len(positions)}")
        lines.append(f"- Current exposure (USDC est): {total_exposure:.2f}")
        lines.append(f"- Cumulative PnL (realized): {cumulative_pnl:.2f}")
        lines.append(f"- Strategy variant: {strategy_variant}")
        lines.append(
            "- Incubation: "
            f"{incubation['stage_label']} | {incubation['recommendation_label']} | "
            f"progress={float(incubation['progress_pct']):.2f}%"
        )
        lines.append(
            "- Next gate: "
            f"{incubation_transition['label']} | next_stage={incubation_transition['next_stage']}"
        )
        if strategy_notes:
            lines.append(f"- Variant notes: {strategy_notes}")
        lines.append("")
        lines.append("## Incubation")
        lines.append(f"- Resolved windows: {int(incubation['resolutions'])}")
        lines.append(f"- Days observed: {float(incubation['days_observed']):.2f}")
        lines.append(f"- Win rate: {float(incubation['win_rate_pct']):.2f}%")
        lines.append(f"- Total PnL: {float(incubation['pnl_total']):.2f}")
        lines.append(f"- Avg PnL / resolution: {float(incubation['avg_pnl']):.2f}")
        lines.append(f"- Deployed notional total: {float(incubation['deployed_total']):.2f}")
        lines.append(f"- Max drawdown: {float(incubation['max_drawdown']):.2f}")
        lines.append(
            "- Targets: "
            f"{int(incubation['min_resolutions'])} resolutions / {int(incubation['min_days'])} days / "
            f"drawdown <= {float(incubation['max_drawdown_limit']):.2f}"
        )
        lines.append(f"- Transition rule: {incubation_transition['reason']}")
        lines.append("")
        lines.append("## Backtest / Experiments")
        if not active_experiment:
            lines.append("- None")
        else:
            lines.append(f"- Generated at: {experiment_payload.get('generated_at') or '-'}")
            lines.append(f"- Status: {active_experiment.get('status') or '-'}")
            lines.append(f"- PnL: {float(active_experiment.get('net_realized_pnl_usdc') or 0.0):.2f}")
            lines.append(f"- Max drawdown: {float(active_experiment.get('max_drawdown_usdc') or 0.0):.2f}")
            lines.append(f"- Fill rate: {float(active_experiment.get('fill_rate') or 0.0) * 100:.2f}%")
            lines.append(f"- Hit rate: {float(active_experiment.get('hit_rate') or 0.0) * 100:.2f}%")
            lines.append(f"- Real edge: {float(active_experiment.get('real_edge_bps') or 0.0):.2f} bps")
        lines.append("")
        lines.append("## Runtime Diagnostics")
        lines.append(f"- Generated at: {diagnostics_payload.get('generated_at') or '-'}")
        lines.append(f"- Status: {diagnostics_payload.get('status') or '-'}")
        lines.append(f"- Summary: {diagnostics_payload.get('summary') or '-'}")
        findings = diagnostics_payload.get("findings") if isinstance(diagnostics_payload.get("findings"), list) else []
        if not findings:
            lines.append("- Findings: None")
        else:
            for item in findings[:4]:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    "- "
                    f"[{item.get('severity') or 'info'}] {item.get('title') or '-'} | "
                    f"{item.get('detail') or ''}"
                )
        lines.append("")
        lines.append("## Native Dataset")
        lines.append(f"- Generated at: {dataset_payload.get('generated_at') or '-'}")
        lines.append(f"- Windows: {int(dataset_payload.get('windows') or 0)}")
        lines.append(f"- Events: {int(dataset_payload.get('events') or 0)}")
        lines.append(f"- Trades: {int(dataset_payload.get('trades') or 0)}")
        lines.append("")
        lines.append("## Wallet Hypotheses")
        hypotheses = wallet_payload.get("hypotheses") if isinstance(wallet_payload.get("hypotheses"), list) else []
        if not hypotheses:
            lines.append("- None")
        else:
            for item in hypotheses[:4]:
                if not isinstance(item, dict):
                    continue
                lines.append(f"- {item.get('title') or '-'} | {item.get('detail') or ''}")
        lines.append("")
        lines.append("## Recent Resolutions")
        if not recent_resolutions:
            lines.append("- None")
        else:
            for row in recent_resolutions:
                lines.append(
                    "- "
                    f"{row['slug']} | pnl={float(row['pnl']):.2f} | "
                    f"deployed={float(row['deployed_notional']):.2f} | winner={row['winning_outcome'] or '-'}"
                )
        lines.append("")
        lines.append("## Copied Positions")

        if not positions:
            lines.append("- None")
        else:
            for row in positions:
                lines.append(
                    "- "
                    f"{row['title']} | asset={row['asset']} | size={float(row['size']):.4f} | "
                    f"avg_price={float(row['avg_price']):.4f} | category={row['category']}"
                )

        lines.append("")
        lines.append("## Recent Executions")
        if not executions:
            lines.append("- None")
        else:
            for row in executions:
                ts = datetime.utcfromtimestamp(int(row["ts"]))
                lines.append(
                    "- "
                    f"{ts.isoformat()}Z | mode={row['mode']} | status={row['status']} | "
                    f"action={row['action']} | side={row['side']} | size={float(row['size']):.4f} | "
                    f"price={float(row['price']):.4f} | pnl_delta={float(row['pnl_delta']):.4f}"
                )

        content = "\n".join(lines) + "\n"
        output_path = self.reports_dir / f"report_{now.strftime('%Y%m%d_%H%M%S')}.md"
        output_path.write_text(content, encoding="utf-8")
        return content, output_path


def _to_int(raw: str | None, *, default: int) -> int:
    try:
        return int(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default


def _to_float(raw: str | None, *, default: float) -> float:
    try:
        return float(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default


def _to_bool(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _active_experiment_row(payload: dict, *, variant: str) -> dict:
    rows = payload.get("variants") if isinstance(payload.get("variants"), list) else []
    safe_variant = str(variant or "").strip().lower()
    for row in rows:
        if str(row.get("variant") or "").strip().lower() == safe_variant:
            return dict(row)
    return {}
