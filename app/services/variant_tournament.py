from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.lab_artifacts import (
    dump_json,
    load_dataset_summary,
    load_experiment_leaderboard,
    tournament_summary_path,
)
from app.core.live_readiness import evaluate_live_readiness
from app.core.strategy_registry import StrategyRegistry
from app.core.strategy_monitoring import build_incubation_summary
from app.db import Database
from app.services.experiment_runner import ExperimentRunner
from app.services.runtime_compare_db import build_runtime_compare_payload
from app.settings import AppSettings


class VariantTournamentService:
    def __init__(
        self,
        db: Database,
        settings: AppSettings,
        *,
        experiment_runner: ExperimentRunner | None = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.experiment_runner = experiment_runner or ExperimentRunner(
            settings.paths.research_dir,
            settings.strategy_registry if settings.strategy_registry is not None else StrategyRegistry(),
        )

    def generate(
        self,
        *,
        run_experiments: bool = True,
        dataset_paths: list[Path] | None = None,
        fallback_inputs: list[Path] | None = None,
        dataset_payload: dict[str, Any] | None = None,
        experiment_payload: dict[str, Any] | None = None,
        runtime_compare_payload: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], Path]:
        safe_dataset_payload = dataset_payload or load_dataset_summary(self.settings.paths.research_dir)
        safe_experiment_payload = experiment_payload
        if safe_experiment_payload is None:
            if run_experiments:
                safe_experiment_payload = self.experiment_runner.run(
                    dataset_paths=dataset_paths,
                    fallback_inputs=fallback_inputs,
                )
            else:
                safe_experiment_payload = load_experiment_leaderboard(self.settings.paths.research_dir)
        safe_runtime_compare = runtime_compare_payload
        if safe_runtime_compare is None:
            try:
                safe_runtime_compare = build_runtime_compare_payload(
                    data_dir=self.settings.paths.root / "data"
                )
            except Exception:  # noqa: BLE001
                safe_runtime_compare = {"available": False, "status": "unavailable", "history": {}}
        incubation_summary = self._incubation_summary()
        payload = build_variant_tournament_payload(
            active_variant=self.settings.config.strategy_variant,
            experiment_payload=safe_experiment_payload,
            dataset_payload=safe_dataset_payload,
            runtime_compare_payload=safe_runtime_compare,
            incubation_summary=incubation_summary,
        )
        output_path = tournament_summary_path(self.settings.paths.research_dir)
        dump_json(output_path, payload)
        markdown_path = self.settings.paths.reports_dir / "tournament_latest.md"
        markdown_path.write_text(render_variant_tournament_markdown(payload), encoding="utf-8")
        return payload, markdown_path

    def _incubation_summary(self) -> dict[str, Any]:
        config = self.settings.config
        runtime_mode = (
            str(self.db.get_bot_state("strategy_runtime_mode") or "").strip().lower()
            or str(config.execution_mode or "paper").strip().lower()
            or "paper"
        )
        return build_incubation_summary(
            self.db.conn,
            variant=config.strategy_variant,
            stage=config.incubation_stage,
            min_days=max(int(config.incubation_min_days), 0),
            min_resolutions=max(int(config.incubation_min_resolutions), 1),
            max_drawdown=max(float(config.incubation_max_drawdown), 0.0),
            runtime_mode=runtime_mode,
        )


def build_variant_tournament_payload(
    *,
    active_variant: str,
    experiment_payload: dict[str, Any] | None,
    dataset_payload: dict[str, Any] | None,
    runtime_compare_payload: dict[str, Any] | None,
    incubation_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    active = str(active_variant or "").strip().lower() or "default"
    experiments = experiment_payload if isinstance(experiment_payload, dict) else {}
    datasets = dataset_payload if isinstance(dataset_payload, dict) else {}
    runtime_compare = runtime_compare_payload if isinstance(runtime_compare_payload, dict) else {}
    incubation = incubation_summary if isinstance(incubation_summary, dict) else {}
    readiness = evaluate_live_readiness(runtime_window_compare=runtime_compare, incubation=incubation)

    raw_rows = experiments.get("variants") if isinstance(experiments.get("variants"), list) else []
    leaderboard: list[dict[str, Any]] = []
    for index, raw_row in enumerate(raw_rows, start=1):
        row = dict(raw_row) if isinstance(raw_row, dict) else {}
        variant_name = str(row.get("variant") or "").strip()
        if not variant_name:
            continue
        leaderboard.append(
            {
                "variant": variant_name,
                "rank": int(row.get("rank") or index),
                "status": str(row.get("status") or ""),
                "gate_passed": bool(row.get("gate_passed")),
                "score": round(float(row.get("score") or 0.0), 4),
                "pnl": round(float(row.get("net_realized_pnl_usdc") or 0.0), 4),
                "drawdown": round(float(row.get("max_drawdown_usdc") or 0.0), 4),
                "fill_rate": round(float(row.get("fill_rate") or 0.0), 4),
                "hit_rate": round(float(row.get("hit_rate") or 0.0), 4),
                "expectancy_window_usdc": round(float(row.get("expectancy_window_usdc") or 0.0), 4),
                "real_edge_bps": round(float(row.get("real_edge_bps") or 0.0), 4),
                "notes": str(row.get("notes") or ""),
            }
        )

    leaderboard.sort(
        key=lambda item: (int(item["rank"]), -float(item["score"]), -float(item["pnl"])),
    )
    active_row = next((row for row in leaderboard if str(row["variant"]).strip().lower() == active), None)
    best_row = leaderboard[0] if leaderboard else None
    best_pass_row = next((row for row in leaderboard if bool(row.get("gate_passed"))), None)
    recommendation = _build_tournament_recommendation(
        active_variant=active,
        active_row=active_row,
        best_row=best_row,
        best_pass_row=best_pass_row,
        readiness=readiness,
    )
    history = runtime_compare.get("history") if isinstance(runtime_compare.get("history"), dict) else {}
    history_summary = history.get("summary") if isinstance(history.get("summary"), dict) else {}

    return {
        "generated_at": generated_at,
        "active_variant": active,
        "active_variant_row": dict(active_row) if active_row else {},
        "best_variant_row": dict(best_row) if best_row else {},
        "best_passing_variant_row": dict(best_pass_row) if best_pass_row else {},
        "variant_count": len(leaderboard),
        "passing_variants": int(sum(1 for row in leaderboard if bool(row.get("gate_passed")))),
        "datasets": {
            "generated_at": str(datasets.get("generated_at") or ""),
            "windows": int(datasets.get("windows") or 0),
            "events": int(datasets.get("events") or 0),
            "trades": int(datasets.get("trades") or 0),
        },
        "runtime_compare": {
            "status": str(runtime_compare.get("status") or ""),
            "same_window": bool(runtime_compare.get("same_window")),
            "shared_slug": str(runtime_compare.get("shared_slug") or ""),
            "shared_window_count": int(history_summary.get("shared_window_count") or 0),
            "shadow_participation_pct": round(float(history_summary.get("shadow_participation_pct") or 0.0), 2),
            "shadow_two_sided_window_pct": round(float(history_summary.get("shadow_two_sided_window_pct") or 0.0), 2),
            "shadow_one_sided_window_pct": round(
                float(
                    (
                        (float(history_summary.get("shadow_one_sided_window_count") or 0.0) * 100.0)
                        / max(float(history_summary.get("shadow_active_window_count") or 0.0), 1.0)
                    )
                    if float(history_summary.get("shadow_active_window_count") or 0.0) > 0
                    else 0.0
                ),
                2,
            ),
            "shadow_settlement_window_pct": round(float(history_summary.get("shadow_settlement_window_pct") or 0.0), 2),
        },
        "incubation": {
            "stage": str(incubation.get("stage") or ""),
            "progress_pct": round(float(incubation.get("progress_pct") or 0.0), 2),
            "ready_to_scale": bool(incubation.get("ready_to_scale")),
            "resolutions": int(incubation.get("resolutions") or 0),
            "max_drawdown": round(float(incubation.get("max_drawdown") or 0.0), 4),
            "max_drawdown_limit": round(float(incubation.get("max_drawdown_limit") or 0.0), 4),
        },
        "live_readiness": readiness,
        "recommendation": recommendation,
        "leaderboard": leaderboard[:8],
    }


def render_variant_tournament_markdown(payload: dict[str, Any]) -> str:
    recommendation = payload.get("recommendation") if isinstance(payload.get("recommendation"), dict) else {}
    readiness = payload.get("live_readiness") if isinstance(payload.get("live_readiness"), dict) else {}
    leaderboard = payload.get("leaderboard") if isinstance(payload.get("leaderboard"), list) else []
    datasets = payload.get("datasets") if isinstance(payload.get("datasets"), dict) else {}
    runtime_compare = payload.get("runtime_compare") if isinstance(payload.get("runtime_compare"), dict) else {}

    lines = [
        f"# Variant Tournament ({payload.get('generated_at') or ''})",
        "",
        "## Recommendation",
        f"- Code: {recommendation.get('code') or '-'}",
        f"- Label: {recommendation.get('label') or '-'}",
        f"- Summary: {recommendation.get('summary') or '-'}",
        f"- Candidate variant: {recommendation.get('candidate_variant') or '-'}",
        f"- Next step: {recommendation.get('next_step') or '-'}",
        "",
        "## Readiness",
        f"- Status: {readiness.get('status') or '-'}",
        f"- Score: {int(readiness.get('score') or 0)}",
        f"- Headline: {readiness.get('headline') or '-'}",
        "",
        "## Dataset",
        f"- Windows: {int(datasets.get('windows') or 0)}",
        f"- Events: {int(datasets.get('events') or 0)}",
        f"- Trades: {int(datasets.get('trades') or 0)}",
        "",
        "## Runtime Compare",
        f"- Status: {runtime_compare.get('status') or '-'}",
        f"- Shared windows: {int(runtime_compare.get('shared_window_count') or 0)}",
        f"- Shadow participation: {float(runtime_compare.get('shadow_participation_pct') or 0.0):.2f}%",
        f"- Shadow two-sided: {float(runtime_compare.get('shadow_two_sided_window_pct') or 0.0):.2f}%",
        "",
        "## Leaderboard",
    ]
    if not leaderboard:
        lines.append("- None")
    else:
        for row in leaderboard[:5]:
            lines.append(
                "- "
                f"#{int(row.get('rank') or 0)} {row.get('variant') or '-'} | "
                f"{row.get('status') or '-'} | pnl={float(row.get('pnl') or 0.0):.2f} | "
                f"exp/win={float(row.get('expectancy_window_usdc') or 0.0):.2f} | "
                f"edge={float(row.get('real_edge_bps') or 0.0):.2f}bps"
            )
    blockers = readiness.get("blockers") if isinstance(readiness.get("blockers"), list) else []
    if blockers:
        lines.extend(["", "## Blockers"])
        for item in blockers[:5]:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _build_tournament_recommendation(
    *,
    active_variant: str,
    active_row: dict[str, Any] | None,
    best_row: dict[str, Any] | None,
    best_pass_row: dict[str, Any] | None,
    readiness: dict[str, Any] | None,
) -> dict[str, Any]:
    safe_readiness = readiness if isinstance(readiness, dict) else {}
    blockers = safe_readiness.get("blockers") if isinstance(safe_readiness.get("blockers"), list) else []
    active_name = str(active_variant or "").strip().lower() or "default"
    best_name = str(best_row.get("variant") or "") if isinstance(best_row, dict) else ""
    best_pass_name = str(best_pass_row.get("variant") or "") if isinstance(best_pass_row, dict) else ""
    readiness_status = str(safe_readiness.get("status") or "")

    if not best_row:
        return {
            "code": "no-tournament-data",
            "label": "Sin evidencia todavia",
            "summary": "Todavia no hay ranking suficiente de variantes para tomar una decision.",
            "candidate_variant": "",
            "next_step": "Seguir capturando ventanas y volver a correr tournament.",
            "reasons": [],
        }
    if not best_pass_row:
        return {
            "code": "keep-paused-no-pass",
            "label": "Mantener live pausado",
            "summary": f"La mejor variante actual es {best_name}, pero ninguna pasa el gate de backtest todavia.",
            "candidate_variant": best_name,
            "next_step": f"Seguir validando en shadow/paper y evitar live hasta que una variante pase el gate.",
            "reasons": blockers[:4],
        }
    if best_pass_name.strip().lower() != active_name:
        return {
            "code": "rotate-shadow-to-best-pass",
            "label": "Cambiar la variante en validacion",
            "summary": (
                f"La variante activa {active_name} no es la mejor candidata. "
                f"La que mejor sale ahora es {best_pass_name}."
            ),
            "candidate_variant": best_pass_name,
            "next_step": f"Pasar {best_pass_name} a shadow/paper principal antes de volver a evaluar live.",
            "reasons": blockers[:4],
        }
    if readiness_status != "ready":
        return {
            "code": "keep-paused-collect-evidence",
            "label": "Mantener live pausado",
            "summary": (
                f"{active_name} es la mejor variante disponible, pero el gate de live sigue en "
                f"{safe_readiness.get('label') or readiness_status}."
            ),
            "candidate_variant": active_name,
            "next_step": "Seguir acumulando muestra comparable y mejorar los bloqueos dominantes antes de live.",
            "reasons": blockers[:4],
        }
    active_pnl = float(active_row.get("pnl") or 0.0) if isinstance(active_row, dict) else 0.0
    return {
        "code": "candidate-controlled-live",
        "label": "Candidata para prueba controlada",
        "summary": (
            f"{active_name} lidera el torneo y el gate de live esta en verde. "
            f"PnL backtest actual {active_pnl:.2f}."
        ),
        "candidate_variant": active_name,
        "next_step": "Hacer una prueba corta y supervisada de live con tamano minimo antes de escalar.",
        "reasons": [],
    }
