from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def build_recent_resolution_windows(
    conn: sqlite3.Connection,
    *,
    variant: str,
    limit: int,
) -> list[dict]:
    strategy_rows = _closed_strategy_windows(conn, variant=variant)
    if strategy_rows:
        ordered = sorted(strategy_rows, key=lambda row: int(row["closed_at"] or 0), reverse=True)[:limit]
        return [
            {
                "slug": str(row["slug"] or ""),
                "resolved_at": int(row["closed_at"] or 0),
                "pnl": round(float(row["realized_pnl"] or 0.0), 4),
                "notional": round(_window_notional(row), 4),
                "deployed_notional": round(float(row["deployed_notional"] or 0.0), 4),
                "planned_budget": round(float(row["planned_budget"] or 0.0), 4),
                "legs": int(row["filled_orders"] or 0),
                "winning_outcome": str(row["winning_outcome"] or ""),
                "strategy_variant": str(row["strategy_variant"] or ""),
            }
            for row in ordered
        ]

    rows = _resolution_execution_rows(conn, variant=variant, limit=400)
    grouped: dict[str, dict] = {}
    for row in rows:
        notes = str(row["notes"] or "")
        parts = notes.split(":")
        slug = parts[1] if len(parts) > 1 else "desconocido"
        outcome = parts[2] if len(parts) > 2 else ""
        entry = grouped.setdefault(
            slug,
            {
                "slug": slug,
                "resolved_at": int(row["ts"] or 0),
                "pnl": 0.0,
                "notional": 0.0,
                "legs": 0,
                "winning_outcome": outcome,
                "strategy_variant": str(row["strategy_variant"] or ""),
                "_best_leg_pnl": float("-inf"),
            },
        )
        pnl_delta = float(row["pnl_delta"] or 0.0)
        entry["resolved_at"] = max(int(row["ts"] or 0), int(entry["resolved_at"]))
        entry["pnl"] += pnl_delta
        entry["notional"] += abs(float(row["notional"] or 0.0))
        entry["legs"] += 1
        if pnl_delta >= float(entry["_best_leg_pnl"]):
            entry["_best_leg_pnl"] = pnl_delta
            entry["winning_outcome"] = outcome

    ordered = sorted(grouped.values(), key=lambda item: int(item["resolved_at"]), reverse=True)[:limit]
    return [
        {
            "slug": str(item["slug"]),
            "resolved_at": int(item["resolved_at"]),
            "pnl": round(float(item["pnl"]), 4),
            "notional": round(float(item["notional"]), 4),
            "deployed_notional": round(float(item["notional"]), 4),
            "planned_budget": 0.0,
            "legs": int(item["legs"]),
            "winning_outcome": str(item["winning_outcome"] or ""),
            "strategy_variant": str(item["strategy_variant"] or ""),
        }
        for item in ordered
    ]


def build_setup_performance(
    conn: sqlite3.Connection,
    *,
    variant: str,
    limit: int,
) -> list[dict]:
    rows = _closed_strategy_windows(conn, variant=variant)
    grouped: dict[tuple[str, str], dict[str, float | int | str]] = {}
    for row in rows:
        price_mode = str(row["price_mode"] or "-")
        timing_regime = str(row["timing_regime"] or "-")
        key = (price_mode, timing_regime)
        entry = grouped.setdefault(
            key,
            {
                "price_mode": price_mode,
                "timing_regime": timing_regime,
                "windows": 0,
                "wins": 0,
                "pnl_total": 0.0,
                "budget_total": 0.0,
                "deployed_total": 0.0,
                "primary_ratio_total": 0.0,
            },
        )
        pnl = float(row["realized_pnl"] or 0.0)
        entry["windows"] = int(entry["windows"]) + 1
        entry["wins"] = int(entry["wins"]) + (1 if pnl > 0 else 0)
        entry["pnl_total"] = float(entry["pnl_total"]) + pnl
        entry["budget_total"] = float(entry["budget_total"]) + float(row["planned_budget"] or 0.0)
        entry["deployed_total"] = float(entry["deployed_total"]) + _window_notional(row)
        entry["primary_ratio_total"] = float(entry["primary_ratio_total"]) + float(row["primary_ratio"] or 0.0)

    items: list[dict] = []
    for entry in grouped.values():
        windows = int(entry["windows"])
        wins = int(entry["wins"])
        pnl_total = float(entry["pnl_total"])
        items.append(
            {
                "price_mode": str(entry["price_mode"]),
                "timing_regime": str(entry["timing_regime"]),
                "windows": windows,
                "wins": wins,
                "win_rate_pct": round((wins / windows) * 100, 2) if windows > 0 else 0.0,
                "pnl_total": round(pnl_total, 4),
                "pnl_avg": round((pnl_total / windows), 4) if windows > 0 else 0.0,
                "budget_total": round(float(entry["budget_total"]), 4),
                "deployed_total": round(float(entry["deployed_total"]), 4),
                "primary_ratio_avg": round(float(entry["primary_ratio_total"]) / windows, 4) if windows > 0 else 0.0,
            }
        )
    items.sort(key=lambda item: (float(item["pnl_total"]), int(item["windows"])), reverse=True)
    return items[:limit]


def build_incubation_summary(
    conn: sqlite3.Connection,
    *,
    variant: str,
    stage: str,
    min_days: int,
    min_resolutions: int,
    max_drawdown: float,
) -> dict[str, float | int | bool | str]:
    rows = _closed_strategy_windows(conn, variant=variant)
    pnl_total = 0.0
    deployed_total = 0.0
    max_drawdown_seen = 0.0
    cumulative_pnl = 0.0
    equity_peak = 0.0
    best_resolution = 0.0
    worst_resolution = 0.0
    wins = 0
    losses = 0
    first_closed_at = int(rows[0]["closed_at"] or rows[0]["opened_at"] or 0) if rows else 0
    last_closed_at = int(rows[-1]["closed_at"] or rows[-1]["opened_at"] or 0) if rows else 0

    for index, row in enumerate(rows):
        pnl = float(row["realized_pnl"] or 0.0)
        pnl_total += pnl
        deployed_total += _window_notional(row)
        cumulative_pnl += pnl
        equity_peak = max(equity_peak, cumulative_pnl)
        max_drawdown_seen = min(max_drawdown_seen, cumulative_pnl - equity_peak)
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        if index == 0:
            best_resolution = pnl
            worst_resolution = pnl
        else:
            best_resolution = max(best_resolution, pnl)
            worst_resolution = min(worst_resolution, pnl)

    resolutions = len(rows)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    days_observed = ((now_ts - first_closed_at) / 86400.0) if first_closed_at > 0 else 0.0
    avg_pnl = (pnl_total / resolutions) if resolutions > 0 else 0.0
    avg_deployed = (deployed_total / resolutions) if resolutions > 0 else 0.0
    win_rate_pct = ((wins / resolutions) * 100.0) if resolutions > 0 else 0.0
    required_days = max(int(min_days), 0)
    required_resolutions = max(int(min_resolutions), 1)
    days_progress = 1.0 if required_days == 0 else min(days_observed / required_days, 1.0)
    resolution_progress = min(resolutions / required_resolutions, 1.0)
    progress_pct = min(days_progress, resolution_progress) * 100.0
    drawdown_limit = max(float(max_drawdown), 0.0)
    drawdown_breached = drawdown_limit > 0 and abs(max_drawdown_seen) >= drawdown_limit
    ready_to_scale = (
        stage in {"paper", "live_small"}
        and resolutions >= required_resolutions
        and days_observed >= required_days
        and pnl_total > 0
        and not drawdown_breached
    )
    recommendation, recommendation_label = _recommendation(
        stage=stage,
        resolutions=resolutions,
        pnl_total=pnl_total,
        progress_pct=progress_pct,
        ready_to_scale=ready_to_scale,
        drawdown_breached=drawdown_breached,
    )
    return {
        "variant": _normalized_variant(variant) or "default",
        "stage": _normalized_stage(stage),
        "stage_label": _stage_label(stage),
        "resolutions": resolutions,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate_pct, 2),
        "pnl_total": round(pnl_total, 4),
        "avg_pnl": round(avg_pnl, 4),
        "deployed_total": round(deployed_total, 4),
        "avg_deployed": round(avg_deployed, 4),
        "max_drawdown": round(max_drawdown_seen, 4),
        "best_resolution": round(best_resolution, 4),
        "worst_resolution": round(worst_resolution, 4),
        "days_observed": round(days_observed, 2),
        "progress_pct": round(progress_pct, 2),
        "min_days": required_days,
        "min_resolutions": required_resolutions,
        "max_drawdown_limit": round(drawdown_limit, 4),
        "drawdown_breached": drawdown_breached,
        "ready_to_scale": ready_to_scale,
        "first_closed_at": first_closed_at,
        "last_closed_at": last_closed_at,
        "recommendation": recommendation,
        "recommendation_label": recommendation_label,
    }


def _closed_strategy_windows(conn: sqlite3.Connection, *, variant: str) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT
            slug,
            strategy_variant,
            opened_at,
            closed_at,
            realized_pnl,
            planned_budget,
            deployed_notional,
            filled_orders,
            winning_outcome,
            price_mode,
            timing_regime,
            primary_ratio
        FROM strategy_windows
        WHERE status = 'closed'
        ORDER BY COALESCE(closed_at, opened_at, 0) ASC
        """
    ).fetchall()
    return _filter_rows_by_variant(rows, variant=variant, field="strategy_variant")


def _resolution_execution_rows(
    conn: sqlite3.Connection,
    *,
    variant: str,
    limit: int,
) -> list[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT ts, notes, pnl_delta, notional, strategy_variant
        FROM executions
        WHERE mode = 'paper' AND (notes LIKE 'strategy_resolution:%' OR notes LIKE 'vidarx_resolution:%')
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return _filter_rows_by_variant(rows, variant=variant, field="strategy_variant")


def _filter_rows_by_variant(
    rows: list[sqlite3.Row],
    *,
    variant: str,
    field: str,
) -> list[sqlite3.Row]:
    active_variant = _normalized_variant(variant)
    if not active_variant:
        return list(rows)
    matched = [row for row in rows if _normalized_variant(row[field]) == active_variant]
    if matched:
        return matched
    return [row for row in rows if not _normalized_variant(row[field])]


def _window_notional(row: sqlite3.Row) -> float:
    deployed_notional = float(row["deployed_notional"] or 0.0)
    if deployed_notional > 0:
        return deployed_notional
    return float(row["planned_budget"] or 0.0)


def _normalized_variant(value: object) -> str:
    return str(value or "").strip()


def _normalized_stage(stage: str) -> str:
    safe_stage = str(stage or "").strip().lower()
    if safe_stage in {"idea", "backtest_pass", "paper", "live_small", "scaled", "paused"}:
        return safe_stage
    return "disabled"


def _stage_label(stage: str) -> str:
    safe_stage = _normalized_stage(stage)
    labels = {
        "disabled": "Sin incubacion",
        "idea": "Idea",
        "backtest_pass": "Backtest aprobado",
        "paper": "Paper incubando",
        "live_small": "Live pequeno",
        "scaled": "Escalada",
        "paused": "Pausada",
    }
    return labels.get(safe_stage, "Sin incubacion")


def _recommendation(
    *,
    stage: str,
    resolutions: int,
    pnl_total: float,
    progress_pct: float,
    ready_to_scale: bool,
    drawdown_breached: bool,
) -> tuple[str, str]:
    safe_stage = _normalized_stage(stage)
    if safe_stage == "disabled":
        return "disabled", "Seguimiento desactivado"
    if safe_stage == "idea":
        return "run_backtest", "Completar backtest"
    if safe_stage == "backtest_pass":
        return "promote_to_paper", "Lista para paper"
    if safe_stage == "paused":
        return "paused", "Pausar y revisar"
    if drawdown_breached:
        return "pause_and_review", "Pausar y revisar"
    if safe_stage == "scaled":
        return "scaled", "Mantener escalada"
    if ready_to_scale:
        return "ready_to_scale", "Lista para escalar"
    if resolutions == 0:
        return "collect_data", "Recoger mas datos"
    if progress_pct >= 50.0 and pnl_total <= 0:
        return "review_edge", "Revisar edge"
    return "keep_incubating", "Seguir incubando"
