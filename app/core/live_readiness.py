from __future__ import annotations


DEFAULT_LIVE_READINESS_THRESHOLDS = {
    "min_shared_windows": 100,
    "min_shadow_participation_pct": 90.0,
    "min_shadow_two_sided_window_pct": 95.0,
    "max_shadow_one_sided_window_pct": 5.0,
    "min_shadow_settlement_window_pct": 90.0,
    "max_shadow_vs_paper_cadence_ratio": 3.0,
    "max_shadow_blocker_pct": 20.0,
}

DEFAULT_LIVE_READINESS_BLOCKER_LABELS = {
    "budget_limited": "shadow se queda sin presupuesto util",
    "degraded_reference": "shadow se bloquea por referencia degradada",
    "waiting_official": "shadow espera priceToBeat oficial",
    "waiting_book": "shadow no ve libro suficiente",
}


def ratio_score(actual: float, target: float) -> float:
    safe_actual = max(float(actual or 0.0), 0.0)
    safe_target = max(float(target or 0.0), 0.0)
    if safe_target <= 0:
        return 1.0
    return max(0.0, min(safe_actual / safe_target, 1.0))


def inverse_ratio_score(actual: float, limit: float) -> float:
    safe_actual = max(float(actual or 0.0), 0.0)
    safe_limit = max(float(limit or 0.0), 0.0)
    if safe_limit <= 0:
        return 1.0 if safe_actual <= 0 else 0.0
    if safe_actual <= safe_limit:
        return 1.0
    return max(0.0, min(safe_limit / safe_actual, 1.0))


def evaluate_live_readiness(
    *,
    runtime_window_compare: dict | None,
    incubation: dict | None,
    thresholds: dict | None = None,
    blocker_labels: dict | None = None,
) -> dict:
    safe_thresholds = dict(DEFAULT_LIVE_READINESS_THRESHOLDS)
    if isinstance(thresholds, dict):
        safe_thresholds.update(thresholds)
    safe_blocker_labels = dict(DEFAULT_LIVE_READINESS_BLOCKER_LABELS)
    if isinstance(blocker_labels, dict):
        safe_blocker_labels.update(blocker_labels)

    compare = runtime_window_compare if isinstance(runtime_window_compare, dict) else {}
    history = compare.get("history") if isinstance(compare.get("history"), dict) else {}
    history_summary = history.get("summary") if isinstance(history.get("summary"), dict) else {}
    sample_summary = history.get("sample_summary") if isinstance(history.get("sample_summary"), dict) else {}
    incubation_summary = incubation if isinstance(incubation, dict) else {}

    shared_window_count = int(history_summary.get("shared_window_count") or 0)
    shadow_participation_pct = float(history_summary.get("shadow_participation_pct") or 0.0)
    shadow_two_sided_window_pct = float(history_summary.get("shadow_two_sided_window_pct") or 0.0)
    shadow_settlement_window_pct = float(history_summary.get("shadow_settlement_window_pct") or 0.0)
    shadow_active_window_count = int(history_summary.get("shadow_active_window_count") or 0)
    shadow_one_sided_window_count = int(history_summary.get("shadow_one_sided_window_count") or 0)
    shadow_one_sided_window_pct = (
        (shadow_one_sided_window_count / shadow_active_window_count) * 100 if shadow_active_window_count > 0 else 0.0
    )
    paper_avg_open_cadence_seconds = float(history_summary.get("paper_avg_open_cadence_seconds") or 0.0)
    shadow_avg_open_cadence_seconds = float(history_summary.get("shadow_avg_open_cadence_seconds") or 0.0)
    cadence_ratio = (
        round(shadow_avg_open_cadence_seconds / paper_avg_open_cadence_seconds, 4)
        if paper_avg_open_cadence_seconds > 0 and shadow_avg_open_cadence_seconds > 0
        else 0.0
    )
    dominant_state = str(sample_summary.get("shadow_dominant_operability_state") or "").strip()
    dominant_pct = float(sample_summary.get("shadow_dominant_operability_pct") or 0.0)
    blocker_state_label = safe_blocker_labels.get(dominant_state, "")

    max_drawdown = float(incubation_summary.get("max_drawdown") or 0.0)
    max_drawdown_limit = float(incubation_summary.get("max_drawdown_limit") or 0.0)
    drawdown_breached = bool(incubation_summary.get("drawdown_breached"))
    ready_to_scale = bool(incubation_summary.get("ready_to_scale"))
    incubation_progress_pct = float(incubation_summary.get("progress_pct") or 0.0)

    sample_ok = shared_window_count >= int(safe_thresholds["min_shared_windows"])
    participation_ok = shadow_participation_pct >= float(safe_thresholds["min_shadow_participation_pct"])
    two_sided_ok = shadow_two_sided_window_pct >= float(safe_thresholds["min_shadow_two_sided_window_pct"])
    one_sided_ok = shadow_one_sided_window_pct <= float(safe_thresholds["max_shadow_one_sided_window_pct"])
    settlement_ok = shadow_settlement_window_pct >= float(safe_thresholds["min_shadow_settlement_window_pct"])
    cadence_ok = bool(cadence_ratio) and cadence_ratio <= float(safe_thresholds["max_shadow_vs_paper_cadence_ratio"])
    blocker_ok = not blocker_state_label or dominant_pct <= float(safe_thresholds["max_shadow_blocker_pct"])
    drawdown_ok = not drawdown_breached
    incubation_ok = ready_to_scale

    blockers: list[str] = []
    strengths: list[str] = []

    if not sample_ok:
        blockers.append(
            f"Muestra corta: {shared_window_count}/{int(safe_thresholds['min_shared_windows'])} ventanas compartidas"
        )
    else:
        strengths.append(f"Muestra comparable suficiente: {shared_window_count} ventanas")
    if not drawdown_ok:
        blockers.append(f"Drawdown max {abs(max_drawdown):.2f} > limite {abs(max_drawdown_limit):.2f}")
    else:
        strengths.append(f"Drawdown dentro de limite ({abs(max_drawdown):.2f} / {abs(max_drawdown_limit):.2f})")
    if not incubation_ok:
        if incubation_progress_pct < 100:
            blockers.append(f"Incubacion interna al {incubation_progress_pct:.0f}%")
        else:
            blockers.append("Incubacion interna aun no lista para escalar")
    else:
        strengths.append("Incubacion interna en verde")
    if not participation_ok:
        blockers.append(
            f"Participacion shadow {shadow_participation_pct:.1f}% < {float(safe_thresholds['min_shadow_participation_pct']):.0f}%"
        )
    else:
        strengths.append(f"Participacion shadow {shadow_participation_pct:.1f}%")
    if not two_sided_ok:
        blockers.append(
            f"Dos patas shadow {shadow_two_sided_window_pct:.1f}% < {float(safe_thresholds['min_shadow_two_sided_window_pct']):.0f}%"
        )
    else:
        strengths.append(f"Dos patas shadow {shadow_two_sided_window_pct:.1f}%")
    if not one_sided_ok:
        blockers.append(
            f"Ventanas cojas shadow {shadow_one_sided_window_pct:.1f}% > {float(safe_thresholds['max_shadow_one_sided_window_pct']):.0f}%"
        )
    if not settlement_ok:
        blockers.append(
            f"Settlement shadow {shadow_settlement_window_pct:.1f}% < {float(safe_thresholds['min_shadow_settlement_window_pct']):.0f}%"
        )
    else:
        strengths.append(f"Settlement shadow {shadow_settlement_window_pct:.1f}%")
    if not cadence_ok:
        if cadence_ratio > 0:
            blockers.append(
                f"Cadencia shadow {cadence_ratio:.2f}x paper > {float(safe_thresholds['max_shadow_vs_paper_cadence_ratio']):.1f}x"
            )
        else:
            blockers.append("Cadencia insuficiente para comparar con paper")
    else:
        strengths.append(f"Cadencia shadow/paper {cadence_ratio:.2f}x")
    if not blocker_ok and blocker_state_label:
        blockers.append(
            f"Bloqueo dominante: {blocker_state_label} {dominant_pct:.1f}% > {float(safe_thresholds['max_shadow_blocker_pct']):.0f}%"
        )
    elif blocker_state_label and dominant_pct > 0:
        strengths.append(f"Bloqueo dominante controlado: {blocker_state_label} {dominant_pct:.1f}%")

    metric_scores = {
        "sample": ratio_score(shared_window_count, safe_thresholds["min_shared_windows"]),
        "participation": ratio_score(shadow_participation_pct, safe_thresholds["min_shadow_participation_pct"]),
        "two_sided": ratio_score(shadow_two_sided_window_pct, safe_thresholds["min_shadow_two_sided_window_pct"]),
        "one_sided": inverse_ratio_score(shadow_one_sided_window_pct, safe_thresholds["max_shadow_one_sided_window_pct"]),
        "settlement": ratio_score(shadow_settlement_window_pct, safe_thresholds["min_shadow_settlement_window_pct"]),
        "cadence": inverse_ratio_score(cadence_ratio, safe_thresholds["max_shadow_vs_paper_cadence_ratio"])
        if cadence_ratio > 0
        else 0.0,
        "dominant_blocker": inverse_ratio_score(dominant_pct, safe_thresholds["max_shadow_blocker_pct"])
        if blocker_state_label
        else 1.0,
        "drawdown": 1.0 if drawdown_ok else 0.0,
        "incubation": 1.0 if incubation_ok else max(0.0, min(incubation_progress_pct / 100.0, 1.0)),
    }
    weights = {
        "sample": 1.0,
        "participation": 1.25,
        "two_sided": 1.25,
        "one_sided": 1.0,
        "settlement": 1.0,
        "cadence": 1.25,
        "dominant_blocker": 1.0,
        "drawdown": 1.5,
        "incubation": 1.25,
    }
    total_weight = sum(weights.values()) or 1.0
    weighted_score = sum(metric_scores[key] * weights[key] for key in weights)
    readiness_score = int(round((weighted_score / total_weight) * 100))

    hard_failures = [
        not drawdown_ok,
        not participation_ok,
        not two_sided_ok,
        not one_sided_ok,
        not settlement_ok,
        not cadence_ok,
        not blocker_ok,
    ]
    if any(hard_failures):
        status = "blocked"
    elif not sample_ok or not incubation_ok:
        status = "warming"
    else:
        status = "ready"

    if status == "ready":
        label = "GO"
        headline = "Listo para activar live"
    elif status == "warming":
        label = "warming"
        headline = "Aun falta muestra antes de live"
    else:
        label = "no-go"
        headline = "Todavia no pasa el gate de live"

    passed_checks = int(
        sum(
            1
            for check in (
                sample_ok,
                participation_ok,
                two_sided_ok,
                one_sided_ok,
                settlement_ok,
                cadence_ok,
                blocker_ok,
                drawdown_ok,
                incubation_ok,
            )
            if check
        )
    )

    return {
        "status": status,
        "label": label,
        "headline": headline,
        "ready": status == "ready",
        "score": readiness_score,
        "passed_checks": passed_checks,
        "total_checks": 9,
        "blockers": blockers[:8],
        "strengths": strengths[:4],
        "thresholds": {
            "min_shared_windows": int(safe_thresholds["min_shared_windows"]),
            "min_shadow_participation_pct": round(float(safe_thresholds["min_shadow_participation_pct"]), 2),
            "min_shadow_two_sided_window_pct": round(float(safe_thresholds["min_shadow_two_sided_window_pct"]), 2),
            "max_shadow_one_sided_window_pct": round(float(safe_thresholds["max_shadow_one_sided_window_pct"]), 2),
            "min_shadow_settlement_window_pct": round(float(safe_thresholds["min_shadow_settlement_window_pct"]), 2),
            "max_shadow_vs_paper_cadence_ratio": round(float(safe_thresholds["max_shadow_vs_paper_cadence_ratio"]), 2),
            "max_shadow_blocker_pct": round(float(safe_thresholds["max_shadow_blocker_pct"]), 2),
        },
        "metrics": {
            "shared_window_count": shared_window_count,
            "shadow_participation_pct": round(shadow_participation_pct, 2),
            "shadow_two_sided_window_pct": round(shadow_two_sided_window_pct, 2),
            "shadow_one_sided_window_pct": round(shadow_one_sided_window_pct, 2),
            "shadow_settlement_window_pct": round(shadow_settlement_window_pct, 2),
            "paper_avg_open_cadence_seconds": round(paper_avg_open_cadence_seconds, 4),
            "shadow_avg_open_cadence_seconds": round(shadow_avg_open_cadence_seconds, 4),
            "cadence_ratio": round(cadence_ratio, 4),
            "shadow_dominant_operability_state": dominant_state,
            "shadow_dominant_operability_pct": round(dominant_pct, 2),
            "max_drawdown": round(max_drawdown, 4),
            "max_drawdown_limit": round(max_drawdown_limit, 4),
            "drawdown_breached": drawdown_breached,
            "incubation_ready_to_scale": incubation_ok,
            "incubation_progress_pct": round(incubation_progress_pct, 2),
        },
    }
