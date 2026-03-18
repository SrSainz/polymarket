from __future__ import annotations

from typing import Any, Mapping


_STAGE_ORDER = ("idea", "backtest_pass", "paper", "live_small", "scaled", "paused", "disabled")


def evaluate_incubation_progress(
    *,
    stage: str,
    live_metrics: Mapping[str, Any],
    backtest_metrics: Mapping[str, Any] | None,
    auto_promote: bool,
) -> dict[str, Any]:
    safe_stage = _normalize_stage(stage)
    live = dict(live_metrics or {})
    backtest = dict(backtest_metrics or {})
    drawdown_breached = bool(live.get("drawdown_breached"))
    live_ready = bool(live.get("ready_to_scale"))
    backtest_passed = bool(backtest.get("gate_passed"))
    backtest_pnl = _safe_float(backtest.get("net_realized_pnl_usdc"))
    backtest_fill_rate = _safe_float(backtest.get("fill_rate"))
    backtest_hit_rate = _safe_float(backtest.get("hit_rate"))
    backtest_edge_bps = _safe_float(backtest.get("real_edge_bps"))

    next_stage = safe_stage
    recommendation = "keep_incubating"
    label = "Seguir incubando"
    reason = "La variante sigue recogiendo datos antes del siguiente gate."
    transition_ready = False

    if safe_stage == "disabled":
        recommendation = "disabled"
        label = "Incubacion desactivada"
        reason = "No hay workflow de incubacion activo para esta variante."
    elif safe_stage == "paused" or drawdown_breached:
        next_stage = "paused"
        recommendation = "pause_and_review"
        label = "Pausar y revisar"
        reason = "Se activo el freno de drawdown o la variante ya esta pausada."
        transition_ready = safe_stage != "paused"
    elif safe_stage == "idea":
        if backtest_passed:
            next_stage = "backtest_pass"
            recommendation = "promote_to_backtest_pass"
            label = "Promocionar a backtest pass"
            reason = "La variante supera los gates de PnL, fill rate, hit rate y edge real en backtest."
            transition_ready = True
        else:
            recommendation = "run_backtest"
            label = "Completar backtest"
            reason = (
                f"Faltan evidencias de backtest: pnl={backtest_pnl:.2f}, "
                f"fill={backtest_fill_rate:.2%}, hit={backtest_hit_rate:.2%}, edge={backtest_edge_bps:.2f}bps."
            )
    elif safe_stage == "backtest_pass":
        if backtest_passed:
            next_stage = "paper"
            recommendation = "promote_to_paper"
            label = "Promocionar a paper"
            reason = "La variante mantiene el gate de backtest y ya puede incubarse contra datos paper."
            transition_ready = True
        else:
            recommendation = "rework_backtest"
            label = "Repetir backtest"
            reason = "La variante perdio el gate de backtest y necesita revalidacion."
    elif safe_stage == "paper":
        if live_ready:
            next_stage = "live_small"
            recommendation = "promote_to_live_small"
            label = "Promocionar a live pequeno"
            reason = "Cumple dias, resoluciones, PnL y drawdown para pasar a capital pequeno."
            transition_ready = True
        else:
            recommendation = str(live.get("recommendation") or "keep_incubating")
            label = "Seguir en paper"
            reason = "Sigue en incubacion paper hasta cumplir el gate operativo."
    elif safe_stage == "live_small":
        if live_ready:
            next_stage = "scaled"
            recommendation = "promote_to_scaled"
            label = "Promocionar a escalada"
            reason = "La variante ya aguanto el tramo de capital pequeno y puede escalar."
            transition_ready = True
        else:
            recommendation = str(live.get("recommendation") or "keep_incubating")
            label = "Seguir en live pequeno"
            reason = "Mantener ticket pequeno hasta completar el gate de live_small."
    elif safe_stage == "scaled":
        recommendation = "keep_scaled"
        label = "Mantener escalada"
        reason = "La variante ya esta en produccion escalada; se monitoriza sin cambiar de etapa."

    return {
        "stage": safe_stage,
        "next_stage": next_stage,
        "transition_ready": transition_ready,
        "recommendation": recommendation,
        "label": label,
        "reason": reason,
        "auto_promote": bool(auto_promote),
        "auto_apply_ready": bool(auto_promote and transition_ready and next_stage != safe_stage),
    }


def _normalize_stage(stage: object) -> str:
    value = str(stage or "").strip().lower()
    if value in _STAGE_ORDER:
        return value
    return "disabled"


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
