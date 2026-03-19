from __future__ import annotations

import json
import logging
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.lab_artifacts import dump_json, runtime_diagnostics_path
from app.db import Database
from app.settings import AppSettings

_CLOSE_ACTIONS = {"close", "reduce"}
_SEVERITY_SCORE = {"info": 0, "low": 1, "medium": 2, "high": 3}
_STATUS_FROM_SEVERITY = {0: "healthy", 1: "watch", 2: "degraded", 3: "critical"}


def evaluate_runtime_guard(
    executions: list[dict[str, Any]] | list[Any],
    *,
    now_ts: int | None = None,
    lookback_minutes: int = 180,
    loss_streak_limit: int = 3,
    max_recent_close_pnl: float = -35.0,
    cooldown_minutes: int = 45,
) -> dict[str, Any]:
    now_ts = int(now_ts or time.time())
    cutoff_ts = now_ts - max(int(lookback_minutes), 1) * 60
    closing_rows = [
        row
        for row in executions
        if int(_row_value(row, "ts", default=0)) >= cutoff_ts
        and str(_row_value(row, "action", default="")).strip().lower() in _CLOSE_ACTIONS
    ]
    recent_close_pnl = sum(float(_row_value(row, "pnl_delta", default=0.0)) for row in closing_rows)
    consecutive_losses = 0
    for row in closing_rows:
        pnl_delta = float(_row_value(row, "pnl_delta", default=0.0))
        if pnl_delta < 0:
            consecutive_losses += 1
            continue
        break
    blocked = bool(
        closing_rows
        and (
            consecutive_losses >= max(int(loss_streak_limit), 1)
            or recent_close_pnl <= float(max_recent_close_pnl)
        )
    )
    reason_parts: list[str] = []
    if consecutive_losses >= max(int(loss_streak_limit), 1):
        reason_parts.append(f"{consecutive_losses} cierres perdedores seguidos")
    if recent_close_pnl <= float(max_recent_close_pnl):
        reason_parts.append(f"PnL reciente {recent_close_pnl:.2f}")
    reason = ", ".join(reason_parts)
    return {
        "blocked": blocked,
        "recent_close_count": len(closing_rows),
        "recent_close_pnl": recent_close_pnl,
        "consecutive_losses": consecutive_losses,
        "cooldown_until": now_ts + max(int(cooldown_minutes), 1) * 60 if blocked else 0,
        "reason": reason,
    }


class RuntimeDiagnosticsService:
    def __init__(
        self,
        db: Database,
        research_dir: Path,
        settings: AppSettings,
        logger: logging.Logger | None = None,
    ) -> None:
        self.db = db
        self.research_dir = research_dir
        self.settings = settings
        self.logger = logger or logging.getLogger(__name__)

    def generate_if_due(self, *, force: bool = False) -> dict[str, Any] | None:
        if not self.settings.config.runtime_diagnostics_enabled:
            return None
        now_ts = int(time.time())
        last_generated_at = _safe_int(self.db.get_bot_state("runtime_diagnostics_updated_at"))
        interval_seconds = max(self.settings.config.runtime_diagnostics_interval_minutes, 1) * 60
        if not force and last_generated_at > 0 and (now_ts - last_generated_at) < interval_seconds:
            return None
        return self.generate(now_ts=now_ts)

    def generate(self, *, now_ts: int | None = None) -> dict[str, Any]:
        now_ts = int(now_ts or time.time())
        lookback_minutes = max(int(self.settings.config.runtime_diagnostics_lookback_minutes), 1)
        execution_limit = max(int(self.settings.config.runtime_diagnostics_execution_limit), 20)
        cutoff_ts = now_ts - lookback_minutes * 60
        execution_rows = self.db.get_recent_executions_since(cutoff_ts, limit=execution_limit)
        execution_metrics = _summarize_executions(execution_rows)
        decision_metrics = self._summarize_decisions(limit=self.settings.config.runtime_diagnostics_decision_limit)
        configured_handler = self._configured_runtime_handler()
        findings = self._build_findings(
            execution_metrics=execution_metrics,
            decision_metrics=decision_metrics,
            configured_handler=configured_handler,
        )
        severity_score = max((_SEVERITY_SCORE.get(item["severity"], 0) for item in findings), default=0)
        status = _STATUS_FROM_SEVERITY.get(severity_score, "healthy")
        summary = self._summary_line(
            status=status,
            execution_metrics=execution_metrics,
            decision_metrics=decision_metrics,
            findings=findings,
        )
        payload = {
            "generated_at": datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(),
            "status": status,
            "summary": summary,
            "lookback_minutes": lookback_minutes,
            "configured_strategy_mode": self.settings.config.strategy_mode,
            "configured_entry_mode": self.settings.config.strategy_entry_mode,
            "configured_variant": self.settings.config.strategy_variant,
            "configured_runtime_handler": configured_handler,
            "executions": execution_metrics,
            "decisions": decision_metrics,
            "findings": findings,
        }
        self._persist(payload=payload, now_ts=now_ts)
        return payload

    def _configured_runtime_handler(self) -> str:
        if self.settings.strategy_registry is None:
            return str(self.settings.config.strategy_entry_mode or "").strip()
        return self.settings.strategy_registry.resolve(
            self.settings.config.strategy_variant,
            entry_mode=self.settings.config.strategy_entry_mode,
        ).runtime_handler

    def _build_findings(
        self,
        *,
        execution_metrics: dict[str, Any],
        decision_metrics: dict[str, Any],
        configured_handler: str,
    ) -> list[dict[str, str]]:
        findings: list[dict[str, str]] = []
        close_count = int(execution_metrics["close_count"])
        close_net_pnl = float(execution_metrics["close_net_pnl"])
        negative_close_rate = float(execution_metrics["negative_close_rate"])
        flat_close_share = float(execution_metrics["flat_close_share"])
        stop_loss_share = float(execution_metrics["stop_loss_share"])
        loss_streak = int(execution_metrics["loss_streak"])
        stale_book_rate = float(decision_metrics["stale_book_rate"])
        dominant_strategy = str(decision_metrics["dominant_strategy"] or "")
        decision_count = int(decision_metrics["count"])

        if close_count >= 3 and (loss_streak >= 3 or close_net_pnl <= -50.0):
            findings.append(
                {
                    "severity": "high",
                    "title": "Reciente sangrado operativo",
                    "detail": (
                        f"Los cierres recientes suman {close_net_pnl:.2f} USDC con racha de "
                        f"{loss_streak} pérdidas seguidas."
                    ),
                    "recommendation": "Pausar entradas nuevas y revisar setup/timing antes de seguir abriendo.",
                }
            )
        elif close_count >= 3 and close_net_pnl < 0 and negative_close_rate >= 0.60:
            findings.append(
                {
                    "severity": "medium",
                    "title": "La señal reciente pierde más de lo que gana",
                    "detail": (
                        f"El {negative_close_rate * 100:.1f}% de los cierres recientes es negativo "
                        f"y el balance es {close_net_pnl:.2f} USDC."
                    ),
                    "recommendation": "Reducir tamaño y endurecer filtros de entrada hasta volver a edge positivo.",
                }
            )

        if close_count >= 3 and stop_loss_share >= 0.20:
            findings.append(
                {
                    "severity": "high",
                    "title": "Demasiadas salidas por stop-loss",
                    "detail": (
                        f"El {stop_loss_share * 100:.1f}% de los cierres recientes termina en stop-loss "
                        "o depreciación forzada."
                    ),
                    "recommendation": "No abras si la señal necesita rescates repetidos para salir.",
                }
            )

        if close_count >= 6 and flat_close_share >= 0.50:
            findings.append(
                {
                    "severity": "medium",
                    "title": "Se está cerrando demasiado plano",
                    "detail": (
                        f"El {flat_close_share * 100:.1f}% de los cierres recientes queda casi en break-even, "
                        "señal de edge insuficiente frente a fees y latencia."
                    ),
                    "recommendation": "Exigir edge más alto o no entrar cuando la ventana no compensa el coste real.",
                }
            )

        if decision_count >= 20 and stale_book_rate >= 0.35:
            findings.append(
                {
                    "severity": "medium",
                    "title": "El libro llega demasiado viejo",
                    "detail": (
                        f"El {stale_book_rate * 100:.1f}% de las decisiones recientes queda bloqueado por "
                        "staleness del orderbook."
                    ),
                    "recommendation": "Priorizar feed WebSocket y evitar decisiones con book envejecido.",
                }
            )

        if dominant_strategy and dominant_strategy != configured_handler:
            findings.append(
                {
                    "severity": "medium",
                    "title": "El runtime observado no coincide con la config",
                    "detail": (
                        f"Las decisiones recientes reportan `{dominant_strategy}` pero la configuración activa "
                        f"espera `{configured_handler}`."
                    ),
                    "recommendation": "Verificar que el proceso en producción se ha reiniciado con la rama y config correctas.",
                }
            )
        return findings

    def _summary_line(
        self,
        *,
        status: str,
        execution_metrics: dict[str, Any],
        decision_metrics: dict[str, Any],
        findings: list[dict[str, str]],
    ) -> str:
        close_count = int(execution_metrics["close_count"])
        close_net_pnl = float(execution_metrics["close_net_pnl"])
        stale_book_rate = float(decision_metrics["stale_book_rate"])
        dominant_strategy = str(decision_metrics["dominant_strategy"] or "-")
        if findings:
            return (
                f"Estado {status}: {len(findings)} hallazgos. "
                f"Cierres={close_count}, pnl_reciente={close_net_pnl:.2f}, "
                f"stale_book={stale_book_rate * 100:.1f}%, runtime={dominant_strategy}."
            )
        return (
            f"Estado {status}: sin hallazgos relevantes. "
            f"Cierres={close_count}, pnl_reciente={close_net_pnl:.2f}, "
            f"stale_book={stale_book_rate * 100:.1f}%."
        )

    def _persist(self, *, payload: dict[str, Any], now_ts: int) -> None:
        dump_json(runtime_diagnostics_path(self.research_dir), payload)
        markdown_lines = [
            f"# Runtime Diagnostics ({payload['generated_at']})",
            "",
            f"- Status: {payload['status']}",
            f"- Summary: {payload['summary']}",
            f"- Configured handler: {payload['configured_runtime_handler']}",
            f"- Recent close PnL: {float(payload['executions']['close_net_pnl']):.2f}",
            f"- Loss streak: {int(payload['executions']['loss_streak'])}",
            f"- Stale book rate: {float(payload['decisions']['stale_book_rate']) * 100:.2f}%",
            "",
            "## Findings",
        ]
        findings = payload.get("findings") if isinstance(payload.get("findings"), list) else []
        if not findings:
            markdown_lines.append("- None")
        else:
            for item in findings:
                markdown_lines.append(
                    f"- [{item.get('severity', 'info')}] {item.get('title', '-')}: "
                    f"{item.get('detail', '')} | accion={item.get('recommendation', '')}"
                )
        md_path = runtime_diagnostics_path(self.research_dir).with_suffix(".md")
        md_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")

        self.db.set_bot_state("runtime_diagnostics_updated_at", str(now_ts))
        self.db.set_bot_state("runtime_diagnostics_status", str(payload["status"]))
        self.db.set_bot_state("runtime_diagnostics_summary", str(payload["summary"]))
        self.db.set_bot_state("runtime_diagnostics_findings_count", str(len(findings)))
        self.db.set_bot_state(
            "runtime_diagnostics_recent_close_pnl",
            f"{float(payload['executions']['close_net_pnl']):.6f}",
        )
        self.db.set_bot_state(
            "runtime_diagnostics_loss_streak",
            str(int(payload["executions"]["loss_streak"])),
        )
        self.db.set_bot_state(
            "runtime_diagnostics_stale_book_rate",
            f"{float(payload['decisions']['stale_book_rate']):.6f}",
        )
        self.db.set_bot_state(
            "runtime_diagnostics_dominant_strategy",
            str(payload["decisions"]["dominant_strategy"] or ""),
        )
        self.db.set_bot_state(
            "runtime_diagnostics_last_finding",
            str(findings[0]["title"]) if findings else "",
        )

    def _summarize_decisions(self, *, limit: int) -> dict[str, Any]:
        path = self.research_dir / "paper_decisions.jsonl"
        if not path.exists():
            return {
                "count": 0,
                "should_trade_count": 0,
                "stale_book_count": 0,
                "stale_book_rate": 0.0,
                "dominant_strategy": "",
                "top_reasons": [],
            }
        recent_lines = deque(maxlen=max(int(limit), 1))
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        recent_lines.append(line)
        except OSError as error:
            self.logger.warning("runtime diagnostics: unable to read %s: %s", path, error)
            return {
                "count": 0,
                "should_trade_count": 0,
                "stale_book_count": 0,
                "stale_book_rate": 0.0,
                "dominant_strategy": "",
                "top_reasons": [],
            }

        strategy_counter: Counter[str] = Counter()
        reason_counter: Counter[str] = Counter()
        should_trade_count = 0
        stale_book_count = 0
        total = 0
        for raw_line in recent_lines:
            try:
                payload = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            decision = payload.get("decision") if isinstance(payload, dict) else {}
            if not isinstance(decision, dict):
                continue
            total += 1
            strategy_name = str(decision.get("strategy_name") or "").strip()
            if strategy_name:
                strategy_counter[strategy_name] += 1
            reason = str(decision.get("reason") or "").strip()
            if reason:
                reason_counter[reason] += 1
            if bool(decision.get("should_trade")):
                should_trade_count += 1
            if "book_age_ms" in reason:
                stale_book_count += 1
        dominant_strategy = strategy_counter.most_common(1)[0][0] if strategy_counter else ""
        return {
            "count": total,
            "should_trade_count": should_trade_count,
            "stale_book_count": stale_book_count,
            "stale_book_rate": (stale_book_count / total) if total > 0 else 0.0,
            "dominant_strategy": dominant_strategy,
            "top_reasons": [{"reason": reason, "count": count} for reason, count in reason_counter.most_common(5)],
        }


def _summarize_executions(rows: list[Any]) -> dict[str, Any]:
    close_rows = [
        row
        for row in rows
        if str(_row_value(row, "action", default="")).strip().lower() in _CLOSE_ACTIONS
    ]
    close_pnls = [float(_row_value(row, "pnl_delta", default=0.0)) for row in close_rows]
    negative_close_count = sum(1 for pnl in close_pnls if pnl < 0)
    positive_close_count = sum(1 for pnl in close_pnls if pnl > 0)
    flat_close_count = sum(1 for pnl in close_pnls if abs(pnl) <= 0.25)
    stop_loss_count = 0
    depreciation_count = 0
    loss_streak = 0
    for row in close_rows:
        pnl_delta = float(_row_value(row, "pnl_delta", default=0.0))
        note = str(_row_value(row, "notes", default="")).strip().lower()
        if "stop_loss" in note:
            stop_loss_count += 1
        if "depreciation" in note:
            depreciation_count += 1
        if pnl_delta < 0:
            loss_streak += 1
            continue
        break
    close_count = len(close_rows)
    total_notional = sum(float(_row_value(row, "notional", default=0.0)) for row in rows)
    return {
        "count": len(rows),
        "close_count": close_count,
        "close_net_pnl": sum(close_pnls),
        "negative_close_count": negative_close_count,
        "positive_close_count": positive_close_count,
        "flat_close_count": flat_close_count,
        "negative_close_rate": (negative_close_count / close_count) if close_count > 0 else 0.0,
        "flat_close_share": (flat_close_count / close_count) if close_count > 0 else 0.0,
        "stop_loss_count": stop_loss_count,
        "depreciation_count": depreciation_count,
        "stop_loss_share": ((stop_loss_count + depreciation_count) / close_count) if close_count > 0 else 0.0,
        "loss_streak": loss_streak,
        "total_notional": total_notional,
    }


def _row_value(row: Any, key: str, *, default: Any) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return getattr(row, key, default)


def _safe_int(raw: str | None) -> int:
    try:
        return int(str(raw or "").strip())
    except (TypeError, ValueError):
        return 0
