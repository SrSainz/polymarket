from __future__ import annotations

"""Evidence-backed sports probability adapters.

This module does not calculate sports forecasts. It only loads forecasts that
were produced and calibrated elsewhere, then binds them to an exact market
identity. Missing or malformed evidence becomes an explicit abstention.
"""

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any

from app.advisor.models import MarketQuote, ModelEvidence


@dataclass(frozen=True)
class JsonProbabilityModel:
    """Load versioned, externally calibrated evidence from a JSON artifact."""

    path: Path

    def estimate(self, quote: MarketQuote, *, now: float) -> ModelEvidence:
        del now  # Evidence age is checked centrally by OpportunityEvaluator.
        payload = self._read()
        if payload is None:
            return _abstention(quote, "evidence_artifact_unavailable")

        shared = payload.get("metadata", {}) if isinstance(payload, dict) else {}
        if not isinstance(shared, dict):
            shared = {}
        rows = payload.get("predictions") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            return _abstention(quote, "evidence_predictions_invalid")

        matches = [
            row for row in rows
            if isinstance(row, dict) and _identity(row) == _identity_from_quote(quote)
        ]
        if len(matches) != 1:
            reason = "evidence_missing" if not matches else "evidence_duplicate"
            return _abstention(quote, reason)

        row = matches[0]
        merged = {**shared, **row}
        evidence = _evidence_from_row(quote, merged)
        return evidence or _abstention(quote, "evidence_boolean_invalid")

    def _read(self) -> dict[str, Any] | list[Any] | None:
        if not self.path or not self.path.is_file():
            return None
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, (dict, list)) else None


def _evidence_from_row(quote: MarketQuote, row: dict[str, Any]) -> ModelEvidence | None:
    calibrated = row.get("calibrated")
    independent = row.get("independent")
    if type(calibrated) is not bool or type(independent) is not bool:
        return None
    probability = _float_or_nan(row.get("probability"))
    lower = _float_or_nan(row.get("lower_probability"))
    upper = _float_or_nan(row.get("upper_probability"))
    as_of = _float_or_zero(row.get("as_of"))
    brier = _optional_float(row.get("brier_score"))
    refs = row.get("source_refs", ())
    source_refs = tuple(str(value).strip() for value in refs if str(value).strip()) if isinstance(refs, (list, tuple)) else ()
    return ModelEvidence(
        model_name=str(row.get("model_name") or "").strip(),
        model_version=str(row.get("model_version") or "").strip(),
        probability=probability,
        lower_probability=lower,
        upper_probability=upper,
        calibrated=calibrated,
        sample_size=_int_or_zero(row.get("sample_size")),
        brier_score=brier,
        as_of=as_of,
        source_refs=source_refs,
        independent=independent,
        market_id=quote.market_id,
        condition_id=quote.condition_id,
        token_id=quote.token_id,
        outcome=quote.outcome,
    )


def _abstention(quote: MarketQuote, reason: str) -> ModelEvidence:
    return ModelEvidence(
        model_name="",
        model_version="",
        probability=0.5,
        lower_probability=0.5,
        upper_probability=0.5,
        calibrated=False,
        sample_size=0,
        brier_score=None,
        as_of=0.0,
        source_refs=(reason,),
        independent=False,
        market_id=quote.market_id,
        condition_id=quote.condition_id,
        token_id=quote.token_id,
        outcome=quote.outcome,
    )


def _identity(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return tuple(
        str(row.get(key) or "").strip()
        for key in ("market_id", "condition_id", "token_id", "outcome")
    )  # type: ignore[return-value]


def _identity_from_quote(quote: MarketQuote) -> tuple[str, str, str, str]:
    return quote.market_id, quote.condition_id, quote.token_id, quote.outcome


def _float_or_nan(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _float_or_zero(value: object) -> float:
    parsed = _float_or_nan(value)
    return parsed if math.isfinite(parsed) else 0.0


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    parsed = _float_or_nan(value)
    return parsed if math.isfinite(parsed) else float("nan")


def _int_or_zero(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed >= 0 else 0
