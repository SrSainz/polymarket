from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml


_DEFAULT_RUNTIME_HANDLERS = {
    "buy_above": "classic",
    "buy_opposite": "classic",
    "vidarx_micro": "vidarx_micro",
    "arb_micro": "arb_micro",
}


@dataclass(frozen=True)
class IncubationProfile:
    stage: str = "idea"
    auto_promote: bool = False
    min_days: int = 14
    min_resolutions: int = 20
    max_drawdown: float = 50.0
    min_backtest_pnl: float = 0.0
    min_backtest_fill_rate: float = 0.20
    min_backtest_hit_rate: float = 0.35
    min_backtest_edge_bps: float = 0.0


@dataclass(frozen=True)
class StrategyVariant:
    name: str
    entry_mode: str
    runtime_handler: str
    enabled: bool = True
    notes: str = ""
    thesis: str = ""
    tags: tuple[str, ...] = ()
    overrides: dict[str, Any] = field(default_factory=dict)
    research_overrides: dict[str, Any] = field(default_factory=dict)
    incubation: IncubationProfile = field(default_factory=IncubationProfile)


@dataclass(frozen=True)
class StrategyRegistry:
    variants: dict[str, StrategyVariant] = field(default_factory=dict)

    def get(self, variant_name: str) -> StrategyVariant | None:
        return self.variants.get(_normalize_variant_name(variant_name))

    def enabled_variants(self) -> list[StrategyVariant]:
        return [variant for variant in self.variants.values() if variant.enabled]

    def resolve(self, variant_name: str, *, entry_mode: str = "") -> StrategyVariant:
        variant = self.get(variant_name)
        if variant is not None:
            return variant
        safe_entry_mode = _normalize_entry_mode(entry_mode)
        runtime_handler = _DEFAULT_RUNTIME_HANDLERS.get(safe_entry_mode, "classic")
        return StrategyVariant(
            name=_normalize_variant_name(variant_name) or "default",
            entry_mode=safe_entry_mode,
            runtime_handler=runtime_handler,
        )

    def variant_count(self) -> int:
        return len(self.variants)


def load_strategy_registry(path: Path) -> StrategyRegistry:
    if not path.exists():
        return StrategyRegistry()
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    variants_payload = payload.get("variants") if isinstance(payload, dict) else {}
    defaults_payload = payload.get("defaults") if isinstance(payload, dict) else {}
    if not isinstance(defaults_payload, dict):
        defaults_payload = {}
    defaults_incubation = _build_incubation_profile(defaults_payload.get("incubation") or {})
    variants: dict[str, StrategyVariant] = {}
    if not isinstance(variants_payload, dict):
        return StrategyRegistry()
    for raw_name, raw_variant in variants_payload.items():
        if not isinstance(raw_variant, dict):
            continue
        variant = _build_variant(
            raw_name,
            raw_variant,
            default_incubation=defaults_incubation,
        )
        variants[variant.name] = variant
    return StrategyRegistry(variants=variants)


def apply_variant_overrides(raw_config: Mapping[str, Any], registry: StrategyRegistry) -> dict[str, Any]:
    merged = deepcopy(dict(raw_config or {}))
    variant_name = _normalize_variant_name(merged.get("strategy_variant"))
    if not variant_name:
        return merged
    variant = registry.get(variant_name)
    if variant is None:
        return merged

    merged["strategy_variant"] = variant.name
    if variant.entry_mode:
        merged["strategy_entry_mode"] = variant.entry_mode
    if variant.notes:
        merged["strategy_notes"] = variant.notes
    merged["incubation_stage"] = variant.incubation.stage
    merged["incubation_auto_promote"] = variant.incubation.auto_promote
    merged["incubation_min_days"] = variant.incubation.min_days
    merged["incubation_min_resolutions"] = variant.incubation.min_resolutions
    merged["incubation_max_drawdown"] = variant.incubation.max_drawdown
    merged["incubation_min_backtest_pnl"] = variant.incubation.min_backtest_pnl
    merged["incubation_min_backtest_fill_rate"] = variant.incubation.min_backtest_fill_rate
    merged["incubation_min_backtest_hit_rate"] = variant.incubation.min_backtest_hit_rate
    merged["incubation_min_backtest_edge_bps"] = variant.incubation.min_backtest_edge_bps
    for key, value in variant.overrides.items():
        merged[key] = deepcopy(value)
    return merged


def active_variant_metadata(registry: StrategyRegistry, *, variant_name: str, entry_mode: str) -> dict[str, str]:
    variant = registry.resolve(variant_name, entry_mode=entry_mode)
    return {
        "strategy_runtime_handler": variant.runtime_handler,
        "strategy_variant_thesis": variant.thesis,
        "strategy_variant_tags": ",".join(variant.tags),
    }


def _build_variant(
    raw_name: object,
    payload: Mapping[str, Any],
    *,
    default_incubation: IncubationProfile,
) -> StrategyVariant:
    name = _normalize_variant_name(raw_name) or "default"
    entry_mode = _normalize_entry_mode(payload.get("entry_mode"))
    runtime_handler = str(payload.get("runtime_handler") or _DEFAULT_RUNTIME_HANDLERS.get(entry_mode, "classic")).strip()
    incubation = _build_incubation_profile(
        payload.get("incubation") or {},
        defaults=default_incubation,
    )
    raw_tags = payload.get("tags") or []
    tags = tuple(str(item).strip() for item in raw_tags if str(item).strip()) if isinstance(raw_tags, list) else ()
    overrides = payload.get("overrides") or {}
    research_overrides = payload.get("research_overrides") or {}
    return StrategyVariant(
        name=name,
        entry_mode=entry_mode,
        runtime_handler=runtime_handler or "classic",
        enabled=bool(payload.get("enabled", True)),
        notes=str(payload.get("notes") or "").strip(),
        thesis=str(payload.get("thesis") or "").strip(),
        tags=tags,
        overrides=dict(overrides) if isinstance(overrides, dict) else {},
        research_overrides=dict(research_overrides) if isinstance(research_overrides, dict) else {},
        incubation=incubation,
    )


def _build_incubation_profile(
    payload: Mapping[str, Any],
    *,
    defaults: IncubationProfile | None = None,
) -> IncubationProfile:
    base = defaults or IncubationProfile()
    if not isinstance(payload, Mapping):
        return base
    return IncubationProfile(
        stage=str(payload.get("stage") or base.stage).strip() or base.stage,
        auto_promote=bool(payload.get("auto_promote", base.auto_promote)),
        min_days=_safe_int(payload.get("min_days"), default=base.min_days),
        min_resolutions=_safe_int(payload.get("min_resolutions"), default=base.min_resolutions),
        max_drawdown=_safe_float(payload.get("max_drawdown"), default=base.max_drawdown),
        min_backtest_pnl=_safe_float(payload.get("min_backtest_pnl"), default=base.min_backtest_pnl),
        min_backtest_fill_rate=_safe_float(
            payload.get("min_backtest_fill_rate"),
            default=base.min_backtest_fill_rate,
        ),
        min_backtest_hit_rate=_safe_float(
            payload.get("min_backtest_hit_rate"),
            default=base.min_backtest_hit_rate,
        ),
        min_backtest_edge_bps=_safe_float(
            payload.get("min_backtest_edge_bps"),
            default=base.min_backtest_edge_bps,
        ),
    )


def _normalize_variant_name(value: object) -> str:
    return str(value or "").strip().lower()


def _normalize_entry_mode(value: object) -> str:
    return str(value or "").strip()


def _safe_int(value: object, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
