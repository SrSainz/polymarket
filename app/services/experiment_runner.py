from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from strategy import ResearchConfig

from app.core.lab_artifacts import dataset_windows_dir, dump_json, experiment_leaderboard_path
from app.core.strategy_registry import StrategyRegistry, StrategyVariant


class ExperimentRunner:
    def __init__(
        self,
        research_root: Path,
        strategy_registry: StrategyRegistry,
        *,
        backtest_runner=None,
    ) -> None:
        self.research_root = research_root
        self.strategy_registry = strategy_registry
        self.backtest_runner = backtest_runner

    def run(self, *, dataset_paths: list[Path] | None = None, fallback_inputs: list[Path] | None = None) -> dict[str, Any]:
        inputs = list(dataset_paths or self._default_inputs())
        if not inputs and fallback_inputs:
            inputs = [Path(item) for item in fallback_inputs if Path(item).exists()]
        variants = self.strategy_registry.enabled_variants()
        if not inputs or not variants:
            payload = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "datasets": [str(path) for path in inputs],
                "variants": [],
            }
            dump_json(experiment_leaderboard_path(self.research_root), payload)
            return payload

        max_workers = max(min(len(variants), 4), 1)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            rows = list(executor.map(lambda variant: self._run_variant(variant, inputs), variants))

        ranked = sorted(rows, key=lambda item: (float(item["score"]), float(item["net_realized_pnl_usdc"])), reverse=True)
        for index, row in enumerate(ranked, start=1):
            row["rank"] = index
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "datasets": [str(path) for path in inputs],
            "variants": ranked,
        }
        dump_json(experiment_leaderboard_path(self.research_root), payload)
        return payload

    def _default_inputs(self) -> list[Path]:
        windows_dir = dataset_windows_dir(self.research_root)
        if not windows_dir.exists():
            return []
        return sorted(path for path in windows_dir.glob("*.json") if path.is_file())

    def _run_variant(self, variant: StrategyVariant, dataset_paths: list[Path]) -> dict[str, Any]:
        runner = self._backtest_runner()
        dataset_rows: list[dict[str, Any]] = []
        total_pnl = 0.0
        total_windows = 0.0
        total_sent_orders = 0.0
        total_deployed_notional = 0.0
        max_drawdown = 0.0
        fill_rates: list[float] = []
        hit_rates: list[float] = []
        expectancy_rows: list[float] = []

        for dataset_path in dataset_paths:
            config = self._research_config_for_variant(variant)
            output_dir = self.research_root / "experiments" / variant.name / dataset_path.stem
            kpis = runner(dataset_path, output_dir=output_dir, config=config)
            total_pnl += float(kpis.get("net_realized_pnl_usdc", 0.0))
            total_windows += float(kpis.get("windows", 0.0))
            total_sent_orders += float(kpis.get("sent_orders", 0.0))
            total_deployed_notional += float(kpis.get("deployed_notional_usdc", 0.0))
            max_drawdown = max(max_drawdown, float(kpis.get("max_drawdown_usdc", 0.0)))
            fill_rates.append(float(kpis.get("fill_rate", 0.0)))
            hit_rates.append(float(kpis.get("hit_rate", 0.0)))
            expectancy_rows.append(float(kpis.get("expectancy_window_usdc", 0.0)))
            dataset_rows.append(
                {
                    "dataset": dataset_path.stem,
                    "net_realized_pnl_usdc": float(kpis.get("net_realized_pnl_usdc", 0.0)),
                    "max_drawdown_usdc": float(kpis.get("max_drawdown_usdc", 0.0)),
                    "fill_rate": float(kpis.get("fill_rate", 0.0)),
                    "hit_rate": float(kpis.get("hit_rate", 0.0)),
                    "real_edge_bps": float(kpis.get("real_edge_bps", 0.0)),
                    "windows": float(kpis.get("windows", 0.0)),
                }
            )

        avg_fill_rate = _mean(fill_rates)
        avg_hit_rate = _mean(hit_rates)
        avg_expectancy_window = _mean(expectancy_rows)
        real_edge_bps = (total_pnl / total_deployed_notional * 10_000) if total_deployed_notional > 0 else 0.0
        gate_passed = (
            total_pnl >= float(variant.incubation.min_backtest_pnl)
            and avg_fill_rate >= float(variant.incubation.min_backtest_fill_rate)
            and avg_hit_rate >= float(variant.incubation.min_backtest_hit_rate)
            and real_edge_bps >= float(variant.incubation.min_backtest_edge_bps)
            and max_drawdown <= float(variant.incubation.max_drawdown)
        )
        score = (
            total_pnl
            + (avg_expectancy_window * 2.0)
            + (avg_fill_rate * 25.0)
            + (avg_hit_rate * 20.0)
            + (real_edge_bps * 0.05)
            - (max_drawdown * 0.30)
        )
        return {
            "variant": variant.name,
            "entry_mode": variant.entry_mode,
            "runtime_handler": variant.runtime_handler,
            "notes": variant.notes,
            "thesis": variant.thesis,
            "datasets": len(dataset_rows),
            "windows": total_windows,
            "sent_orders": total_sent_orders,
            "deployed_notional_usdc": round(total_deployed_notional, 4),
            "net_realized_pnl_usdc": round(total_pnl, 4),
            "max_drawdown_usdc": round(max_drawdown, 4),
            "fill_rate": round(avg_fill_rate, 4),
            "hit_rate": round(avg_hit_rate, 4),
            "expectancy_window_usdc": round(avg_expectancy_window, 4),
            "real_edge_bps": round(real_edge_bps, 4),
            "gate_passed": gate_passed,
            "status": "pass" if gate_passed else "fail",
            "score": round(score, 4),
            "dataset_rows": dataset_rows,
        }

    def _research_config_for_variant(self, variant: StrategyVariant) -> ResearchConfig:
        config = deepcopy(ResearchConfig())
        _apply_overrides(config, variant.research_overrides)
        return config

    def _backtest_runner(self):
        if self.backtest_runner is not None:
            return self.backtest_runner
        from backtest import run_backtest

        return run_backtest


def _apply_overrides(target: Any, overrides: dict[str, Any]) -> None:
    for key, value in overrides.items():
        if not hasattr(target, key):
            continue
        current = getattr(target, key)
        if isinstance(value, dict) and hasattr(current, "__dataclass_fields__"):
            _apply_overrides(current, value)
            continue
        setattr(target, key, deepcopy(value))


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)
