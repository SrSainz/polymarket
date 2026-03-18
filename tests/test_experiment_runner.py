from __future__ import annotations

from pathlib import Path

from app.core.strategy_registry import IncubationProfile, StrategyRegistry, StrategyVariant
from app.services.experiment_runner import ExperimentRunner


def test_experiment_runner_persists_variant_leaderboard(tmp_path: Path) -> None:
    research_root = tmp_path / "research"
    registry = StrategyRegistry(
        variants={
            "arb-test": StrategyVariant(
                name="arb-test",
                entry_mode="arb_micro",
                runtime_handler="arb_micro",
                notes="test variant",
                research_overrides={"sub_strategy": "underround_arb"},
                incubation=IncubationProfile(stage="idea"),
            ),
            "mm-test": StrategyVariant(
                name="mm-test",
                entry_mode="vidarx_micro",
                runtime_handler="vidarx_micro",
                notes="maker variant",
                research_overrides={"sub_strategy": "market_making"},
                incubation=IncubationProfile(stage="idea"),
            ),
        }
    )
    sample_path = Path(__file__).resolve().parents[1] / "sample.json"
    counter = {"calls": 0}

    def _fake_backtest_runner(input_path, *, output_dir, config):  # noqa: ANN001
        counter["calls"] += 1
        output_dir.mkdir(parents=True, exist_ok=True)
        return {
            "net_realized_pnl_usdc": 5.0 if config.sub_strategy == "underround_arb" else 2.0,
            "max_drawdown_usdc": 1.5,
            "fill_rate": 0.8,
            "hit_rate": 0.55,
            "expectancy_window_usdc": 1.25,
            "real_edge_bps": 9.5,
            "windows": 4.0,
            "sent_orders": 6.0,
            "deployed_notional_usdc": 50.0,
        }

    payload = ExperimentRunner(research_root, registry, backtest_runner=_fake_backtest_runner).run(
        dataset_paths=[sample_path]
    )

    assert len(payload["variants"]) == 2
    assert "real_edge_bps" in payload["variants"][0]
    assert counter["calls"] == 2
    assert (research_root / "experiments" / "variant_leaderboard.json").exists()
