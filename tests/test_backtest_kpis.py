from pathlib import Path

from backtest import run_backtest, run_stress_tests, run_temporal_kfold, run_walk_forward
from strategy import ResearchConfig


def test_backtest_generates_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    kpis = run_backtest("sample.json", output_dir=output_dir, config=ResearchConfig())

    assert "expectancy_trade_usdc" in kpis
    assert "fill_rate" in kpis
    assert (output_dir / "kpis.csv").exists()
    assert (output_dir / "equity_curve.csv").exists()
    assert (output_dir / "backtest_log.jsonl").exists()


def test_validation_and_stress_exports(tmp_path: Path) -> None:
    config = ResearchConfig()
    walk_rows = run_walk_forward(
        "sample.json",
        output_dir=tmp_path / "walk",
        config=config,
        train_windows=3,
        test_windows=1,
    )
    kfold_rows = run_temporal_kfold(
        "sample.json",
        output_dir=tmp_path / "kfold",
        config=config,
        folds=4,
    )
    stress_rows = run_stress_tests(
        "sample.json",
        output_dir=tmp_path / "stress",
        config=config,
    )

    assert walk_rows
    assert kfold_rows
    assert any(row["scenario"] == "slippage_x2" for row in stress_rows)
    assert (tmp_path / "walk" / "walk_forward_folds.csv").exists()
    assert (tmp_path / "kfold" / "temporal_kfold_folds.csv").exists()
    assert (tmp_path / "stress" / "stress_summary.csv").exists()
