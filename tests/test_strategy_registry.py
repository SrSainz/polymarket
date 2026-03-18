from __future__ import annotations

from pathlib import Path

from app.settings import load_settings


def test_load_settings_applies_strategy_registry_variant(tmp_path: Path) -> None:
    root = tmp_path
    config_dir = root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text(
        """
strategy_variant: "arb-test"
strategy_entry_mode: "buy_opposite"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (config_dir / "strategy_registry.yaml").write_text(
        """
variants:
  arb-test:
    entry_mode: arb_micro
    runtime_handler: arb_micro
    notes: "registry-controlled variant"
    overrides:
      strategy_trade_allocation_pct: 0.07
    incubation:
      stage: backtest_pass
      min_days: 3
      min_resolutions: 5
      max_drawdown: 12.0
      min_backtest_pnl: 1.5
      min_backtest_fill_rate: 0.4
      min_backtest_hit_rate: 0.5
      min_backtest_edge_bps: 8.0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    settings = load_settings(root)

    assert settings.config.strategy_variant == "arb-test"
    assert settings.config.strategy_entry_mode == "arb_micro"
    assert settings.config.strategy_notes == "registry-controlled variant"
    assert settings.config.strategy_trade_allocation_pct == 0.07
    assert settings.config.incubation_stage == "backtest_pass"
    assert settings.config.incubation_min_days == 3
    assert settings.config.incubation_min_resolutions == 5
    assert settings.config.incubation_max_drawdown == 12.0
    assert settings.config.incubation_min_backtest_pnl == 1.5
    assert settings.config.incubation_min_backtest_fill_rate == 0.4
    assert settings.config.incubation_min_backtest_hit_rate == 0.5
    assert settings.config.incubation_min_backtest_edge_bps == 8.0
    assert settings.strategy_registry is not None
    assert settings.strategy_registry.variant_count() == 1
