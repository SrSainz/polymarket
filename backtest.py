from __future__ import annotations

import argparse
import copy
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from strategy import (
    MarketDiscovery,
    ResearchConfig,
    StrategyState,
    attach_latency_record,
    compute_signal,
    mark_state_to_market,
    place_orders_paper,
    update_books_from_event,
)


WINDOW_MS = 5 * 60 * 1000


@dataclass(slots=True)
class ReplayBundle:
    meta: dict[str, Any]
    events: list[dict[str, Any]]
    trades: list[dict[str, Any]]


@dataclass(slots=True)
class WindowResult:
    window_id: int
    started_ts_ms: int
    ended_ts_ms: int
    pnl_usdc: float
    fills: int
    sent_orders: int
    canceled_orders: int
    deployed_notional: float


@dataclass(slots=True)
class ValidationFoldResult:
    method: str
    fold_index: int
    train_start_window: int
    train_end_window: int
    test_start_window: int
    test_end_window: int
    selected_strategy: str
    selected_execution: str
    selected_max_usdc_per_trade: float
    expectancy_window_usdc: float
    net_realized_pnl_usdc: float
    hit_rate: float
    fill_rate: float
    max_drawdown_usdc: float


class BacktestEngine:
    def __init__(
        self,
        bundle: ReplayBundle,
        config: ResearchConfig,
        output_dir: Path | None,
        *,
        scenario_name: str = "baseline",
    ) -> None:
        self.bundle = bundle
        self.config = config
        self.output_dir = output_dir
        self.scenario_name = scenario_name
        self.discovery = MarketDiscovery(
            market_id=str(bundle.meta.get("market_id") or "sample-market"),
            slug=str(bundle.meta.get("slug") or "sample-btc-5m"),
            title=str(bundle.meta.get("title") or "Sample BTC Up/Down 5m"),
            token_id_yes=str(bundle.meta.get("token_yes") or config.token_id_yes or "TOKEN_YES"),
            token_id_no=str(bundle.meta.get("token_no") or config.token_id_no or "TOKEN_NO"),
            yes_outcome="YES",
            no_outcome="NO",
            fees_enabled=bool(bundle.meta.get("fees_enabled", True)),
            fee_rate_bps_yes=float(bundle.meta.get("fee_rate_bps", config.fee_model.default_taker_fee_bps)),
            fee_rate_bps_no=float(bundle.meta.get("fee_rate_bps", config.fee_model.default_taker_fee_bps)),
            condition_id=str(bundle.meta.get("condition_id") or ""),
            end_date_iso=str(bundle.meta.get("end_date_iso") or ""),
            market_payload=dict(bundle.meta),
        )
        self.state = StrategyState(
            token_id_yes=self.discovery.token_id_yes,
            token_id_no=self.discovery.token_id_no,
            cash_usdc=10_000.0,
            equity_usdc=10_000.0,
            peak_equity_usdc=10_000.0,
        )
        self.structured_logs: list[dict[str, Any]] = []
        self.equity_rows: list[dict[str, Any]] = []
        self.window_results: list[WindowResult] = []
        self._window_start_equity = self.state.equity_usdc
        self._window_start_ts = 0
        self._window_sent_orders = 0
        self._window_canceled_orders = 0
        self._window_fills = 0
        self._window_deployed_notional = 0.0
        self._spread_samples: list[float] = []
        self._depth_samples: list[float] = []

    def run(self, *, export_outputs: bool = True) -> dict[str, float]:
        events = sorted(self.bundle.events, key=lambda row: int(row.get("ts_ms", 0)))
        if not events:
            raise RuntimeError("No hay eventos para reproducir.")
        self._window_start_ts = int(events[0]["ts_ms"])
        self.state.last_window_id = self._window_id(self._window_start_ts)

        for event in events:
            self._process_event(event)
        self._settle_window(int(events[-1]["ts_ms"]) + 1, force_close=True)
        kpis = self._compute_kpis()
        if export_outputs and self.output_dir is not None:
            self._export_outputs(kpis)
        return kpis

    def _process_event(self, event: dict[str, Any]) -> None:
        event_ts = int(event["ts_ms"])
        window_id = self._window_id(event_ts)
        if self.state.last_window_id is None:
            self.state.last_window_id = window_id
        elif window_id != self.state.last_window_id:
            self._settle_window(event_ts, force_close=True)
            self.state.last_window_id = window_id
            self._window_start_ts = event_ts
            self._window_start_equity = self.state.equity_usdc
            self._window_sent_orders = 0
            self._window_canceled_orders = 0
            self._window_fills = 0
            self._window_deployed_notional = 0.0

        update_books_from_event(self.state.books, event)
        self._capture_book_metrics()
        if event.get("event") == "trade":
            extra = event.get("extra") or {}
            self.state.trade_flow.append(
                (
                    event_ts,
                    float(extra.get("price") or 0.0),
                    float(extra.get("size") or 0.0),
                )
            )

        feed_recv_ts = event_ts
        normalize_ts = event_ts + 1
        decision = compute_signal(self.state, self.discovery, self.config, now_ts_ms=normalize_ts)
        decision_ts = normalize_ts + 1
        self.structured_logs.append(
            {
                "kind": "decision",
                "ts_ms": decision_ts,
                "window_id": window_id,
                "scenario": self.scenario_name,
                "decision": {
                    "strategy_name": decision.strategy_name,
                    "should_trade": decision.should_trade,
                    "reason": decision.reason,
                    "signal_edge_frac": decision.signal_edge_frac,
                    "signal_edge_usdc": decision.signal_edge_usdc,
                    "metrics": decision.metrics,
                },
            }
        )

        if decision.should_trade and decision.orders:
            order_ts = decision_ts + self.config.latency_budget_ms
            fills = place_orders_paper(self.state, decision.orders, self.discovery, self.config, now_ts_ms=order_ts)
            fill_ts = order_ts + 1
            persisted_ts = fill_ts + 1
            attach_latency_record(
                self.state,
                feed_recv_ts=feed_recv_ts,
                normalize_ts=normalize_ts,
                decision_ts=decision_ts,
                order_sent_ts=order_ts,
                fill_ts=fill_ts,
                persisted_ts=persisted_ts,
            )
            self._window_sent_orders += len(decision.orders)
            self._window_canceled_orders += sum(1 for fill in fills if fill.status == "missed")
            self._window_fills += sum(1 for fill in fills if fill.filled_size > 0)
            self._window_deployed_notional += sum(fill.notional for fill in fills)
            for fill in fills:
                self.structured_logs.append(
                    {
                        "kind": "fill",
                        "ts_ms": persisted_ts,
                        "window_id": window_id,
                        "scenario": self.scenario_name,
                        "fill": fill.to_log(),
                    }
                )

        equity = mark_state_to_market(self.state, self.state.books)
        self.equity_rows.append(
            {
                "ts_ms": event_ts,
                "window_id": window_id,
                "scenario": self.scenario_name,
                "cash_usdc": self.state.cash_usdc,
                "realized_pnl_usdc": self.state.realized_pnl_usdc,
                "equity_usdc": equity,
                "inventory_usdc": self.state.inventory_usdc(),
            }
        )

    def _capture_book_metrics(self) -> None:
        yes = self.state.books.get(self.discovery.token_id_yes)
        no = self.state.books.get(self.discovery.token_id_no)
        if yes and no and yes.spread() is not None and no.spread() is not None:
            self._spread_samples.append(float(yes.spread() or 0.0) + float(no.spread() or 0.0))
            self._depth_samples.append(yes.total_depth("asks", 3) + no.total_depth("asks", 3))

    def _settle_window(self, ended_ts_ms: int, *, force_close: bool) -> None:
        if self.state.last_window_id is None:
            return
        if force_close:
            self._flatten_inventory_to_midpoint()
        equity = mark_state_to_market(self.state, self.state.books)
        self.window_results.append(
            WindowResult(
                window_id=self.state.last_window_id,
                started_ts_ms=self._window_start_ts,
                ended_ts_ms=ended_ts_ms,
                pnl_usdc=equity - self._window_start_equity,
                fills=self._window_fills,
                sent_orders=self._window_sent_orders,
                canceled_orders=self._window_canceled_orders,
                deployed_notional=self._window_deployed_notional,
            )
        )
        self._window_start_equity = equity

    def _flatten_inventory_to_midpoint(self) -> None:
        for token_id, shares in list(self.state.inventory_shares.items()):
            if abs(shares) <= 1e-9:
                continue
            book = self.state.books.get(token_id)
            if book is None:
                continue
            exit_price = book.midpoint() or book.best_bid or book.best_ask or self.state.last_mark_price.get(token_id, 0.0)
            avg_cost = self.state.inventory_cost.get(token_id, 0.0) / max(shares, 1e-9)
            realized = (exit_price - avg_cost) * shares
            self.state.cash_usdc += shares * exit_price
            self.state.realized_pnl_usdc += realized
            self.state.inventory_shares[token_id] = 0.0
            self.state.inventory_cost[token_id] = 0.0
            self.state.last_mark_price[token_id] = exit_price

    def _compute_kpis(self) -> dict[str, float]:
        equity_df = pd.DataFrame(self.equity_rows)
        window_df = pd.DataFrame([asdict(item) for item in self.window_results])
        if equity_df.empty or window_df.empty:
            raise RuntimeError("No hay datos suficientes para calcular KPIs.")

        trade_expectancy = self.state.realized_pnl_usdc / max(self.state.trade_count, 1)
        window_expectancy = float(window_df["pnl_usdc"].mean())
        hit_rate = float((window_df["pnl_usdc"] > 0).mean())
        returns = window_df["pnl_usdc"].to_numpy(dtype=float)
        sharpe_window = _annualized_sharpe(returns, periods_per_year=365 * 24 * 12)
        sortino_window = _annualized_sortino(returns, periods_per_year=365 * 24 * 12)

        equity_series = equity_df["equity_usdc"].to_numpy(dtype=float)
        peaks = np.maximum.accumulate(equity_series)
        drawdowns = peaks - equity_series
        max_drawdown = float(drawdowns.max(initial=0.0))
        time_to_recover = _time_to_recover_seconds(equity_df["ts_ms"].to_numpy(dtype=float), equity_series)

        fill_rate = self.state.filled_orders / max(self.state.sent_orders, 1)
        cancel_rate = self.state.canceled_orders / max(self.state.sent_orders, 1)
        avg_slippage_usdc = self.state.cumulative_slippage_usdc / max(self.state.fill_count, 1)
        avg_slippage_bps = 0.0
        if self.state.fill_count:
            fill_rows = [row["fill"] for row in self.structured_logs if row["kind"] == "fill"]
            if fill_rows:
                avg_slippage_bps = float(np.mean([float(item["slippage_bps"]) for item in fill_rows]))

        latency_e2e = [
            item["persisted_ts"] - item["feed_recv_ts"]
            for item in self.state.latency_records
            if item["persisted_ts"] >= item["feed_recv_ts"]
        ]
        avg_latency_e2e_ms = float(np.mean(latency_e2e)) if latency_e2e else 0.0
        avg_spread = float(np.mean(self._spread_samples)) if self._spread_samples else 0.0
        avg_depth = float(np.mean(self._depth_samples)) if self._depth_samples else 0.0

        return {
            "expectancy_trade_usdc": float(trade_expectancy),
            "expectancy_window_usdc": window_expectancy,
            "sharpe_window_annualized": sharpe_window,
            "sortino_window_annualized": sortino_window,
            "hit_rate": hit_rate,
            "max_drawdown_usdc": max_drawdown,
            "time_to_recover_s": float(time_to_recover),
            "fill_rate": float(fill_rate),
            "cancel_rate": float(cancel_rate),
            "avg_slippage_usdc": float(avg_slippage_usdc),
            "avg_slippage_bps": avg_slippage_bps,
            "avg_latency_e2e_ms": avg_latency_e2e_ms,
            "avg_spread": avg_spread,
            "avg_depth_top3": avg_depth,
            "net_realized_pnl_usdc": float(self.state.realized_pnl_usdc),
            "ending_equity_usdc": float(equity_series[-1]),
            "windows": float(len(window_df)),
            "sent_orders": float(self.state.sent_orders),
            "scenario": self.scenario_name,
        }

    def _export_outputs(self, kpis: dict[str, float]) -> None:
        assert self.output_dir is not None
        self.output_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"metric": key, "value": value} for key, value in kpis.items()]).to_csv(
            self.output_dir / "kpis.csv",
            index=False,
        )
        pd.DataFrame(self.equity_rows).to_csv(self.output_dir / "equity_curve.csv", index=False)
        with (self.output_dir / "backtest_log.jsonl").open("w", encoding="utf-8") as handle:
            for row in self.structured_logs:
                handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    @staticmethod
    def _window_id(ts_ms: int) -> int:
        return int(ts_ms // WINDOW_MS)


def _annualized_sharpe(values: np.ndarray, periods_per_year: int) -> float:
    if values.size == 0:
        return 0.0
    std = float(np.std(values, ddof=1)) if values.size > 1 else 0.0
    if std <= 0:
        return 0.0
    return float(np.mean(values) / std * np.sqrt(periods_per_year))


def _annualized_sortino(values: np.ndarray, periods_per_year: int) -> float:
    if values.size == 0:
        return 0.0
    downside = values[values < 0]
    std = float(np.std(downside, ddof=1)) if downside.size > 1 else 0.0
    if std <= 0:
        return 0.0
    return float(np.mean(values) / std * np.sqrt(periods_per_year))


def _time_to_recover_seconds(ts_ms: np.ndarray, equity: np.ndarray) -> float:
    if equity.size == 0:
        return 0.0
    peak_value = float(equity[0])
    drawdown_start = None
    max_recovery = 0.0
    for idx, value in enumerate(equity):
        if value >= peak_value:
            peak_value = value
            if drawdown_start is not None:
                max_recovery = max(max_recovery, (ts_ms[idx] - drawdown_start) / 1000.0)
                drawdown_start = None
        elif drawdown_start is None:
            drawdown_start = ts_ms[idx]
    return max_recovery


def load_replay_input(path: str | Path) -> ReplayBundle:
    input_path = Path(path)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    return ReplayBundle(
        meta=dict(payload.get("meta") or {}),
        events=[dict(item) for item in payload.get("events") or []],
        trades=[dict(item) for item in payload.get("trades") or []],
    )


def run_backtest(
    input_path: str | Path,
    *,
    output_dir: str | Path = "data/research/backtest",
    config: ResearchConfig | None = None,
) -> dict[str, float]:
    bundle = load_replay_input(input_path)
    engine = BacktestEngine(bundle, config or ResearchConfig(), Path(output_dir))
    return engine.run()


def run_walk_forward(
    input_path: str | Path,
    *,
    output_dir: str | Path = "data/research/validation/walk_forward",
    config: ResearchConfig | None = None,
    train_windows: int = 3,
    test_windows: int = 1,
) -> list[dict[str, Any]]:
    base_config = config or ResearchConfig()
    bundle = load_replay_input(input_path)
    window_ids = _bundle_window_ids(bundle)
    if len(window_ids) < train_windows + test_windows:
        raise RuntimeError("No hay suficientes ventanas para walk-forward.")
    output_path = Path(output_dir)
    rows: list[dict[str, Any]] = []
    fold_index = 0
    for offset in range(train_windows, len(window_ids) - test_windows + 1):
        train_ids = window_ids[offset - train_windows : offset]
        test_ids = window_ids[offset : offset + test_windows]
        selected_label, tuned_config = _calibrate_config(_slice_bundle(bundle, train_ids), base_config)
        test_kpis = BacktestEngine(_slice_bundle(bundle, test_ids), tuned_config, None, scenario_name=f"walk-forward-{fold_index}").run(
            export_outputs=False
        )
        row = asdict(
            ValidationFoldResult(
                method="walk_forward",
                fold_index=fold_index,
                train_start_window=train_ids[0],
                train_end_window=train_ids[-1],
                test_start_window=test_ids[0],
                test_end_window=test_ids[-1],
                selected_strategy=tuned_config.sub_strategy,
                selected_execution=tuned_config.execution.mode,
                selected_max_usdc_per_trade=tuned_config.sizing.max_usdc_per_trade,
                expectancy_window_usdc=float(test_kpis["expectancy_window_usdc"]),
                net_realized_pnl_usdc=float(test_kpis["net_realized_pnl_usdc"]),
                hit_rate=float(test_kpis["hit_rate"]),
                fill_rate=float(test_kpis["fill_rate"]),
                max_drawdown_usdc=float(test_kpis["max_drawdown_usdc"]),
            )
        )
        row["selected_label"] = selected_label
        rows.append(row)
        fold_index += 1
    _export_validation_rows(output_path, "walk_forward", rows)
    return rows


def run_temporal_kfold(
    input_path: str | Path,
    *,
    output_dir: str | Path = "data/research/validation/temporal_kfold",
    config: ResearchConfig | None = None,
    folds: int = 4,
) -> list[dict[str, Any]]:
    base_config = config or ResearchConfig()
    bundle = load_replay_input(input_path)
    window_ids = _bundle_window_ids(bundle)
    if len(window_ids) < max(folds, 3):
        raise RuntimeError("No hay suficientes ventanas para temporal k-fold.")
    contiguous_folds = np.array_split(window_ids, folds)
    rows: list[dict[str, Any]] = []
    output_path = Path(output_dir)
    for fold_index in range(1, len(contiguous_folds)):
        train_ids = [item for chunk in contiguous_folds[:fold_index] for item in chunk.tolist()]
        test_ids = contiguous_folds[fold_index].tolist()
        if not train_ids or not test_ids:
            continue
        selected_label, tuned_config = _calibrate_config(_slice_bundle(bundle, train_ids), base_config)
        test_kpis = BacktestEngine(_slice_bundle(bundle, test_ids), tuned_config, None, scenario_name=f"temporal-kfold-{fold_index}").run(
            export_outputs=False
        )
        row = asdict(
            ValidationFoldResult(
                method="temporal_kfold",
                fold_index=fold_index,
                train_start_window=train_ids[0],
                train_end_window=train_ids[-1],
                test_start_window=test_ids[0],
                test_end_window=test_ids[-1],
                selected_strategy=tuned_config.sub_strategy,
                selected_execution=tuned_config.execution.mode,
                selected_max_usdc_per_trade=tuned_config.sizing.max_usdc_per_trade,
                expectancy_window_usdc=float(test_kpis["expectancy_window_usdc"]),
                net_realized_pnl_usdc=float(test_kpis["net_realized_pnl_usdc"]),
                hit_rate=float(test_kpis["hit_rate"]),
                fill_rate=float(test_kpis["fill_rate"]),
                max_drawdown_usdc=float(test_kpis["max_drawdown_usdc"]),
            )
        )
        row["selected_label"] = selected_label
        rows.append(row)
    _export_validation_rows(output_path, "temporal_kfold", rows)
    return rows


def run_stress_tests(
    input_path: str | Path,
    *,
    output_dir: str | Path = "data/research/validation/stress",
    config: ResearchConfig | None = None,
) -> list[dict[str, Any]]:
    base_config = config or ResearchConfig()
    bundle = load_replay_input(input_path)
    output_path = Path(output_dir)
    scenarios = {
        "baseline": lambda cfg, src: (cfg, src),
        "slippage_x2": lambda cfg, src: (_mutate_config(cfg, slippage_multiplier=2.0), src),
        "slippage_x3": lambda cfg, src: (_mutate_config(cfg, slippage_multiplier=3.0), src),
        "fill_prob_degraded": lambda cfg, src: (_mutate_config(cfg, maker_fill_probability_multiplier=0.5, taker_fill_ratio=0.65), src),
        "latency_x3": lambda cfg, src: (_mutate_config(cfg, latency_budget_ms=cfg.latency_budget_ms * 3), src),
        "ws_dropout": lambda cfg, src: (cfg, _simulate_ws_dropout(src, keep_every=3)),
        "fees_off": lambda cfg, src: (_mutate_config(cfg, default_taker_fee_bps=0.0, use_fee_rate_endpoint=False), _without_fees(src)),
    }
    rows: list[dict[str, Any]] = []
    for name, scenario in scenarios.items():
        scenario_config, scenario_bundle = scenario(_clone_config(base_config), _clone_bundle(bundle))
        kpis = BacktestEngine(scenario_bundle, scenario_config, output_path / name, scenario_name=name).run(export_outputs=True)
        row = {"scenario": name, **kpis}
        rows.append(row)
    output_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path / "stress_summary.csv", index=False)
    return rows


def _clone_config(config: ResearchConfig) -> ResearchConfig:
    return copy.deepcopy(config)


def _clone_bundle(bundle: ReplayBundle) -> ReplayBundle:
    return ReplayBundle(meta=copy.deepcopy(bundle.meta), events=copy.deepcopy(bundle.events), trades=copy.deepcopy(bundle.trades))


def _mutate_config(
    config: ResearchConfig,
    *,
    slippage_multiplier: float | None = None,
    maker_fill_probability_multiplier: float | None = None,
    taker_fill_ratio: float | None = None,
    latency_budget_ms: int | None = None,
    default_taker_fee_bps: float | None = None,
    use_fee_rate_endpoint: bool | None = None,
) -> ResearchConfig:
    if slippage_multiplier is not None:
        config.slippage_model.slippage_multiplier = slippage_multiplier
    if maker_fill_probability_multiplier is not None:
        config.slippage_model.maker_fill_probability_multiplier = maker_fill_probability_multiplier
    if taker_fill_ratio is not None:
        config.slippage_model.taker_fill_ratio = taker_fill_ratio
    if latency_budget_ms is not None:
        config.latency_budget_ms = latency_budget_ms
    if default_taker_fee_bps is not None:
        config.fee_model.default_taker_fee_bps = default_taker_fee_bps
    if use_fee_rate_endpoint is not None:
        config.fee_model.use_fee_rate_endpoint = use_fee_rate_endpoint
    return config


def _simulate_ws_dropout(bundle: ReplayBundle, *, keep_every: int) -> ReplayBundle:
    degraded_events: list[dict[str, Any]] = []
    snapshot_count = 0
    for event in bundle.events:
        kind = str(event.get("event") or "")
        if kind == "book":
            degraded_events.append(event)
            snapshot_count += 1
            continue
        if snapshot_count == 0:
            degraded_events.append(event)
            continue
        if len(degraded_events) % max(keep_every, 1) == 0:
            degraded_events.append(event)
    return ReplayBundle(meta=copy.deepcopy(bundle.meta), events=degraded_events, trades=copy.deepcopy(bundle.trades))


def _without_fees(bundle: ReplayBundle) -> ReplayBundle:
    updated = _clone_bundle(bundle)
    updated.meta["fees_enabled"] = False
    updated.meta["fee_rate_bps"] = 0.0
    return updated


def _bundle_window_ids(bundle: ReplayBundle) -> list[int]:
    window_ids = {int(event["ts_ms"]) // WINDOW_MS for event in bundle.events if "ts_ms" in event}
    return sorted(window_ids)


def _slice_bundle(bundle: ReplayBundle, window_ids: list[int]) -> ReplayBundle:
    window_set = set(window_ids)
    events = [event for event in bundle.events if int(event["ts_ms"]) // WINDOW_MS in window_set]
    trades = [trade for trade in bundle.trades if int(trade["ts_ms"]) // WINDOW_MS in window_set]
    return ReplayBundle(meta=copy.deepcopy(bundle.meta), events=events, trades=trades)


def _candidate_configs(base_config: ResearchConfig) -> list[tuple[str, ResearchConfig]]:
    candidates: list[tuple[str, ResearchConfig]] = []
    for strategy_name, execution_mode, trade_size in (
        ("underround_arb", "taker_only", 25.0),
        ("underround_arb", "taker_only", 50.0),
        ("underround_arb", "hybrid", 50.0),
        ("underround_arb", "hybrid", 75.0),
        ("market_making", "maker_only", 20.0),
        ("market_making", "hybrid", 30.0),
    ):
        candidate = _clone_config(base_config)
        candidate.sub_strategy = strategy_name
        candidate.execution.mode = execution_mode
        candidate.sizing.max_usdc_per_trade = trade_size
        candidates.append((f"{strategy_name}:{execution_mode}:{trade_size:.0f}", candidate))
    return candidates


def _score_kpis(kpis: dict[str, float]) -> float:
    return (
        float(kpis["expectancy_window_usdc"]) * 2.0
        + float(kpis["net_realized_pnl_usdc"]) * 0.25
        + float(kpis["hit_rate"]) * 5.0
        + float(kpis["fill_rate"]) * 1.0
        - float(kpis["max_drawdown_usdc"]) * 0.05
        - abs(float(kpis["avg_slippage_bps"])) * 0.02
    )


def _calibrate_config(bundle: ReplayBundle, base_config: ResearchConfig) -> tuple[str, ResearchConfig]:
    best_label = "default"
    best_config = _clone_config(base_config)
    best_score = float("-inf")
    for label, candidate in _candidate_configs(base_config):
        kpis = BacktestEngine(bundle, candidate, None, scenario_name=f"train-{label}").run(export_outputs=False)
        score = _score_kpis(kpis)
        if score > best_score:
            best_score = score
            best_label = label
            best_config = candidate
    return best_label, best_config


def _export_validation_rows(output_dir: Path, stem: str, rows: list[dict[str, Any]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(output_dir / f"{stem}_folds.csv", index=False)
    if frame.empty:
        return
    numeric_cols = [column for column in frame.columns if frame[column].dtype.kind in {"i", "f"}]
    summary = frame[numeric_cols].mean(numeric_only=True).to_dict()
    summary["folds"] = len(frame)
    pd.DataFrame([summary]).to_csv(output_dir / f"{stem}_summary.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay/backtest de BTC Up/Down 5m para Polymarket")
    parser.add_argument("--input", required=True, help="JSON normalizado para replay")
    parser.add_argument("--output-dir", default="data/research/backtest", help="Directorio de salida")
    parser.add_argument("--config", default="", help="YAML opcional")
    parser.add_argument("--walk-forward", action="store_true", help="Ejecuta walk-forward validation")
    parser.add_argument("--temporal-kfold", action="store_true", help="Ejecuta temporal k-fold validation")
    parser.add_argument("--stress", action="store_true", help="Ejecuta stress tests")
    parser.add_argument("--train-windows", type=int, default=3, help="Ventanas de train para walk-forward")
    parser.add_argument("--test-windows", type=int, default=1, help="Ventanas de test para walk-forward")
    parser.add_argument("--folds", type=int, default=4, help="Numero de folds temporales")
    args = parser.parse_args()

    config = ResearchConfig.from_yaml(args.config or None)
    results: dict[str, Any] = {
        "backtest": run_backtest(args.input, output_dir=args.output_dir, config=config),
    }
    if args.walk_forward:
        results["walk_forward"] = run_walk_forward(
            args.input,
            output_dir=Path(args.output_dir) / "walk_forward",
            config=config,
            train_windows=args.train_windows,
            test_windows=args.test_windows,
        )
    if args.temporal_kfold:
        results["temporal_kfold"] = run_temporal_kfold(
            args.input,
            output_dir=Path(args.output_dir) / "temporal_kfold",
            config=config,
            folds=args.folds,
        )
    if args.stress:
        results["stress"] = run_stress_tests(
            args.input,
            output_dir=Path(args.output_dir) / "stress",
            config=config,
        )
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
