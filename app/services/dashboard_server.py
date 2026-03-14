from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

_MIDPOINT_CACHE: dict[str, tuple[float | None, float]] = {}
_MIDPOINT_CACHE_TTL_SECONDS = 20


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def run_dashboard_server(
    db_path: Path,
    static_dir: Path,
    clob_host: str,
    execution_mode: str,
    live_trading_enabled: bool,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> None:
    handler_class = _build_handler(
        db_path=db_path,
        static_dir=static_dir,
        clob_host=clob_host,
        execution_mode=execution_mode,
        live_trading_enabled=live_trading_enabled,
    )
    server = ReusableThreadingHTTPServer((host, port), handler_class)
    print(f"dashboard => http://{host}:{port}")
    server.serve_forever()


def _build_handler(
    db_path: Path,
    static_dir: Path,
    clob_host: str,
    execution_mode: str,
    live_trading_enabled: bool,
):
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self.send_response(HTTPStatus.NO_CONTENT)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
                self.end_headers()
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/":
                self._serve_file(static_dir / "index.html", "text/html; charset=utf-8")
                return
            if path == "/assets/styles.css":
                self._serve_file(static_dir / "assets" / "styles.css", "text/css; charset=utf-8")
                return
            if path == "/assets/app.js":
                self._serve_file(static_dir / "assets" / "app.js", "text/javascript; charset=utf-8")
                return
            if path == "/api/health":
                self._json({"ok": True})
                return
            if path == "/api/summary":
                self._json(
                    _summary_payload(
                        db_path,
                        clob_host=clob_host,
                        execution_mode=execution_mode,
                        live_trading_enabled=live_trading_enabled,
                    )
                )
                return
            if path == "/api/positions":
                self._json(_positions_payload(db_path, clob_host=clob_host))
                return
            if path == "/api/executions":
                query = parse_qs(parsed.query)
                limit = _safe_int(query.get("limit", ["50"])[0], default=50, minimum=1, maximum=500)
                self._json(_executions_payload(db_path, limit=limit))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/reset":
                payload = self._read_json_body()
                if str(payload.get("confirm") or "").strip().lower() != "reset":
                    self._json(
                        {"ok": False, "error": "confirmation required", "hint": "send JSON {\"confirm\":\"reset\"}"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                result = _reset_runtime_state(db_path)
                self._json(result)
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _serve_file(self, file_path: Path, content_type: str) -> None:
            if not file_path.exists():
                self.send_error(HTTPStatus.NOT_FOUND, "Asset not found")
                return
            content = file_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _json(self, payload: dict | list, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_length)
            except (TypeError, ValueError):
                return {}
            if length <= 0:
                return {}
            try:
                raw = self.rfile.read(length).decode("utf-8")
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    return payload
            except (UnicodeDecodeError, json.JSONDecodeError):
                return {}
            return {}

    return DashboardHandler


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    return connection


def _summary_payload(db_path: Path, *, clob_host: str, execution_mode: str, live_trading_enabled: bool) -> dict:
    today_utc = datetime.now(timezone.utc).date().isoformat()
    with _connect(db_path) as conn:
        open_positions = _single_float(conn, "SELECT COUNT(*) AS value FROM copy_positions")
        exposure = _single_float(conn, "SELECT COALESCE(SUM(ABS(size * avg_price)), 0) AS value FROM copy_positions")
        realized_pnl = _single_float(conn, "SELECT COALESCE(SUM(pnl), 0) AS value FROM daily_pnl")
        daily_realized_pnl = _single_float(
            conn,
            "SELECT COALESCE(SUM(pnl_delta), 0) AS value FROM executions WHERE strftime('%Y-%m-%d', ts, 'unixepoch') = ?",
            (today_utc,),
        )
        daily_profit_gross = _single_float(
            conn,
            """
            SELECT COALESCE(SUM(CASE WHEN pnl_delta > 0 THEN pnl_delta ELSE 0 END), 0) AS value
            FROM executions
            WHERE strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (today_utc,),
        )
        daily_loss_gross = _single_float(
            conn,
            """
            SELECT COALESCE(ABS(SUM(CASE WHEN pnl_delta < 0 THEN pnl_delta ELSE 0 END)), 0) AS value
            FROM executions
            WHERE strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (today_utc,),
        )
        pending_signals = _single_float(
            conn,
            """
            SELECT COUNT(*) AS value
            FROM signals
            WHERE status IN ('pending', 'awaiting_approval', 'awaiting_execution')
            """,
        )
        executed_signals = _single_float(conn, "SELECT COUNT(*) AS value FROM signals WHERE status='executed'")
        failed_signals = _single_float(conn, "SELECT COUNT(*) AS value FROM signals WHERE status='failed'")
        live_executions_total = _single_float(
            conn, "SELECT COUNT(*) AS value FROM executions WHERE mode = 'live'"
        )
        live_executions_today = _single_float(
            conn,
            """
            SELECT COUNT(*) AS value
            FROM executions
            WHERE mode = 'live' AND strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (today_utc,),
        )
        live_realized_pnl_today = _single_float(
            conn,
            """
            SELECT COALESCE(SUM(pnl_delta), 0) AS value
            FROM executions
            WHERE mode = 'live' AND strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (today_utc,),
        )
        last_live_execution_ts = _single_float(
            conn, "SELECT COALESCE(MAX(ts), 0) AS value FROM executions WHERE mode = 'live'"
        )
        live_cash_balance = _bot_state_float(conn, "live_cash_balance")
        live_cash_allowance = _bot_state_float(conn, "live_cash_allowance")
        live_total_capital = _bot_state_float(conn, "live_total_capital")
        live_balance_updated_at = _bot_state_int(conn, "live_balance_updated_at")
        strategy_mode = _bot_state_text(conn, "strategy_mode")
        strategy_entry_mode = _bot_state_text(conn, "strategy_entry_mode")
        strategy_runtime_mode = _bot_state_text(conn, "strategy_runtime_mode")
        strategy_market_slug = _bot_state_text(conn, "strategy_market_slug")
        strategy_market_title = _bot_state_text(conn, "strategy_market_title")
        strategy_target_outcome = _bot_state_text(conn, "strategy_target_outcome")
        strategy_target_price = _bot_state_float(conn, "strategy_target_price")
        strategy_trigger_outcome = _bot_state_text(conn, "strategy_trigger_outcome")
        strategy_trigger_price_seen = _bot_state_float(conn, "strategy_trigger_price_seen")
        strategy_pair_sum = _bot_state_float(conn, "strategy_pair_sum")
        strategy_edge_pct = _bot_state_float(conn, "strategy_edge_pct")
        strategy_fair_value = _bot_state_float(conn, "strategy_fair_value")
        strategy_spot_price = _bot_state_float(conn, "strategy_spot_price")
        strategy_spot_anchor = _bot_state_float(conn, "strategy_spot_anchor")
        strategy_spot_delta_bps = _bot_state_float(conn, "strategy_spot_delta_bps")
        strategy_spot_fair_up = _bot_state_float(conn, "strategy_spot_fair_up")
        strategy_spot_fair_down = _bot_state_float(conn, "strategy_spot_fair_down")
        strategy_spot_source = _bot_state_text(conn, "strategy_spot_source")
        strategy_spot_age_ms = _bot_state_int(conn, "strategy_spot_age_ms")
        strategy_spot_binance = _bot_state_float(conn, "strategy_spot_binance")
        strategy_spot_chainlink = _bot_state_float(conn, "strategy_spot_chainlink")
        strategy_last_note = _bot_state_text(conn, "strategy_last_note")
        strategy_last_updated_at = _bot_state_int(conn, "strategy_last_updated_at")
        strategy_market_bias = _bot_state_text(conn, "strategy_market_bias")
        strategy_plan_legs = _bot_state_int(conn, "strategy_plan_legs")
        strategy_window_seconds = _bot_state_int(conn, "strategy_window_seconds")
        strategy_cycle_budget = _bot_state_float(conn, "strategy_cycle_budget")
        strategy_current_market_exposure = _bot_state_float(conn, "strategy_current_market_exposure")
        strategy_resolution_mode = _bot_state_text(conn, "strategy_resolution_mode")
        strategy_timing_regime = _bot_state_text(conn, "strategy_timing_regime")
        strategy_price_mode = _bot_state_text(conn, "strategy_price_mode")
        strategy_primary_ratio = _bot_state_float(conn, "strategy_primary_ratio")
        strategy_desired_up_ratio = _bot_state_float(conn, "strategy_desired_up_ratio")
        strategy_desired_down_ratio = _bot_state_float(conn, "strategy_desired_down_ratio")
        strategy_current_up_ratio = _bot_state_float(conn, "strategy_current_up_ratio")
        strategy_bracket_phase = _bot_state_text(conn, "strategy_bracket_phase")
        strategy_primary_outcome = _bot_state_text(conn, "strategy_primary_outcome")
        strategy_hedge_outcome = _bot_state_text(conn, "strategy_hedge_outcome")
        strategy_primary_exposure = _bot_state_float(conn, "strategy_primary_exposure")
        strategy_hedge_exposure = _bot_state_float(conn, "strategy_hedge_exposure")
        strategy_replenishment_count = _bot_state_int(conn, "strategy_replenishment_count")
        strategy_data_source = _bot_state_text(conn, "strategy_data_source")
        strategy_feed_connected = _bot_state_int(conn, "strategy_feed_connected")
        strategy_feed_age_ms = _bot_state_int(conn, "strategy_feed_age_ms")
        strategy_feed_tracked_assets = _bot_state_int(conn, "strategy_feed_tracked_assets")
        strategy_resolution_count_today = _single_float(
            conn,
            """
            SELECT COUNT(*) AS value
            FROM executions
            WHERE mode = 'paper'
              AND (notes LIKE 'strategy_resolution:%' OR notes LIKE 'vidarx_resolution:%')
              AND strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (today_utc,),
        )
        strategy_resolution_pnl_today = _single_float(
            conn,
            """
            SELECT COALESCE(SUM(pnl_delta), 0) AS value
            FROM executions
            WHERE mode = 'paper'
              AND (notes LIKE 'strategy_resolution:%' OR notes LIKE 'vidarx_resolution:%')
              AND strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (today_utc,),
        )
        positions = conn.execute(
            "SELECT asset, condition_id, size, avg_price, slug, title, outcome FROM copy_positions"
        ).fetchall()
        recent_resolution_windows = _recent_vidarx_resolution_windows(conn, limit=6)
        setup_performance = _vidarx_setup_performance(conn, limit=8)

    unrealized_pnl = 0.0
    exposure_mark = 0.0
    market_groups: dict[str, dict] = {}
    for row in positions:
        asset = str(row["asset"])
        size = float(row["size"])
        avg_price = float(row["avg_price"])
        mark_price = _midpoint_for_asset(clob_host=clob_host, asset=asset)
        if mark_price is None:
            mark_price = avg_price
        line_unrealized = (mark_price - avg_price) * size
        unrealized_pnl += line_unrealized
        exposure_mark += abs(size * mark_price)
        market_key = str(row["slug"] or row["condition_id"] or row["asset"])
        group = market_groups.setdefault(
            market_key,
            {
                "slug": str(row["slug"] or ""),
                "title": str(row["title"] or row["slug"] or row["asset"]),
                "condition_id": str(row["condition_id"] or ""),
                "total_exposure": 0.0,
                "total_shares": 0.0,
                "unrealized_pnl": 0.0,
                "outcomes": {},
            },
        )
        line_exposure = abs(size * avg_price)
        group["total_exposure"] += line_exposure
        group["total_shares"] += abs(size)
        group["unrealized_pnl"] += line_unrealized
        outcome_key = str(row["outcome"] or "-")
        outcome_group = group["outcomes"].setdefault(
            outcome_key,
            {"outcome": outcome_key, "exposure": 0.0, "shares": 0.0, "unrealized_pnl": 0.0},
        )
        outcome_group["exposure"] += line_exposure
        outcome_group["shares"] += abs(size)
        outcome_group["unrealized_pnl"] += line_unrealized

    pnl_total = realized_pnl + unrealized_pnl
    live_mode_active = execution_mode == "live" and live_trading_enabled
    live_available_to_trade = _available_to_trade(
        live_cash_balance=live_cash_balance,
        live_cash_allowance=live_cash_allowance,
    )
    live_equity_estimate = live_cash_balance + exposure_mark
    current_market_group = market_groups.get(strategy_market_slug) if strategy_market_slug else None
    if current_market_group is None and strategy_market_title:
        current_market_group = next(
            (item for item in market_groups.values() if str(item["title"]) == strategy_market_title),
            None,
        )
    current_market_breakdown: list[dict] = []
    current_market_live_pnl = 0.0
    primary_exposure_actual = 0.0
    hedge_exposure_actual = 0.0
    current_market_total_exposure = strategy_current_market_exposure
    current_market_total_shares = 0.0
    if current_market_group is not None:
        current_market_live_pnl = float(current_market_group["unrealized_pnl"])
        current_market_total_exposure = float(current_market_group["total_exposure"])
        current_market_total_shares = float(current_market_group["total_shares"])
        for outcome_row in sorted(
            current_market_group["outcomes"].values(),
            key=lambda item: float(item["exposure"]),
            reverse=True,
        ):
            payout_share_pct = (
                (float(outcome_row["shares"]) / current_market_total_shares) * 100
                if current_market_total_shares > 0
                else 0.0
            )
            money_share_pct = (
                (float(outcome_row["exposure"]) / current_market_total_exposure) * 100
                if current_market_total_exposure > 0
                else 0.0
            )
            current_market_breakdown.append(
                {
                    "outcome": outcome_row["outcome"],
                    "exposure": round(float(outcome_row["exposure"]), 4),
                    "shares": round(float(outcome_row["shares"]), 4),
                    "unrealized_pnl": round(float(outcome_row["unrealized_pnl"]), 4),
                    "share_pct": round(payout_share_pct, 2),
                    "money_share_pct": round(money_share_pct, 2),
                }
            )
        primary_exposure_actual = float(
            current_market_group["outcomes"].get(strategy_primary_outcome or "", {}).get("exposure", 0.0)
        )
        hedge_exposure_actual = float(
            current_market_group["outcomes"].get(strategy_hedge_outcome or "", {}).get("exposure", 0.0)
        )

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "configured_execution_mode": execution_mode,
        "live_trading_enabled": live_trading_enabled,
        "live_mode_active": live_mode_active,
        "open_positions": int(open_positions),
        "exposure": round(exposure, 4),
        "exposure_mark": round(exposure_mark, 4),
        "cumulative_pnl": round(realized_pnl, 4),
        "realized_pnl": round(realized_pnl, 4),
        "unrealized_pnl": round(unrealized_pnl, 4),
        "pnl_total": round(pnl_total, 4),
        "pending_signals": int(pending_signals),
        "executed_signals": int(executed_signals),
        "failed_signals": int(failed_signals),
        "live_executions_total": int(live_executions_total),
        "live_executions_today": int(live_executions_today),
        "live_realized_pnl_today": round(live_realized_pnl_today, 4),
        "last_live_execution_ts": int(last_live_execution_ts),
        "live_cash_balance": round(live_cash_balance, 4),
        "live_cash_allowance": round(live_cash_allowance, 4),
        "live_total_capital": round(live_total_capital, 4),
        "live_available_to_trade": round(live_available_to_trade, 4),
        "live_equity_estimate": round(live_equity_estimate, 4),
        "live_balance_updated_at": int(live_balance_updated_at),
        "strategy_mode": strategy_mode,
        "strategy_entry_mode": strategy_entry_mode,
        "strategy_runtime_mode": strategy_runtime_mode,
        "strategy_market_slug": strategy_market_slug,
        "strategy_market_title": strategy_market_title,
        "strategy_target_outcome": strategy_target_outcome,
        "strategy_target_price": round(strategy_target_price, 4),
        "strategy_trigger_outcome": strategy_trigger_outcome,
        "strategy_trigger_price_seen": round(strategy_trigger_price_seen, 4),
        "strategy_pair_sum": round(strategy_pair_sum, 4),
        "strategy_edge_pct": round(strategy_edge_pct, 6),
        "strategy_fair_value": round(strategy_fair_value, 4),
        "strategy_spot_price": round(strategy_spot_price, 4),
        "strategy_spot_anchor": round(strategy_spot_anchor, 4),
        "strategy_spot_delta_bps": round(strategy_spot_delta_bps, 2),
        "strategy_spot_fair_up": round(strategy_spot_fair_up, 4),
        "strategy_spot_fair_down": round(strategy_spot_fair_down, 4),
        "strategy_spot_source": strategy_spot_source,
        "strategy_spot_age_ms": int(strategy_spot_age_ms),
        "strategy_spot_binance": round(strategy_spot_binance, 4),
        "strategy_spot_chainlink": round(strategy_spot_chainlink, 4),
        "strategy_last_note": strategy_last_note,
        "strategy_last_updated_at": int(strategy_last_updated_at),
        "strategy_market_bias": strategy_market_bias,
        "strategy_plan_legs": int(strategy_plan_legs),
        "strategy_window_seconds": int(strategy_window_seconds),
        "strategy_cycle_budget": round(strategy_cycle_budget, 4),
        "strategy_current_market_exposure": round(strategy_current_market_exposure, 4),
        "strategy_resolution_mode": strategy_resolution_mode,
        "strategy_timing_regime": strategy_timing_regime,
        "strategy_price_mode": strategy_price_mode,
        "strategy_primary_ratio": round(strategy_primary_ratio, 4),
        "strategy_desired_up_ratio": round(strategy_desired_up_ratio, 4),
        "strategy_desired_down_ratio": round(strategy_desired_down_ratio, 4),
        "strategy_current_up_ratio": round(strategy_current_up_ratio, 4),
        "strategy_bracket_phase": strategy_bracket_phase,
        "strategy_primary_outcome": strategy_primary_outcome,
        "strategy_hedge_outcome": strategy_hedge_outcome,
        "strategy_primary_exposure": round(strategy_primary_exposure, 4),
        "strategy_hedge_exposure": round(strategy_hedge_exposure, 4),
        "strategy_replenishment_count": int(strategy_replenishment_count),
        "strategy_data_source": strategy_data_source,
        "strategy_feed_connected": bool(strategy_feed_connected),
        "strategy_feed_age_ms": int(strategy_feed_age_ms),
        "strategy_feed_tracked_assets": int(strategy_feed_tracked_assets),
        "strategy_current_market_live_pnl": round(current_market_live_pnl, 4),
        "strategy_current_market_total_exposure": round(current_market_total_exposure, 4),
        "strategy_current_market_total_shares": round(current_market_total_shares, 4),
        "strategy_current_market_primary_exposure": round(primary_exposure_actual, 4),
        "strategy_current_market_hedge_exposure": round(hedge_exposure_actual, 4),
        "strategy_current_market_breakdown": current_market_breakdown,
        "strategy_recent_resolutions": recent_resolution_windows,
        "strategy_setup_performance": setup_performance,
        "strategy_resolution_count_today": int(strategy_resolution_count_today),
        "strategy_resolution_pnl_today": round(strategy_resolution_pnl_today, 4),
        "strategy_is_lab": strategy_entry_mode in {"vidarx_micro", "arb_micro"},
        "daily_realized_pnl": round(daily_realized_pnl, 4),
        "daily_profit_gross": round(daily_profit_gross, 4),
        "daily_loss_gross": round(daily_loss_gross, 4),
    }


def _positions_payload(db_path: Path, *, clob_host: str) -> dict:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT asset, condition_id, size, avg_price, realized_pnl, updated_at, title, slug, outcome, category
            FROM copy_positions
            ORDER BY updated_at DESC
            """
        ).fetchall()

    positions = []
    for row in rows:
        positions.append(
            {
                "asset": row["asset"],
                "condition_id": row["condition_id"],
                "size": float(row["size"]),
                "avg_price": float(row["avg_price"]),
                "realized_pnl": float(row["realized_pnl"]),
                "updated_at": int(row["updated_at"]),
                "title": row["title"] or "",
                "slug": row["slug"] or "",
                "outcome": row["outcome"] or "",
                "category": row["category"] or "",
            }
        )
        mark_price = _midpoint_for_asset(clob_host=clob_host, asset=str(row["asset"]))
        if mark_price is None:
            mark_price = float(row["avg_price"])
        positions[-1]["mark_price"] = float(mark_price)
        positions[-1]["unrealized_pnl"] = float((mark_price - float(row["avg_price"])) * float(row["size"]))
    return {"items": positions}


def _executions_payload(db_path: Path, limit: int) -> dict:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, ts, mode, status, action, side, asset, condition_id, size, price, notional,
                   source_wallet, source_signal_id, notes, pnl_delta
            FROM executions
            ORDER BY ts DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    items = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"]),
                "ts": int(row["ts"]),
                "mode": row["mode"],
                "status": row["status"],
                "action": row["action"],
                "side": row["side"],
                "asset": row["asset"],
                "condition_id": row["condition_id"],
                "size": float(row["size"]),
                "price": float(row["price"]),
                "notional": float(row["notional"]),
                "source_wallet": row["source_wallet"] or "",
                "source_signal_id": int(row["source_signal_id"]) if row["source_signal_id"] is not None else 0,
                "notes": row["notes"] or "",
                "pnl_delta": float(row["pnl_delta"]),
            }
        )
    return {"items": items}


def _signals_payload(db_path: Path, limit: int) -> dict:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, event_key, detected_at, wallet, asset, condition_id, action, prev_size, new_size, delta_size,
                   reference_price, title, slug, outcome, category, status, note
            FROM signals
            ORDER BY detected_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    items = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"]),
                "event_key": row["event_key"],
                "detected_at": int(row["detected_at"]),
                "wallet": row["wallet"],
                "asset": row["asset"],
                "condition_id": row["condition_id"],
                "action": row["action"],
                "prev_size": float(row["prev_size"]),
                "new_size": float(row["new_size"]),
                "delta_size": float(row["delta_size"]),
                "reference_price": float(row["reference_price"]),
                "title": row["title"] or "",
                "slug": row["slug"] or "",
                "outcome": row["outcome"] or "",
                "category": row["category"] or "",
                "status": row["status"],
                "note": row["note"] or "",
            }
        )
    return {"items": items}


def _selected_wallets_payload(db_path: Path, limit: int) -> dict:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT wallet, rank, score, win_rate, recent_trades, pnl, selected_at
            FROM selected_wallets
            ORDER BY rank ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    items = []
    for row in rows:
        items.append(
            {
                "wallet": row["wallet"],
                "rank": int(row["rank"]),
                "score": float(row["score"]),
                "win_rate": float(row["win_rate"]),
                "recent_trades": int(row["recent_trades"]),
                "pnl": float(row["pnl"]),
                "selected_at": int(row["selected_at"]),
            }
        )
    return {"items": items}


def _risk_blocks_payload(db_path: Path, *, limit: int, hours: int) -> dict:
    cutoff = int(time.time()) - (hours * 3600)
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT note, COUNT(*) AS total
            FROM signals
            WHERE status = 'blocked' AND detected_at >= ? AND note <> ''
            GROUP BY note
            ORDER BY total DESC
            LIMIT ?
            """,
            (cutoff, limit),
        ).fetchall()
        blocked_total = _single_float(
            conn,
            "SELECT COUNT(*) AS value FROM signals WHERE status = 'blocked' AND detected_at >= ?",
            (cutoff,),
        )

    items = []
    for row in rows:
        items.append({"reason": row["note"], "count": int(row["total"])})
    return {"items": items, "hours": hours, "blocked_total": int(blocked_total)}


def _single_float(conn: sqlite3.Connection, query: str, params: tuple = ()) -> float:
    row = conn.execute(query, params).fetchone()
    if row is None:
        return 0.0
    value = row["value"]
    if value is None:
        return 0.0
    return float(value)


def _recent_vidarx_resolution_windows(conn: sqlite3.Connection, *, limit: int) -> list[dict]:
    strategy_rows = conn.execute(
        """
        SELECT slug, closed_at, realized_pnl, planned_budget, filled_orders, winning_outcome
        FROM strategy_windows
        WHERE status = 'closed'
        ORDER BY COALESCE(closed_at, 0) DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    if strategy_rows:
        return [
            {
                "slug": str(row["slug"] or ""),
                "resolved_at": int(row["closed_at"] or 0),
                "pnl": round(float(row["realized_pnl"] or 0.0), 4),
                "notional": round(float(row["planned_budget"] or 0.0), 4),
                "legs": int(row["filled_orders"] or 0),
                "winning_outcome": str(row["winning_outcome"] or ""),
            }
            for row in strategy_rows
        ]

    rows = conn.execute(
        """
        SELECT ts, notes, pnl_delta, notional
        FROM executions
        WHERE mode = 'paper' AND notes LIKE 'vidarx_resolution:%'
        ORDER BY ts DESC
        LIMIT 400
        """
    ).fetchall()

    grouped: dict[str, dict] = {}
    for row in rows:
        notes = str(row["notes"] or "")
        parts = notes.split(":")
        slug = parts[1] if len(parts) > 1 else "desconocido"
        outcome = parts[2] if len(parts) > 2 else ""
        entry = grouped.setdefault(
            slug,
            {
                "slug": slug,
                "resolved_at": int(row["ts"]),
                "pnl": 0.0,
                "notional": 0.0,
                "legs": 0,
                "winning_outcome": outcome,
                "_best_leg_pnl": float("-inf"),
            },
        )
        pnl_delta = float(row["pnl_delta"] or 0.0)
        entry["resolved_at"] = max(int(row["ts"]), int(entry["resolved_at"]))
        entry["pnl"] += pnl_delta
        entry["notional"] += abs(float(row["notional"] or 0.0))
        entry["legs"] += 1
        if pnl_delta >= float(entry["_best_leg_pnl"]):
            entry["_best_leg_pnl"] = pnl_delta
            entry["winning_outcome"] = outcome

    ordered = sorted(grouped.values(), key=lambda item: int(item["resolved_at"]), reverse=True)[:limit]
    return [
        {
            "slug": str(item["slug"]),
            "resolved_at": int(item["resolved_at"]),
            "pnl": round(float(item["pnl"]), 4),
            "notional": round(float(item["notional"]), 4),
            "legs": int(item["legs"]),
            "winning_outcome": str(item["winning_outcome"] or ""),
        }
        for item in ordered
    ]


def _vidarx_setup_performance(conn: sqlite3.Connection, *, limit: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            price_mode,
            timing_regime,
            COUNT(*) AS windows,
            COALESCE(SUM(realized_pnl), 0) AS pnl_total,
            COALESCE(AVG(realized_pnl), 0) AS pnl_avg,
            COALESCE(SUM(planned_budget), 0) AS budget_total,
            COALESCE(AVG(primary_ratio), 0) AS primary_ratio_avg,
            SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins
        FROM strategy_windows
        WHERE status = 'closed'
        GROUP BY price_mode, timing_regime
        HAVING windows > 0
        ORDER BY pnl_total DESC, windows DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    items: list[dict] = []
    for row in rows:
        windows = int(row["windows"] or 0)
        wins = int(row["wins"] or 0)
        win_rate = (wins / windows) * 100 if windows > 0 else 0.0
        items.append(
            {
                "price_mode": str(row["price_mode"] or "-"),
                "timing_regime": str(row["timing_regime"] or "-"),
                "windows": windows,
                "wins": wins,
                "win_rate_pct": round(win_rate, 2),
                "pnl_total": round(float(row["pnl_total"] or 0.0), 4),
                "pnl_avg": round(float(row["pnl_avg"] or 0.0), 4),
                "budget_total": round(float(row["budget_total"] or 0.0), 4),
                "primary_ratio_avg": round(float(row["primary_ratio_avg"] or 0.0), 4),
            }
        )
    return items


def _bot_state_text(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,)).fetchone()
    if row is None or row["value"] is None:
        return ""
    return str(row["value"])


def _bot_state_float(conn: sqlite3.Connection, key: str) -> float:
    raw_value = _bot_state_text(conn, key)
    try:
        return float(raw_value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _bot_state_int(conn: sqlite3.Connection, key: str) -> int:
    raw_value = _bot_state_text(conn, key)
    try:
        return int(float(raw_value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_int(raw: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


def _available_to_trade(*, live_cash_balance: float, live_cash_allowance: float) -> float:
    balance = max(float(live_cash_balance or 0.0), 0.0)
    allowance = max(float(live_cash_allowance or 0.0), 0.0)
    if allowance <= 0:
        return balance
    return min(balance, allowance)


def _midpoint_for_asset(*, clob_host: str, asset: str) -> float | None:
    now = time.time()
    cached = _MIDPOINT_CACHE.get(asset)
    if cached and now < cached[1]:
        return cached[0]

    midpoint: float | None = None
    try:
        response = requests.get(
            f"{clob_host.rstrip('/')}/midpoint",
            params={"token_id": asset},
            timeout=4,
        )
        if response.status_code != 404:
            response.raise_for_status()
            payload = response.json()
            raw_mid = payload.get("mid")
            if raw_mid is not None:
                midpoint = float(raw_mid)
    except requests.RequestException:
        midpoint = None

    _MIDPOINT_CACHE[asset] = (midpoint, now + _MIDPOINT_CACHE_TTL_SECONDS)
    return midpoint


def _reset_runtime_state(db_path: Path) -> dict:
    tables = [
        "source_positions_current",
        "source_positions_history",
        "signals",
        "copy_positions",
        "executions",
        "daily_pnl",
        "selected_wallets",
        "position_mark_history",
        "trade_approvals",
    ]
    deleted: dict[str, int] = {}
    with _connect(db_path) as conn:
        for table in tables:
            count_row = conn.execute(f"SELECT COUNT(*) AS value FROM {table}").fetchone()
            deleted[table] = int(count_row["value"]) if count_row else 0
        with conn:
            for table in tables:
                conn.execute(f"DELETE FROM {table}")
    _MIDPOINT_CACHE.clear()
    return {"ok": True, "deleted": deleted, "reset_at_utc": datetime.now(timezone.utc).isoformat()}
