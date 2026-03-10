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
    server = ThreadingHTTPServer((host, port), handler_class)
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
            if path == "/api/signals":
                query = parse_qs(parsed.query)
                limit = _safe_int(query.get("limit", ["100"])[0], default=100, minimum=1, maximum=500)
                self._json(_signals_payload(db_path, limit=limit))
                return
            if path == "/api/selected-wallets":
                query = parse_qs(parsed.query)
                limit = _safe_int(query.get("limit", ["5"])[0], default=5, minimum=1, maximum=20)
                self._json(_selected_wallets_payload(db_path, limit=limit))
                return
            if path == "/api/risk-blocks":
                query = parse_qs(parsed.query)
                limit = _safe_int(query.get("limit", ["5"])[0], default=5, minimum=1, maximum=20)
                hours = _safe_int(query.get("hours", ["24"])[0], default=24, minimum=1, maximum=24 * 30)
                self._json(_risk_blocks_payload(db_path, limit=limit, hours=hours))
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
        positions = conn.execute(
            "SELECT asset, size, avg_price FROM copy_positions"
        ).fetchall()

    unrealized_pnl = 0.0
    exposure_mark = 0.0
    for row in positions:
        asset = str(row["asset"])
        size = float(row["size"])
        avg_price = float(row["avg_price"])
        mark_price = _midpoint_for_asset(clob_host=clob_host, asset=asset)
        if mark_price is None:
            mark_price = avg_price
        unrealized_pnl += (mark_price - avg_price) * size
        exposure_mark += abs(size * mark_price)

    pnl_total = realized_pnl + unrealized_pnl
    live_mode_active = execution_mode == "live" and live_trading_enabled

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


def _safe_int(raw: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, value))


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
