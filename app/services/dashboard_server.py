from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def run_dashboard_server(db_path: Path, static_dir: Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    handler_class = _build_handler(db_path=db_path, static_dir=static_dir)
    server = ThreadingHTTPServer((host, port), handler_class)
    print(f"dashboard => http://{host}:{port}")
    server.serve_forever()


def _build_handler(db_path: Path, static_dir: Path):
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/"):
                self.send_response(HTTPStatus.NO_CONTENT)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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
                self._json(_summary_payload(db_path))
                return
            if path == "/api/positions":
                self._json(_positions_payload(db_path))
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

        def _json(self, payload: dict | list) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return DashboardHandler


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    return connection


def _summary_payload(db_path: Path) -> dict:
    with _connect(db_path) as conn:
        open_positions = _single_float(conn, "SELECT COUNT(*) AS value FROM copy_positions")
        exposure = _single_float(conn, "SELECT COALESCE(SUM(ABS(size * avg_price)), 0) AS value FROM copy_positions")
        cumulative_pnl = _single_float(conn, "SELECT COALESCE(SUM(pnl), 0) AS value FROM daily_pnl")
        pending_signals = _single_float(conn, "SELECT COUNT(*) AS value FROM signals WHERE status='pending'")
        executed_signals = _single_float(conn, "SELECT COUNT(*) AS value FROM signals WHERE status='executed'")
        failed_signals = _single_float(conn, "SELECT COUNT(*) AS value FROM signals WHERE status='failed'")

    return {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "open_positions": int(open_positions),
        "exposure": round(exposure, 4),
        "cumulative_pnl": round(cumulative_pnl, 4),
        "pending_signals": int(pending_signals),
        "executed_signals": int(executed_signals),
        "failed_signals": int(failed_signals),
    }


def _positions_payload(db_path: Path) -> dict:
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


def _single_float(conn: sqlite3.Connection, query: str) -> float:
    row = conn.execute(query).fetchone()
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
