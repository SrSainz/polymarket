from __future__ import annotations

import sqlite3
import time
from pathlib import Path


_COMPARE_SCHEMA = """
CREATE TABLE IF NOT EXISTS runtime_compare_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_compare_snapshots (
    runtime_mode TEXT PRIMARY KEY,
    generated_at INTEGER NOT NULL,
    db_path TEXT NOT NULL,
    strategy_runtime_mode TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    market_title TEXT NOT NULL,
    price_mode TEXT NOT NULL,
    operability_state TEXT NOT NULL,
    last_note TEXT NOT NULL,
    cycle_budget REAL NOT NULL,
    desired_up_ratio REAL NOT NULL,
    current_up_ratio REAL NOT NULL,
    window_seconds INTEGER NOT NULL,
    open_legs INTEGER NOT NULL,
    exposure REAL NOT NULL,
    open_execution_count INTEGER NOT NULL,
    open_total_notional REAL NOT NULL,
    open_avg_notional REAL NOT NULL,
    open_min_notional REAL NOT NULL,
    open_max_notional REAL NOT NULL,
    last_execution_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_compare_breakdown (
    runtime_mode TEXT NOT NULL,
    outcome TEXT NOT NULL,
    shares REAL NOT NULL,
    exposure REAL NOT NULL,
    share_pct REAL NOT NULL,
    PRIMARY KEY (runtime_mode, outcome)
);

CREATE TABLE IF NOT EXISTS runtime_compare_recent_executions (
    runtime_mode TEXT NOT NULL,
    execution_id INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    status TEXT NOT NULL,
    action TEXT NOT NULL,
    side TEXT NOT NULL,
    asset TEXT NOT NULL,
    price REAL NOT NULL,
    notional REAL NOT NULL,
    notes TEXT NOT NULL,
    PRIMARY KEY (runtime_mode, execution_id)
);
"""

_COMPARE_RUNTIME_FILES = {
    "paper": "bot.db",
    "shadow": "bot_shadow.db",
}


def build_runtime_compare_payload(*, data_dir: Path, target_slug: str = "", target_title: str = "") -> dict:
    compare_db_path = data_dir / "runtime_compare.db"
    snapshots = {
        runtime_mode: _read_runtime_snapshot(
            db_path=data_dir / db_name,
            runtime_mode=runtime_mode,
            target_slug=target_slug,
            target_title=target_title,
        )
        for runtime_mode, db_name in _COMPARE_RUNTIME_FILES.items()
    }

    now_ts = int(time.time())
    _write_compare_db(compare_db_path=compare_db_path, generated_at=now_ts, snapshots=snapshots)

    paper = snapshots["paper"]
    shadow = snapshots["shadow"]
    shared_slug = str(shadow.get("slug") or "")
    same_window = bool(shared_slug and shared_slug == str(paper.get("slug") or ""))
    status = "shared" if same_window else "mismatch"
    if not paper.get("has_window"):
        status = "paper-missing"

    return {
        "available": bool(paper.get("db_exists") or shadow.get("db_exists")),
        "db_path": str(compare_db_path),
        "generated_at": now_ts,
        "status": status,
        "same_window": same_window,
        "shared_slug": shared_slug,
        "paper": paper,
        "shadow": shadow,
    }


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _bot_state_text(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,)).fetchone()
    if row is None or row["value"] is None:
        return ""
    return str(row["value"])


def _bot_state_float(conn: sqlite3.Connection, key: str) -> float:
    raw = _bot_state_text(conn, key)
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _bot_state_int(conn: sqlite3.Connection, key: str) -> int:
    raw = _bot_state_text(conn, key)
    try:
        return int(float(raw or 0))
    except (TypeError, ValueError):
        return 0


def _read_runtime_snapshot(*, db_path: Path, runtime_mode: str, target_slug: str, target_title: str) -> dict:
    if not db_path.exists():
        return _empty_runtime_snapshot(runtime_mode=runtime_mode, db_path=db_path)

    with _connect(db_path) as conn:
        strategy_runtime_mode = _bot_state_text(conn, "strategy_runtime_mode") or runtime_mode
        market_slug = _bot_state_text(conn, "strategy_market_slug")
        market_title = _bot_state_text(conn, "strategy_market_title")
        effective_slug = str(target_slug or market_slug or "").strip()
        effective_title = str(target_title or market_title or "").strip()

        rows = conn.execute(
            """
            SELECT slug, title, outcome, size, avg_price
            FROM copy_positions
            WHERE (? <> '' AND slug = ?)
               OR (? = '' AND ? <> '' AND title = ?)
            ORDER BY outcome ASC
            """,
            (
                effective_slug,
                effective_slug,
                effective_slug,
                effective_title,
                effective_title,
            ),
        ).fetchall()

        breakdown: list[dict] = []
        total_exposure = 0.0
        total_shares = 0.0
        for row in rows:
            shares = abs(float(row["size"] or 0.0))
            exposure = abs(float(row["size"] or 0.0) * float(row["avg_price"] or 0.0))
            total_exposure += exposure
            total_shares += shares
            breakdown.append(
                {
                    "outcome": str(row["outcome"] or ""),
                    "shares": round(shares, 4),
                    "exposure": round(exposure, 4),
                }
            )
        for item in breakdown:
            item["share_pct"] = round((float(item["shares"]) / total_shares) * 100, 2) if total_shares > 0 else 0.0

        execution_rows = conn.execute(
            """
            SELECT id, ts, status, action, side, asset, price, notional, notes
            FROM executions
            WHERE action = 'open' AND mode = ?
            ORDER BY ts DESC
            LIMIT 50
            """,
            (strategy_runtime_mode,),
        ).fetchall()
        execution_agg = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(notional), 0) AS total_notional,
                COALESCE(AVG(notional), 0) AS avg_notional,
                COALESCE(MIN(notional), 0) AS min_notional,
                COALESCE(MAX(notional), 0) AS max_notional,
                COALESCE(MAX(ts), 0) AS last_ts
            FROM executions
            WHERE action = 'open' AND mode = ?
            """,
            (strategy_runtime_mode,),
        ).fetchone()

        recent_executions = [
            {
                "execution_id": int(row["id"]),
                "ts": int(row["ts"]),
                "status": str(row["status"] or ""),
                "action": str(row["action"] or ""),
                "side": str(row["side"] or ""),
                "asset": str(row["asset"] or ""),
                "price": round(float(row["price"] or 0.0), 6),
                "notional": round(float(row["notional"] or 0.0), 4),
                "notes": str(row["notes"] or ""),
            }
            for row in execution_rows
        ]

        return {
            "db_exists": True,
            "db_path": str(db_path),
            "runtime_mode": strategy_runtime_mode,
            "slug": effective_slug,
            "title": effective_title,
            "has_window": bool(effective_slug or effective_title),
            "price_mode": _bot_state_text(conn, "strategy_price_mode"),
            "operability_state": _bot_state_text(conn, "strategy_operability_state"),
            "last_note": _bot_state_text(conn, "strategy_last_note"),
            "cycle_budget": round(_bot_state_float(conn, "strategy_cycle_budget"), 4),
            "desired_up_ratio": round(_bot_state_float(conn, "strategy_desired_up_ratio"), 4),
            "current_up_ratio": round(_bot_state_float(conn, "strategy_current_up_ratio"), 4),
            "window_seconds": int(_bot_state_int(conn, "strategy_window_seconds")),
            "open_legs": int(len(rows)),
            "exposure": round(total_exposure, 4),
            "breakdown": breakdown,
            "open_execution_count": int(execution_agg["total"] or 0),
            "open_total_notional": round(float(execution_agg["total_notional"] or 0.0), 4),
            "open_avg_notional": round(float(execution_agg["avg_notional"] or 0.0), 4),
            "open_min_notional": round(float(execution_agg["min_notional"] or 0.0), 4),
            "open_max_notional": round(float(execution_agg["max_notional"] or 0.0), 4),
            "last_execution_ts": int(execution_agg["last_ts"] or 0),
            "recent_executions": recent_executions,
        }


def _empty_runtime_snapshot(*, runtime_mode: str, db_path: Path) -> dict:
    return {
        "db_exists": False,
        "db_path": str(db_path),
        "runtime_mode": runtime_mode,
        "slug": "",
        "title": "",
        "has_window": False,
        "price_mode": "",
        "operability_state": "",
        "last_note": "",
        "cycle_budget": 0.0,
        "desired_up_ratio": 0.0,
        "current_up_ratio": 0.0,
        "window_seconds": 0,
        "open_legs": 0,
        "exposure": 0.0,
        "breakdown": [],
        "open_execution_count": 0,
        "open_total_notional": 0.0,
        "open_avg_notional": 0.0,
        "open_min_notional": 0.0,
        "open_max_notional": 0.0,
        "last_execution_ts": 0,
        "recent_executions": [],
    }


def _write_compare_db(*, compare_db_path: Path, generated_at: int, snapshots: dict[str, dict]) -> None:
    compare_db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(compare_db_path) as conn:
        with conn:
            conn.executescript(_COMPARE_SCHEMA)
            conn.execute("DELETE FROM runtime_compare_meta")
            conn.execute("DELETE FROM runtime_compare_snapshots")
            conn.execute("DELETE FROM runtime_compare_breakdown")
            conn.execute("DELETE FROM runtime_compare_recent_executions")
            conn.execute(
                "INSERT INTO runtime_compare_meta(key, value) VALUES (?, ?)",
                ("generated_at", str(generated_at)),
            )

            for runtime_mode, snapshot in snapshots.items():
                conn.execute(
                    """
                    INSERT INTO runtime_compare_snapshots (
                        runtime_mode, generated_at, db_path, strategy_runtime_mode, market_slug, market_title,
                        price_mode, operability_state, last_note, cycle_budget, desired_up_ratio, current_up_ratio,
                        window_seconds, open_legs, exposure, open_execution_count, open_total_notional,
                        open_avg_notional, open_min_notional, open_max_notional, last_execution_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        runtime_mode,
                        generated_at,
                        str(snapshot.get("db_path") or ""),
                        str(snapshot.get("runtime_mode") or runtime_mode),
                        str(snapshot.get("slug") or ""),
                        str(snapshot.get("title") or ""),
                        str(snapshot.get("price_mode") or ""),
                        str(snapshot.get("operability_state") or ""),
                        str(snapshot.get("last_note") or ""),
                        float(snapshot.get("cycle_budget") or 0.0),
                        float(snapshot.get("desired_up_ratio") or 0.0),
                        float(snapshot.get("current_up_ratio") or 0.0),
                        int(snapshot.get("window_seconds") or 0),
                        int(snapshot.get("open_legs") or 0),
                        float(snapshot.get("exposure") or 0.0),
                        int(snapshot.get("open_execution_count") or 0),
                        float(snapshot.get("open_total_notional") or 0.0),
                        float(snapshot.get("open_avg_notional") or 0.0),
                        float(snapshot.get("open_min_notional") or 0.0),
                        float(snapshot.get("open_max_notional") or 0.0),
                        int(snapshot.get("last_execution_ts") or 0),
                    ),
                )
                for item in snapshot.get("breakdown") or []:
                    conn.execute(
                        """
                        INSERT INTO runtime_compare_breakdown(runtime_mode, outcome, shares, exposure, share_pct)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            runtime_mode,
                            str(item.get("outcome") or ""),
                            float(item.get("shares") or 0.0),
                            float(item.get("exposure") or 0.0),
                            float(item.get("share_pct") or 0.0),
                        ),
                    )
                for item in snapshot.get("recent_executions") or []:
                    conn.execute(
                        """
                        INSERT INTO runtime_compare_recent_executions(
                            runtime_mode, execution_id, ts, status, action, side, asset, price, notional, notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            runtime_mode,
                            int(item.get("execution_id") or 0),
                            int(item.get("ts") or 0),
                            str(item.get("status") or ""),
                            str(item.get("action") or ""),
                            str(item.get("side") or ""),
                            str(item.get("asset") or ""),
                            float(item.get("price") or 0.0),
                            float(item.get("notional") or 0.0),
                            str(item.get("notes") or ""),
                        ),
                    )
