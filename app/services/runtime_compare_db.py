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
    remaining_cycle_budget REAL NOT NULL,
    effective_min_notional REAL NOT NULL,
    desired_up_ratio REAL NOT NULL,
    current_up_ratio REAL NOT NULL,
    window_seconds INTEGER NOT NULL,
    open_legs INTEGER NOT NULL,
    exposure REAL NOT NULL,
    spot_price REAL NOT NULL,
    official_price_to_beat REAL NOT NULL,
    fair_up REAL NOT NULL,
    fair_down REAL NOT NULL,
    reference_quality TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS runtime_compare_history (
    runtime_mode TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    market_title TEXT NOT NULL,
    ts INTEGER NOT NULL,
    window_status TEXT NOT NULL,
    realized_pnl REAL NOT NULL,
    deployed_notional REAL NOT NULL,
    filled_orders INTEGER NOT NULL,
    replenishment_count INTEGER NOT NULL,
    primary_ratio REAL NOT NULL,
    price_mode TEXT NOT NULL,
    cumulative_realized_pnl REAL NOT NULL,
    PRIMARY KEY (runtime_mode, market_slug)
);

CREATE TABLE IF NOT EXISTS runtime_compare_window_pairs (
    market_slug TEXT PRIMARY KEY,
    market_title TEXT NOT NULL,
    ts INTEGER NOT NULL,
    paper_status TEXT NOT NULL,
    shadow_status TEXT NOT NULL,
    paper_realized_pnl REAL NOT NULL,
    shadow_realized_pnl REAL NOT NULL,
    pnl_gap REAL NOT NULL,
    paper_deployed_notional REAL NOT NULL,
    shadow_deployed_notional REAL NOT NULL,
    deployed_gap REAL NOT NULL,
    paper_filled_orders INTEGER NOT NULL,
    shadow_filled_orders INTEGER NOT NULL,
    filled_orders_gap INTEGER NOT NULL,
    paper_cumulative_realized_pnl REAL NOT NULL,
    shadow_cumulative_realized_pnl REAL NOT NULL,
    cumulative_pnl_gap REAL NOT NULL
);
"""

_COMPARE_RUNTIME_FILES = {
    "paper": "bot.db",
    "shadow": "bot_shadow.db",
}
_COMPARE_HISTORY_LIMIT = 24


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
    history = _build_compare_history(data_dir=data_dir, limit=_COMPARE_HISTORY_LIMIT)

    now_ts = int(time.time())
    _write_compare_db(compare_db_path=compare_db_path, generated_at=now_ts, snapshots=snapshots, history=history)

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
        "history": history,
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
            SELECT slug, title, outcome, size, avg_price, condition_id
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
        condition_ids = sorted({str(row["condition_id"] or "").strip() for row in rows if str(row["condition_id"] or "").strip()})
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

        execution_filter_sql = "WHERE action = 'open' AND mode = ?"
        execution_params: list[str] = [strategy_runtime_mode]
        if condition_ids:
            placeholders = ", ".join("?" for _ in condition_ids)
            execution_filter_sql += f" AND condition_id IN ({placeholders})"
            execution_params.extend(condition_ids)

        execution_rows = conn.execute(
            f"""
            SELECT id, ts, status, action, side, asset, price, notional, notes
            FROM executions
            {execution_filter_sql}
            ORDER BY ts DESC
            LIMIT 50
            """,
            tuple(execution_params),
        ).fetchall()
        execution_agg = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(notional), 0) AS total_notional,
                COALESCE(AVG(notional), 0) AS avg_notional,
                COALESCE(MIN(notional), 0) AS min_notional,
                COALESCE(MAX(notional), 0) AS max_notional,
                COALESCE(MAX(ts), 0) AS last_ts
            FROM executions
            {execution_filter_sql}
            """,
            tuple(execution_params),
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
            "remaining_cycle_budget": round(
                max(_bot_state_float(conn, "strategy_cycle_budget") - total_exposure, 0.0),
                4,
            ),
            "effective_min_notional": round(_bot_state_float(conn, "strategy_effective_min_notional"), 4),
            "desired_up_ratio": round(_bot_state_float(conn, "strategy_desired_up_ratio"), 4),
            "current_up_ratio": round(_bot_state_float(conn, "strategy_current_up_ratio"), 4),
            "window_seconds": int(_bot_state_int(conn, "strategy_window_seconds")),
            "open_legs": int(len(rows)),
            "exposure": round(total_exposure, 4),
            "spot_price": round(_bot_state_float(conn, "strategy_spot_price"), 4),
            "official_price_to_beat": round(_bot_state_float(conn, "strategy_official_price_to_beat"), 4),
            "fair_up": round(_bot_state_float(conn, "strategy_spot_fair_up"), 6),
            "fair_down": round(_bot_state_float(conn, "strategy_spot_fair_down"), 6),
            "reference_quality": _bot_state_text(conn, "strategy_reference_quality"),
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
        "remaining_cycle_budget": 0.0,
        "effective_min_notional": 0.0,
        "desired_up_ratio": 0.0,
        "current_up_ratio": 0.0,
        "window_seconds": 0,
        "open_legs": 0,
        "exposure": 0.0,
        "spot_price": 0.0,
        "official_price_to_beat": 0.0,
        "fair_up": 0.0,
        "fair_down": 0.0,
        "reference_quality": "",
        "breakdown": [],
        "open_execution_count": 0,
        "open_total_notional": 0.0,
        "open_avg_notional": 0.0,
        "open_min_notional": 0.0,
        "open_max_notional": 0.0,
        "last_execution_ts": 0,
        "recent_executions": [],
    }


def _read_runtime_history(*, db_path: Path, runtime_mode: str, limit: int) -> list[dict]:
    if not db_path.exists():
        return []

    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                slug,
                title,
                COALESCE(closed_at, last_trade_at, opened_at, 0) AS ts,
                status,
                realized_pnl,
                deployed_notional,
                filled_orders,
                replenishment_count,
                primary_ratio,
                price_mode
            FROM strategy_windows
            WHERE slug <> ''
            ORDER BY COALESCE(closed_at, last_trade_at, opened_at, 0) DESC, slug DESC
            LIMIT ?
            """,
            (max(int(limit), 1),),
        ).fetchall()

    ordered_rows = list(reversed(rows))
    cumulative_realized_pnl = 0.0
    history: list[dict] = []
    for row in ordered_rows:
        cumulative_realized_pnl += float(row["realized_pnl"] or 0.0)
        history.append(
            {
                "runtime_mode": runtime_mode,
                "slug": str(row["slug"] or ""),
                "title": str(row["title"] or ""),
                "ts": int(row["ts"] or 0),
                "status": str(row["status"] or ""),
                "realized_pnl": round(float(row["realized_pnl"] or 0.0), 4),
                "deployed_notional": round(float(row["deployed_notional"] or 0.0), 4),
                "filled_orders": int(row["filled_orders"] or 0),
                "replenishment_count": int(row["replenishment_count"] or 0),
                "primary_ratio": round(float(row["primary_ratio"] or 0.0), 4),
                "price_mode": str(row["price_mode"] or ""),
                "cumulative_realized_pnl": round(cumulative_realized_pnl, 4),
            }
        )
    return history


def _empty_compare_history(*, limit: int) -> dict:
    return {
        "available": False,
        "window_limit": int(limit),
        "series": {"paper": [], "shadow": []},
        "points": [],
        "summary": {
            "paper_window_count": 0,
            "shadow_window_count": 0,
            "shared_window_count": 0,
            "paper_participation_pct": 0.0,
            "shadow_participation_pct": 0.0,
            "paper_total_realized_pnl": 0.0,
            "shadow_total_realized_pnl": 0.0,
            "cumulative_pnl_gap": 0.0,
            "paper_total_deployed_notional": 0.0,
            "shadow_total_deployed_notional": 0.0,
            "avg_deployed_gap": 0.0,
            "paper_avg_deployed_notional": 0.0,
            "shadow_avg_deployed_notional": 0.0,
            "paper_total_filled_orders": 0,
            "shadow_total_filled_orders": 0,
            "filled_orders_gap": 0,
            "paper_avg_filled_orders": 0.0,
            "shadow_avg_filled_orders": 0.0,
        },
    }


def _build_compare_history(*, data_dir: Path, limit: int) -> dict:
    safe_limit = max(int(limit), 1)
    series = {
        runtime_mode: _read_runtime_history(
            db_path=data_dir / db_name,
            runtime_mode=runtime_mode,
            limit=safe_limit,
        )
        for runtime_mode, db_name in _COMPARE_RUNTIME_FILES.items()
    }
    paper_series = series["paper"]
    shadow_series = series["shadow"]
    if not paper_series and not shadow_series:
        return _empty_compare_history(limit=safe_limit)

    paper_by_slug = {str(item["slug"]): item for item in paper_series if str(item["slug"])}
    shadow_by_slug = {str(item["slug"]): item for item in shadow_series if str(item["slug"])}

    recent_points: list[dict] = []
    for slug in set(paper_by_slug) | set(shadow_by_slug):
        paper_item = paper_by_slug.get(slug)
        shadow_item = shadow_by_slug.get(slug)
        recent_points.append(
            {
                "slug": str(slug),
                "title": str((paper_item or {}).get("title") or (shadow_item or {}).get("title") or ""),
                "ts": max(int((paper_item or {}).get("ts") or 0), int((shadow_item or {}).get("ts") or 0)),
                "paper_present": bool(paper_item),
                "shadow_present": bool(shadow_item),
                "paper_status": str((paper_item or {}).get("status") or "missing"),
                "shadow_status": str((shadow_item or {}).get("status") or "missing"),
                "paper_realized_pnl": round(float((paper_item or {}).get("realized_pnl") or 0.0), 4),
                "shadow_realized_pnl": round(float((shadow_item or {}).get("realized_pnl") or 0.0), 4),
                "paper_deployed_notional": round(float((paper_item or {}).get("deployed_notional") or 0.0), 4),
                "shadow_deployed_notional": round(float((shadow_item or {}).get("deployed_notional") or 0.0), 4),
                "paper_filled_orders": int((paper_item or {}).get("filled_orders") or 0),
                "shadow_filled_orders": int((shadow_item or {}).get("filled_orders") or 0),
            }
        )

    recent_points.sort(key=lambda item: (int(item["ts"]), str(item["slug"])))
    if len(recent_points) > safe_limit:
        recent_points = recent_points[-safe_limit:]

    paper_cumulative_realized_pnl = 0.0
    shadow_cumulative_realized_pnl = 0.0
    paper_window_count = 0
    shadow_window_count = 0
    shared_window_count = 0
    paper_total_realized_pnl = 0.0
    shadow_total_realized_pnl = 0.0
    paper_total_deployed_notional = 0.0
    shadow_total_deployed_notional = 0.0
    paper_total_filled_orders = 0
    shadow_total_filled_orders = 0
    points: list[dict] = []

    for item in recent_points:
        paper_present = bool(item["paper_present"])
        shadow_present = bool(item["shadow_present"])
        if paper_present:
            paper_window_count += 1
        if shadow_present:
            shadow_window_count += 1
        if paper_present and shadow_present:
            shared_window_count += 1

        paper_realized_pnl = float(item["paper_realized_pnl"] or 0.0)
        shadow_realized_pnl = float(item["shadow_realized_pnl"] or 0.0)
        paper_deployed_notional = float(item["paper_deployed_notional"] or 0.0)
        shadow_deployed_notional = float(item["shadow_deployed_notional"] or 0.0)
        paper_filled_orders = int(item["paper_filled_orders"] or 0)
        shadow_filled_orders = int(item["shadow_filled_orders"] or 0)

        paper_total_realized_pnl += paper_realized_pnl
        shadow_total_realized_pnl += shadow_realized_pnl
        paper_total_deployed_notional += paper_deployed_notional
        shadow_total_deployed_notional += shadow_deployed_notional
        paper_total_filled_orders += paper_filled_orders
        shadow_total_filled_orders += shadow_filled_orders

        paper_cumulative_realized_pnl += paper_realized_pnl
        shadow_cumulative_realized_pnl += shadow_realized_pnl
        points.append(
            {
                "slug": str(item["slug"]),
                "title": str(item["title"]),
                "ts": int(item["ts"] or 0),
                "paper_status": str(item["paper_status"]),
                "shadow_status": str(item["shadow_status"]),
                "paper_realized_pnl": round(paper_realized_pnl, 4),
                "shadow_realized_pnl": round(shadow_realized_pnl, 4),
                "pnl_gap": round(paper_realized_pnl - shadow_realized_pnl, 4),
                "paper_deployed_notional": round(paper_deployed_notional, 4),
                "shadow_deployed_notional": round(shadow_deployed_notional, 4),
                "deployed_gap": round(paper_deployed_notional - shadow_deployed_notional, 4),
                "paper_filled_orders": paper_filled_orders,
                "shadow_filled_orders": shadow_filled_orders,
                "filled_orders_gap": int(paper_filled_orders - shadow_filled_orders),
                "paper_cumulative_realized_pnl": round(paper_cumulative_realized_pnl, 4),
                "shadow_cumulative_realized_pnl": round(shadow_cumulative_realized_pnl, 4),
                "cumulative_pnl_gap": round(paper_cumulative_realized_pnl - shadow_cumulative_realized_pnl, 4),
            }
        )

    point_count = len(points)
    paper_participation_pct = (paper_window_count / point_count) * 100 if point_count > 0 else 0.0
    shadow_participation_pct = (shadow_window_count / point_count) * 100 if point_count > 0 else 0.0

    return {
        "available": bool(points),
        "window_limit": safe_limit,
        "series": series,
        "points": points,
        "summary": {
            "paper_window_count": int(paper_window_count),
            "shadow_window_count": int(shadow_window_count),
            "shared_window_count": int(shared_window_count),
            "paper_participation_pct": round(paper_participation_pct, 2),
            "shadow_participation_pct": round(shadow_participation_pct, 2),
            "paper_total_realized_pnl": round(paper_total_realized_pnl, 4),
            "shadow_total_realized_pnl": round(shadow_total_realized_pnl, 4),
            "cumulative_pnl_gap": round(paper_total_realized_pnl - shadow_total_realized_pnl, 4),
            "paper_total_deployed_notional": round(paper_total_deployed_notional, 4),
            "shadow_total_deployed_notional": round(shadow_total_deployed_notional, 4),
            "avg_deployed_gap": round(
                (paper_total_deployed_notional / paper_window_count if paper_window_count > 0 else 0.0)
                - (shadow_total_deployed_notional / shadow_window_count if shadow_window_count > 0 else 0.0),
                4,
            ),
            "paper_avg_deployed_notional": round(
                paper_total_deployed_notional / paper_window_count if paper_window_count > 0 else 0.0,
                4,
            ),
            "shadow_avg_deployed_notional": round(
                shadow_total_deployed_notional / shadow_window_count if shadow_window_count > 0 else 0.0,
                4,
            ),
            "paper_total_filled_orders": int(paper_total_filled_orders),
            "shadow_total_filled_orders": int(shadow_total_filled_orders),
            "filled_orders_gap": int(paper_total_filled_orders - shadow_total_filled_orders),
            "paper_avg_filled_orders": round(
                paper_total_filled_orders / paper_window_count if paper_window_count > 0 else 0.0,
                4,
            ),
            "shadow_avg_filled_orders": round(
                shadow_total_filled_orders / shadow_window_count if shadow_window_count > 0 else 0.0,
                4,
            ),
        },
    }


def _write_compare_db(*, compare_db_path: Path, generated_at: int, snapshots: dict[str, dict], history: dict) -> None:
    compare_db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(compare_db_path) as conn:
        with conn:
            conn.executescript(_COMPARE_SCHEMA)
            conn.execute("DELETE FROM runtime_compare_meta")
            conn.execute("DELETE FROM runtime_compare_snapshots")
            conn.execute("DELETE FROM runtime_compare_breakdown")
            conn.execute("DELETE FROM runtime_compare_recent_executions")
            conn.execute("DELETE FROM runtime_compare_history")
            conn.execute("DELETE FROM runtime_compare_window_pairs")
            conn.execute(
                "INSERT INTO runtime_compare_meta(key, value) VALUES (?, ?)",
                ("generated_at", str(generated_at)),
            )
            conn.execute(
                "INSERT INTO runtime_compare_meta(key, value) VALUES (?, ?)",
                ("history_limit", str(int(history.get("window_limit") or 0))),
            )

            for runtime_mode, snapshot in snapshots.items():
                conn.execute(
                    """
                    INSERT INTO runtime_compare_snapshots (
                        runtime_mode, generated_at, db_path, strategy_runtime_mode, market_slug, market_title,
                        price_mode, operability_state, last_note, cycle_budget, remaining_cycle_budget,
                        effective_min_notional, desired_up_ratio, current_up_ratio,
                        window_seconds, open_legs, exposure, spot_price, official_price_to_beat, fair_up,
                        fair_down, reference_quality, open_execution_count, open_total_notional,
                        open_avg_notional, open_min_notional, open_max_notional, last_execution_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        float(snapshot.get("remaining_cycle_budget") or 0.0),
                        float(snapshot.get("effective_min_notional") or 0.0),
                        float(snapshot.get("desired_up_ratio") or 0.0),
                        float(snapshot.get("current_up_ratio") or 0.0),
                        int(snapshot.get("window_seconds") or 0),
                        int(snapshot.get("open_legs") or 0),
                        float(snapshot.get("exposure") or 0.0),
                        float(snapshot.get("spot_price") or 0.0),
                        float(snapshot.get("official_price_to_beat") or 0.0),
                        float(snapshot.get("fair_up") or 0.0),
                        float(snapshot.get("fair_down") or 0.0),
                        str(snapshot.get("reference_quality") or ""),
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
            for runtime_mode, items in (history.get("series") or {}).items():
                for item in items or []:
                    conn.execute(
                        """
                        INSERT INTO runtime_compare_history(
                            runtime_mode, market_slug, market_title, ts, window_status, realized_pnl,
                            deployed_notional, filled_orders, replenishment_count, primary_ratio,
                            price_mode, cumulative_realized_pnl
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(runtime_mode or ""),
                            str(item.get("slug") or ""),
                            str(item.get("title") or ""),
                            int(item.get("ts") or 0),
                            str(item.get("status") or ""),
                            float(item.get("realized_pnl") or 0.0),
                            float(item.get("deployed_notional") or 0.0),
                            int(item.get("filled_orders") or 0),
                            int(item.get("replenishment_count") or 0),
                            float(item.get("primary_ratio") or 0.0),
                            str(item.get("price_mode") or ""),
                            float(item.get("cumulative_realized_pnl") or 0.0),
                        ),
                    )
            for item in history.get("points") or []:
                conn.execute(
                    """
                    INSERT INTO runtime_compare_window_pairs(
                        market_slug, market_title, ts, paper_status, shadow_status,
                        paper_realized_pnl, shadow_realized_pnl, pnl_gap,
                        paper_deployed_notional, shadow_deployed_notional, deployed_gap,
                        paper_filled_orders, shadow_filled_orders, filled_orders_gap,
                        paper_cumulative_realized_pnl, shadow_cumulative_realized_pnl, cumulative_pnl_gap
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(item.get("slug") or ""),
                        str(item.get("title") or ""),
                        int(item.get("ts") or 0),
                        str(item.get("paper_status") or ""),
                        str(item.get("shadow_status") or ""),
                        float(item.get("paper_realized_pnl") or 0.0),
                        float(item.get("shadow_realized_pnl") or 0.0),
                        float(item.get("pnl_gap") or 0.0),
                        float(item.get("paper_deployed_notional") or 0.0),
                        float(item.get("shadow_deployed_notional") or 0.0),
                        float(item.get("deployed_gap") or 0.0),
                        int(item.get("paper_filled_orders") or 0),
                        int(item.get("shadow_filled_orders") or 0),
                        int(item.get("filled_orders_gap") or 0),
                        float(item.get("paper_cumulative_realized_pnl") or 0.0),
                        float(item.get("shadow_cumulative_realized_pnl") or 0.0),
                        float(item.get("cumulative_pnl_gap") or 0.0),
                    ),
                )
