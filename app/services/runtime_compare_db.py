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

CREATE TABLE IF NOT EXISTS runtime_compare_samples (
    runtime_mode TEXT NOT NULL,
    sample_ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    market_title TEXT NOT NULL,
    price_mode TEXT NOT NULL,
    operability_state TEXT NOT NULL,
    cycle_budget REAL NOT NULL,
    remaining_cycle_budget REAL NOT NULL,
    effective_min_notional REAL NOT NULL,
    open_legs INTEGER NOT NULL,
    exposure REAL NOT NULL,
    open_execution_count INTEGER NOT NULL,
    open_total_notional REAL NOT NULL,
    desired_up_ratio REAL NOT NULL,
    current_up_ratio REAL NOT NULL,
    spot_price REAL NOT NULL,
    fair_up REAL NOT NULL,
    fair_down REAL NOT NULL,
    PRIMARY KEY (runtime_mode, sample_ts)
);
"""

_COMPARE_RUNTIME_FILES = {
    "paper": "bot.db",
    "shadow": "bot_shadow.db",
}
_COMPARE_HISTORY_LIMIT = 24
_COMPARE_SAMPLE_LIMIT = 60
_COMPARE_SAMPLE_RETENTION = 720


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
    sample_history = _read_compare_samples(compare_db_path=compare_db_path, limit=_COMPARE_SAMPLE_LIMIT)
    history = {
        **history,
        "sample_available": bool(sample_history.get("available")),
        "sample_limit": int(sample_history.get("sample_limit") or 0),
        "sample_series": sample_history.get("series") or {"paper": [], "shadow": []},
        "sample_summary": sample_history.get("summary") or {},
    }

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
        closed_window_agg = conn.execute(
            """
            SELECT
                COUNT(*) AS closed_count,
                COALESCE(SUM(realized_pnl), 0) AS total_realized_pnl,
                COALESCE(SUM(deployed_notional), 0) AS total_deployed_notional,
                COALESCE(SUM(filled_orders), 0) AS total_filled_orders
            FROM strategy_windows
            WHERE slug <> '' AND status = 'closed'
            """
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
            "official_price_slug": _bot_state_text(conn, "strategy_official_price_slug"),
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
            "closed_window_count": int(closed_window_agg["closed_count"] or 0),
            "total_realized_pnl": round(float(closed_window_agg["total_realized_pnl"] or 0.0), 4),
            "historical_deployed_notional": round(float(closed_window_agg["total_deployed_notional"] or 0.0), 4),
            "historical_filled_orders": int(closed_window_agg["total_filled_orders"] or 0),
            "recent_executions": recent_executions,
        }


def _empty_runtime_snapshot(*, runtime_mode: str, db_path: Path) -> dict:
    return {
        "db_exists": False,
        "db_path": str(db_path),
        "runtime_mode": runtime_mode,
        "slug": "",
        "title": "",
        "official_price_slug": "",
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
        "closed_window_count": 0,
        "total_realized_pnl": 0.0,
        "historical_deployed_notional": 0.0,
        "historical_filled_orders": 0,
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
                condition_id,
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
        execution_metrics = _window_execution_metrics(
            conn,
            runtime_mode=runtime_mode,
            condition_id=str(row["condition_id"] or ""),
        )
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
                "open_asset_count": int(execution_metrics["open_asset_count"]),
                "two_sided": bool(execution_metrics["two_sided"]),
                "one_sided": bool(execution_metrics["one_sided"]),
                "settlement_count": int(execution_metrics["settlement_count"]),
                "settlement_visible": bool(execution_metrics["settlement_visible"]),
                "close_execution_count": int(execution_metrics["close_execution_count"]),
                "open_cadence_seconds": round(float(execution_metrics["open_cadence_seconds"]), 4),
                "open_span_seconds": round(float(execution_metrics["open_span_seconds"]), 4),
            }
        )
    return history


def _window_execution_metrics(
    conn: sqlite3.Connection,
    *,
    runtime_mode: str,
    condition_id: str,
) -> dict[str, float | int | bool]:
    if not str(condition_id or "").strip():
        return {
            "open_asset_count": 0,
            "two_sided": False,
            "one_sided": False,
            "settlement_count": 0,
            "settlement_visible": False,
            "close_execution_count": 0,
            "open_cadence_seconds": 0.0,
            "open_span_seconds": 0.0,
        }

    rows = conn.execute(
        """
        SELECT ts, status, action, asset, notes
        FROM executions
        WHERE mode = ? AND condition_id = ?
        ORDER BY ts ASC, id ASC
        """,
        (runtime_mode, condition_id),
    ).fetchall()

    open_rows = [
        row
        for row in rows
        if str(row["status"] or "").strip().lower() == "filled" and str(row["action"] or "").strip().lower() == "open"
    ]
    open_assets = {
        str(row["asset"] or "").strip()
        for row in open_rows
        if str(row["asset"] or "").strip()
    }
    open_ts = [int(row["ts"] or 0) for row in open_rows if int(row["ts"] or 0) > 0]
    open_intervals = [
        float(curr - prev)
        for prev, curr in zip(open_ts, open_ts[1:])
        if curr > prev
    ]

    settlement_count = 0
    close_execution_count = 0
    for row in rows:
        if str(row["status"] or "").strip().lower() != "filled":
            continue
        action = str(row["action"] or "").strip().lower()
        if action != "close":
            continue
        close_execution_count += 1
        note = str(row["notes"] or "").strip().lower()
        if note.startswith("strategy_resolution:"):
            settlement_count += 1

    open_asset_count = len(open_assets)
    return {
        "open_asset_count": open_asset_count,
        "two_sided": open_asset_count >= 2,
        "one_sided": open_asset_count == 1,
        "settlement_count": settlement_count,
        "settlement_visible": settlement_count > 0,
        "close_execution_count": close_execution_count,
        "open_cadence_seconds": _average(open_intervals),
        "open_span_seconds": float(open_ts[-1] - open_ts[0]) if len(open_ts) >= 2 else 0.0,
    }


def _average(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _empty_compare_history(*, limit: int) -> dict:
    return {
        "available": False,
        "window_limit": int(limit),
        "series": {"paper": [], "shadow": []},
        "points": [],
        "summary": {
            "point_count": 0,
            "paper_window_count": 0,
            "shadow_window_count": 0,
            "shared_window_count": 0,
            "paper_active_window_count": 0,
            "shadow_active_window_count": 0,
            "paper_participation_pct": 0.0,
            "shadow_participation_pct": 0.0,
            "paper_comparable_realized_pnl": 0.0,
            "shadow_comparable_realized_pnl": 0.0,
            "comparable_pnl_gap": 0.0,
            "paper_comparable_deployed_notional": 0.0,
            "shadow_comparable_deployed_notional": 0.0,
            "paper_comparable_filled_orders": 0,
            "shadow_comparable_filled_orders": 0,
            "comparable_filled_orders_gap": 0,
            "paper_total_realized_pnl": 0.0,
            "shadow_total_realized_pnl": 0.0,
            "total_pnl_gap": 0.0,
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
            "paper_two_sided_window_count": 0,
            "shadow_two_sided_window_count": 0,
            "paper_one_sided_window_count": 0,
            "shadow_one_sided_window_count": 0,
            "paper_two_sided_window_pct": 0.0,
            "shadow_two_sided_window_pct": 0.0,
            "paper_settlement_window_count": 0,
            "shadow_settlement_window_count": 0,
            "paper_settlement_window_pct": 0.0,
            "shadow_settlement_window_pct": 0.0,
            "paper_avg_open_cadence_seconds": 0.0,
            "shadow_avg_open_cadence_seconds": 0.0,
            "paper_avg_open_span_seconds": 0.0,
            "shadow_avg_open_span_seconds": 0.0,
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
    paper_active_window_count = 0
    shadow_active_window_count = 0
    paper_comparable_realized_pnl = 0.0
    shadow_comparable_realized_pnl = 0.0
    paper_comparable_deployed_notional = 0.0
    shadow_comparable_deployed_notional = 0.0
    paper_comparable_filled_orders = 0
    shadow_comparable_filled_orders = 0
    paper_total_realized_pnl = 0.0
    shadow_total_realized_pnl = 0.0
    paper_total_deployed_notional = 0.0
    shadow_total_deployed_notional = 0.0
    paper_total_filled_orders = 0
    shadow_total_filled_orders = 0
    paper_two_sided_window_count = 0
    shadow_two_sided_window_count = 0
    paper_one_sided_window_count = 0
    shadow_one_sided_window_count = 0
    paper_settlement_window_count = 0
    shadow_settlement_window_count = 0
    paper_cadence_samples: list[float] = []
    shadow_cadence_samples: list[float] = []
    paper_span_samples: list[float] = []
    shadow_span_samples: list[float] = []
    points: list[dict] = []

    for item in recent_points:
        slug = str(item["slug"] or "")
        paper_item = paper_by_slug.get(slug)
        shadow_item = shadow_by_slug.get(slug)
        paper_present = bool(item["paper_present"])
        shadow_present = bool(item["shadow_present"])
        if paper_present:
            paper_window_count += 1
        if shadow_present:
            shadow_window_count += 1

        paper_realized_pnl = float(item["paper_realized_pnl"] or 0.0)
        shadow_realized_pnl = float(item["shadow_realized_pnl"] or 0.0)
        paper_deployed_notional = float(item["paper_deployed_notional"] or 0.0)
        shadow_deployed_notional = float(item["shadow_deployed_notional"] or 0.0)
        paper_filled_orders = int(item["paper_filled_orders"] or 0)
        shadow_filled_orders = int(item["shadow_filled_orders"] or 0)
        paper_two_sided = bool((paper_item or {}).get("two_sided"))
        shadow_two_sided = bool((shadow_item or {}).get("two_sided"))
        paper_settlement_visible = bool((paper_item or {}).get("settlement_visible"))
        shadow_settlement_visible = bool((shadow_item or {}).get("settlement_visible"))
        paper_open_cadence_seconds = float((paper_item or {}).get("open_cadence_seconds") or 0.0)
        shadow_open_cadence_seconds = float((shadow_item or {}).get("open_cadence_seconds") or 0.0)
        paper_open_span_seconds = float((paper_item or {}).get("open_span_seconds") or 0.0)
        shadow_open_span_seconds = float((shadow_item or {}).get("open_span_seconds") or 0.0)
        paper_active = paper_present and (paper_deployed_notional > 0 or paper_filled_orders > 0)
        shadow_active = shadow_present and (shadow_deployed_notional > 0 or shadow_filled_orders > 0)

        if paper_active:
            paper_active_window_count += 1
            if paper_two_sided:
                paper_two_sided_window_count += 1
            else:
                paper_one_sided_window_count += 1
            if paper_settlement_visible:
                paper_settlement_window_count += 1
            if paper_open_cadence_seconds > 0:
                paper_cadence_samples.append(paper_open_cadence_seconds)
            if paper_open_span_seconds > 0:
                paper_span_samples.append(paper_open_span_seconds)

        if shadow_active:
            shadow_active_window_count += 1
            if shadow_two_sided:
                shadow_two_sided_window_count += 1
            else:
                shadow_one_sided_window_count += 1
            if shadow_settlement_visible:
                shadow_settlement_window_count += 1
            if shadow_open_cadence_seconds > 0:
                shadow_cadence_samples.append(shadow_open_cadence_seconds)
            if shadow_open_span_seconds > 0:
                shadow_span_samples.append(shadow_open_span_seconds)

        if paper_present and shadow_present:
            shared_window_count += 1
            paper_comparable_realized_pnl += paper_realized_pnl
            shadow_comparable_realized_pnl += shadow_realized_pnl
            paper_comparable_deployed_notional += paper_deployed_notional
            shadow_comparable_deployed_notional += shadow_deployed_notional
            paper_comparable_filled_orders += paper_filled_orders
            shadow_comparable_filled_orders += shadow_filled_orders

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
                "paper_two_sided": paper_two_sided,
                "shadow_two_sided": shadow_two_sided,
                "paper_settlement_visible": paper_settlement_visible,
                "shadow_settlement_visible": shadow_settlement_visible,
                "paper_open_cadence_seconds": round(paper_open_cadence_seconds, 4),
                "shadow_open_cadence_seconds": round(shadow_open_cadence_seconds, 4),
                "paper_open_span_seconds": round(paper_open_span_seconds, 4),
                "shadow_open_span_seconds": round(shadow_open_span_seconds, 4),
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
            "point_count": int(point_count),
            "paper_window_count": int(paper_window_count),
            "shadow_window_count": int(shadow_window_count),
            "shared_window_count": int(shared_window_count),
            "paper_active_window_count": int(paper_active_window_count),
            "shadow_active_window_count": int(shadow_active_window_count),
            "paper_participation_pct": round(paper_participation_pct, 2),
            "shadow_participation_pct": round(shadow_participation_pct, 2),
            "paper_comparable_realized_pnl": round(paper_comparable_realized_pnl, 4),
            "shadow_comparable_realized_pnl": round(shadow_comparable_realized_pnl, 4),
            "comparable_pnl_gap": round(paper_comparable_realized_pnl - shadow_comparable_realized_pnl, 4),
            "paper_comparable_deployed_notional": round(paper_comparable_deployed_notional, 4),
            "shadow_comparable_deployed_notional": round(shadow_comparable_deployed_notional, 4),
            "paper_comparable_filled_orders": int(paper_comparable_filled_orders),
            "shadow_comparable_filled_orders": int(shadow_comparable_filled_orders),
            "comparable_filled_orders_gap": int(paper_comparable_filled_orders - shadow_comparable_filled_orders),
            "paper_total_realized_pnl": round(paper_total_realized_pnl, 4),
            "shadow_total_realized_pnl": round(shadow_total_realized_pnl, 4),
            "total_pnl_gap": round(paper_total_realized_pnl - shadow_total_realized_pnl, 4),
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
            "paper_two_sided_window_count": int(paper_two_sided_window_count),
            "shadow_two_sided_window_count": int(shadow_two_sided_window_count),
            "paper_one_sided_window_count": int(paper_one_sided_window_count),
            "shadow_one_sided_window_count": int(shadow_one_sided_window_count),
            "paper_two_sided_window_pct": round(
                (paper_two_sided_window_count / paper_active_window_count) * 100 if paper_active_window_count > 0 else 0.0,
                2,
            ),
            "shadow_two_sided_window_pct": round(
                (shadow_two_sided_window_count / shadow_active_window_count) * 100 if shadow_active_window_count > 0 else 0.0,
                2,
            ),
            "paper_settlement_window_count": int(paper_settlement_window_count),
            "shadow_settlement_window_count": int(shadow_settlement_window_count),
            "paper_settlement_window_pct": round(
                (paper_settlement_window_count / paper_active_window_count) * 100 if paper_active_window_count > 0 else 0.0,
                2,
            ),
            "shadow_settlement_window_pct": round(
                (shadow_settlement_window_count / shadow_active_window_count) * 100 if shadow_active_window_count > 0 else 0.0,
                2,
            ),
            "paper_avg_open_cadence_seconds": round(_average(paper_cadence_samples), 4),
            "shadow_avg_open_cadence_seconds": round(_average(shadow_cadence_samples), 4),
            "paper_avg_open_span_seconds": round(_average(paper_span_samples), 4),
            "shadow_avg_open_span_seconds": round(_average(shadow_span_samples), 4),
        },
    }


def _empty_compare_samples(*, limit: int) -> dict:
    return {
        "available": False,
        "sample_limit": int(limit),
        "series": {"paper": [], "shadow": []},
        "summary": {
            "paper_sample_count": 0,
            "shadow_sample_count": 0,
            "paper_latest_notional": 0.0,
            "shadow_latest_notional": 0.0,
            "notional_gap": 0.0,
            "paper_latest_exposure": 0.0,
            "shadow_latest_exposure": 0.0,
            "exposure_gap": 0.0,
        },
    }


def _read_compare_samples(*, compare_db_path: Path, limit: int) -> dict:
    safe_limit = max(int(limit), 1)
    if not compare_db_path.exists():
        return _empty_compare_samples(limit=safe_limit)

    with _connect(compare_db_path) as conn:
        conn.executescript(_COMPARE_SCHEMA)
        series: dict[str, list[dict]] = {}
        for runtime_mode in _COMPARE_RUNTIME_FILES:
            rows = conn.execute(
                """
                SELECT
                    sample_ts,
                    market_slug,
                    market_title,
                    price_mode,
                    operability_state,
                    cycle_budget,
                    remaining_cycle_budget,
                    effective_min_notional,
                    open_legs,
                    exposure,
                    open_execution_count,
                    open_total_notional,
                    desired_up_ratio,
                    current_up_ratio,
                    spot_price,
                    fair_up,
                    fair_down
                FROM runtime_compare_samples
                WHERE runtime_mode = ?
                ORDER BY sample_ts DESC
                LIMIT ?
                """,
                (runtime_mode, safe_limit),
            ).fetchall()
            series[runtime_mode] = [
                {
                    "ts": int(row["sample_ts"] or 0),
                    "slug": str(row["market_slug"] or ""),
                    "title": str(row["market_title"] or ""),
                    "price_mode": str(row["price_mode"] or ""),
                    "operability_state": str(row["operability_state"] or ""),
                    "cycle_budget": round(float(row["cycle_budget"] or 0.0), 4),
                    "remaining_cycle_budget": round(float(row["remaining_cycle_budget"] or 0.0), 4),
                    "effective_min_notional": round(float(row["effective_min_notional"] or 0.0), 4),
                    "open_legs": int(row["open_legs"] or 0),
                    "exposure": round(float(row["exposure"] or 0.0), 4),
                    "open_execution_count": int(row["open_execution_count"] or 0),
                    "open_total_notional": round(float(row["open_total_notional"] or 0.0), 4),
                    "desired_up_ratio": round(float(row["desired_up_ratio"] or 0.0), 4),
                    "current_up_ratio": round(float(row["current_up_ratio"] or 0.0), 4),
                    "spot_price": round(float(row["spot_price"] or 0.0), 4),
                    "fair_up": round(float(row["fair_up"] or 0.0), 6),
                    "fair_down": round(float(row["fair_down"] or 0.0), 6),
                }
                for row in reversed(rows)
            ]

    paper_series = series.get("paper") or []
    shadow_series = series.get("shadow") or []
    if not paper_series and not shadow_series:
        return _empty_compare_samples(limit=safe_limit)

    paper_latest = paper_series[-1] if paper_series else {}
    shadow_latest = shadow_series[-1] if shadow_series else {}
    paper_latest_notional = float(paper_latest.get("open_total_notional") or 0.0)
    shadow_latest_notional = float(shadow_latest.get("open_total_notional") or 0.0)
    paper_latest_exposure = float(paper_latest.get("exposure") or 0.0)
    shadow_latest_exposure = float(shadow_latest.get("exposure") or 0.0)
    paper_state, paper_state_count, paper_state_pct = _dominant_operability_state(paper_series)
    shadow_state, shadow_state_count, shadow_state_pct = _dominant_operability_state(shadow_series)

    return {
        "available": True,
        "sample_limit": safe_limit,
        "series": series,
        "summary": {
            "paper_sample_count": len(paper_series),
            "shadow_sample_count": len(shadow_series),
            "paper_latest_notional": round(paper_latest_notional, 4),
            "shadow_latest_notional": round(shadow_latest_notional, 4),
            "notional_gap": round(paper_latest_notional - shadow_latest_notional, 4),
            "paper_latest_exposure": round(paper_latest_exposure, 4),
            "shadow_latest_exposure": round(shadow_latest_exposure, 4),
            "exposure_gap": round(paper_latest_exposure - shadow_latest_exposure, 4),
            "paper_dominant_operability_state": paper_state,
            "paper_dominant_operability_count": int(paper_state_count),
            "paper_dominant_operability_pct": round(paper_state_pct, 2),
            "shadow_dominant_operability_state": shadow_state,
            "shadow_dominant_operability_count": int(shadow_state_count),
            "shadow_dominant_operability_pct": round(shadow_state_pct, 2),
        },
    }


def _dominant_operability_state(series: list[dict]) -> tuple[str, int, float]:
    counts: dict[str, int] = {}
    for item in series:
        state = str(item.get("operability_state") or "").strip()
        if not state:
            continue
        counts[state] = counts.get(state, 0) + 1
    if not counts:
        return "", 0, 0.0
    state, count = max(counts.items(), key=lambda item: (item[1], item[0]))
    pct = (count / len(series)) * 100 if series else 0.0
    return state, count, pct


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
                conn.execute(
                    """
                    INSERT OR REPLACE INTO runtime_compare_samples(
                        runtime_mode, sample_ts, market_slug, market_title, price_mode, operability_state,
                        cycle_budget, remaining_cycle_budget, effective_min_notional, open_legs,
                        exposure, open_execution_count, open_total_notional, desired_up_ratio,
                        current_up_ratio, spot_price, fair_up, fair_down
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        runtime_mode,
                        generated_at,
                        str(snapshot.get("slug") or ""),
                        str(snapshot.get("title") or ""),
                        str(snapshot.get("price_mode") or ""),
                        str(snapshot.get("operability_state") or ""),
                        float(snapshot.get("cycle_budget") or 0.0),
                        float(snapshot.get("remaining_cycle_budget") or 0.0),
                        float(snapshot.get("effective_min_notional") or 0.0),
                        int(snapshot.get("open_legs") or 0),
                        float(snapshot.get("exposure") or 0.0),
                        int(snapshot.get("open_execution_count") or 0),
                        float(snapshot.get("open_total_notional") or 0.0),
                        float(snapshot.get("desired_up_ratio") or 0.0),
                        float(snapshot.get("current_up_ratio") or 0.0),
                        float(snapshot.get("spot_price") or 0.0),
                        float(snapshot.get("fair_up") or 0.0),
                        float(snapshot.get("fair_down") or 0.0),
                    ),
                )
                conn.execute(
                    """
                    DELETE FROM runtime_compare_samples
                    WHERE runtime_mode = ?
                      AND sample_ts NOT IN (
                        SELECT sample_ts
                        FROM runtime_compare_samples
                        WHERE runtime_mode = ?
                        ORDER BY sample_ts DESC
                        LIMIT ?
                      )
                    """,
                    (
                        runtime_mode,
                        runtime_mode,
                        _COMPARE_SAMPLE_RETENTION,
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
