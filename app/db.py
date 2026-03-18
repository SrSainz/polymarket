from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from app.models import ExecutionResult, NormalizedSignal, SignalAction, SourcePosition


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS source_positions_current (
    wallet TEXT NOT NULL,
    asset TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    size REAL NOT NULL,
    avg_price REAL NOT NULL,
    current_price REAL NOT NULL,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    category TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (wallet, asset)
);

CREATE TABLE IF NOT EXISTS source_positions_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    wallet TEXT NOT NULL,
    asset TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    size REAL NOT NULL,
    avg_price REAL NOT NULL,
    current_price REAL NOT NULL,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    category TEXT,
    observed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    detected_at INTEGER NOT NULL,
    wallet TEXT NOT NULL,
    asset TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    action TEXT NOT NULL,
    prev_size REAL NOT NULL,
    new_size REAL NOT NULL,
    delta_size REAL NOT NULL,
    reference_price REAL NOT NULL,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    category TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    note TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS copy_positions (
    asset TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    size REAL NOT NULL,
    avg_price REAL NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    action TEXT NOT NULL,
    side TEXT NOT NULL,
    asset TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    size REAL NOT NULL,
    price REAL NOT NULL,
    notional REAL NOT NULL,
    source_wallet TEXT,
    source_signal_id INTEGER,
    strategy_variant TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    pnl_delta REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS daily_pnl (
    day TEXT PRIMARY KEY,
    pnl REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS selected_wallets (
    wallet TEXT PRIMARY KEY,
    rank INTEGER NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    win_rate REAL NOT NULL DEFAULT 0,
    recent_trades INTEGER NOT NULL DEFAULT 0,
    pnl REAL NOT NULL DEFAULT 0,
    selected_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS position_mark_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    ts INTEGER NOT NULL,
    mark_price REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_position_mark_history_asset_ts
ON position_mark_history(asset, ts);

CREATE TABLE IF NOT EXISTS trade_approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    source_signal_id INTEGER,
    asset TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    action TEXT NOT NULL,
    side_proposed TEXT NOT NULL,
    side_decided TEXT,
    size REAL NOT NULL,
    price REAL NOT NULL,
    notional REAL NOT NULL,
    source_wallet TEXT NOT NULL,
    title TEXT,
    slug TEXT,
    outcome TEXT,
    category TEXT,
    reason TEXT NOT NULL DEFAULT '',
    decision_source TEXT,
    message_id INTEGER,
    decision_note TEXT NOT NULL DEFAULT '',
    decided_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_trade_approvals_status_expires
ON trade_approvals(status, expires_at);

CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_windows (
    slug TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    strategy_variant TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    opened_at INTEGER NOT NULL,
    first_trade_at INTEGER NOT NULL DEFAULT 0,
    last_trade_at INTEGER NOT NULL DEFAULT 0,
    closed_at INTEGER,
    price_mode TEXT NOT NULL DEFAULT '',
    timing_regime TEXT NOT NULL DEFAULT '',
    primary_outcome TEXT NOT NULL DEFAULT '',
    hedge_outcome TEXT NOT NULL DEFAULT '',
    primary_ratio REAL NOT NULL DEFAULT 0,
    planned_budget REAL NOT NULL DEFAULT 0,
    deployed_notional REAL NOT NULL DEFAULT 0,
    current_exposure REAL NOT NULL DEFAULT 0,
    filled_orders INTEGER NOT NULL DEFAULT 0,
    replenishment_count INTEGER NOT NULL DEFAULT 0,
    realized_pnl REAL NOT NULL DEFAULT 0,
    winning_outcome TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
"""


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = self._connect_with_retry(db_path)
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()

    def init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(SCHEMA_SQL)
            self._migrate_schema()

    def close(self) -> None:
        self.conn.close()

    def _connect_with_retry(self, db_path: Path, attempts: int = 6) -> sqlite3.Connection:
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(1, attempts + 1):
            try:
                return sqlite3.connect(str(db_path), timeout=30.0)
            except sqlite3.OperationalError as error:
                last_error = error
                if attempt >= attempts:
                    raise
                time.sleep(min(0.15 * attempt, 1.0))
        if last_error is not None:
            raise last_error
        raise sqlite3.OperationalError(f"unable to connect to sqlite database: {db_path}")

    def _configure_connection(self) -> None:
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._set_preferred_journal_mode()
        self.conn.execute("PRAGMA synchronous=NORMAL")

    def _set_preferred_journal_mode(self) -> None:
        for journal_mode in ("WAL", "TRUNCATE", "DELETE"):
            row = self.conn.execute(f"PRAGMA journal_mode={journal_mode}").fetchone()
            active_mode = str(row[0] if row else "").strip().upper()
            if active_mode == journal_mode:
                return
        self.conn.execute("PRAGMA journal_mode=DELETE")

    def _migrate_schema(self) -> None:
        strategy_window_columns = self._table_columns("strategy_windows")
        if "deployed_notional" not in strategy_window_columns:
            self.conn.execute(
                "ALTER TABLE strategy_windows ADD COLUMN deployed_notional REAL NOT NULL DEFAULT 0"
            )
        if "strategy_variant" not in strategy_window_columns:
            self.conn.execute(
                "ALTER TABLE strategy_windows ADD COLUMN strategy_variant TEXT NOT NULL DEFAULT ''"
            )

        execution_columns = self._table_columns("executions")
        if "strategy_variant" not in execution_columns:
            self.conn.execute(
                "ALTER TABLE executions ADD COLUMN strategy_variant TEXT NOT NULL DEFAULT ''"
            )

    def _table_columns(self, table_name: str) -> dict[str, str]:
        return {
            str(row["name"]): str(row["type"] or "")
            for row in self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }

    def _current_strategy_variant(self) -> str:
        variant = self.get_bot_state("strategy_variant")
        if variant is None:
            return ""
        return str(variant).strip()

    def get_source_positions(self, wallet: str) -> dict[str, SourcePosition]:
        rows = self.conn.execute(
            """
            SELECT wallet, asset, condition_id, size, avg_price, current_price, title, slug, outcome, category, updated_at
            FROM source_positions_current
            WHERE wallet = ?
            """,
            (wallet,),
        ).fetchall()

        output: dict[str, SourcePosition] = {}
        for row in rows:
            output[row["asset"]] = SourcePosition(
                wallet=row["wallet"],
                asset=row["asset"],
                condition_id=row["condition_id"],
                size=float(row["size"]),
                avg_price=float(row["avg_price"]),
                current_price=float(row["current_price"]),
                title=row["title"] or "",
                slug=row["slug"] or "",
                outcome=row["outcome"] or "",
                category=row["category"] or "",
                observed_at=int(row["updated_at"]),
            )
        return output

    def list_source_wallets(self) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT DISTINCT wallet
            FROM source_positions_current
            ORDER BY wallet ASC
            """
        ).fetchall()
        return [str(row["wallet"]) for row in rows]

    def replace_source_positions(self, wallet: str, positions: list[SourcePosition], run_id: str) -> None:
        now_ts = int(time.time())
        with self.conn:
            self.conn.execute("DELETE FROM source_positions_current WHERE wallet = ?", (wallet,))

            for position in positions:
                self.conn.execute(
                    """
                    INSERT INTO source_positions_current (
                        wallet, asset, condition_id, size, avg_price, current_price, title, slug, outcome, category, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        position.wallet,
                        position.asset,
                        position.condition_id,
                        position.size,
                        position.avg_price,
                        position.current_price,
                        position.title,
                        position.slug,
                        position.outcome,
                        position.category,
                        now_ts,
                    ),
                )
                self.conn.execute(
                    """
                    INSERT INTO source_positions_history (
                        run_id, wallet, asset, condition_id, size, avg_price, current_price, title, slug, outcome, category, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        position.wallet,
                        position.asset,
                        position.condition_id,
                        position.size,
                        position.avg_price,
                        position.current_price,
                        position.title,
                        position.slug,
                        position.outcome,
                        position.category,
                        position.observed_at,
                    ),
                )

    def delete_source_wallet_positions(self, wallet: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM source_positions_current WHERE wallet = ?", (wallet,))

    def replace_selected_wallets(self, rows: list[dict[str, float | int | str]]) -> None:
        now_ts = int(time.time())
        with self.conn:
            self.conn.execute("DELETE FROM selected_wallets")
            for rank, row in enumerate(rows, start=1):
                self.conn.execute(
                    """
                    INSERT INTO selected_wallets (
                        wallet, rank, score, win_rate, recent_trades, pnl, selected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row.get("wallet") or "").strip().lower(),
                        rank,
                        float(row.get("score") or 0.0),
                        float(row.get("win_rate") or 0.0),
                        int(row.get("recent_trades") or 0),
                        float(row.get("pnl") or 0.0),
                        now_ts,
                    ),
                )

    def record_position_mark(self, asset: str, mark_price: float) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO position_mark_history (asset, ts, mark_price)
                VALUES (?, ?, ?)
                """,
                (asset, int(time.time()), mark_price),
            )

    def get_position_mark_before(self, asset: str, cutoff_ts: int) -> float | None:
        row = self.conn.execute(
            """
            SELECT mark_price
            FROM position_mark_history
            WHERE asset = ? AND ts <= ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (asset, cutoff_ts),
        ).fetchone()
        if row is None:
            return None
        return float(row["mark_price"])

    def get_last_autonomous_sell_ts(self, asset: str) -> int | None:
        row = self.conn.execute(
            """
            SELECT ts
            FROM executions
            WHERE asset = ? AND source_wallet = 'autonomous' AND side = 'sell'
            ORDER BY ts DESC
            LIMIT 1
            """,
            (asset,),
        ).fetchone()
        if row is None:
            return None
        return int(row["ts"])

    def create_trade_approval(
        self,
        *,
        source_signal_id: int | None,
        asset: str,
        condition_id: str,
        action: str,
        side_proposed: str,
        size: float,
        price: float,
        notional: float,
        source_wallet: str,
        title: str,
        slug: str,
        outcome: str,
        category: str,
        reason: str,
        timeout_minutes: int,
    ) -> int:
        now_ts = int(time.time())
        expires_ts = now_ts + (timeout_minutes * 60)
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO trade_approvals (
                    created_at, expires_at, status, source_signal_id, asset, condition_id, action, side_proposed,
                    size, price, notional, source_wallet, title, slug, outcome, category, reason
                ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ts,
                    expires_ts,
                    source_signal_id,
                    asset,
                    condition_id,
                    action,
                    side_proposed,
                    size,
                    price,
                    notional,
                    source_wallet,
                    title,
                    slug,
                    outcome,
                    category,
                    reason,
                ),
            )
            return int(cursor.lastrowid)

    def list_pending_trade_approvals(self, limit: int = 200) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT *
            FROM trade_approvals
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def get_trade_approval(self, approval_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM trade_approvals WHERE id = ?",
            (approval_id,),
        ).fetchone()

    def set_trade_approval_message_id(self, approval_id: int, message_id: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE trade_approvals SET message_id = ? WHERE id = ?",
                (message_id, approval_id),
            )

    def set_trade_approval_decision(
        self,
        *,
        approval_id: int,
        side_decided: str,
        decision_source: str,
        decision_note: str = "",
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE trade_approvals
                SET side_decided = ?, decision_source = ?, decision_note = ?, decided_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (side_decided, decision_source, decision_note, int(time.time()), approval_id),
            )

    def reject_trade_approval(self, approval_id: int, decision_source: str, decision_note: str = "") -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE trade_approvals
                SET status = 'rejected', decision_source = ?, decision_note = ?, decided_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (decision_source, decision_note, int(time.time()), approval_id),
            )

    def mark_trade_approval_executed(self, approval_id: int, decision_note: str = "") -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE trade_approvals
                SET status = 'executed', decision_note = CASE WHEN decision_note = '' THEN ? ELSE decision_note END
                WHERE id = ?
                """,
                (decision_note, approval_id),
            )

    def mark_trade_approval_failed(self, approval_id: int, decision_note: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE trade_approvals
                SET status = 'failed', decision_note = ?
                WHERE id = ?
                """,
                (decision_note, approval_id),
            )

    def get_bot_state(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def set_bot_state(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO bot_state (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def insert_signal(self, signal: NormalizedSignal) -> bool:
        try:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO signals (
                        event_key, detected_at, wallet, asset, condition_id, action, prev_size, new_size, delta_size,
                        reference_price, title, slug, outcome, category, status, note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '')
                    """,
                    (
                        signal.event_key,
                        signal.detected_at,
                        signal.wallet,
                        signal.asset,
                        signal.condition_id,
                        signal.action.value,
                        signal.prev_size,
                        signal.new_size,
                        signal.delta_size,
                        signal.reference_price,
                        signal.title,
                        signal.slug,
                        signal.outcome,
                        signal.category,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def list_pending_signals(self, limit: int = 200) -> list[NormalizedSignal]:
        rows = self.conn.execute(
            """
            SELECT id, event_key, detected_at, wallet, asset, condition_id, action, prev_size, new_size,
                   delta_size, reference_price, title, slug, outcome, category
            FROM signals
            WHERE status = 'pending'
            ORDER BY detected_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        output: list[NormalizedSignal] = []
        for row in rows:
            output.append(
                NormalizedSignal(
                    id=int(row["id"]),
                    event_key=row["event_key"],
                    detected_at=int(row["detected_at"]),
                    wallet=row["wallet"],
                    asset=row["asset"],
                    condition_id=row["condition_id"],
                    action=SignalAction(row["action"]),
                    prev_size=float(row["prev_size"]),
                    new_size=float(row["new_size"]),
                    delta_size=float(row["delta_size"]),
                    reference_price=float(row["reference_price"]),
                    title=row["title"] or "",
                    slug=row["slug"] or "",
                    outcome=row["outcome"] or "",
                    category=row["category"] or "",
                )
            )
        return output

    def mark_signal_status(self, signal_id: int, status: str, note: str = "") -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE signals SET status = ?, note = ? WHERE id = ?",
                (status, note, signal_id),
            )

    def get_copy_position(self, asset: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM copy_positions WHERE asset = ?",
            (asset,),
        ).fetchone()

    def list_copy_positions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM copy_positions ORDER BY updated_at DESC"
        ).fetchall()

    def get_strategy_window(self, slug: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM strategy_windows WHERE slug = ?",
            (slug,),
        ).fetchone()

    def upsert_copy_position(
        self,
        *,
        asset: str,
        condition_id: str,
        size: float,
        avg_price: float,
        realized_pnl: float,
        title: str,
        slug: str,
        outcome: str,
        category: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO copy_positions (asset, condition_id, size, avg_price, realized_pnl, updated_at, title, slug, outcome, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    condition_id=excluded.condition_id,
                    size=excluded.size,
                    avg_price=excluded.avg_price,
                    realized_pnl=excluded.realized_pnl,
                    updated_at=excluded.updated_at,
                    title=excluded.title,
                    slug=excluded.slug,
                    outcome=excluded.outcome,
                    category=excluded.category
                """,
                (
                    asset,
                    condition_id,
                    size,
                    avg_price,
                    realized_pnl,
                    int(time.time()),
                    title,
                    slug,
                    outcome,
                    category,
                ),
            )

    def delete_copy_position(self, asset: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM copy_positions WHERE asset = ?", (asset,))

    def record_execution(
        self,
        *,
        result: ExecutionResult,
        side: str,
        condition_id: str,
        source_wallet: str,
        source_signal_id: int,
        notes: str,
        strategy_variant: str | None = None,
    ) -> None:
        active_variant = str(strategy_variant or self._current_strategy_variant()).strip()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO executions (
                    ts, mode, status, action, side, asset, condition_id, size, price, notional,
                    source_wallet, source_signal_id, strategy_variant, notes, pnl_delta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(time.time()),
                    result.mode,
                    result.status,
                    result.action.value,
                    side,
                    result.asset,
                    condition_id,
                    result.size,
                    result.price,
                    result.notional,
                    source_wallet,
                    source_signal_id,
                    active_variant,
                    notes,
                    result.pnl_delta,
                ),
            )

    def get_recent_executions(self, limit: int = 25) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM executions ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()

    def get_daily_pnl(self, day: str) -> float:
        row = self.conn.execute("SELECT pnl FROM daily_pnl WHERE day = ?", (day,)).fetchone()
        if row is None:
            return 0.0
        return float(row["pnl"])

    def get_daily_profit_gross(self, day: str) -> float:
        row = self.conn.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN pnl_delta > 0 THEN pnl_delta ELSE 0 END), 0) AS profit
            FROM executions
            WHERE strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (day,),
        ).fetchone()
        if row is None:
            return 0.0
        return float(row["profit"])

    def get_daily_loss_gross(self, day: str) -> float:
        row = self.conn.execute(
            """
            SELECT COALESCE(ABS(SUM(CASE WHEN pnl_delta < 0 THEN pnl_delta ELSE 0 END)), 0) AS loss
            FROM executions
            WHERE strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (day,),
        ).fetchone()
        if row is None:
            return 0.0
        return float(row["loss"])

    def get_daily_execution_counts(self, day: str) -> dict[str, int]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) AS buys,
                SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS sells
            FROM executions
            WHERE strftime('%Y-%m-%d', ts, 'unixepoch') = ?
            """,
            (day,),
        ).fetchone()
        if row is None:
            return {"total": 0, "buys": 0, "sells": 0}
        return {
            "total": int(row["total"] or 0),
            "buys": int(row["buys"] or 0),
            "sells": int(row["sells"] or 0),
        }

    def add_daily_pnl(self, day: str, delta: float) -> None:
        current = self.get_daily_pnl(day)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO daily_pnl (day, pnl) VALUES (?, ?)
                ON CONFLICT(day) DO UPDATE SET pnl = excluded.pnl
                """,
                (day, current + delta),
            )

    def get_cumulative_pnl(self) -> float:
        row = self.conn.execute("SELECT COALESCE(SUM(pnl), 0) AS total FROM daily_pnl").fetchone()
        return float(row["total"])

    def get_cumulative_pnl_before(self, day: str) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) AS total FROM daily_pnl WHERE day < ?",
            (day,),
        ).fetchone()
        if row is None:
            return 0.0
        return float(row["total"])

    def get_cumulative_profit_gross_before(self, day: str) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END), 0) AS total FROM daily_pnl WHERE day < ?",
            (day,),
        ).fetchone()
        if row is None:
            return 0.0
        return float(row["total"])

    def get_total_exposure(self) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(ABS(size * avg_price)), 0) AS exposure FROM copy_positions"
        ).fetchone()
        return float(row["exposure"])

    def upsert_strategy_window(
        self,
        *,
        slug: str,
        condition_id: str,
        title: str,
        price_mode: str,
        timing_regime: str,
        primary_outcome: str,
        hedge_outcome: str,
        primary_ratio: float,
        planned_budget: float,
        current_exposure: float,
        notes: str,
        strategy_variant: str | None = None,
    ) -> None:
        now_ts = int(time.time())
        active_variant = str(strategy_variant or self._current_strategy_variant()).strip()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO strategy_windows (
                    slug, condition_id, title, strategy_variant, status, opened_at, price_mode, timing_regime,
                    primary_outcome, hedge_outcome, primary_ratio, planned_budget, current_exposure, notes
                ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    condition_id=excluded.condition_id,
                    title=excluded.title,
                    strategy_variant=excluded.strategy_variant,
                    price_mode=excluded.price_mode,
                    timing_regime=excluded.timing_regime,
                    primary_outcome=excluded.primary_outcome,
                    hedge_outcome=excluded.hedge_outcome,
                    primary_ratio=excluded.primary_ratio,
                    planned_budget=excluded.planned_budget,
                    current_exposure=excluded.current_exposure,
                    notes=excluded.notes
                """,
                (
                    slug,
                    condition_id,
                    title,
                    active_variant,
                    now_ts,
                    price_mode,
                    timing_regime,
                    primary_outcome,
                    hedge_outcome,
                    primary_ratio,
                    planned_budget,
                    current_exposure,
                    notes,
                ),
            )

    def record_strategy_window_fills(
        self,
        *,
        slug: str,
        fill_count: int,
        added_notional: float,
        replenishment_count: int,
        notes: str,
    ) -> None:
        now_ts = int(time.time())
        with self.conn:
            self.conn.execute(
                """
                UPDATE strategy_windows
                SET status = 'open',
                    first_trade_at = CASE WHEN first_trade_at = 0 THEN ? ELSE first_trade_at END,
                    last_trade_at = ?,
                    filled_orders = filled_orders + ?,
                    deployed_notional = deployed_notional + ?,
                    current_exposure = current_exposure + ?,
                    replenishment_count = replenishment_count + ?,
                    notes = ?
                WHERE slug = ?
                """,
                (
                    now_ts,
                    now_ts,
                    fill_count,
                    added_notional,
                    added_notional,
                    replenishment_count,
                    notes,
                    slug,
                ),
            )

    def close_strategy_window(
        self,
        *,
        slug: str,
        realized_pnl: float,
        winning_outcome: str,
        current_exposure: float,
        notes: str,
    ) -> None:
        now_ts = int(time.time())
        with self.conn:
            self.conn.execute(
                """
                UPDATE strategy_windows
                SET status = 'closed',
                    closed_at = ?,
                    realized_pnl = realized_pnl + ?,
                    winning_outcome = ?,
                    current_exposure = ?,
                    notes = ?
                WHERE slug = ?
                """,
                (
                    now_ts,
                    realized_pnl,
                    winning_outcome,
                    current_exposure,
                    notes,
                    slug,
                ),
            )

    def get_strategy_setup_stats(self, *, price_mode: str, timing_regime: str) -> dict[str, float]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS windows,
                COALESCE(SUM(realized_pnl), 0) AS pnl_total,
                COALESCE(AVG(realized_pnl), 0) AS pnl_avg,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins
            FROM strategy_windows
            WHERE status = 'closed' AND price_mode = ? AND timing_regime = ?
            """,
            (price_mode, timing_regime),
        ).fetchone()
        windows = int(row["windows"] or 0) if row is not None else 0
        wins = int(row["wins"] or 0) if row is not None else 0
        win_rate = (wins / windows) if windows > 0 else 0.0
        return {
            "windows": float(windows),
            "wins": float(wins),
            "win_rate": float(win_rate),
            "pnl_total": float(row["pnl_total"] or 0.0) if row is not None else 0.0,
            "pnl_avg": float(row["pnl_avg"] or 0.0) if row is not None else 0.0,
        }
