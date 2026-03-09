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
"""


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self.conn.close()

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
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO executions (
                    ts, mode, status, action, side, asset, condition_id, size, price, notional,
                    source_wallet, source_signal_id, notes, pnl_delta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def get_total_exposure(self) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(ABS(size * avg_price)), 0) AS exposure FROM copy_positions"
        ).fetchone()
        return float(row["exposure"])
