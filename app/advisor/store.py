from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import hmac
import math
import sqlite3
import time
from pathlib import Path

from app.advisor.models import TradeProposal
from app.advisor.proposals import (
    confirmation_code_hash,
    proposal_fingerprint,
    proposal_payload,
    proposal_fingerprint_payload,
    proposal_integrity_token,
    approval_receipt_token,
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS advisor_proposals (
    proposal_id TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    quote_expires_at REAL NOT NULL,
    status TEXT NOT NULL,
    confirmation_code_hash TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    integrity_token TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    confirmed_at REAL,
    confirmed_from TEXT,
    claimed_at REAL,
    decision_note TEXT NOT NULL DEFAULT '',
    approval_at REAL,
    approval_message_id TEXT NOT NULL DEFAULT '',
    approval_payload_hash TEXT NOT NULL DEFAULT '',
    approval_receipt TEXT NOT NULL DEFAULT '',
    order_id TEXT NOT NULL DEFAULT '',
    submitted_at REAL,
    execution_status TEXT NOT NULL DEFAULT '',
    filled_size REAL,
    finalized_at REAL,
    realized_pnl_usdc REAL
);

CREATE TABLE IF NOT EXISTS advisor_outbox (
    proposal_id TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    provider_message_id TEXT,
    claimed_at REAL,
    last_error TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(proposal_id) REFERENCES advisor_proposals(proposal_id)
);

CREATE TABLE IF NOT EXISTS advisor_inbound_messages (
    provider_message_id TEXT PRIMARY KEY,
    received_at REAL NOT NULL,
    sender TEXT NOT NULL,
    payload_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS advisor_daily_risk (
    risk_day TEXT PRIMARY KEY,
    realized_loss_usdc REAL NOT NULL DEFAULT 0,
    reserved_loss_usdc REAL NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS advisor_risk_reservations (
    proposal_id TEXT PRIMARY KEY,
    risk_day TEXT NOT NULL,
    amount_usdc REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved',
    created_at REAL NOT NULL,
    released_at REAL,
    FOREIGN KEY(proposal_id) REFERENCES advisor_proposals(proposal_id)
);

CREATE TABLE IF NOT EXISTS advisor_daily_cycles (
    cycle_day TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    proposal_id TEXT,
    decision_note TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(proposal_id) REFERENCES advisor_proposals(proposal_id)
);

CREATE TABLE IF NOT EXISTS advisor_execution_claims (
    proposal_id TEXT PRIMARY KEY,
    claimed_at REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'claimed',
    order_id TEXT NOT NULL DEFAULT '',
    submitted_at REAL,
    FOREIGN KEY(proposal_id) REFERENCES advisor_proposals(proposal_id)
);

CREATE TABLE IF NOT EXISTS advisor_legacy_risk_migrations (
    proposal_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    amount_usdc REAL NOT NULL DEFAULT 0,
    migrated_at REAL NOT NULL,
    FOREIGN KEY(proposal_id) REFERENCES advisor_proposals(proposal_id)
);
"""


@dataclass(frozen=True)
class ApprovalResult:
    status: str
    proposal_id: str | None = None
    reason: str = ""


class AdvisorStore:
    def __init__(self, db_path: Path, *, integrity_key: str = "", confirmation_key: str = "") -> None:
        self.db_path = db_path
        self.integrity_key = str(integrity_key or "")
        self.confirmation_key = str(confirmation_key or "")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # A scheduler may be hosted by a different worker thread. SQLite
        # still serializes writes through BEGIN IMMEDIATE; webhook requests
        # use separate store instances.
        self.conn = sqlite3.connect(str(db_path), timeout=30.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self._migrate_schema()
        self.conn.commit()
        self.conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def _immediate_transaction(self):
        """Serialize risk reservations across SQLite connections."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def _migrate_schema(self) -> None:
        column_definitions = {
            "advisor_proposals": {
                "confirmed_at": "REAL",
                "confirmed_from": "TEXT",
                "claimed_at": "REAL",
                "decision_note": "TEXT NOT NULL DEFAULT ''",
                "integrity_token": "TEXT NOT NULL DEFAULT ''",
                "order_id": "TEXT NOT NULL DEFAULT ''",
                "submitted_at": "REAL",
                "approval_at": "REAL",
                "approval_message_id": "TEXT NOT NULL DEFAULT ''",
                "approval_payload_hash": "TEXT NOT NULL DEFAULT ''",
                "approval_receipt": "TEXT NOT NULL DEFAULT ''",
                "execution_status": "TEXT NOT NULL DEFAULT ''",
                "filled_size": "REAL",
                "finalized_at": "REAL",
                "realized_pnl_usdc": "REAL",
            },
            "advisor_outbox": {
                "claimed_at": "REAL",
                "provider_message_id": "TEXT",
                "last_error": "TEXT NOT NULL DEFAULT ''",
            },
            "advisor_daily_risk": {
                "realized_loss_usdc": "REAL NOT NULL DEFAULT 0",
                "reserved_loss_usdc": "REAL NOT NULL DEFAULT 0",
                "updated_at": "REAL NOT NULL DEFAULT 0",
            },
            "advisor_risk_reservations": {
                "risk_day": "TEXT NOT NULL DEFAULT ''",
                "amount_usdc": "REAL NOT NULL DEFAULT 0",
                "status": "TEXT NOT NULL DEFAULT 'reserved'",
                "created_at": "REAL NOT NULL DEFAULT 0",
                "released_at": "REAL",
            },
            "advisor_execution_claims": {
                "claimed_at": "REAL",
                "status": "TEXT NOT NULL DEFAULT 'claimed'",
                "order_id": "TEXT NOT NULL DEFAULT ''",
                "submitted_at": "REAL",
            },
            "advisor_legacy_risk_migrations": {
                "action": "TEXT NOT NULL DEFAULT ''",
                "amount_usdc": "REAL NOT NULL DEFAULT 0",
                "migrated_at": "REAL NOT NULL DEFAULT 0",
            },
        }
        for table, definitions in column_definitions.items():
            columns = {
                str(row[1])
                for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for column, definition in definitions.items():
                if column not in columns:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

        cycle_columns = {
            str(row[1]): row
            for row in self.conn.execute("PRAGMA table_info(advisor_daily_cycles)").fetchall()
        }
        if "proposal_id" not in cycle_columns:
            self.conn.execute("ALTER TABLE advisor_daily_cycles ADD COLUMN proposal_id TEXT")
        if "decision_note" not in cycle_columns:
            self.conn.execute(
                "ALTER TABLE advisor_daily_cycles ADD COLUMN decision_note TEXT NOT NULL DEFAULT ''"
            )
        cycle_columns = {
            str(row[1]): row
            for row in self.conn.execute("PRAGMA table_info(advisor_daily_cycles)").fetchall()
        }
        proposal_column = cycle_columns.get("proposal_id")
        if proposal_column is not None and int(proposal_column[3]) == 1:
            self.conn.execute("ALTER TABLE advisor_daily_cycles RENAME TO advisor_daily_cycles_legacy")
            self.conn.execute(
                """
                CREATE TABLE advisor_daily_cycles (
                    cycle_day TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    proposal_id TEXT,
                    decision_note TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(proposal_id) REFERENCES advisor_proposals(proposal_id)
                )
                """
            )
            self.conn.execute(
                """
                INSERT INTO advisor_daily_cycles
                    (cycle_day, status, created_at, proposal_id, decision_note)
                SELECT cycle_day, status, created_at, NULLIF(proposal_id, ''), decision_note
                FROM advisor_daily_cycles_legacy
                """
            )
            self.conn.execute("DROP TABLE advisor_daily_cycles_legacy")

        self.conn.execute(
            """
            INSERT OR IGNORE INTO advisor_execution_claims
                (proposal_id, claimed_at, status, order_id, submitted_at)
            SELECT proposal_id,
                   COALESCE(claimed_at, submitted_at, created_at),
                   CASE
                       WHEN status = 'reconciliation_required' THEN 'reconciliation_required'
                       WHEN status IN ('submitted', 'filled', 'partial', 'settled') THEN status
                       ELSE 'claimed'
                   END,
                   COALESCE(order_id, ''),
                   submitted_at
            FROM advisor_proposals
            WHERE status IN ('revalidating', 'reconciliation_required', 'submitted', 'filled', 'partial', 'settled')
            """
        )
        self._migrate_legacy_risk()

    def _migrate_legacy_risk(self) -> None:
        """Carry old execution exposure into the durable risk ledger once."""
        rows = self.conn.execute(
            """
            SELECT p.proposal_id, p.status, p.created_at, p.submitted_at,
                   p.realized_pnl_usdc, p.payload_json
            FROM advisor_proposals p
            WHERE p.status IN ('submitted', 'filled', 'partial', 'settled', 'reconciliation_required')
            """
        ).fetchall()
        migration_time = time.time()
        for row in rows:
            proposal_id = str(row["proposal_id"])
            if self.conn.execute(
                "SELECT 1 FROM advisor_legacy_risk_migrations WHERE proposal_id = ?",
                (proposal_id,),
            ).fetchone() is not None:
                continue

            status = str(row["status"] or "")
            if status == "settled":
                reservation = self.conn.execute(
                    """
                    SELECT status, risk_day, amount_usdc
                    FROM advisor_risk_reservations
                    WHERE proposal_id = ?
                    """,
                    (proposal_id,),
                ).fetchone()
                if reservation is None:
                    # A settled legacy proposal without a linked reservation is
                    # ambiguous: the old risk ledger may already contain its
                    # loss. Importing it could double-count the loss.
                    self._mark_legacy_risk_blocked(proposal_id, "settled_legacy_ambiguous", migration_time)
                    continue
                if str(reservation["status"] or "") != "settled":
                    self._mark_legacy_risk_blocked(
                        proposal_id,
                        "settled_reservation_unresolved",
                        migration_time,
                    )
                    continue
                # A settled reservation proves that the previous ledger already
                # accounted for the exposure. Do not import the PnL again.
                self._record_legacy_migration(
                    proposal_id,
                    "settled_existing_reservation",
                    _finite_float(reservation["amount_usdc"]) or 0.0,
                    migration_time,
                )
                continue

            amount = _legacy_notional(row["payload_json"])
            if amount is None:
                self._mark_legacy_risk_blocked(proposal_id, "unknown_open_exposure", migration_time)
                continue

            risk_day = _risk_day(float(row["submitted_at"] or row["created_at"]))
            self._ensure_daily_risk_row(risk_day, migration_time)
            reservation = self.conn.execute(
                """
                SELECT status, risk_day, amount_usdc
                FROM advisor_risk_reservations
                WHERE proposal_id = ?
                """,
                (proposal_id,),
            ).fetchone()
            if reservation is not None:
                existing_amount = _finite_float(reservation["amount_usdc"])
                if (
                    str(reservation["status"] or "") != "reserved"
                    or str(reservation["risk_day"] or "") != risk_day
                    or existing_amount is None
                    or not math.isclose(existing_amount, amount, rel_tol=0.0, abs_tol=1e-6)
                ):
                    self._mark_legacy_risk_blocked(proposal_id, "reservation_mismatch", migration_time)
                    continue
                self._record_legacy_migration(
                    proposal_id,
                    "reserved_open_execution_existing",
                    amount,
                    migration_time,
                )
                continue

            daily_cursor = self.conn.execute(
                "UPDATE advisor_daily_risk SET reserved_loss_usdc = reserved_loss_usdc + ?, updated_at = ? WHERE risk_day = ?",
                (amount, migration_time, risk_day),
            )
            if daily_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("legacy_risk_day_missing")
            reservation_cursor = self.conn.execute(
                """
                INSERT INTO advisor_risk_reservations
                    (proposal_id, risk_day, amount_usdc, status, created_at)
                VALUES (?, ?, ?, 'reserved', ?)
                """,
                (proposal_id, risk_day, amount, migration_time),
            )
            if reservation_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("legacy_risk_reservation_missing")
            self._record_legacy_migration(proposal_id, "reserved_open_execution", amount, migration_time)

    def _mark_legacy_risk_blocked(self, proposal_id: str, reason: str, migrated_at: float) -> None:
        self.conn.execute(
            "UPDATE advisor_proposals SET status = 'reconciliation_required', decision_note = ? WHERE proposal_id = ?",
            (f"legacy_risk_{reason}", proposal_id),
        )
        self.conn.execute(
            "UPDATE advisor_execution_claims SET status = 'reconciliation_required' WHERE proposal_id = ?",
            (proposal_id,),
        )
        self._record_legacy_migration(proposal_id, f"blocked_{reason}", 0.0, migrated_at)

    def _ensure_daily_risk_row(self, risk_day: str, updated_at: float) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO advisor_daily_risk
                (risk_day, realized_loss_usdc, reserved_loss_usdc, updated_at)
            VALUES (?, 0, 0, ?)
            """,
            (risk_day, updated_at),
        )
        if self.conn.execute(
            "SELECT 1 FROM advisor_daily_risk WHERE risk_day = ?",
            (risk_day,),
        ).fetchone() is None:
            raise sqlite3.IntegrityError("daily_risk_row_missing")

    def _record_legacy_migration(
        self,
        proposal_id: str,
        action: str,
        amount_usdc: float,
        migrated_at: float,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO advisor_legacy_risk_migrations
                (proposal_id, action, amount_usdc, migrated_at)
            VALUES (?, ?, ?, ?)
            """,
            (proposal_id, action, amount_usdc, migrated_at),
        )

    def save_proposal(self, proposal: TradeProposal) -> None:
        with self.conn:
            self._insert_proposal_locked(proposal)

    def create_daily_proposal(self, cycle_day: str, proposal: TradeProposal) -> bool:
        """Atomically claim a UTC day and persist its only proposal."""
        normalized_day = str(cycle_day or "").strip()
        if not normalized_day or normalized_day != _risk_day(proposal.created_at):
            return False
        try:
            with self._immediate_transaction():
                self._insert_proposal_locked(proposal)
                cycle = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO advisor_daily_cycles
                        (cycle_day, status, created_at, proposal_id)
                    VALUES (?, 'proposal_created', ?, ?)
                    """,
                    (normalized_day, proposal.created_at, proposal.proposal_id),
                )
                if cycle.rowcount != 1:
                    cycle_update = self.conn.execute(
                        """
                        UPDATE advisor_daily_cycles
                        SET status = 'proposal_created', created_at = ?, proposal_id = ?, decision_note = ''
                        WHERE cycle_day = ? AND status = 'running'
                        """,
                        (proposal.created_at, proposal.proposal_id, normalized_day),
                    )
                    if cycle_update.rowcount != 1:
                        raise sqlite3.IntegrityError("daily_cycle_already_processed")
                self.conn.execute(
                    "INSERT INTO advisor_outbox (proposal_id, created_at) VALUES (?, ?)",
                    (proposal.proposal_id, proposal.created_at),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def complete_daily_cycle(
        self,
        cycle_day: str,
        status: str,
        *,
        decision_note: str = "",
        now: float | None = None,
    ) -> bool:
        normalized_day = str(cycle_day or "").strip()
        normalized_status = str(status or "").strip().lower()
        if not normalized_day or normalized_status not in {"no_opportunity", "failed"}:
            return False
        current_time = time.time() if now is None else float(now)
        if not math.isfinite(current_time) or normalized_day != _risk_day(current_time):
            return False
        with self._immediate_transaction():
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO advisor_daily_cycles
                    (cycle_day, status, created_at, proposal_id, decision_note)
                VALUES (?, ?, ?, ?, ?)
                """,
                (normalized_day, normalized_status, current_time, None, str(decision_note)[:500]),
            )
            if cursor.rowcount != 1:
                cursor = self.conn.execute(
                    """
                    UPDATE advisor_daily_cycles
                    SET status = ?, created_at = ?, decision_note = ?
                    WHERE cycle_day = ? AND status = 'running'
                    """,
                    (normalized_status, current_time, str(decision_note)[:500], normalized_day),
                )
        return cursor.rowcount == 1

    def begin_daily_cycle(
        self,
        cycle_day: str,
        *,
        now: float | None = None,
        stale_after_seconds: float = 1_800.0,
    ) -> bool:
        """Persist an in-progress claim before external discovery/model calls."""
        normalized_day = str(cycle_day or "").strip()
        current_time = time.time() if now is None else float(now)
        if (
            not normalized_day
            or not math.isfinite(current_time)
            or normalized_day != _risk_day(current_time)
            or not math.isfinite(float(stale_after_seconds))
            or float(stale_after_seconds) <= 0
        ):
            return False
        with self._immediate_transaction():
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO advisor_daily_cycles
                    (cycle_day, status, created_at, proposal_id, decision_note)
                VALUES (?, 'running', ?, NULL, 'discovery_in_progress')
                """,
                (normalized_day, current_time),
            )
            if cursor.rowcount == 1:
                return True
            takeover = self.conn.execute(
                """
                UPDATE advisor_daily_cycles
                SET status = 'running', created_at = ?, proposal_id = NULL, decision_note = 'discovery_restarted'
                WHERE cycle_day = ? AND status = 'running' AND created_at <= ?
                """,
                (current_time, normalized_day, current_time - float(stale_after_seconds)),
            )
        return takeover.rowcount == 1

    def get_daily_cycle(self, cycle_day: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM advisor_daily_cycles WHERE cycle_day = ?",
            (str(cycle_day or "").strip(),),
        ).fetchone()

    def confirmed_proposals(self, *, limit: int = 20) -> list[sqlite3.Row]:
        safe_limit = max(1, min(int(limit), 100))
        return self.conn.execute(
            """
            SELECT p.*
            FROM advisor_proposals p
            JOIN advisor_outbox o ON o.proposal_id = p.proposal_id
            WHERE p.status = 'confirmed' AND o.status = 'sent'
            ORDER BY p.approval_at ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    def _insert_proposal_locked(self, proposal: TradeProposal) -> None:
        payload = _proposal_payload(proposal)
        fingerprint = proposal_fingerprint(proposal)
        self.conn.execute(
            """
            INSERT INTO advisor_proposals (
                proposal_id, created_at, expires_at, quote_expires_at, status,
                confirmation_code_hash, fingerprint, payload_json, integrity_token
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)
            """,
            (
                proposal.proposal_id,
                proposal.created_at,
                proposal.expires_at,
                proposal.quote_expires_at,
                confirmation_code_hash(proposal.confirmation_code),
                fingerprint,
                payload,
                proposal_integrity_token(fingerprint, self.integrity_key),
            ),
        )

    def enqueue(self, proposal_id: str, *, now: float | None = None) -> None:
        created_at = time.time() if now is None else float(now)
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO advisor_outbox (proposal_id, created_at) VALUES (?, ?)",
                (proposal_id, created_at),
            )

    def mark_sent(self, proposal_id: str, provider_message_id: str) -> bool:
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE advisor_outbox
                SET status = 'sent', provider_message_id = ?, last_error = ''
                WHERE proposal_id = ? AND status = 'sending'
                """,
                (provider_message_id, proposal_id),
            )
            if cursor.rowcount == 1:
                self.conn.execute(
                    "UPDATE advisor_proposals SET status = 'sent' WHERE proposal_id = ? AND status = 'pending'",
                    (proposal_id,),
                )
        return cursor.rowcount == 1

    def claim_outbox(self, proposal_id: str, *, now: float | None = None) -> bool:
        current_time = time.time() if now is None else float(now)
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE advisor_outbox
                SET status = 'sending', claimed_at = ?
                WHERE proposal_id = ? AND status = 'pending'
                  AND EXISTS (
                        SELECT 1 FROM advisor_proposals
                        WHERE advisor_proposals.proposal_id = advisor_outbox.proposal_id
                          AND advisor_proposals.status = 'pending'
                        AND advisor_proposals.expires_at > ?
                        AND advisor_proposals.quote_expires_at > ?
                  )
                """,
                (current_time, proposal_id, current_time, current_time),
            )
        return cursor.rowcount == 1

    def mark_send_failed(self, proposal_id: str, error: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE advisor_outbox SET status = 'failed', last_error = ? WHERE proposal_id = ? AND status = 'sending'",
                (str(error)[:500], proposal_id),
            )

    def mark_send_integrity_failed(self, proposal_id: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE advisor_outbox SET status = 'failed', last_error = 'proposal_integrity_invalid' WHERE proposal_id = ? AND status = 'sending'",
                (proposal_id,),
            )
            self.conn.execute(
                "UPDATE advisor_proposals SET status = 'integrity_failed', decision_note = 'proposal_integrity_invalid' WHERE proposal_id = ? AND status IN ('pending', 'notification_uncertain')",
                (proposal_id,),
            )

    def mark_send_reconciliation_required(self, proposal_id: str, note: str) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE advisor_outbox SET status = 'reconciliation_required', last_error = ? WHERE proposal_id = ? AND status = 'sending'",
                (str(note)[:500], proposal_id),
            )
            self.conn.execute(
                "UPDATE advisor_proposals SET status = 'notification_uncertain', decision_note = ? WHERE proposal_id = ? AND status = 'pending'",
                (str(note)[:500], proposal_id),
            )

    def reconcile_notification(
        self,
        proposal_id: str,
        outcome: str,
        *,
        provider_message_id: str = "",
        note: str = "",
        now: float | None = None,
    ) -> bool:
        """Resolve an ambiguous WhatsApp send after checking the provider.

        This is deliberately an operator-facing transition. It never guesses
        whether a message was delivered and never retries implicitly.
        """
        current_time = time.time() if now is None else float(now)
        normalized = str(outcome or "").strip().lower()
        if not math.isfinite(current_time):
            return False
        if normalized in {"sent", "delivered", "confirmed"} and not provider_message_id:
            return False
        if normalized not in {
            "sent",
            "delivered",
            "confirmed",
            "failed",
            "rejected",
            "expired",
            "retry",
        }:
            return False

        with self._immediate_transaction():
            row = self.conn.execute(
                """
                SELECT o.status AS outbox_status, p.status AS proposal_status,
                       p.expires_at, p.quote_expires_at
                FROM advisor_outbox o
                JOIN advisor_proposals p ON p.proposal_id = o.proposal_id
                WHERE o.proposal_id = ?
                """,
                (proposal_id,),
            ).fetchone()
            if row is None or str(row["outbox_status"]) != "reconciliation_required":
                return False

            if normalized in {"sent", "delivered", "confirmed"}:
                proposal_status = (
                    "sent"
                    if min(float(row["expires_at"]), float(row["quote_expires_at"])) > current_time
                    else "expired"
                )
                decision_note = note or (
                    "notification_sent" if proposal_status == "sent" else "notification_sent_after_expiry"
                )
                outbox_cursor = self.conn.execute(
                    """
                    UPDATE advisor_outbox
                    SET status = 'sent', provider_message_id = ?, last_error = ?
                    WHERE proposal_id = ? AND status = 'reconciliation_required'
                    """,
                    (provider_message_id, decision_note[:500], proposal_id),
                )
                if outbox_cursor.rowcount != 1:
                    return False
                self.conn.execute(
                    """
                    UPDATE advisor_proposals
                    SET status = ?, decision_note = ?
                    WHERE proposal_id = ? AND status = 'notification_uncertain'
                    """,
                    (proposal_status, decision_note[:500], proposal_id),
                )
                return True

            if normalized == "retry":
                if min(float(row["expires_at"]), float(row["quote_expires_at"])) <= current_time:
                    return False
                outbox_cursor = self.conn.execute(
                    """
                    UPDATE advisor_outbox
                    SET status = 'pending', provider_message_id = NULL,
                        claimed_at = NULL, last_error = ?
                    WHERE proposal_id = ? AND status = 'reconciliation_required'
                    """,
                    (note or "operator_requested_retry", proposal_id),
                )
                if outbox_cursor.rowcount != 1:
                    return False
                self.conn.execute(
                    """
                    UPDATE advisor_proposals
                    SET status = 'pending', decision_note = ?
                    WHERE proposal_id = ? AND status = 'notification_uncertain'
                    """,
                    (note or "operator_requested_retry", proposal_id),
                )
                return True

            next_status = "expired" if normalized == "expired" else "failed"
            decision_note = note or f"notification_{normalized}"
            outbox_cursor = self.conn.execute(
                """
                UPDATE advisor_outbox
                SET status = ?, last_error = ?
                WHERE proposal_id = ? AND status = 'reconciliation_required'
                """,
                (next_status, decision_note[:500], proposal_id),
            )
            if outbox_cursor.rowcount != 1:
                return False
            self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = ?, decision_note = ?
                WHERE proposal_id = ? AND status = 'notification_uncertain'
                """,
                (next_status, decision_note[:500], proposal_id),
            )
            return True

    def recover_uncertain_sends(self, *, cutoff: float) -> int:
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE advisor_outbox
                SET status = 'reconciliation_required', last_error = 'send_result_unknown'
                WHERE status = 'sending' AND claimed_at <= ?
                """,
                (float(cutoff),),
            )
            if cursor.rowcount:
                cursor = self.conn.execute(
                    """
                    UPDATE advisor_proposals
                    SET status = 'notification_uncertain', decision_note = 'send_result_unknown'
                    WHERE proposal_id IN (
                        SELECT proposal_id FROM advisor_outbox
                        WHERE status = 'reconciliation_required' AND last_error = 'send_result_unknown'
                    ) AND status = 'pending'
                    """
                )
        return cursor.rowcount

    def get_proposal(self, proposal_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM advisor_proposals WHERE proposal_id = ?",
            (proposal_id,),
        ).fetchone()

    def pending_outbox(self, limit: int = 50, *, now: float | None = None) -> list[sqlite3.Row]:
        current_time = time.time() if now is None else float(now)
        return self.conn.execute(
            """
            SELECT o.*, p.payload_json, p.expires_at, p.quote_expires_at
            FROM advisor_outbox o
            JOIN advisor_proposals p ON p.proposal_id = o.proposal_id
            WHERE o.status = 'pending' AND p.status = 'pending'
              AND p.expires_at > ? AND p.quote_expires_at > ?
            ORDER BY o.created_at ASC
            LIMIT ?
            """,
            (current_time, current_time, limit),
        ).fetchall()

    def expire_due(self, *, now: float | None = None) -> int:
        current_time = time.time() if now is None else float(now)
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = 'expired', decision_note = 'proposal_expired'
                WHERE status IN ('pending', 'sent', 'confirmed', 'notification_uncertain')
                  AND (expires_at <= ? OR quote_expires_at <= ?)
                """,
                (current_time, current_time),
            )
            self.conn.execute(
                """
                UPDATE advisor_outbox
                SET status = 'expired', last_error = 'proposal_expired'
                WHERE status = 'pending'
                  AND proposal_id IN (
                      SELECT proposal_id FROM advisor_proposals WHERE status = 'expired'
                  )
                """
            )
            self.conn.execute(
                """
                UPDATE advisor_outbox
                SET status = 'reconciliation_required', last_error = 'send_result_unknown_after_expiry'
                WHERE status = 'sending'
                  AND proposal_id IN (
                      SELECT proposal_id FROM advisor_proposals WHERE status = 'expired'
                  )
                """
            )
        return cursor.rowcount

    def daily_risk_usdc(self, *, now: float | None = None) -> float:
        current_time = time.time() if now is None else float(now)
        if not math.isfinite(current_time):
            return float("inf")
        blocked_legacy = self.conn.execute(
            """
            SELECT 1
            FROM advisor_legacy_risk_migrations m
            JOIN advisor_proposals p ON p.proposal_id = m.proposal_id
            WHERE m.action LIKE 'blocked_%'
              AND p.status = 'reconciliation_required'
            LIMIT 1
            """,
        ).fetchone()
        if blocked_legacy is not None:
            return float("inf")
        risk_day = _risk_day(current_time)
        realized = self.conn.execute(
            "SELECT COALESCE(SUM(realized_loss_usdc), 0) FROM advisor_daily_risk WHERE risk_day = ?",
            (risk_day,),
        ).fetchone()[0]
        open_reserved = self.conn.execute(
            "SELECT COALESCE(SUM(amount_usdc), 0) FROM advisor_risk_reservations WHERE status = 'reserved'",
        ).fetchone()[0]
        return float(realized) + float(open_reserved)

    def reserve_daily_loss(
        self,
        proposal_id: str,
        amount_usdc: float,
        max_daily_loss_usdc: float,
        *,
        now: float | None = None,
    ) -> bool:
        current_time = time.time() if now is None else float(now)
        amount = float(amount_usdc)
        daily_limit = float(max_daily_loss_usdc)
        if (
            not math.isfinite(current_time)
            or not math.isfinite(amount)
            or not math.isfinite(daily_limit)
            or amount <= 0
            or daily_limit <= 0
        ):
            return False
        risk_day = _risk_day(current_time)
        with self._immediate_transaction():
            existing = self.conn.execute(
                "SELECT status FROM advisor_risk_reservations WHERE proposal_id = ?",
                (proposal_id,),
            ).fetchone()
            if existing is not None:
                return str(existing[0]) == "reserved"
            self._ensure_daily_risk_row(risk_day, current_time)
            realized = float(
                self.conn.execute(
                    "SELECT COALESCE(SUM(realized_loss_usdc), 0) FROM advisor_daily_risk WHERE risk_day = ?",
                    (risk_day,),
                ).fetchone()[0]
            )
            open_reserved = float(
                self.conn.execute(
                    "SELECT COALESCE(SUM(amount_usdc), 0) FROM advisor_risk_reservations WHERE status = 'reserved'",
                ).fetchone()[0]
            )
            proposal = self.conn.execute(
                "SELECT status FROM advisor_proposals WHERE proposal_id = ?",
                (proposal_id,),
            ).fetchone()
            if (
                proposal is None
                or str(proposal[0]) != "revalidating"
                or realized + open_reserved + amount > daily_limit
            ):
                return False
            risk_cursor = self.conn.execute(
                "UPDATE advisor_daily_risk SET reserved_loss_usdc = reserved_loss_usdc + ?, updated_at = ? WHERE risk_day = ?",
                (amount, current_time, risk_day),
            )
            if risk_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("daily_risk_reservation_update_failed")
            self.conn.execute(
                """
                INSERT INTO advisor_risk_reservations
                    (proposal_id, risk_day, amount_usdc, status, created_at)
                VALUES (?, ?, ?, 'reserved', ?)
                """,
                (proposal_id, risk_day, amount, current_time),
            )
        return True

    def release_loss_reservation(self, proposal_id: str, *, now: float | None = None) -> bool:
        current_time = time.time() if now is None else float(now)
        if not math.isfinite(current_time):
            return False
        with self._immediate_transaction():
            row = self.conn.execute(
                """
                SELECT r.risk_day, r.amount_usdc, p.status FROM advisor_risk_reservations r
                JOIN advisor_proposals p ON p.proposal_id = r.proposal_id
                WHERE r.proposal_id = ? AND r.status = 'reserved'
                """,
                (proposal_id,),
            ).fetchone()
            if row is None or str(row[2]) not in {"failed", "rejected", "expired", "integrity_failed"}:
                return False
            return self._release_reservation_locked(proposal_id, row, current_time)

    def settle_loss_reservation(
        self,
        proposal_id: str,
        realized_loss_usdc: float,
        *,
        realized_pnl_usdc: float | None = None,
        now: float | None = None,
    ) -> bool:
        current_time = time.time() if now is None else float(now)
        realized_loss = float(realized_loss_usdc)
        realized_pnl = -realized_loss if realized_pnl_usdc is None else float(realized_pnl_usdc)
        if (
            not math.isfinite(current_time)
            or not math.isfinite(realized_loss)
            or realized_loss < 0
            or not math.isfinite(realized_pnl)
        ):
            return False
        expected_loss = max(-realized_pnl, 0.0)
        if not math.isclose(realized_loss, expected_loss, rel_tol=0.0, abs_tol=1e-6):
            return False
        risk_loss = expected_loss
        with self._immediate_transaction():
            row = self.conn.execute(
                """
                SELECT r.risk_day, r.amount_usdc, p.status FROM advisor_risk_reservations r
                JOIN advisor_proposals p ON p.proposal_id = r.proposal_id
                WHERE r.proposal_id = ? AND r.status = 'reserved'
                """,
                (proposal_id,),
            ).fetchone()
            if row is None or str(row[2]) not in {"submitted", "filled", "partial"}:
                return False
            if realized_loss > float(row[1]) + 1e-6:
                return False
            self._ensure_daily_risk_row(str(row[0]), current_time)
            reserved_cursor = self.conn.execute(
                """
                UPDATE advisor_daily_risk
                SET reserved_loss_usdc = MAX(reserved_loss_usdc - ?, 0),
                    updated_at = ?
                WHERE risk_day = ?
                """,
                (float(row[1]), current_time, str(row[0])),
            )
            if reserved_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("daily_risk_settlement_release_failed")
            self._ensure_daily_risk_row(_risk_day(current_time), current_time)
            realized_cursor = self.conn.execute(
                """
                UPDATE advisor_daily_risk
                SET realized_loss_usdc = realized_loss_usdc + ?, updated_at = ?
                WHERE risk_day = ?
                """,
                (risk_loss, current_time, _risk_day(current_time)),
            )
            if realized_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("daily_risk_settlement_update_failed")
            cursor = self.conn.execute(
                """
                UPDATE advisor_risk_reservations
                SET status = 'settled', released_at = ?
                WHERE proposal_id = ? AND status = 'reserved'
                """,
                (current_time, proposal_id),
            )
            proposal_cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = 'settled', finalized_at = ?, realized_pnl_usdc = ?,
                    execution_status = CASE WHEN execution_status = '' THEN 'submitted' ELSE execution_status END,
                    decision_note = 'execution_settled'
                WHERE proposal_id = ? AND status IN ('submitted', 'filled', 'partial')
                """,
                (current_time, realized_pnl, proposal_id),
            )
            claim_cursor = self.conn.execute(
                """
                UPDATE advisor_execution_claims
                SET status = 'settled'
                WHERE proposal_id = ? AND status IN ('claimed', 'submitted', 'filled', 'partial', 'reconciliation_required')
                """,
                (proposal_id,),
            )
            if cursor.rowcount != 1 or proposal_cursor.rowcount != 1 or claim_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("settlement_transition_failed")
        return True

    def record_and_decide(
        self,
        *,
        code: str,
        decision: str,
        sender: str,
        provider_message_id: str,
        reply_message_id: str,
        payload_hash: str,
        now: float | None = None,
    ) -> ApprovalResult:
        current_time = time.time() if now is None else float(now)
        normalized_decision = decision.strip().lower()
        if normalized_decision not in {"yes", "no"}:
            return ApprovalResult("rejected", reason="decision_invalid")
        if not provider_message_id:
            return ApprovalResult("rejected", reason="message_id_missing")
        if not reply_message_id:
            return ApprovalResult("rejected", reason="sent_message_context_missing")

        try:
            with self.conn:
                row = self.conn.execute(
                    """
                    SELECT p.*, o.provider_message_id
                    FROM advisor_proposals p
                    JOIN advisor_outbox o ON o.proposal_id = p.proposal_id
                    WHERE p.confirmation_code_hash = ?
                      AND o.status = 'sent' AND o.provider_message_id = ?
                    """,
                    (confirmation_code_hash(code), reply_message_id),
                ).fetchone()
                if row is None:
                    return ApprovalResult("rejected", reason="proposal_not_found")
                proposal_id = str(row["proposal_id"])
                if not self._verify_row_integrity(row):
                    return ApprovalResult("rejected", proposal_id, "proposal_integrity_invalid")
                duplicate = self.conn.execute(
                    "SELECT 1 FROM advisor_inbound_messages WHERE provider_message_id = ?",
                    (provider_message_id,),
                ).fetchone()
                if duplicate is not None:
                    return ApprovalResult("duplicate", proposal_id, "message_already_processed")
                self.conn.execute(
                    """
                    INSERT INTO advisor_inbound_messages
                    (provider_message_id, received_at, sender, payload_hash)
                    VALUES (?, ?, ?, ?)
                    """,
                    (provider_message_id, current_time, sender, payload_hash),
                )
                if str(row["status"]) != "sent":
                    return ApprovalResult("rejected", proposal_id, "proposal_not_pending")
                if min(float(row["expires_at"]), float(row["quote_expires_at"])) <= current_time:
                    self.conn.execute(
                        "UPDATE advisor_proposals SET status = 'expired', decision_note = 'reply_after_expiry' WHERE proposal_id = ?",
                        (proposal_id,),
                    )
                    return ApprovalResult("expired", proposal_id, "proposal_expired")
                next_status = "confirmed" if normalized_decision == "yes" else "rejected"
                cursor = self.conn.execute(
                    """
                    UPDATE advisor_proposals
                    SET status = ?, approval_at = ?, approval_message_id = ?, approval_payload_hash = ?,
                        approval_receipt = ?,
                        confirmed_at = CASE WHEN ? = 'confirmed' THEN ? ELSE confirmed_at END,
                        confirmed_from = CASE WHEN ? = 'confirmed' THEN ? ELSE confirmed_from END,
                        decision_note = ?
                    WHERE proposal_id = ? AND status IN ('pending', 'sent')
                    """,
                    (
                        next_status,
                        current_time,
                        provider_message_id,
                        payload_hash,
                        approval_receipt_token(
                            proposal_id=proposal_id,
                            fingerprint=str(row["fingerprint"]),
                            confirmation_code_hash_value=str(row["confirmation_code_hash"]),
                            decision=normalized_decision,
                            sender=sender,
                            inbound_message_id=provider_message_id,
                            outbound_message_id=reply_message_id,
                            payload_hash=payload_hash,
                            approved_at=current_time,
                            integrity_key=self.integrity_key,
                        ),
                        next_status,
                        current_time,
                        next_status,
                        sender,
                        f"whatsapp_{normalized_decision}",
                        proposal_id,
                    ),
                )
                if cursor.rowcount != 1:
                    return ApprovalResult("duplicate", proposal_id, "proposal_already_decided")
                return ApprovalResult(next_status, proposal_id)
        except sqlite3.IntegrityError:
            return ApprovalResult("duplicate", reason="message_already_processed")
        except sqlite3.OperationalError:
            return ApprovalResult("retry", reason="database_busy")

    def claim_confirmed(self, proposal_id: str, *, now: float | None = None) -> bool:
        current_time = time.time() if now is None else float(now)
        with self._immediate_transaction():
            row = self.conn.execute(
                "SELECT * FROM advisor_proposals WHERE proposal_id = ?",
                (proposal_id,),
            ).fetchone()
            if (
                row is None
                or row["claimed_at"] is not None
                or str(row["order_id"] or "")
                or row["submitted_at"] is not None
                or not self._verify_row_integrity(row)
                or not self._approval_receipt_valid(row)
            ):
                return False
            claim = self.conn.execute(
                """
                INSERT INTO advisor_execution_claims
                    (proposal_id, claimed_at, status)
                SELECT ?, ?, 'claimed'
                WHERE EXISTS (
                    SELECT 1 FROM advisor_proposals
                    WHERE proposal_id = ? AND status = 'confirmed'
                      AND claimed_at IS NULL AND order_id = ''
                      AND submitted_at IS NULL AND expires_at > ? AND quote_expires_at > ?
                )
                  AND NOT EXISTS (
                    SELECT 1 FROM advisor_execution_claims
                    WHERE proposal_id = ?
                  )
                """,
                (proposal_id, current_time, proposal_id, current_time, current_time, proposal_id),
            )
            if claim.rowcount != 1:
                return False
            cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = 'revalidating', claimed_at = ?
                WHERE proposal_id = ? AND status = 'confirmed' AND claimed_at IS NULL
                  AND order_id = '' AND submitted_at IS NULL
                  AND expires_at > ? AND quote_expires_at > ?
                """,
                (current_time, proposal_id, current_time, current_time),
            )
        return cursor.rowcount == 1

    def proposal_integrity_valid(self, proposal_id: str) -> bool:
        row = self.get_proposal(proposal_id)
        return row is not None and self._verify_row_integrity(row)

    def mark_execution_submitted(
        self,
        proposal_id: str,
        order_id: str,
        note: str = "",
        *,
        execution_status: str = "submitted",
        filled_size: float | None = None,
        now: float | None = None,
    ) -> bool:
        if not order_id:
            return False
        normalized_status = str(execution_status or "").strip().lower()
        if normalized_status not in {"submitted", "filled", "partial"}:
            return False
        if filled_size is not None and (not math.isfinite(float(filled_size)) or float(filled_size) < 0):
            return False
        if normalized_status in {"filled", "partial"} and (filled_size is None or float(filled_size) <= 0):
            return False
        submitted_at = time.time() if now is None else float(now)
        if not math.isfinite(submitted_at):
            return False
        with self._immediate_transaction():
            claim = self.conn.execute(
                """
                SELECT status, order_id FROM advisor_execution_claims
                WHERE proposal_id = ? AND status IN ('claimed', 'reconciliation_required')
                """,
                (proposal_id,),
            ).fetchone()
            if claim is None:
                return False
            proposal_row = self.conn.execute(
                "SELECT order_id FROM advisor_proposals WHERE proposal_id = ? AND status = 'revalidating'",
                (proposal_id,),
            ).fetchone()
            if proposal_row is None or not _order_ids_compatible(
                str(proposal_row["order_id"] or ""),
                str(claim["order_id"] or ""),
                str(order_id),
            ):
                return False
            cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = ?, order_id = ?, submitted_at = ?, execution_status = ?,
                    filled_size = ?, decision_note = ?
                WHERE proposal_id = ? AND status = 'revalidating'
                """,
                (normalized_status, order_id, submitted_at, normalized_status, filled_size, note, proposal_id),
            )
            if cursor.rowcount == 1:
                claim_cursor = self.conn.execute(
                    """
                    UPDATE advisor_execution_claims
                    SET status = 'submitted', order_id = ?, submitted_at = ?
                    WHERE proposal_id = ? AND status IN ('claimed', 'reconciliation_required')
                    """,
                    (order_id, submitted_at, proposal_id),
                )
                if claim_cursor.rowcount != 1:
                    raise sqlite3.IntegrityError("execution_claim_transition_failed")
                self.conn.execute(
                    "UPDATE advisor_execution_claims SET status = ? WHERE proposal_id = ?",
                    (normalized_status, proposal_id),
                )
        return cursor.rowcount == 1

    def recover_stuck_executions(self, *, cutoff: float) -> int:
        with self._immediate_transaction():
            cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = 'reconciliation_required', decision_note = 'execution_result_unknown'
                WHERE status = 'revalidating' AND claimed_at <= ?
                """,
                (float(cutoff),),
            )
            self.conn.execute(
                """
                UPDATE advisor_execution_claims
                SET status = 'reconciliation_required'
                WHERE status = 'claimed' AND proposal_id IN (
                    SELECT proposal_id FROM advisor_proposals
                    WHERE status = 'reconciliation_required'
                )
                """
            )
        return cursor.rowcount

    def mark_execution_failed(self, proposal_id: str, note: str) -> bool:
        with self._immediate_transaction():
            row = self.conn.execute(
                """
                SELECT r.risk_day, r.amount_usdc, p.status FROM advisor_risk_reservations r
                JOIN advisor_proposals p ON p.proposal_id = r.proposal_id
                WHERE r.proposal_id = ? AND r.status = 'reserved'
                """,
                (proposal_id,),
            ).fetchone()
            cursor = self.conn.execute(
                "UPDATE advisor_proposals SET status = 'failed', decision_note = ? WHERE proposal_id = ? AND status = 'revalidating'",
                (str(note)[:500], proposal_id),
            )
            if cursor.rowcount == 1 and row is not None:
                self._release_reservation_locked(proposal_id, row, time.time())
            if cursor.rowcount == 1:
                self.conn.execute(
                    "UPDATE advisor_execution_claims SET status = 'failed' WHERE proposal_id = ? AND status IN ('claimed', 'reconciliation_required')",
                    (proposal_id,),
                )
        return cursor.rowcount == 1

    def _release_reservation_locked(
        self,
        proposal_id: str,
        row: sqlite3.Row,
        current_time: float,
    ) -> bool:
        self._ensure_daily_risk_row(str(row[0]), current_time)
        risk_cursor = self.conn.execute(
            """
            UPDATE advisor_daily_risk
            SET reserved_loss_usdc = MAX(reserved_loss_usdc - ?, 0), updated_at = ?
            WHERE risk_day = ?
            """,
            (float(row[1]), current_time, str(row[0])),
        )
        if risk_cursor.rowcount != 1:
            raise sqlite3.IntegrityError("daily_risk_release_update_failed")
        cursor = self.conn.execute(
            """
            UPDATE advisor_risk_reservations
            SET status = 'released', released_at = ?
            WHERE proposal_id = ? AND status = 'reserved'
            """,
            (current_time, proposal_id),
        )
        return cursor.rowcount == 1

    def mark_reconciliation_required(self, proposal_id: str, note: str, *, order_id: str = "") -> bool:
        with self._immediate_transaction():
            row = self.conn.execute(
                """
                SELECT p.order_id AS proposal_order_id, c.order_id AS claim_order_id
                FROM advisor_proposals p
                LEFT JOIN advisor_execution_claims c ON c.proposal_id = p.proposal_id
                WHERE p.proposal_id = ? AND p.status = 'revalidating'
                """,
                (proposal_id,),
            ).fetchone()
            if row is None or not _order_ids_compatible(
                str(row["proposal_order_id"] or ""),
                str(row["claim_order_id"] or ""),
                str(order_id or ""),
            ):
                return False
            cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = 'reconciliation_required',
                    order_id = CASE WHEN order_id = '' THEN ? ELSE order_id END,
                    decision_note = ?
                WHERE proposal_id = ? AND status = 'revalidating'
                """,
                (str(order_id or ""), str(note)[:500], proposal_id),
            )
            if cursor.rowcount != 1:
                return False
            claim_cursor = self.conn.execute(
                """
                UPDATE advisor_execution_claims
                SET status = 'reconciliation_required',
                    order_id = CASE WHEN order_id = '' THEN ? ELSE order_id END
                WHERE proposal_id = ? AND status IN ('claimed', 'reconciliation_required')
                """,
                (str(order_id or ""), proposal_id),
            )
            if claim_cursor.rowcount != 1:
                raise sqlite3.IntegrityError("reconciliation_claim_transition_failed")
        return True

    def reconcile_execution(
        self,
        proposal_id: str,
        outcome: str,
        *,
        order_id: str = "",
        filled_size: float | None = None,
        note: str = "",
        now: float | None = None,
    ) -> bool:
        """Resolve an ambiguous provider result without editing SQLite manually."""
        current_time = time.time() if now is None else float(now)
        normalized = str(outcome or "").strip().lower()
        if not math.isfinite(current_time):
            return False
        if filled_size is not None and (not math.isfinite(float(filled_size)) or float(filled_size) < 0):
            return False
        with self._immediate_transaction():
            row = self.conn.execute(
                """
                SELECT r.risk_day, r.amount_usdc, p.status, p.order_id,
                       c.order_id AS claim_order_id
                FROM advisor_proposals p
                LEFT JOIN advisor_risk_reservations r
                  ON r.proposal_id = p.proposal_id AND r.status = 'reserved'
                LEFT JOIN advisor_execution_claims c ON c.proposal_id = p.proposal_id
                WHERE p.proposal_id = ?
                """,
                (proposal_id,),
            ).fetchone()
            if row is None or str(row["status"]) != "reconciliation_required":
                return False
            proposal_order_id = str(row["order_id"] or "")
            claim_order_id = str(row["claim_order_id"] or "")
            incoming_order_id = str(order_id or "")
            if not _order_ids_compatible(proposal_order_id, claim_order_id, incoming_order_id):
                return False
            if normalized in {"submitted", "filled", "partial", "confirmed"}:
                # A successful result must have a reservation. Without one,
                # the process may have crashed before the broker call and an
                # operator must not manufacture an open exposure in the ledger.
                effective_order_id = proposal_order_id or claim_order_id or incoming_order_id
                if not effective_order_id or row["risk_day"] is None:
                    return False
                execution_status = "submitted" if normalized in {"submitted", "confirmed"} else normalized
                if execution_status in {"filled", "partial"} and (filled_size is None or float(filled_size) <= 0):
                    return False
                cursor = self.conn.execute(
                    """
                    UPDATE advisor_proposals
                    SET status = ?, order_id = ?, submitted_at = ?, execution_status = ?,
                        filled_size = ?, decision_note = ?
                    WHERE proposal_id = ? AND status = 'reconciliation_required'
                    """,
                    (execution_status, effective_order_id, current_time, execution_status, filled_size, note or normalized, proposal_id),
                )
                if cursor.rowcount == 1:
                    claim_cursor = self.conn.execute(
                        "UPDATE advisor_execution_claims SET status = ?, order_id = ?, submitted_at = ? WHERE proposal_id = ? AND status = 'reconciliation_required'",
                        (execution_status, effective_order_id, current_time, proposal_id),
                    )
                    if claim_cursor.rowcount != 1:
                        raise sqlite3.IntegrityError("reconciliation_claim_transition_failed")
                return cursor.rowcount == 1
            if normalized == "cancelled" and filled_size != 0:
                return False
            if filled_size is not None and float(filled_size) != 0:
                return False
            if normalized not in {"failed", "rejected", "cancelled", "invalid", "insufficient_funds"}:
                return False
            cursor = self.conn.execute(
                """
                UPDATE advisor_proposals
                SET status = 'failed', decision_note = ?
                WHERE proposal_id = ? AND status = 'reconciliation_required'
                """,
                (note or normalized, proposal_id),
            )
            if cursor.rowcount != 1:
                return False
            if row["risk_day"] is None:
                self.conn.execute(
                    "UPDATE advisor_execution_claims SET status = 'failed' WHERE proposal_id = ? AND status = 'reconciliation_required'",
                    (proposal_id,),
                )
                return True
            released = self._release_reservation_locked(proposal_id, row, current_time)
            if released:
                self.conn.execute(
                    "UPDATE advisor_execution_claims SET status = 'failed' WHERE proposal_id = ? AND status = 'reconciliation_required'",
                    (proposal_id,),
                )
            return released

    def _verify_row_integrity(self, row: sqlite3.Row) -> bool:
        if not self.integrity_key:
            return False
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(payload, dict):
            return False
        fingerprint = proposal_fingerprint_payload(payload)
        if not hmac.compare_digest(fingerprint, str(row["fingerprint"] or "")):
            return False
        expected = proposal_integrity_token(fingerprint, self.integrity_key)
        return hmac.compare_digest(expected, str(row["integrity_token"] or ""))

    def _approval_receipt_valid(self, row: sqlite3.Row) -> bool:
        if str(row["status"] or "") != "confirmed" or not self.integrity_key:
            return False
        approval_at = row["approval_at"]
        inbound_id = str(row["approval_message_id"] or "")
        payload_hash = str(row["approval_payload_hash"] or "")
        receipt = str(row["approval_receipt"] or "")
        sender = str(row["confirmed_from"] or "")
        if approval_at is None or not inbound_id or not payload_hash or not receipt or not sender:
            return False
        outbound = self.conn.execute(
            "SELECT provider_message_id FROM advisor_outbox WHERE proposal_id = ? AND status = 'sent'",
            (str(row["proposal_id"]),),
        ).fetchone()
        inbound = self.conn.execute(
            "SELECT sender, payload_hash FROM advisor_inbound_messages WHERE provider_message_id = ?",
            (inbound_id,),
        ).fetchone()
        if outbound is None or inbound is None:
            return False
        if str(inbound["sender"]) != sender or str(inbound["payload_hash"]) != payload_hash:
            return False
        expected = approval_receipt_token(
            proposal_id=str(row["proposal_id"]),
            fingerprint=str(row["fingerprint"]),
            confirmation_code_hash_value=str(row["confirmation_code_hash"]),
            decision="yes",
            sender=sender,
            inbound_message_id=inbound_id,
            outbound_message_id=str(outbound[0]),
            payload_hash=payload_hash,
            approved_at=float(approval_at),
            integrity_key=self.integrity_key,
        )
        return hmac.compare_digest(expected, receipt)


def _proposal_payload(proposal: TradeProposal) -> str:
    return json.dumps(
        proposal_payload(proposal),
        sort_keys=True,
        separators=(",", ":"),
    )


def _risk_day(timestamp: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(timestamp))


def _finite_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _legacy_notional(payload_json: object) -> float | None:
    try:
        payload = json.loads(str(payload_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    amount = _finite_float(payload.get("max_notional_usdc"))
    return amount if amount is not None and amount > 0 else None


def _order_ids_compatible(proposal_order_id: str, claim_order_id: str, incoming_order_id: str) -> bool:
    """Allow filling an empty ID or repeating one exact ID, never replacing it."""
    existing = {value for value in (proposal_order_id, claim_order_id) if value}
    if len(existing) > 1:
        return False
    return not incoming_order_id or not existing or incoming_order_id in existing
