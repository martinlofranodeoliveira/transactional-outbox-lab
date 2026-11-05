"""SQLite outbox with explicit transactions, persisted logical time and fencing."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from outbox_lab.errors import (
    ClockRegressionError,
    DomainVersionConflictError,
    DuplicateMessageError,
    InvalidStateTransitionError,
    LeaseExpiredError,
    MessageNotFoundError,
    OutboxConstraintError,
    OutboxError,
    OutboxValidationError,
    StaleFencingTokenError,
    StoreBusyError,
)
from outbox_lab.models import AuditEntry, DomainMutation, DomainRecord, OutboxDraft, OutboxRecord, OutboxState
from outbox_lab.validation import (
    INT64_MAX,
    MAX_ATTEMPTS_LIMIT,
    MAX_AUDIT_ROWS_LIMIT,
    MAX_BATCH_SIZE,
    MAX_LEASE_DURATION_MS,
    compute_retry_backoff_ms,
    freeze_canonical_json,
    sanitize_error_reason,
    validate_error_code,
    validate_identifier,
    validate_positive_int,
    validate_timestamp_ms,
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS outbox_meta (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    clock_ms INTEGER NOT NULL CHECK (clock_ms >= 0),
    last_fencing_token INTEGER NOT NULL CHECK (last_fencing_token >= 0)
);
INSERT OR IGNORE INTO outbox_meta VALUES (1, 0, 0);
CREATE TABLE IF NOT EXISTS domain_state (
    aggregate_type TEXT NOT NULL CHECK (length(aggregate_type) BETWEEN 1 AND 64),
    aggregate_id TEXT NOT NULL CHECK (length(aggregate_id) BETWEEN 1 AND 128),
    version INTEGER NOT NULL CHECK (version >= 1),
    state_json TEXT NOT NULL CHECK (length(CAST(state_json AS BLOB)) BETWEEN 2 AND 16384),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),
    PRIMARY KEY (aggregate_type, aggregate_id)
);
CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY NOT NULL CHECK (length(message_id) BETWEEN 1 AND 128),
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL CHECK (aggregate_version >= 1),
    event_type TEXT NOT NULL CHECK (length(event_type) BETWEEN 1 AND 128),
    payload_json TEXT NOT NULL CHECK (length(CAST(payload_json AS BLOB)) BETWEEN 2 AND 16384),
    state TEXT NOT NULL CHECK (state IN ('pending', 'leased', 'published', 'dead_letter')),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0 AND attempt_count <= max_attempts),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    next_attempt_at_ms INTEGER,
    lease_owner TEXT,
    lease_expires_at_ms INTEGER,
    fencing_token INTEGER NOT NULL CHECK (fencing_token >= 0),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
    published_at_ms INTEGER,
    dead_lettered_at_ms INTEGER,
    last_error_code TEXT,
    last_error_reason TEXT,
    FOREIGN KEY (aggregate_type, aggregate_id) REFERENCES domain_state (aggregate_type, aggregate_id),
    CHECK (
        (state = 'pending' AND attempt_count < max_attempts
            AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND next_attempt_at_ms IS NOT NULL AND published_at_ms IS NULL AND dead_lettered_at_ms IS NULL)
        OR (state = 'leased' AND attempt_count < max_attempts
            AND lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL
            AND next_attempt_at_ms IS NOT NULL AND fencing_token >= 1
            AND published_at_ms IS NULL AND dead_lettered_at_ms IS NULL)
        OR (state = 'published' AND attempt_count >= 1
            AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND next_attempt_at_ms IS NULL AND published_at_ms IS NOT NULL AND dead_lettered_at_ms IS NULL)
        OR (state = 'dead_letter' AND attempt_count = max_attempts
            AND lease_owner IS NULL AND lease_expires_at_ms IS NULL
            AND next_attempt_at_ms IS NULL AND published_at_ms IS NULL AND dead_lettered_at_ms IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_outbox_claim_order
ON outbox_messages (state, created_at_ms, message_id);
CREATE TRIGGER IF NOT EXISTS outbox_event_immutable
BEFORE UPDATE OF message_id, aggregate_type, aggregate_id, aggregate_version,
    event_type, payload_json, max_attempts, created_at_ms ON outbox_messages
BEGIN
    SELECT RAISE(ABORT, 'immutable outbox event fields');
END;
CREATE TABLE IF NOT EXISTS outbox_audit (
    audit_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
    action TEXT NOT NULL,
    message_id TEXT NOT NULL,
    worker_id TEXT,
    from_state TEXT,
    to_state TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    fencing_token INTEGER NOT NULL,
    detail_code TEXT
);
"""


class OutboxStore:
    """Single-thread-owned connection; separate workers use separate connections."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_ms: int = 0,
        max_audit_rows: int = 500,
        initialize_schema: bool = True,
    ) -> None:
        validate_positive_int(busy_timeout_ms, "busy_timeout_ms", minimum=0, maximum=60_000)
        validate_positive_int(max_audit_rows, "max_audit_rows", maximum=MAX_AUDIT_ROWS_LIMIT)
        if type(initialize_schema) is not bool:
            raise OutboxValidationError("initialize_schema must be a boolean")
        if not isinstance(db_path, (str, Path)):
            raise OutboxValidationError("db_path must be a string or Path")
        self._db_path = str(db_path)
        self._max_audit_rows = max_audit_rows
        self._conn: sqlite3.Connection | None = None
        try:
            self._conn = sqlite3.connect(
                self._db_path, timeout=busy_timeout_ms / 1000, isolation_level=None
            )
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON")
            if initialize_schema:
                self._conn.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA_SQL + "\nCOMMIT;")
        except sqlite3.Error as exc:
            self.close()
            self._raise_storage_error(exc)

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def max_audit_rows(self) -> int:
        return self._max_audit_rows

    @property
    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            raise OutboxConstraintError("Store is closed")
        return self._conn

    def close(self) -> None:
        """Idempotent, including after explicit close inside a with block."""
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                if conn.in_transaction:
                    conn.rollback()
            finally:
                conn.close()

    def __enter__(self) -> OutboxStore:
        self._db
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    @staticmethod
    def _raise_storage_error(exc: sqlite3.Error) -> None:
        code = getattr(exc, "sqlite_errorcode", 0) & 255
        if code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            raise StoreBusyError("SQLite database is busy or locked") from exc
        raise OutboxConstraintError("SQLite operation failed; no raw database diagnostic is exposed") from exc

    def _rollback(self) -> None:
        if self._conn is not None and self._conn.in_transaction:
            try:
                self._conn.rollback()
            except sqlite3.Error:
                pass

    @contextmanager
    def _transaction(
        self,
        now_ms: int,
        worker: tuple[str, str, int] | None = None,
    ) -> Iterator[None]:
        """Keep clock and semantic rejection in one write lock, not a second best-effort transaction."""
        conn = self._db
        try:
            conn.execute("BEGIN IMMEDIATE")
            meta = conn.execute("SELECT clock_ms FROM outbox_meta WHERE singleton_id = 1").fetchone()
            if meta is None:
                raise OutboxConstraintError("Store metadata is missing")
            if now_ms < meta["clock_ms"]:
                raise ClockRegressionError("Caller timestamp precedes the persisted store clock")
            conn.execute("UPDATE outbox_meta SET clock_ms = ? WHERE singleton_id = 1", (now_ms,))
            conn.execute("SAVEPOINT operation_effects")
            try:
                yield
            except (OutboxError, sqlite3.IntegrityError) as exc:
                if not conn.in_transaction:
                    raise
                conn.execute("ROLLBACK TO operation_effects")
                conn.execute("RELEASE operation_effects")
                if worker is not None:
                    self._audit_rejection(now_ms, worker, exc)
                conn.execute("COMMIT")
                if isinstance(exc, sqlite3.IntegrityError):
                    raise OutboxConstraintError("Database integrity constraint rejected the operation") from exc
                raise
            else:
                conn.execute("RELEASE operation_effects")
                conn.execute("COMMIT")
        except sqlite3.Error as exc:
            self._rollback()
            self._raise_storage_error(exc)
        except BaseException:
            self._rollback()
            raise

    def _audit_rejection(
        self, now_ms: int, worker: tuple[str, str, int], exc: Exception
    ) -> None:
        actions = {
            StaleFencingTokenError: "rejected_stale_fencing_token",
            InvalidStateTransitionError: "rejected_invalid_state",
            LeaseExpiredError: "rejected_lease_expired",
        }
        action = actions.get(type(exc))
        if action is None:
            return
        message_id, worker_id, token = worker
        row = self._db.execute("SELECT * FROM outbox_messages WHERE message_id = ?", (message_id,)).fetchone()
        if row is not None:
            self._audit(
                now_ms, action, message_id, worker_id, row["state"], row["state"],
                row["attempt_count"], token, exc.error_code,
            )

    def _audit(
        self, now_ms: int, action: str, message_id: str, worker_id: str | None,
        from_state: str | None, to_state: str, attempts: int, token: int,
        detail: str | None,
    ) -> None:
        self._db.execute(
            """INSERT INTO outbox_audit
            (recorded_at_ms, action, message_id, worker_id, from_state, to_state,
             attempt_count, fencing_token, detail_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (now_ms, action, message_id, worker_id, from_state, to_state, attempts, token, detail),
        )
        self._db.execute(
            """DELETE FROM outbox_audit WHERE audit_seq <= (
                SELECT audit_seq FROM outbox_audit ORDER BY audit_seq DESC LIMIT 1 OFFSET ?
            )""", (self._max_audit_rows,),
        )

    def _query(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        try:
            return self._db.execute(sql, parameters).fetchall()
        except sqlite3.Error as exc:
            self._raise_storage_error(exc)
        raise AssertionError("unreachable")

    def get_clock_ms(self) -> int:
        rows = self._query("SELECT clock_ms FROM outbox_meta WHERE singleton_id = 1")
        if not rows:
            raise OutboxConstraintError("Store metadata is missing")
        return rows[0][0]

    def get_last_fencing_token(self) -> int:
        rows = self._query("SELECT last_fencing_token FROM outbox_meta WHERE singleton_id = 1")
        if not rows:
            raise OutboxConstraintError("Store metadata is missing")
        return rows[0][0]

    def commit_domain_and_outbox(
        self, domain: DomainMutation, outbox: OutboxDraft, *, now_ms: int,
    ) -> tuple[DomainRecord, OutboxRecord]:
        validate_timestamp_ms(now_ms)
        if not isinstance(domain, DomainMutation) or not isinstance(outbox, OutboxDraft):
            raise OutboxValidationError("Expected DomainMutation and OutboxDraft")
        aggregate_type = validate_identifier(domain.aggregate_type, "aggregate_type", max_len=64)
        aggregate_id = validate_identifier(domain.aggregate_id, "aggregate_id")
        version = validate_positive_int(domain.version, "version")
        message_id = validate_identifier(outbox.message_id, "message_id")
        event_type = validate_identifier(outbox.event_type, "event_type")
        maximum = validate_positive_int(outbox.max_attempts, "max_attempts", maximum=MAX_ATTEMPTS_LIMIT)
        state_json = freeze_canonical_json(domain.state, field_name="domain.state")
        payload_json = freeze_canonical_json(outbox.payload, field_name="outbox.payload")
        with self._transaction(now_ms):
            previous = self._db.execute(
                "SELECT version FROM domain_state WHERE aggregate_type = ? AND aggregate_id = ?",
                (aggregate_type, aggregate_id),
            ).fetchone()
            expected = 1 if previous is None else previous[0] + 1
            if version != expected:
                raise DomainVersionConflictError("Aggregate version must start at 1 and advance by exactly 1")
            self._db.execute(
                """INSERT INTO domain_state VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(aggregate_type, aggregate_id) DO UPDATE SET
                    version = excluded.version, state_json = excluded.state_json,
                    updated_at_ms = excluded.updated_at_ms""",
                (aggregate_type, aggregate_id, version, state_json, now_ms),
            )
            if self._db.execute("SELECT 1 FROM outbox_messages WHERE message_id = ?", (message_id,)).fetchone():
                raise DuplicateMessageError("Outbox message_id already exists")
            self._db.execute(
                """INSERT INTO outbox_messages
                (message_id, aggregate_type, aggregate_id, aggregate_version, event_type,
                 payload_json, state, attempt_count, max_attempts, next_attempt_at_ms,
                 fencing_token, created_at_ms, updated_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, 0, ?, ?)""",
                (message_id, aggregate_type, aggregate_id, version, event_type, payload_json,
                 maximum, now_ms, now_ms, now_ms),
            )
            self._audit(now_ms, "staged", message_id, None, None, "pending", 0, 0, "atomic_commit")
            domain_record = self.get_domain_record(aggregate_type, aggregate_id)
            outbox_record = self.get_outbox_record(message_id)
            if domain_record is None or outbox_record is None:
                raise OutboxConstraintError("Atomic write did not produce both records")
        return domain_record, outbox_record

    def claim_batch(
        self, *, worker_id: str, now_ms: int, lease_duration_ms: int, batch_size: int = 10,
    ) -> list[OutboxRecord]:
        validate_identifier(worker_id, "worker_id", max_len=64)
        validate_timestamp_ms(now_ms)
        validate_positive_int(lease_duration_ms, "lease_duration_ms", maximum=MAX_LEASE_DURATION_MS)
        validate_positive_int(batch_size, "batch_size", maximum=MAX_BATCH_SIZE)
        with self._transaction(now_ms):
            expires = self._add_time(now_ms, lease_duration_ms)
            rows = self._db.execute(
                """SELECT * FROM outbox_messages
                WHERE (state = 'pending' AND next_attempt_at_ms <= ?)
                   OR (state = 'leased' AND lease_expires_at_ms <= ?)
                ORDER BY created_at_ms, message_id LIMIT ?""", (now_ms, now_ms, batch_size),
            ).fetchall()
            token = self.get_last_fencing_token()
            if len(rows) > INT64_MAX - token:
                raise OutboxConstraintError("Fencing token space is exhausted")
            result = []
            for row in rows:
                token += 1
                self._db.execute(
                    """UPDATE outbox_messages SET state = 'leased', lease_owner = ?,
                    lease_expires_at_ms = ?, fencing_token = ?, updated_at_ms = ? WHERE message_id = ?""",
                    (worker_id, expires, token, now_ms, row["message_id"]),
                )
                reclaimed = row["state"] == "leased"
                self._audit(
                    now_ms, "lease_reclaimed" if reclaimed else "lease_claimed", row["message_id"],
                    worker_id, row["state"], "leased", row["attempt_count"], token,
                    "expired_lease_takeover" if reclaimed else "initial_or_retry_claim",
                )
                result.append(self._fetch_outbox(row["message_id"]))
            self._db.execute("UPDATE outbox_meta SET last_fencing_token = ? WHERE singleton_id = 1", (token,))
        return result

    @staticmethod
    def _add_time(now_ms: int, duration: int) -> int:
        if duration > INT64_MAX - now_ms:
            raise OutboxConstraintError("Scheduled timestamp exceeds signed 64-bit bounds")
        return now_ms + duration

    @staticmethod
    def _worker_input(message_id: str, worker_id: str, fencing_token: int, now_ms: int) -> None:
        validate_identifier(message_id, "message_id")
        validate_identifier(worker_id, "worker_id", max_len=64)
        validate_positive_int(fencing_token, "fencing_token")
        validate_timestamp_ms(now_ms)

    def _lease(self, message_id: str, worker_id: str, token: int, now_ms: int) -> sqlite3.Row:
        row = self._db.execute("SELECT * FROM outbox_messages WHERE message_id = ?", (message_id,)).fetchone()
        if row is None:
            raise MessageNotFoundError("Outbox message does not exist")
        if row["fencing_token"] != token:
            raise StaleFencingTokenError("Fencing token does not match the current record")
        if row["state"] != "leased":
            raise InvalidStateTransitionError("Operation requires a leased record")
        if row["lease_owner"] != worker_id:
            raise StaleFencingTokenError("Worker does not own the current lease")
        if now_ms >= row["lease_expires_at_ms"]:
            raise LeaseExpiredError("Lease has expired")
        return row

    def check_lease(
        self, *, message_id: str, worker_id: str, fencing_token: int, now_ms: int,
    ) -> OutboxRecord:
        """Persist observed time and return an authoritative fenced snapshot, without renewal."""
        self._worker_input(message_id, worker_id, fencing_token, now_ms)
        with self._transaction(now_ms, (message_id, worker_id, fencing_token)):
            record = self._row_to_outbox_record(self._lease(message_id, worker_id, fencing_token, now_ms))
        return record

    def _fenced_update(
        self, assignments: str, values: tuple[Any, ...], message_id: str,
        worker_id: str, token: int, now_ms: int,
    ) -> None:
        # assignments is always a fixed internal SQL literal, never caller input.
        cursor = self._db.execute(
            "UPDATE outbox_messages SET " + assignments + """
            WHERE message_id = ? AND state = 'leased' AND fencing_token = ?
            AND lease_owner = ? AND lease_expires_at_ms > ?""",
            values + (message_id, token, worker_id, now_ms),
        )
        if cursor.rowcount != 1:
            raise OutboxConstraintError("Fenced update did not affect exactly one record")

    def mark_published(
        self, *, message_id: str, worker_id: str, fencing_token: int, now_ms: int,
    ) -> OutboxRecord:
        self._worker_input(message_id, worker_id, fencing_token, now_ms)
        with self._transaction(now_ms, (message_id, worker_id, fencing_token)):
            row = self._lease(message_id, worker_id, fencing_token, now_ms)
            attempt = row["attempt_count"] + 1
            self._fenced_update(
                """state = 'published', attempt_count = ?, next_attempt_at_ms = NULL,
                lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?, published_at_ms = ?""",
                (attempt, now_ms, now_ms), message_id, worker_id, fencing_token, now_ms,
            )
            self._audit(now_ms, "published", message_id, worker_id, "leased", "published", attempt, fencing_token, "ack_committed")
            result = self._fetch_outbox(message_id)
        return result

    def record_failure(
        self, *, message_id: str, worker_id: str, fencing_token: int, now_ms: int,
        error_code: str, error_reason: str, base_backoff_ms: int = 1_000,
        max_backoff_ms: int = 60_000,
    ) -> OutboxRecord:
        self._worker_input(message_id, worker_id, fencing_token, now_ms)
        code = validate_error_code(error_code)
        reason = sanitize_error_reason(error_reason)
        compute_retry_backoff_ms(1, base_backoff_ms=base_backoff_ms, max_backoff_ms=max_backoff_ms)
        with self._transaction(now_ms, (message_id, worker_id, fencing_token)):
            row = self._lease(message_id, worker_id, fencing_token, now_ms)
            attempt = row["attempt_count"] + 1
            exhausted = attempt == row["max_attempts"]
            state = "dead_letter" if exhausted else "pending"
            next_time = None if exhausted else self._add_time(
                now_ms, compute_retry_backoff_ms(attempt, base_backoff_ms=base_backoff_ms, max_backoff_ms=max_backoff_ms)
            )
            self._fenced_update(
                """state = ?, attempt_count = ?, next_attempt_at_ms = ?, lease_owner = NULL,
                lease_expires_at_ms = NULL, updated_at_ms = ?, dead_lettered_at_ms = ?,
                last_error_code = ?, last_error_reason = ?""",
                (state, attempt, next_time, now_ms, now_ms if exhausted else None, code, reason),
                message_id, worker_id, fencing_token, now_ms,
            )
            self._audit(
                now_ms, "dead_lettered" if exhausted else "retry_scheduled", message_id,
                worker_id, "leased", state, attempt, fencing_token, code,
            )
            result = self._fetch_outbox(message_id)
        return result

    def renew_lease(
        self, *, message_id: str, worker_id: str, fencing_token: int, now_ms: int,
        lease_duration_ms: int,
    ) -> OutboxRecord:
        self._worker_input(message_id, worker_id, fencing_token, now_ms)
        validate_positive_int(lease_duration_ms, "lease_duration_ms", maximum=MAX_LEASE_DURATION_MS)
        with self._transaction(now_ms, (message_id, worker_id, fencing_token)):
            row = self._lease(message_id, worker_id, fencing_token, now_ms)
            expires = max(row["lease_expires_at_ms"], self._add_time(now_ms, lease_duration_ms))
            self._fenced_update(
                "lease_expires_at_ms = ?, updated_at_ms = ?", (expires, now_ms),
                message_id, worker_id, fencing_token, now_ms,
            )
            self._audit(now_ms, "lease_renewed", message_id, worker_id, "leased", "leased", row["attempt_count"], fencing_token, "heartbeat_extended")
            result = self._fetch_outbox(message_id)
        return result

    def get_domain_record(self, aggregate_type: str, aggregate_id: str) -> DomainRecord | None:
        validate_identifier(aggregate_type, "aggregate_type", max_len=64)
        validate_identifier(aggregate_id, "aggregate_id")
        rows = self._query(
            "SELECT * FROM domain_state WHERE aggregate_type = ? AND aggregate_id = ?",
            (aggregate_type, aggregate_id),
        )
        return DomainRecord(**dict(rows[0])) if rows else None

    def get_outbox_record(self, message_id: str) -> OutboxRecord | None:
        validate_identifier(message_id, "message_id")
        rows = self._query("SELECT * FROM outbox_messages WHERE message_id = ?", (message_id,))
        return self._row_to_outbox_record(rows[0]) if rows else None

    def _fetch_outbox(self, message_id: str) -> OutboxRecord:
        record = self.get_outbox_record(message_id)
        if record is None:
            raise OutboxConstraintError("Expected outbox record is missing")
        return record

    @staticmethod
    def _page(limit: int, offset: int) -> None:
        validate_positive_int(limit, "limit", maximum=MAX_BATCH_SIZE)
        validate_positive_int(offset, "offset", minimum=0)

    def list_outbox_records(
        self, *, state: OutboxState | str | None = None, limit: int = 500, offset: int = 0,
    ) -> list[OutboxRecord]:
        self._page(limit, offset)
        if state is not None:
            try:
                state = OutboxState(state)
            except (ValueError, TypeError):
                raise OutboxValidationError("Unknown outbox state filter") from None
            rows = self._query(
                "SELECT * FROM outbox_messages WHERE state = ? ORDER BY created_at_ms, message_id LIMIT ? OFFSET ?",
                (state.value, limit, offset),
            )
        else:
            rows = self._query(
                "SELECT * FROM outbox_messages ORDER BY created_at_ms, message_id LIMIT ? OFFSET ?", (limit, offset)
            )
        return [self._row_to_outbox_record(row) for row in rows]

    def list_domain_records(self, *, limit: int = 500, offset: int = 0) -> list[DomainRecord]:
        self._page(limit, offset)
        return [DomainRecord(**dict(row)) for row in self._query(
            "SELECT * FROM domain_state ORDER BY aggregate_type, aggregate_id LIMIT ? OFFSET ?", (limit, offset)
        )]

    def list_audit_log(self) -> list[AuditEntry]:
        return [AuditEntry(**dict(row)) for row in self._query(
            "SELECT * FROM outbox_audit ORDER BY audit_seq DESC LIMIT ?", (MAX_AUDIT_ROWS_LIMIT,)
        )][::-1]

    @staticmethod
    def _row_to_outbox_record(row: sqlite3.Row) -> OutboxRecord:
        values = dict(row)
        values["state"] = OutboxState(values["state"])
        return OutboxRecord(**values)
