"""Structured exceptions with stable machine-readable error codes."""

from __future__ import annotations


class OutboxError(Exception):
    """Base error for all transactional-outbox-lab operations."""

    error_code: str = "outbox_error"

    def __init__(self, message: str, *, error_code: str | None = None) -> None:
        super().__init__(message)
        if error_code is not None:
            self.error_code = error_code


class OutboxValidationError(OutboxError):
    """Raised when caller input violates structural, bound, or type constraints."""

    error_code = "validation_error"


class ClockRegressionError(OutboxError):
    """Raised when a caller-supplied timestamp is smaller than the store clock."""

    error_code = "clock_regression"


class DuplicateMessageError(OutboxError):
    """Raised when inserting an outbox message whose message_id already exists."""

    error_code = "duplicate_message_id"


class DomainVersionConflictError(OutboxError):
    """Raised when a domain mutation violates optimistic aggregate version order."""

    error_code = "domain_version_conflict"


class OutboxConstraintError(OutboxError):
    """Raised when a database integrity constraint or check fails during commit."""

    error_code = "constraint_violation"


class MessageNotFoundError(OutboxError):
    """Raised when referencing an unknown outbox message_id."""

    error_code = "message_not_found"


class StaleFencingTokenError(OutboxError):
    """Raised when a worker presents a stale fencing token or wrong lease owner."""

    error_code = "stale_fencing_token"


class LeaseExpiredError(OutboxError):
    """Raised when a worker attempts to mutate a lease at now_ms >= lease_expires_at_ms."""

    error_code = "lease_expired"


class InvalidStateTransitionError(OutboxError):
    """Raised when an operation is invalid for the record's current state."""

    error_code = "invalid_state_transition"


class StoreBusyError(OutboxError):
    """Raised when SQLite reports SQLITE_BUSY / database lock contention."""

    error_code = "store_busy"
