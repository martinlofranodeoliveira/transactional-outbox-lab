"""Immutable domain, outbox, audit, and broker data models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping


def _deep_freeze(value: Any) -> Any:
    """Convert parsed JSON structures into deeply immutable views."""
    if isinstance(value, dict):
        return MappingProxyType({k: _deep_freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


class OutboxState(StrEnum):
    """Lifecycle states for an outbox record."""

    PENDING = "pending"
    LEASED = "leased"
    PUBLISHED = "published"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True, slots=True)
class DomainMutation:
    """Specification for a domain aggregate write staged in the same transaction."""

    aggregate_type: str
    aggregate_id: str
    version: int
    state: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class OutboxDraft:
    """Specification for an immutable outbox message staged with a domain write."""

    message_id: str
    event_type: str
    payload: Mapping[str, Any]
    max_attempts: int = 3


@dataclass(frozen=True, slots=True)
class DomainRecord:
    """Persisted domain aggregate snapshot."""

    aggregate_type: str
    aggregate_id: str
    version: int
    state_json: str
    updated_at_ms: int

    @property
    def state(self) -> Mapping[str, Any]:
        """Return a deeply immutable view of the aggregate state JSON."""
        return _deep_freeze(json.loads(self.state_json))

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "version": self.version,
            "state": json.loads(self.state_json),
            "updated_at_ms": self.updated_at_ms,
        }


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """Persisted transactional outbox record snapshot."""

    message_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    event_type: str
    payload_json: str
    state: OutboxState
    attempt_count: int
    max_attempts: int
    next_attempt_at_ms: int | None
    lease_owner: str | None
    lease_expires_at_ms: int | None
    fencing_token: int
    created_at_ms: int
    updated_at_ms: int
    published_at_ms: int | None
    dead_lettered_at_ms: int | None
    last_error_code: str | None
    last_error_reason: str | None

    @property
    def payload(self) -> Mapping[str, Any]:
        """Return a deeply immutable view of the canonical JSON payload."""
        return _deep_freeze(json.loads(self.payload_json))

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_version": self.aggregate_version,
            "event_type": self.event_type,
            "payload": json.loads(self.payload_json),
            "payload_json": self.payload_json,
            "state": str(self.state),
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "next_attempt_at_ms": self.next_attempt_at_ms,
            "lease_owner": self.lease_owner,
            "lease_expires_at_ms": self.lease_expires_at_ms,
            "fencing_token": self.fencing_token,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "published_at_ms": self.published_at_ms,
            "dead_lettered_at_ms": self.dead_lettered_at_ms,
            "last_error_code": self.last_error_code,
            "last_error_reason": self.last_error_reason,
        }


@dataclass(frozen=True, slots=True)
class AuditEntry:
    """Bounded metadata-only audit log entry (never stores payloads or raw tracebacks)."""

    audit_seq: int
    recorded_at_ms: int
    action: str
    message_id: str
    worker_id: str | None
    from_state: str | None
    to_state: str
    attempt_count: int
    fencing_token: int
    detail_code: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_seq": self.audit_seq,
            "recorded_at_ms": self.recorded_at_ms,
            "action": self.action,
            "message_id": self.message_id,
            "worker_id": self.worker_id,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "attempt_count": self.attempt_count,
            "fencing_token": self.fencing_token,
            "detail_code": self.detail_code,
        }


@dataclass(frozen=True, slots=True)
class BrokerDelivery:
    """Immutable record of a message frame received by the downstream transport."""

    delivery_seq: int
    message_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    event_type: str
    payload_json: str
    worker_id: str
    fencing_token: int
    delivered_at_ms: int

    @property
    def payload(self) -> Mapping[str, Any]:
        return _deep_freeze(json.loads(self.payload_json))

    def to_dict(self) -> dict[str, Any]:
        return {
            "delivery_seq": self.delivery_seq,
            "message_id": self.message_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_version": self.aggregate_version,
            "event_type": self.event_type,
            "payload": json.loads(self.payload_json),
            "payload_json": self.payload_json,
            "worker_id": self.worker_id,
            "fencing_token": self.fencing_token,
            "delivered_at_ms": self.delivered_at_ms,
        }


@dataclass(frozen=True, slots=True)
class InboxRecord:
    """Deduplicated state entry in the reference downstream consumer inbox."""

    message_id: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    event_type: str
    payload_json: str
    first_delivery_seq: int
    first_received_at_ms: int
    last_received_at_ms: int
    first_fencing_token: int
    last_fencing_token: int
    raw_delivery_count: int

    @property
    def payload(self) -> Mapping[str, Any]:
        return _deep_freeze(json.loads(self.payload_json))

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_version": self.aggregate_version,
            "event_type": self.event_type,
            "payload": json.loads(self.payload_json),
            "payload_json": self.payload_json,
            "first_delivery_seq": self.first_delivery_seq,
            "first_received_at_ms": self.first_received_at_ms,
            "last_received_at_ms": self.last_received_at_ms,
            "first_fencing_token": self.first_fencing_token,
            "last_fencing_token": self.last_fencing_token,
            "raw_delivery_count": self.raw_delivery_count,
        }
