"""Bounded, single-threaded offline transport and fault-injection fixtures."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from outbox_lab.errors import OutboxConstraintError, OutboxValidationError
from outbox_lab.models import BrokerDelivery, InboxRecord, OutboxRecord
from outbox_lab.store import OutboxStore
from outbox_lab.validation import (
    INT64_MAX,
    compute_retry_backoff_ms,
    freeze_canonical_json,
    sanitize_error_reason,
    validate_error_code,
    validate_identifier,
    validate_positive_int,
    validate_timestamp_ms,
)


def _capacity(value: int, name: str) -> int:
    return validate_positive_int(value, name, maximum=10_000)


class FaultAction(StrEnum):
    NONE = "none"
    FAIL_BEFORE_SEND = "fail_before_send"
    DROP_ACK_AFTER_SEND = "drop_ack_after_send"


@dataclass(frozen=True, slots=True)
class FaultRule:
    action: FaultAction
    error_code: str = "broker_unavailable"
    error_reason: str = "Simulated transient transport failure"

    def __post_init__(self) -> None:
        if not isinstance(self.action, FaultAction):
            raise OutboxValidationError("action must be a FaultAction")
        validate_error_code(self.error_code)
        object.__setattr__(self, "error_reason", sanitize_error_reason(self.error_reason))


class FaultInjectionPlan:
    """Rules and per-message dispatch counters each have a hard entry limit."""

    def __init__(self, *, max_entries: int = 1000) -> None:
        self._capacity = _capacity(max_entries, "max_entries")
        self._rules: dict[tuple[str, int], FaultRule] = {}
        self._dispatch_counts: dict[str, int] = {}

    def configure(
        self, message_id: str, dispatch_ordinal: int, action: FaultAction, *,
        error_code: str = "broker_unavailable",
        error_reason: str = "Simulated transient transport failure",
    ) -> None:
        validate_identifier(message_id, "message_id")
        validate_positive_int(dispatch_ordinal, "dispatch_ordinal")
        rule = FaultRule(action, error_code, error_reason)
        key = (message_id, dispatch_ordinal)
        if key not in self._rules and len(self._rules) >= self._capacity:
            raise OutboxValidationError("Fault rule capacity exceeded")
        self._rules[key] = rule

    def next_rule_for(self, message_id: str) -> tuple[int, FaultRule]:
        validate_identifier(message_id, "message_id")
        if message_id not in self._dispatch_counts and len(self._dispatch_counts) >= self._capacity:
            raise OutboxValidationError("Dispatch counter capacity exceeded")
        ordinal = self._dispatch_counts.get(message_id, 0) + 1
        validate_positive_int(ordinal, "dispatch_ordinal")
        self._dispatch_counts[message_id] = ordinal
        return ordinal, self._rules.get((message_id, ordinal), FaultRule(FaultAction.NONE))


def _identity(record: BrokerDelivery | InboxRecord) -> tuple[Any, ...]:
    return (
        record.aggregate_type, record.aggregate_id, record.aggregate_version,
        record.event_type, record.payload_json,
    )


def _validate_delivery(delivery: BrokerDelivery) -> None:
    if not isinstance(delivery, BrokerDelivery):
        raise OutboxValidationError("Expected a BrokerDelivery")
    validate_identifier(delivery.message_id, "message_id")
    validate_identifier(delivery.aggregate_type, "aggregate_type", max_len=64)
    validate_identifier(delivery.aggregate_id, "aggregate_id")
    validate_positive_int(delivery.aggregate_version, "aggregate_version")
    validate_identifier(delivery.event_type, "event_type")
    validate_identifier(delivery.worker_id, "worker_id", max_len=64)
    validate_positive_int(delivery.fencing_token, "fencing_token")
    validate_positive_int(delivery.delivery_seq, "delivery_seq")
    validate_timestamp_ms(delivery.delivered_at_ms)
    # Bound the raw representation before parsing, even for manually built fixtures.
    if type(delivery.payload_json) is not str or len(delivery.payload_json) > 16_384:
        raise OutboxValidationError("Invalid delivery payload representation")
    try:
        payload = delivery.payload
    except (ValueError, RecursionError):
        raise OutboxValidationError("Invalid delivery JSON") from None
    if freeze_canonical_json(payload) != delivery.payload_json:
        raise OutboxValidationError("Delivery payload must use the bounded canonical representation")


class DeduplicatingConsumerInbox:
    """In-memory reference projection, not a durable consumer transaction implementation."""

    def __init__(self, *, max_deliveries: int = 1000) -> None:
        self._capacity = _capacity(max_deliveries, "max_deliveries")
        self._raw_deliveries: list[BrokerDelivery] = []
        self._deduplicated: dict[str, InboxRecord] = {}

    def receive(self, delivery: BrokerDelivery) -> bool:
        _validate_delivery(delivery)
        if len(self._raw_deliveries) >= self._capacity:
            raise OutboxValidationError("Inbox delivery capacity exceeded")
        existing = self._deduplicated.get(delivery.message_id)
        if existing is not None and _identity(existing) != _identity(delivery):
            raise OutboxConstraintError("message_id was reused with a different immutable event")
        if existing is not None and delivery.delivered_at_ms < existing.last_received_at_ms:
            raise OutboxValidationError("Delivery time regressed for this message")
        if existing is None:
            updated = InboxRecord(
                message_id=delivery.message_id, aggregate_type=delivery.aggregate_type,
                aggregate_id=delivery.aggregate_id, aggregate_version=delivery.aggregate_version,
                event_type=delivery.event_type, payload_json=delivery.payload_json,
                first_delivery_seq=delivery.delivery_seq, first_received_at_ms=delivery.delivered_at_ms,
                last_received_at_ms=delivery.delivered_at_ms, first_fencing_token=delivery.fencing_token,
                last_fencing_token=delivery.fencing_token, raw_delivery_count=1,
            )
        else:
            updated = replace(
                existing, last_received_at_ms=delivery.delivered_at_ms,
                last_fencing_token=delivery.fencing_token, raw_delivery_count=existing.raw_delivery_count + 1,
            )
        self._raw_deliveries.append(delivery)
        self._deduplicated[delivery.message_id] = updated
        return existing is None

    def raw_delivery_count(self, message_id: str | None = None) -> int:
        if message_id is None:
            return len(self._raw_deliveries)
        return sum(d.message_id == message_id for d in self._raw_deliveries)

    def deduplicated_count(self, message_id: str | None = None) -> int:
        return len(self._deduplicated) if message_id is None else int(message_id in self._deduplicated)

    def list_raw_deliveries(self, message_id: str | None = None) -> list[BrokerDelivery]:
        return [d for d in self._raw_deliveries if message_id is None or d.message_id == message_id]

    def get_inbox_record(self, message_id: str) -> InboxRecord | None:
        return self._deduplicated.get(message_id)

    def list_inbox_records(self) -> list[InboxRecord]:
        return [self._deduplicated[key] for key in sorted(self._deduplicated)]


class RecordingBroker:
    def __init__(
        self, inbox: DeduplicatingConsumerInbox | None = None, *, max_deliveries: int = 1000,
    ) -> None:
        self._capacity = _capacity(max_deliveries, "max_deliveries")
        self._deliveries: list[BrokerDelivery] = []
        self._inbox = inbox if inbox is not None else DeduplicatingConsumerInbox(max_deliveries=max_deliveries)

    @property
    def inbox(self) -> DeduplicatingConsumerInbox:
        return self._inbox

    def publish(
        self, record: OutboxRecord, *, worker_id: str, fencing_token: int, now_ms: int,
    ) -> BrokerDelivery:
        if not isinstance(record, OutboxRecord):
            raise OutboxValidationError("Expected an OutboxRecord")
        if len(self._deliveries) >= self._capacity:
            raise OutboxValidationError("Broker delivery capacity exceeded")
        delivery = BrokerDelivery(
            delivery_seq=len(self._deliveries) + 1, message_id=record.message_id,
            aggregate_type=record.aggregate_type, aggregate_id=record.aggregate_id,
            aggregate_version=record.aggregate_version, event_type=record.event_type,
            payload_json=record.payload_json, worker_id=worker_id,
            fencing_token=fencing_token, delivered_at_ms=now_ms,
        )
        # A rejected inbox delivery must not leave a half-recorded broker fixture.
        self._inbox.receive(delivery)
        self._deliveries.append(delivery)
        return delivery

    def list_deliveries(self, message_id: str | None = None) -> list[BrokerDelivery]:
        return [d for d in self._deliveries if message_id is None or d.message_id == message_id]


@dataclass(frozen=True, slots=True)
class RelayStepOutcome:
    worker_id: str
    message_id: str
    fencing_token: int
    dispatch_ordinal: int
    fault_action: FaultAction
    broker_received: bool
    ack_committed: bool
    resulting_state: str
    attempt_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id, "message_id": self.message_id,
            "fencing_token": self.fencing_token, "dispatch_ordinal": self.dispatch_ordinal,
            "fault_action": str(self.fault_action), "broker_received": self.broker_received,
            "ack_committed": self.ack_committed, "resulting_state": self.resulting_state,
            "attempt_count": self.attempt_count,
        }


class RelayWorker:
    def __init__(
        self, worker_id: str, store: OutboxStore, broker: RecordingBroker, *,
        fault_plan: FaultInjectionPlan | None = None,
        base_backoff_ms: int = 1_000, max_backoff_ms: int = 60_000,
    ) -> None:
        self._worker_id = validate_identifier(worker_id, "worker_id", max_len=64)
        compute_retry_backoff_ms(1, base_backoff_ms=base_backoff_ms, max_backoff_ms=max_backoff_ms)
        self._store = store
        self._broker = broker
        self._fault_plan = fault_plan if fault_plan is not None else FaultInjectionPlan()
        self._base_backoff_ms = base_backoff_ms
        self._max_backoff_ms = max_backoff_ms

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def claim(
        self, *, now_ms: int, lease_duration_ms: int, batch_size: int = 10,
    ) -> list[OutboxRecord]:
        return self._store.claim_batch(
            worker_id=self._worker_id, now_ms=now_ms,
            lease_duration_ms=lease_duration_ms, batch_size=batch_size,
        )

    def deliver_claimed(self, record: OutboxRecord, *, now_ms: int) -> RelayStepOutcome:
        if not isinstance(record, OutboxRecord):
            raise OutboxValidationError("Expected an OutboxRecord")
        # This short transaction ends before transport work. It is not a broker fence.
        current = self._store.check_lease(
            message_id=record.message_id, worker_id=self._worker_id,
            fencing_token=record.fencing_token, now_ms=now_ms,
        )
        ordinal, rule = self._fault_plan.next_rule_for(current.message_id)
        received = rule.action != FaultAction.FAIL_BEFORE_SEND
        acknowledged = False
        if not received:
            current = self._store.record_failure(
                message_id=current.message_id, worker_id=self._worker_id,
                fencing_token=current.fencing_token, now_ms=now_ms,
                error_code=rule.error_code, error_reason=rule.error_reason,
                base_backoff_ms=self._base_backoff_ms, max_backoff_ms=self._max_backoff_ms,
            )
        else:
            self._broker.publish(
                current, worker_id=self._worker_id, fencing_token=current.fencing_token, now_ms=now_ms,
            )
            if rule.action != FaultAction.DROP_ACK_AFTER_SEND:
                current = self._store.mark_published(
                    message_id=current.message_id, worker_id=self._worker_id,
                    fencing_token=current.fencing_token, now_ms=now_ms,
                )
                acknowledged = True
        return RelayStepOutcome(
            self._worker_id, current.message_id, current.fencing_token, ordinal,
            rule.action, received, acknowledged, str(current.state), current.attempt_count,
        )
