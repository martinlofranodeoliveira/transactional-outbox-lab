"""Transactional Outbox SQLite library and deterministic fault-injection lab."""

from outbox_lab.demo import format_demo_text, run_demo_scenario
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
from outbox_lab.models import (
    AuditEntry,
    BrokerDelivery,
    DomainMutation,
    DomainRecord,
    InboxRecord,
    OutboxDraft,
    OutboxRecord,
    OutboxState,
)
from outbox_lab.relay import (
    DeduplicatingConsumerInbox,
    FaultAction,
    FaultInjectionPlan,
    FaultRule,
    RecordingBroker,
    RelayStepOutcome,
    RelayWorker,
)
from outbox_lab.store import OutboxStore
from outbox_lab.validation import compute_retry_backoff_ms, freeze_canonical_json

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "AuditEntry",
    "BrokerDelivery",
    "ClockRegressionError",
    "DeduplicatingConsumerInbox",
    "DomainMutation",
    "DomainRecord",
    "DomainVersionConflictError",
    "DuplicateMessageError",
    "FaultAction",
    "FaultInjectionPlan",
    "FaultRule",
    "InboxRecord",
    "InvalidStateTransitionError",
    "LeaseExpiredError",
    "MessageNotFoundError",
    "OutboxConstraintError",
    "OutboxDraft",
    "OutboxError",
    "OutboxRecord",
    "OutboxState",
    "OutboxStore",
    "OutboxValidationError",
    "RecordingBroker",
    "RelayStepOutcome",
    "RelayWorker",
    "StaleFencingTokenError",
    "StoreBusyError",
    "compute_retry_backoff_ms",
    "format_demo_text",
    "freeze_canonical_json",
    "run_demo_scenario",
]
