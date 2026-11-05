"""Independent boundary, rollback, forged-input and fixture-capacity regressions."""

from __future__ import annotations

import multiprocessing
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from outbox_lab import (
    ClockRegressionError,
    DeduplicatingConsumerInbox,
    DomainMutation,
    DuplicateMessageError,
    FaultAction,
    FaultInjectionPlan,
    FaultRule,
    InvalidStateTransitionError,
    LeaseExpiredError,
    MessageNotFoundError,
    OutboxConstraintError,
    OutboxDraft,
    OutboxState,
    OutboxStore,
    OutboxValidationError,
    RecordingBroker,
    RelayWorker,
    StaleFencingTokenError,
    compute_retry_backoff_ms,
    freeze_canonical_json,
)
from outbox_lab.validation import INT64_MAX, MAX_LEASE_DURATION_MS, sanitize_error_reason


def _reclaim_in_child(path: str, result: object) -> None:
    with OutboxStore(path, initialize_schema=False) as store:
        claim = store.claim_batch(worker_id="child", now_ms=10, lease_duration_ms=20)[0]
        result.put(claim.fencing_token)


class AdversarialContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "lab.sqlite3"
        self.store = OutboxStore(self.path, max_audit_rows=5)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def stage(self, name: str = "m", *, now: int = 0, attempts: int = 3):
        return self.store.commit_domain_and_outbox(
            DomainMutation("task", name, 1, {"status": "queued"}),
            OutboxDraft(name, "task.queued", {"value": [1, {"x": "original"}]}, attempts),
            now_ms=now,
        )[1]

    def claim(self, *, now: int = 0, duration: int = 10):
        return self.store.claim_batch(worker_id="a", now_ms=now, lease_duration_ms=duration)[0]

    def test_close_is_idempotent_and_closed_use_is_structured(self) -> None:
        self.store.close()
        self.store.close()
        with self.assertRaises(OutboxConstraintError):
            self.store.get_clock_ms()
        with OutboxStore(self.path) as other:
            other.close()
            other.close()

    def test_duplicate_rolls_back_an_existing_domain_update_and_audit(self) -> None:
        self.stage()
        before = self.store.get_domain_record("task", "m")
        with self.assertRaises(DuplicateMessageError):
            self.store.commit_domain_and_outbox(
                DomainMutation("task", "m", 2, {"status": "must-not-persist"}),
                OutboxDraft("m", "task.changed", {"changed": True}), now_ms=11,
            )
        self.assertEqual(self.store.get_domain_record("task", "m"), before)
        self.assertEqual(len(self.store.list_outbox_records()), 1)
        self.assertEqual([entry.action for entry in self.store.list_audit_log()], ["staged"])
        with OutboxStore(self.path) as independent:
            self.assertEqual(independent.get_clock_ms(), 11)
            with self.assertRaises(ClockRegressionError):
                independent.claim_batch(worker_id="b", now_ms=9, lease_duration_ms=10)

    def test_audit_constraint_rolls_back_both_new_records_but_not_clock(self) -> None:
        with sqlite3.connect(self.path) as raw:
            raw.executescript("""
                CREATE TRIGGER reject_audit BEFORE INSERT ON outbox_audit
                BEGIN SELECT RAISE(ABORT, 'synthetic diagnostic that must stay private'); END;
            """)
        with self.assertRaises(OutboxConstraintError) as caught:
            self.stage(now=11)
        self.assertNotIn("synthetic diagnostic", str(caught.exception))
        self.assertEqual(self.store.list_domain_records(), [])
        self.assertEqual(self.store.list_outbox_records(), [])
        self.assertEqual(self.store.list_audit_log(), [])
        self.assertEqual(self.store.get_clock_ms(), 11)

    def test_every_worker_operation_rejects_expiry_without_takeover(self) -> None:
        self.stage()
        claim = self.claim()
        common = dict(message_id="m", worker_id="a", fencing_token=claim.fencing_token, now_ms=10)
        operations = (
            lambda: self.store.mark_published(**common),
            lambda: self.store.record_failure(**common, error_code="timeout", error_reason="Synthetic failure"),
            lambda: self.store.renew_lease(**common, lease_duration_ms=50),
            lambda: self.store.check_lease(**common),
        )
        for operation in operations:
            with self.assertRaises(LeaseExpiredError):
                operation()
        self.assertEqual(self.store.get_outbox_record("m"), claim)
        self.assertEqual(self.store.get_clock_ms(), 10)
        with OutboxStore(self.path) as other:
            with self.assertRaises(ClockRegressionError):
                other.mark_published(**(common | {"now_ms": 9}))

    def test_wrong_owner_and_missing_record_advance_clock_without_mutating_event(self) -> None:
        self.stage()
        claim = self.claim(duration=100)
        with self.assertRaises(StaleFencingTokenError):
            self.store.renew_lease(message_id="m", worker_id="wrong", fencing_token=claim.fencing_token, now_ms=11, lease_duration_ms=100)
        self.assertEqual(self.store.get_outbox_record("m"), claim)
        with self.assertRaises(MessageNotFoundError):
            self.store.check_lease(message_id="missing", worker_id="a", fencing_token=1, now_ms=12)
        self.assertEqual(self.store.get_clock_ms(), 12)

    def test_renewal_never_shortens_a_lease_or_changes_its_token(self) -> None:
        self.stage()
        claim = self.claim(duration=100)
        renewed = self.store.renew_lease(message_id="m", worker_id="a", fencing_token=claim.fencing_token, now_ms=1, lease_duration_ms=1)
        self.assertEqual(renewed.lease_expires_at_ms, 100)
        self.assertEqual(renewed.fencing_token, claim.fencing_token)
        self.assertEqual(renewed.attempt_count, 0)

    def test_process_takeover_fences_all_parent_worker_mutations(self) -> None:
        self.stage()
        old = self.claim()
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        child = context.Process(target=_reclaim_in_child, args=(str(self.path), queue))
        child.start()
        try:
            child.join(timeout=10)
            self.assertEqual(child.exitcode, 0)
            self.assertEqual(queue.get(timeout=5), 2)
        finally:
            if child.is_alive():
                child.terminate()
                child.join(timeout=5)
            queue.close()
            queue.join_thread()
        common = dict(message_id="m", worker_id="a", fencing_token=old.fencing_token, now_ms=11)
        for operation in (
            lambda: self.store.mark_published(**common),
            lambda: self.store.record_failure(**common, error_code="old", error_reason="Stale failure"),
            lambda: self.store.renew_lease(**common, lease_duration_ms=10),
            lambda: self.store.check_lease(**common),
        ):
            with self.assertRaises(StaleFencingTokenError):
                operation()
        current = self.store.get_outbox_record("m")
        self.assertEqual((current.lease_owner, current.fencing_token, current.attempt_count), ("child", 2, 0))

    def test_token_exhaustion_rolls_back_whole_batch(self) -> None:
        self.stage("a")
        self.stage("b")
        with sqlite3.connect(self.path) as raw:
            raw.execute("UPDATE outbox_meta SET last_fencing_token = ?", (INT64_MAX - 1,))
        with self.assertRaises(OutboxConstraintError):
            self.store.claim_batch(worker_id="a", now_ms=1, lease_duration_ms=10, batch_size=2)
        self.assertEqual(self.store.get_last_fencing_token(), INT64_MAX - 1)
        self.assertEqual([r.state for r in self.store.list_outbox_records()], [OutboxState.PENDING, OutboxState.PENDING])
        self.assertEqual(self.store.get_clock_ms(), 1)
        last = self.store.claim_batch(worker_id="a", now_ms=1, lease_duration_ms=10, batch_size=1)[0]
        self.assertEqual(last.fencing_token, INT64_MAX)
        with self.assertRaises(OutboxConstraintError):
            self.store.claim_batch(worker_id="b", now_ms=2, lease_duration_ms=10)

    def test_timestamp_overflow_preserves_clock_and_does_not_release_lease(self) -> None:
        self.stage(now=INT64_MAX - 3)
        claim = self.claim(now=INT64_MAX - 3, duration=3)
        with self.assertRaises(OutboxConstraintError):
            self.store.record_failure(
                message_id="m", worker_id="a", fencing_token=claim.fencing_token,
                now_ms=INT64_MAX - 2, error_code="retry", error_reason="Synthetic retry",
                base_backoff_ms=3, max_backoff_ms=3,
            )
        self.assertEqual(self.store.get_outbox_record("m"), claim)
        self.assertEqual(self.store.get_clock_ms(), INT64_MAX - 2)
        with self.assertRaises(ClockRegressionError):
            self.store.mark_published(message_id="m", worker_id="a", fencing_token=claim.fencing_token, now_ms=INT64_MAX - 3)
        with self.assertRaises(OutboxConstraintError):
            self.store.claim_batch(worker_id="b", now_ms=INT64_MAX, lease_duration_ms=1)
        self.assertEqual(self.store.get_clock_ms(), INT64_MAX)

    def test_first_failure_can_exhaust_and_terminal_state_is_not_reacknowledgeable(self) -> None:
        self.stage(attempts=1)
        claim = self.claim()
        dead = self.store.record_failure(
            message_id="m", worker_id="a", fencing_token=claim.fencing_token, now_ms=1,
            error_code="unavailable", error_reason="  Synthetic\n failure  ",
        )
        self.assertEqual((dead.state, dead.attempt_count, dead.max_attempts), (OutboxState.DEAD_LETTER, 1, 1))
        self.assertIsNone(dead.next_attempt_at_ms)
        self.assertEqual(dead.last_error_reason, "Synthetic failure")
        self.assertEqual(dead.dead_lettered_at_ms, 1)
        with self.assertRaises(InvalidStateTransitionError):
            self.store.mark_published(message_id="m", worker_id="a", fencing_token=claim.fencing_token, now_ms=2)
        self.assertEqual(self.store.claim_batch(worker_id="b", now_ms=100, lease_duration_ms=1), [])

    def test_backoff_matches_formula_beyond_old_exponent_cap(self) -> None:
        expected = [7, 14, 28, 31, 31]
        self.assertEqual([compute_retry_backoff_ms(i, base_backoff_ms=7, max_backoff_ms=31) for i in range(1, 6)], expected)
        self.assertEqual(compute_retry_backoff_ms(36, base_backoff_ms=1, max_backoff_ms=MAX_LEASE_DURATION_MS), MAX_LEASE_DURATION_MS)
        self.assertEqual(compute_retry_backoff_ms(100, base_backoff_ms=0, max_backoff_ms=0), 0)

    def test_payload_exact_byte_limit_unicode_cycles_and_shared_expansion(self) -> None:
        exact = {"a": "a" * 4096, "b": "b" * 4096, "c": "c" * 4096, "d": "d" * 4067}
        self.assertEqual(len(freeze_canonical_json(exact).encode("utf-8")), 16384)
        with self.assertRaises(OutboxValidationError):
            freeze_canonical_json(exact | {"d": "d" * 4068})
        cycle = {}
        cycle["self"] = cycle
        shared = [0] * 256
        expanded = {"shared": [[shared] * 256] * 256}
        for bad in ({"x": "\ud800"}, {"\udfff": 0}, cycle, expanded, {"x": 1 << 63}, {"x": float("nan")}):
            with self.assertRaises(OutboxValidationError):
                freeze_canonical_json(bad)
        self.assertEqual(freeze_canonical_json({"é": [True, None, -1]}), '{"é":[true,null,-1]}')

    def test_identifier_newlines_and_diagnostic_controls_are_rejected_before_clock(self) -> None:
        for name in ("m\n", "m\r", "m\x00"):
            with self.assertRaises(OutboxValidationError):
                self.stage(name, now=100)
        for reason in ("x\x00y", "x\u202ey", "x\ud800y", "x" * 1025):
            with self.assertRaises(OutboxValidationError):
                sanitize_error_reason(reason)
        with self.assertRaises(OutboxValidationError):
            FaultRule(FaultAction.NONE, error_code="timeout\n")
        self.assertEqual(self.store.get_clock_ms(), 0)

    def test_event_fields_have_database_immutability_guard(self) -> None:
        original = self.stage()
        with sqlite3.connect(self.path) as raw:
            with self.assertRaises(sqlite3.IntegrityError):
                raw.execute("UPDATE outbox_messages SET payload_json = '{}' WHERE message_id = 'm'")
        self.assertEqual(self.store.get_outbox_record("m"), original)

    def test_listing_has_bounded_pages_and_validated_filters(self) -> None:
        self.stage("a")
        self.stage("b")
        self.assertEqual([r.message_id for r in self.store.list_outbox_records(limit=1, offset=1)], ["b"])
        self.assertEqual(len(self.store.list_domain_records(limit=1)), 1)
        for call in (
            lambda: self.store.list_outbox_records(limit=501),
            lambda: self.store.list_outbox_records(offset=-1),
            lambda: self.store.list_outbox_records(state="unknown"),
            lambda: self.store.list_domain_records(limit=True),
        ):
            with self.assertRaises(OutboxValidationError):
                call()

    def test_relay_refreshes_payload_and_ack_loss_persists_observed_time(self) -> None:
        self.stage()
        claim = self.claim()
        plan = FaultInjectionPlan()
        plan.configure("m", 1, FaultAction.DROP_ACK_AFTER_SEND)
        broker = RecordingBroker()
        worker = RelayWorker("a", self.store, broker, fault_plan=plan)
        outcome = worker.deliver_claimed(replace(claim, payload_json='{"forged":true}'), now_ms=5)
        self.assertFalse(outcome.ack_committed)
        self.assertEqual(self.store.get_clock_ms(), 5)
        self.assertEqual(broker.list_deliveries()[0].payload_json, claim.payload_json)
        self.assertEqual(self.store.get_outbox_record("m").attempt_count, 0)
        with self.assertRaises(ClockRegressionError):
            worker.deliver_claimed(claim, now_ms=4)
        with self.assertRaises(LeaseExpiredError):
            worker.deliver_claimed(claim, now_ms=10)
        self.assertEqual(len(broker.list_deliveries()), 1)

    def test_fixture_capacity_identity_conflicts_and_fault_input_validation(self) -> None:
        record = self.stage()
        inbox = DeduplicatingConsumerInbox(max_deliveries=2)
        broker = RecordingBroker(inbox, max_deliveries=3)
        delivery = broker.publish(record, worker_id="a", fencing_token=1, now_ms=0)
        with self.assertRaises(OutboxConstraintError):
            broker.publish(replace(record, payload_json='{"different":true}'), worker_id="a", fencing_token=2, now_ms=1)
        self.assertEqual((len(broker.list_deliveries()), inbox.raw_delivery_count()), (1, 1))
        self.assertFalse(inbox.receive(replace(delivery, delivery_seq=2, delivered_at_ms=1)))
        with self.assertRaises(OutboxValidationError):
            broker.publish(record, worker_id="a", fencing_token=2, now_ms=2)
        self.assertEqual(len(broker.list_deliveries()), 1)
        self.assertEqual((inbox.raw_delivery_count(), inbox.deduplicated_count()), (2, 1))
        plan = FaultInjectionPlan(max_entries=1)
        for ordinal in (True, 1.5, 0, INT64_MAX + 1):
            with self.assertRaises(OutboxValidationError):
                plan.configure("m", ordinal, FaultAction.NONE)
        with self.assertRaises(OutboxValidationError):
            plan.configure("m", 1, "drop_ack_after_send")
        plan.configure("m", 1, FaultAction.NONE)
        with self.assertRaises(OutboxValidationError):
            plan.configure("n", 1, FaultAction.NONE)
        plan.next_rule_for("m")
        with self.assertRaises(OutboxValidationError):
            plan.next_rule_for("n")
        with self.assertRaises(OutboxValidationError):
            RelayWorker("a", self.store, broker, base_backoff_ms=10, max_backoff_ms=1)
