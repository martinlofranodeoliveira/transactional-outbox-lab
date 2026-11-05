"""Tests for post-send ack failure duplicate delivery and SQLite connection/process contention."""

from __future__ import annotations

import multiprocessing
import sqlite3
import tempfile
import unittest
from multiprocessing.synchronize import Event as MpEvent
from pathlib import Path

from outbox_lab import (
    DeduplicatingConsumerInbox,
    DomainMutation,
    FaultAction,
    FaultInjectionPlan,
    OutboxDraft,
    OutboxState,
    OutboxStore,
    RecordingBroker,
    RelayWorker,
    StoreBusyError,
)


def _child_hold_write_lock_and_claim(
    db_path: str,
    lock_acquired_evt: MpEvent,
    release_lock_evt: MpEvent,
) -> int:
    """Child process worker that acquires a BEGIN IMMEDIATE write lock and claims a batch."""
    conn = sqlite3.connect(db_path, timeout=0.0, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        lock_acquired_evt.set()
        released = release_lock_evt.wait(timeout=10.0)
        if not released:
            conn.execute("ROLLBACK")
            return -1
        conn.execute("ROLLBACK")
    finally:
        conn.close()

    with OutboxStore(db_path, busy_timeout_ms=0, initialize_schema=False) as child_store:
        claimed = child_store.claim_batch(
            worker_id="proc-child-worker",
            now_ms=200,
            lease_duration_ms=50,
            batch_size=1,
        )
        return claimed[0].fencing_token if claimed else 0


class AtLeastOnceAndContentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="outbox_test_contention_")
        self.db_path = Path(self._tmp.name) / "outbox.sqlite3"
        self.store = OutboxStore(self.db_path, busy_timeout_ms=0)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_post_send_ack_loss_causes_duplicate_delivery_and_inbox_deduplication(self) -> None:
        inbox = DeduplicatingConsumerInbox()
        broker = RecordingBroker(inbox=inbox)
        fault_plan = FaultInjectionPlan()
        fault_plan.configure(
            "msg-dup-sim",
            1,
            FaultAction.DROP_ACK_AFTER_SEND,
            error_code="lost_ack",
            error_reason="Simulated crash after broker accepted publish",
        )

        worker_1 = RelayWorker("worker-1", self.store, broker, fault_plan=fault_plan)
        worker_2 = RelayWorker("worker-2", self.store, broker, fault_plan=fault_plan)

        self.store.commit_domain_and_outbox(
            DomainMutation("ledger", "led-1", 1, {"entry": "credit", "cents": 2500}),
            OutboxDraft(
                message_id="msg-dup-sim",
                event_type="ledger.credited",
                payload={"ledger_id": "led-1", "cents": 2500},
                max_attempts=3,
            ),
            now_ms=100,
        )

        # Worker 1 claims at t=100 (expires at 200) and delivers at t=120; ack is lost after broker send.
        claim_1 = worker_1.claim(now_ms=100, lease_duration_ms=100, batch_size=1)[0]
        outcome_1 = worker_1.deliver_claimed(claim_1, now_ms=120)
        self.assertTrue(outcome_1.broker_received)
        self.assertFalse(outcome_1.ack_committed)
        self.assertEqual(outcome_1.resulting_state, OutboxState.LEASED.value)
        self.assertEqual(inbox.raw_delivery_count("msg-dup-sim"), 1)
        self.assertEqual(inbox.deduplicated_count("msg-dup-sim"), 1)

        # At t=200, lease expires; Worker 2 reclaims with fencing_token=2 and re-sends at t=210.
        claim_2 = worker_2.claim(now_ms=200, lease_duration_ms=100, batch_size=1)[0]
        self.assertEqual(claim_2.fencing_token, 2)
        outcome_2 = worker_2.deliver_claimed(claim_2, now_ms=210)
        self.assertTrue(outcome_2.broker_received)
        self.assertTrue(outcome_2.ack_committed)
        self.assertEqual(outcome_2.resulting_state, OutboxState.PUBLISHED.value)

        # Verify exact duplicate message_id + payload emitted twice, deduplicated to 1 state in inbox.
        raw_deliveries = broker.list_deliveries("msg-dup-sim")
        self.assertEqual(len(raw_deliveries), 2)
        self.assertEqual(raw_deliveries[0].message_id, "msg-dup-sim")
        self.assertEqual(raw_deliveries[1].message_id, "msg-dup-sim")
        self.assertEqual(raw_deliveries[0].payload_json, raw_deliveries[1].payload_json)
        self.assertEqual(raw_deliveries[0].fencing_token, 1)
        self.assertEqual(raw_deliveries[1].fencing_token, 2)

        self.assertEqual(inbox.raw_delivery_count("msg-dup-sim"), 2)
        self.assertEqual(inbox.deduplicated_count("msg-dup-sim"), 1)
        inbox_rec = inbox.get_inbox_record("msg-dup-sim")
        self.assertIsNotNone(inbox_rec)
        assert inbox_rec is not None
        self.assertEqual(inbox_rec.raw_delivery_count, 2)
        self.assertEqual(inbox_rec.first_fencing_token, 1)
        self.assertEqual(inbox_rec.last_fencing_token, 2)

    def test_two_connection_write_lock_contention_without_sleeps(self) -> None:
        self.store.commit_domain_and_outbox(
            DomainMutation("task", "t-1", 1, {"status": "open"}),
            OutboxDraft("msg-conn-1", "task.opened", {"task_id": "t-1"}),
            now_ms=10,
        )

        # Open a second independent connection with timeout=0 and acquire BEGIN IMMEDIATE lock.
        other_conn = sqlite3.connect(self.db_path, timeout=0.0, isolation_level=None)
        try:
            other_conn.execute("BEGIN IMMEDIATE")

            # Primary store with busy_timeout_ms=0 must fail immediately with StoreBusyError.
            with self.assertRaises(StoreBusyError) as ctx:
                self.store.claim_batch(
                    worker_id="worker-main", now_ms=20, lease_duration_ms=50, batch_size=1
                )
            self.assertEqual(ctx.exception.error_code, "store_busy")

            other_conn.execute("ROLLBACK")
        finally:
            other_conn.close()

        # Once the competing transaction rolls back, claim_batch succeeds immediately.
        claimed = self.store.claim_batch(
            worker_id="worker-main", now_ms=20, lease_duration_ms=50, batch_size=1
        )
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0].fencing_token, 1)

    def test_independent_process_contention_and_cross_process_fencing_without_sleeps(self) -> None:
        self.store.commit_domain_and_outbox(
            DomainMutation("task", "t-proc", 1, {"status": "queued"}),
            OutboxDraft("msg-proc-1", "task.queued", {"task_id": "t-proc"}),
            now_ms=100,
        )

        ctx = multiprocessing.get_context("spawn")
        lock_acquired_evt = ctx.Event()
        release_lock_evt = ctx.Event()
        result_queue: multiprocessing.Queue[int] = ctx.Queue()

        proc = ctx.Process(
            target=_child_process_entry,
            args=(str(self.db_path), lock_acquired_evt, release_lock_evt, result_queue),
        )
        proc.start()
        try:
            self.assertTrue(lock_acquired_evt.wait(timeout=10.0))

            # While child process holds BEGIN IMMEDIATE, parent process gets StoreBusyError.
            with self.assertRaises(StoreBusyError) as busy_ctx:
                self.store.claim_batch(
                    worker_id="proc-parent-worker",
                    now_ms=150,
                    lease_duration_ms=50,
                    batch_size=1,
                )
            self.assertEqual(busy_ctx.exception.error_code, "store_busy")

            # Release child process so it claims msg-proc-1 at now_ms=200 with lease_duration=50 (expires 250).
            release_lock_evt.set()
            proc.join(timeout=10.0)
            self.assertEqual(proc.exitcode, 0)
            child_token = result_queue.get(timeout=5.0)
            self.assertEqual(child_token, 1)
        finally:
            if proc.is_alive():
                release_lock_evt.set()
                proc.terminate()
                proc.join(timeout=5.0)

        # Parent sees the child's persisted clock (200) and reclaims after expiry at now_ms=250.
        self.assertEqual(self.store.get_clock_ms(), 200)
        reclaimed = self.store.claim_batch(
            worker_id="proc-parent-worker",
            now_ms=250,
            lease_duration_ms=50,
            batch_size=1,
        )
        self.assertEqual(len(reclaimed), 1)
        self.assertEqual(reclaimed[0].fencing_token, 2)


def _child_process_entry(
    db_path: str,
    lock_acquired_evt: MpEvent,
    release_lock_evt: MpEvent,
    result_queue: multiprocessing.Queue[int],
) -> None:
    token = _child_hold_write_lock_and_claim(db_path, lock_acquired_evt, release_lock_evt)
    result_queue.put(token)
