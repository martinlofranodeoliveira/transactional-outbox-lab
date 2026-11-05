"""Tests for deterministic retry backoff, retry exhaustion to dead_letter, and bounded audit log."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from outbox_lab import (
    DomainMutation,
    OutboxDraft,
    OutboxState,
    OutboxStore,
)


class RetriesDeadLetterAndAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="outbox_test_retry_")
        self.db_path = Path(self._tmp.name) / "outbox.sqlite3"
        self.store = OutboxStore(self.db_path, max_audit_rows=8)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_retry_backoff_schedule_and_exact_exhaustion_to_dead_letter(self) -> None:
        self.store.commit_domain_and_outbox(
            DomainMutation("shipment", "shp-1", 1, {"shipment_id": "shp-1"}),
            OutboxDraft(
                message_id="msg-shp-1",
                event_type="shipment.created",
                payload={"shipment_id": "shp-1", "secret_token": "DO_NOT_LEAK_IN_AUDIT"},
                max_attempts=3,
            ),
            now_ms=1_000,
        )

        # Attempt 1: claim at 1000, fail at 1050 with base_backoff=500 -> next_attempt_at = 1550
        c1 = self.store.claim_batch(
            worker_id="w1", now_ms=1_000, lease_duration_ms=200, batch_size=1
        )[0]
        r1 = self.store.record_failure(
            message_id="msg-shp-1",
            worker_id="w1",
            fencing_token=c1.fencing_token,
            now_ms=1_050,
            error_code="smtp_421",
            error_reason="Temporary downstream rate limit",
            base_backoff_ms=500,
            max_backoff_ms=5_000,
        )
        self.assertEqual(r1.state, OutboxState.PENDING)
        self.assertEqual(r1.attempt_count, 1)
        self.assertEqual(r1.next_attempt_at_ms, 1_550)
        self.assertIsNone(r1.lease_owner)
        self.assertIsNone(r1.lease_expires_at_ms)
        self.assertIsNone(r1.dead_lettered_at_ms)

        # Not claimable before 1550.
        self.assertEqual(
            self.store.claim_batch(
                worker_id="w1", now_ms=1_549, lease_duration_ms=200, batch_size=1
            ),
            [],
        )

        # Attempt 2: claim at 1550, fail at 1600 -> backoff = 500 * 2^1 = 1000 -> next_attempt_at = 2600
        c2 = self.store.claim_batch(
            worker_id="w1", now_ms=1_550, lease_duration_ms=200, batch_size=1
        )[0]
        self.assertGreater(c2.fencing_token, c1.fencing_token)
        r2 = self.store.record_failure(
            message_id="msg-shp-1",
            worker_id="w1",
            fencing_token=c2.fencing_token,
            now_ms=1_600,
            error_code="smtp_421",
            error_reason="Temporary downstream rate limit again",
            base_backoff_ms=500,
            max_backoff_ms=5_000,
        )
        self.assertEqual(r2.state, OutboxState.PENDING)
        self.assertEqual(r2.attempt_count, 2)
        self.assertEqual(r2.next_attempt_at_ms, 2_600)

        # Attempt 3 (max_attempts == 3): claim at 2600, fail at 2650 -> transitions directly to dead_letter.
        c3 = self.store.claim_batch(
            worker_id="w2", now_ms=2_600, lease_duration_ms=200, batch_size=1
        )[0]
        r3 = self.store.record_failure(
            message_id="msg-shp-1",
            worker_id="w2",
            fencing_token=c3.fencing_token,
            now_ms=2_650,
            error_code="smtp_550",
            error_reason="Permanent mailbox failure on final attempt",
            base_backoff_ms=500,
            max_backoff_ms=5_000,
        )
        self.assertEqual(r3.state, OutboxState.DEAD_LETTER)
        self.assertEqual(r3.attempt_count, 3)
        self.assertIsNone(r3.next_attempt_at_ms)
        self.assertIsNone(r3.lease_owner)
        self.assertIsNone(r3.lease_expires_at_ms)
        self.assertEqual(r3.dead_lettered_at_ms, 2_650)
        self.assertEqual(r3.last_error_code, "smtp_550")

        # Dead-lettered record is never claimed again.
        self.assertEqual(
            self.store.claim_batch(
                worker_id="w2", now_ms=50_000, lease_duration_ms=200, batch_size=10
            ),
            [],
        )

    def test_bounded_audit_retention_and_no_payload_leakage(self) -> None:
        # Store configured with max_audit_rows=8 in setUp.
        for idx in range(12):
            msg_id = f"msg-audit-{idx}"
            self.store.commit_domain_and_outbox(
                DomainMutation("metric", f"m-{idx}", 1, {"v": idx}),
                OutboxDraft(
                    message_id=msg_id,
                    event_type="metric.recorded",
                    payload={"sensitive_field": f"CONFIDENTIAL_{idx}"},
                    max_attempts=2,
                ),
                now_ms=100 + idx * 10,
            )
            claimed = self.store.claim_batch(
                worker_id="w-audit",
                now_ms=105 + idx * 10,
                lease_duration_ms=50,
                batch_size=1,
            )[0]
            self.store.mark_published(
                message_id=msg_id,
                worker_id="w-audit",
                fencing_token=claimed.fencing_token,
                now_ms=108 + idx * 10,
            )

        audit_entries = self.store.list_audit_log()
        self.assertEqual(len(audit_entries), 8)
        # Verify strictly increasing sequence numbers for the retained tail.
        seqs = [a.audit_seq for a in audit_entries]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(seqs[-1] - seqs[0], 7)

        # Verify no payload content appears anywhere in the audit records.
        serialized_audit = repr([a.to_dict() for a in audit_entries])
        self.assertNotIn("CONFIDENTIAL_", serialized_audit)
        self.assertNotIn("sensitive_field", serialized_audit)
