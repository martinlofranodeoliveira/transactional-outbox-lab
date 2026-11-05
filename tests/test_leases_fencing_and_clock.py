"""Tests for deterministic batch claiming, fencing tokens, lease expiration, and logical clock."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from outbox_lab import (
    ClockRegressionError,
    DomainMutation,
    InvalidStateTransitionError,
    LeaseExpiredError,
    OutboxDraft,
    OutboxState,
    OutboxStore,
    OutboxValidationError,
    StaleFencingTokenError,
)


class LeasesFencingAndClockTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="outbox_test_fencing_")
        self.db_path = Path(self._tmp.name) / "outbox.sqlite3"
        self.store = OutboxStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def _stage_message(self, message_id: str, agg_id: str, now_ms: int) -> None:
        self.store.commit_domain_and_outbox(
            DomainMutation("job", agg_id, 1, {"job_id": agg_id}),
            OutboxDraft(message_id, "job.queued", {"job_id": agg_id}, max_attempts=3),
            now_ms=now_ms,
        )

    def test_claim_batch_orders_deterministically_and_assigns_monotonic_tokens(self) -> None:
        self._stage_message("msg-b", "job-b", now_ms=10)
        self._stage_message("msg-a", "job-a", now_ms=10)
        self._stage_message("msg-c", "job-c", now_ms=20)

        claimed = self.store.claim_batch(
            worker_id="worker-1",
            now_ms=25,
            lease_duration_ms=100,
            batch_size=2,
        )
        self.assertEqual([r.message_id for r in claimed], ["msg-a", "msg-b"])
        self.assertEqual([r.fencing_token for r in claimed], [1, 2])
        self.assertEqual(claimed[0].lease_expires_at_ms, 125)

        claimed_next = self.store.claim_batch(
            worker_id="worker-1",
            now_ms=25,
            lease_duration_ms=100,
            batch_size=2,
        )
        self.assertEqual([r.message_id for r in claimed_next], ["msg-c"])
        self.assertEqual(claimed_next[0].fencing_token, 3)

    def test_fencing_rejects_stale_worker_on_publish_failure_and_renew(self) -> None:
        self._stage_message("msg-fence-1", "job-1", now_ms=0)

        claim_a = self.store.claim_batch(
            worker_id="worker-a", now_ms=0, lease_duration_ms=10, batch_size=1
        )[0]
        self.assertEqual(claim_a.fencing_token, 1)
        self.assertEqual(claim_a.lease_expires_at_ms, 10)

        # At now_ms=10 (lease expired: 10 >= 10), worker-b reclaims the lease.
        claim_b = self.store.claim_batch(
            worker_id="worker-b", now_ms=10, lease_duration_ms=20, batch_size=1
        )[0]
        self.assertEqual(claim_b.fencing_token, 2)
        self.assertEqual(claim_b.lease_owner, "worker-b")

        # Worker A cannot mark_published, record_failure, or renew_lease with token 1.
        with self.assertRaises(StaleFencingTokenError) as pub_ctx:
            self.store.mark_published(
                message_id="msg-fence-1",
                worker_id="worker-a",
                fencing_token=claim_a.fencing_token,
                now_ms=12,
            )
        self.assertEqual(pub_ctx.exception.error_code, "stale_fencing_token")

        with self.assertRaises(StaleFencingTokenError) as fail_ctx:
            self.store.record_failure(
                message_id="msg-fence-1",
                worker_id="worker-a",
                fencing_token=claim_a.fencing_token,
                now_ms=13,
                error_code="worker_a_timeout",
                error_reason="Stale worker A failure report",
            )
        self.assertEqual(fail_ctx.exception.error_code, "stale_fencing_token")

        with self.assertRaises(StaleFencingTokenError) as renew_ctx:
            self.store.renew_lease(
                message_id="msg-fence-1",
                worker_id="worker-a",
                fencing_token=claim_a.fencing_token,
                now_ms=14,
                lease_duration_ms=50,
            )
        self.assertEqual(renew_ctx.exception.error_code, "stale_fencing_token")

        # Worker B can renew and then publish with token 2.
        renewed = self.store.renew_lease(
            message_id="msg-fence-1",
            worker_id="worker-b",
            fencing_token=claim_b.fencing_token,
            now_ms=15,
            lease_duration_ms=30,
        )
        self.assertEqual(renewed.lease_expires_at_ms, 45)

        published = self.store.mark_published(
            message_id="msg-fence-1",
            worker_id="worker-b",
            fencing_token=claim_b.fencing_token,
            now_ms=20,
        )
        self.assertEqual(published.state, OutboxState.PUBLISHED)

        # Even after Worker B published the message, Worker A presenting token 1 gets StaleFencingTokenError.
        with self.assertRaises(StaleFencingTokenError):
            self.store.mark_published(
                message_id="msg-fence-1",
                worker_id="worker-a",
                fencing_token=claim_a.fencing_token,
                now_ms=21,
            )

        # Worker B presenting active token 2 again after publishing gets InvalidStateTransitionError.
        with self.assertRaises(InvalidStateTransitionError):
            self.store.mark_published(
                message_id="msg-fence-1",
                worker_id="worker-b",
                fencing_token=claim_b.fencing_token,
                now_ms=22,
            )

    def test_expired_lease_rejected_without_takeover_and_clock_regression_cannot_resurrect_lease(
        self,
    ) -> None:
        """Verify that rejecting an expired ack at now=11 persists clock=11 so a call at now=9 cannot resurrect it."""
        self._stage_message("msg-exp-1", "job-exp-1", now_ms=0)
        claim = self.store.claim_batch(
            worker_id="worker-a", now_ms=0, lease_duration_ms=10, batch_size=1
        )[0]
        self.assertEqual(claim.lease_expires_at_ms, 10)

        # At now=11 (no takeover by any other worker), mark_published must be rejected as lease_expired.
        with self.assertRaises(LeaseExpiredError) as exp_ctx:
            self.store.mark_published(
                message_id="msg-exp-1",
                worker_id="worker-a",
                fencing_token=claim.fencing_token,
                now_ms=11,
            )
        self.assertEqual(exp_ctx.exception.error_code, "lease_expired")
        self.assertEqual(self.store.get_clock_ms(), 11)

        # Attempting to call mark_published or record_failure at now=9 on the same store must fail with ClockRegressionError.
        with self.assertRaises(ClockRegressionError) as reg_ctx:
            self.store.mark_published(
                message_id="msg-exp-1",
                worker_id="worker-a",
                fencing_token=claim.fencing_token,
                now_ms=9,
            )
        self.assertEqual(reg_ctx.exception.error_code, "clock_regression")

        # Even closing and reopening a fresh OutboxStore on the SQLite file must reject now=9.
        self.store.close()
        with OutboxStore(self.db_path) as reopened:
            self.assertEqual(reopened.get_clock_ms(), 11)
            with self.assertRaises(ClockRegressionError):
                reopened.mark_published(
                    message_id="msg-exp-1",
                    worker_id="worker-a",
                    fencing_token=claim.fencing_token,
                    now_ms=9,
                )
            with self.assertRaises(ClockRegressionError):
                reopened.record_failure(
                    message_id="msg-exp-1",
                    worker_id="worker-a",
                    fencing_token=claim.fencing_token,
                    now_ms=9,
                    error_code="sneak_retry",
                    error_reason="Attempted clock rewind",
                )
            # Record must still be leased (expired) and never published.
            rec = reopened.get_outbox_record("msg-exp-1")
            self.assertIsNotNone(rec)
            assert rec is not None
            self.assertEqual(rec.state, OutboxState.LEASED)

    def test_exact_lease_expiry_boundary_at_now_equals_lease_expires_at(self) -> None:
        self._stage_message("msg-bound-1", "job-bound-1", now_ms=100)
        claim = self.store.claim_batch(
            worker_id="worker-a", now_ms=100, lease_duration_ms=50, batch_size=1
        )[0]
        self.assertEqual(claim.lease_expires_at_ms, 150)

        # At exact boundary now_ms == 150, lease is already expired.
        with self.assertRaises(LeaseExpiredError):
            self.store.record_failure(
                message_id="msg-bound-1",
                worker_id="worker-a",
                fencing_token=claim.fencing_token,
                now_ms=150,
                error_code="timeout",
                error_reason="Boundary expiry check",
            )

    def test_boolean_and_negative_timestamps_rejected(self) -> None:
        with self.assertRaises(OutboxValidationError):
            self.store.claim_batch(
                worker_id="worker-a",
                now_ms=True,  # type: ignore[arg-type]
                lease_duration_ms=10,
            )
        with self.assertRaises(OutboxValidationError):
            self.store.claim_batch(
                worker_id="worker-a",
                now_ms=-1,
                lease_duration_ms=10,
            )
