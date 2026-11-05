"""Tests for atomic domain+outbox persistence, rollback guarantees, and payload validation."""

from __future__ import annotations

import math
import sqlite3
import tempfile
import unittest
from pathlib import Path

from outbox_lab import (
    DomainMutation,
    DomainVersionConflictError,
    DuplicateMessageError,
    OutboxConstraintError,
    OutboxDraft,
    OutboxState,
    OutboxStore,
    OutboxValidationError,
)


class AtomicCommitAndValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="outbox_test_atomic_")
        self.db_path = Path(self._tmp.name) / "outbox.sqlite3"
        self.store = OutboxStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_atomic_commit_persists_domain_and_outbox_together(self) -> None:
        domain_rec, outbox_rec = self.store.commit_domain_and_outbox(
            DomainMutation(
                aggregate_type="account",
                aggregate_id="acc-1",
                version=1,
                state={"balance_cents": 5000, "owner": "synthetic-user-1"},
            ),
            OutboxDraft(
                message_id="msg-acc-1-created",
                event_type="account.created",
                payload={"account_id": "acc-1", "balance_cents": 5000},
                max_attempts=4,
            ),
            now_ms=100,
        )
        self.assertEqual(domain_rec.version, 1)
        self.assertEqual(domain_rec.state["balance_cents"], 5000)
        self.assertEqual(outbox_rec.state, OutboxState.PENDING)
        self.assertEqual(outbox_rec.attempt_count, 0)
        self.assertEqual(outbox_rec.max_attempts, 4)
        self.assertEqual(outbox_rec.next_attempt_at_ms, 100)
        self.assertEqual(outbox_rec.fencing_token, 0)
        self.assertIsNone(outbox_rec.lease_owner)
        self.assertIsNone(outbox_rec.lease_expires_at_ms)

    def test_duplicate_message_id_rolls_back_accompanying_domain_write(self) -> None:
        self.store.commit_domain_and_outbox(
            DomainMutation(
                aggregate_type="order",
                aggregate_id="ord-1",
                version=1,
                state={"status": "created"},
            ),
            OutboxDraft(
                message_id="msg-dup-1",
                event_type="order.created",
                payload={"order_id": "ord-1"},
            ),
            now_ms=100,
        )

        with self.assertRaises(DuplicateMessageError) as ctx:
            self.store.commit_domain_and_outbox(
                DomainMutation(
                    aggregate_type="order",
                    aggregate_id="ord-2",
                    version=1,
                    state={"status": "must_not_persist"},
                ),
                OutboxDraft(
                    message_id="msg-dup-1",
                    event_type="order.created",
                    payload={"order_id": "ord-2"},
                ),
                now_ms=110,
            )
        self.assertEqual(ctx.exception.error_code, "duplicate_message_id")

        # Accompanying domain write for ord-2 must be completely rolled back.
        self.assertIsNone(self.store.get_domain_record("order", "ord-2"))
        self.assertEqual(len(self.store.list_domain_records()), 1)
        self.assertEqual(len(self.store.list_outbox_records()), 1)
        # Clock must still have advanced to 110.
        self.assertEqual(self.store.get_clock_ms(), 110)

    def test_database_trigger_abort_on_outbox_insert_rolls_back_domain_update(self) -> None:
        # Install a SQLite BEFORE INSERT trigger on outbox_messages that aborts on a specific event_type.
        raw_conn = sqlite3.connect(self.db_path)
        try:
            raw_conn.execute(
                """
                CREATE TRIGGER abort_poison_event
                BEFORE INSERT ON outbox_messages
                WHEN NEW.event_type = 'poison.event'
                BEGIN
                    SELECT RAISE(ABORT, 'simulated outbox table constraint failure');
                END;
                """
            )
            raw_conn.commit()
        finally:
            raw_conn.close()

        with self.assertRaises(OutboxConstraintError) as ctx:
            self.store.commit_domain_and_outbox(
                DomainMutation(
                    aggregate_type="payment",
                    aggregate_id="pay-77",
                    version=1,
                    state={"status": "authorized", "amount": 900},
                ),
                OutboxDraft(
                    message_id="msg-poison-77",
                    event_type="poison.event",
                    payload={"payment_id": "pay-77"},
                ),
                now_ms=200,
            )
        self.assertEqual(ctx.exception.error_code, "constraint_violation")
        self.assertIsNone(self.store.get_domain_record("payment", "pay-77"))
        self.assertIsNone(self.store.get_outbox_record("msg-poison-77"))
        self.assertEqual(len(self.store.list_domain_records()), 0)
        self.assertEqual(len(self.store.list_outbox_records()), 0)

    def test_domain_version_conflict_rolls_back_and_leaves_no_outbox_row(self) -> None:
        with self.assertRaises(DomainVersionConflictError):
            self.store.commit_domain_and_outbox(
                DomainMutation(
                    aggregate_type="cart",
                    aggregate_id="cart-1",
                    version=2,
                    state={"items": 1},
                ),
                OutboxDraft(
                    message_id="msg-cart-1",
                    event_type="cart.updated",
                    payload={"items": 1},
                ),
                now_ms=50,
            )
        self.assertIsNone(self.store.get_domain_record("cart", "cart-1"))
        self.assertIsNone(self.store.get_outbox_record("msg-cart-1"))

    def test_payload_freezing_prevents_caller_mutation_and_sorts_keys(self) -> None:
        caller_state = {"tags": ["a", "b"], "nested": {"k": "v1"}}
        caller_payload = {"z_key": 1, "a_key": ["x", {"inner": "initial"}]}

        domain_rec, outbox_rec = self.store.commit_domain_and_outbox(
            DomainMutation(
                aggregate_type="doc",
                aggregate_id="doc-1",
                version=1,
                state=caller_state,
            ),
            OutboxDraft(
                message_id="msg-doc-1",
                event_type="doc.saved",
                payload=caller_payload,
            ),
            now_ms=300,
        )

        # Mutate caller objects after commit.
        caller_state["tags"].append("mutated")
        caller_state["nested"]["k"] = "mutated"
        caller_payload["a_key"][1]["inner"] = "mutated"

        # Stored canonical JSON and returned frozen views must remain untouched.
        self.assertEqual(outbox_rec.payload_json, '{"a_key":["x",{"inner":"initial"}],"z_key":1}')
        self.assertEqual(outbox_rec.payload["a_key"][1]["inner"], "initial")
        self.assertEqual(domain_rec.state["nested"]["k"], "v1")

        with self.assertRaises(TypeError):
            outbox_rec.payload["z_key"] = 999  # type: ignore[index]
        with self.assertRaises(TypeError):
            outbox_rec.payload["a_key"][1]["inner"] = "hack"  # type: ignore[index]

    def test_rejects_nonfinite_floats_deep_nesting_and_oversized_payloads(self) -> None:
        for bad_float in (math.nan, math.inf, -math.inf):
            with self.assertRaises(OutboxValidationError):
                self.store.commit_domain_and_outbox(
                    DomainMutation("item", "i-1", 1, {"ok": 1}),
                    OutboxDraft("msg-bad-float", "item.bad", {"score": bad_float}),
                    now_ms=400,
                )

        deep: dict[str, object] = {"leaf": 1}
        for _ in range(20):
            deep = {"child": deep}
        with self.assertRaises(OutboxValidationError):
            self.store.commit_domain_and_outbox(
                DomainMutation("item", "i-1", 1, {"ok": 1}),
                OutboxDraft("msg-deep", "item.deep", deep),
                now_ms=400,
            )

        oversized = {f"k{i}": "x" * 500 for i in range(40)}
        with self.assertRaises(OutboxValidationError):
            self.store.commit_domain_and_outbox(
                DomainMutation("item", "i-1", 1, {"ok": 1}),
                OutboxDraft("msg-big", "item.big", oversized),
                now_ms=400,
            )

        self.assertEqual(len(self.store.list_outbox_records()), 0)
