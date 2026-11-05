"""Deterministic multi-worker outbox simulation and invariant verification."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from outbox_lab.errors import DuplicateMessageError, StaleFencingTokenError
from outbox_lab.models import DomainMutation, OutboxDraft, OutboxState
from outbox_lab.relay import (
    DeduplicatingConsumerInbox,
    FaultAction,
    FaultInjectionPlan,
    RecordingBroker,
    RelayStepOutcome,
    RelayWorker,
)
from outbox_lab.store import OutboxStore


def run_demo_scenario() -> dict[str, Any]:
    """Execute the complete deterministic multi-worker outbox scenario against a temporary SQLite DB.

    Covers:
      1. Atomic domain + outbox commit and rollback on duplicate message_id collision.
      2. Transient publish failure with deterministic exponential retry backoff.
      3. Post-send acknowledgment loss (DROP_ACK_AFTER_SEND), lease timeout takeover by
         a second worker with a higher fencing token, rejection of the stale worker's
         delayed mark_published call (stale_fencing_token), and duplicate delivery
         deduplicated by the downstream consumer inbox (raw=2, deduplicated=1).
      4. Retry exhaustion transitioning directly from 'leased' to 'dead_letter'.
    """
    with tempfile.TemporaryDirectory(prefix="outbox_lab_demo_") as tmp_dir:
        db_file = Path(tmp_dir) / "demo_outbox.sqlite3"
        with OutboxStore(db_file, busy_timeout_ms=0, max_audit_rows=500) as store:
            inbox = DeduplicatingConsumerInbox()
            broker = RecordingBroker(inbox=inbox)
            fault_plan = FaultInjectionPlan()

            fault_plan.configure(
                "msg-order-101",
                1,
                FaultAction.FAIL_BEFORE_SEND,
                error_code="broker_timeout",
                error_reason="Transient broker socket timeout before publish",
            )
            fault_plan.configure(
                "msg-invoice-202",
                1,
                FaultAction.DROP_ACK_AFTER_SEND,
                error_code="ack_lost",
                error_reason="Worker lost connection after broker accepted frame",
            )
            fault_plan.configure(
                "msg-webhook-303",
                1,
                FaultAction.FAIL_BEFORE_SEND,
                error_code="endpoint_503",
                error_reason="Downstream gateway returned HTTP 503 on attempt 1",
            )
            fault_plan.configure(
                "msg-webhook-303",
                2,
                FaultAction.FAIL_BEFORE_SEND,
                error_code="endpoint_503",
                error_reason="Downstream gateway returned HTTP 503 on final attempt 2",
            )

            worker_alpha = RelayWorker(
                "worker-alpha",
                store,
                broker,
                fault_plan=fault_plan,
                base_backoff_ms=1_000,
                max_backoff_ms=30_000,
            )
            worker_beta = RelayWorker(
                "worker-beta",
                store,
                broker,
                fault_plan=fault_plan,
                base_backoff_ms=1_000,
                max_backoff_ms=30_000,
            )

            # 1. Stage three synthetic domain mutations + outbox messages
            store.commit_domain_and_outbox(
                DomainMutation(
                    aggregate_type="order",
                    aggregate_id="ord-101",
                    version=1,
                    state={"order_id": "ord-101", "status": "confirmed", "total_cents": 14500},
                ),
                OutboxDraft(
                    message_id="msg-order-101",
                    event_type="order.confirmed",
                    payload={"order_id": "ord-101", "total_cents": 14500, "currency": "BRL"},
                    max_attempts=3,
                ),
                now_ms=1_000,
            )

            # Verify atomic rollback when outbox message_id collides
            atomic_rollback_verified = False
            try:
                store.commit_domain_and_outbox(
                    DomainMutation(
                        aggregate_type="order",
                        aggregate_id="ord-orphan-999",
                        version=1,
                        state={"order_id": "ord-orphan-999", "status": "should_rollback"},
                    ),
                    OutboxDraft(
                        message_id="msg-order-101",
                        event_type="order.duplicate",
                        payload={"order_id": "ord-orphan-999"},
                        max_attempts=3,
                    ),
                    now_ms=1_005,
                )
            except DuplicateMessageError as exc:
                orphan_domain = store.get_domain_record("order", "ord-orphan-999")
                atomic_rollback_verified = (
                    exc.error_code == "duplicate_message_id" and orphan_domain is None
                )

            store.commit_domain_and_outbox(
                DomainMutation(
                    aggregate_type="invoice",
                    aggregate_id="inv-202",
                    version=1,
                    state={"invoice_id": "inv-202", "order_id": "ord-101", "amount_cents": 14500},
                ),
                OutboxDraft(
                    message_id="msg-invoice-202",
                    event_type="invoice.issued",
                    payload={"invoice_id": "inv-202", "order_id": "ord-101", "amount_cents": 14500},
                    max_attempts=3,
                ),
                now_ms=1_010,
            )

            store.commit_domain_and_outbox(
                DomainMutation(
                    aggregate_type="webhook",
                    aggregate_id="wh-303",
                    version=1,
                    state={"webhook_id": "wh-303", "target": "partner-erp", "active": True},
                ),
                OutboxDraft(
                    message_id="msg-webhook-303",
                    event_type="webhook.dispatch",
                    payload={"webhook_id": "wh-303", "event": "invoice.issued"},
                    max_attempts=2,
                ),
                now_ms=1_020,
            )

            relay_steps: list[RelayStepOutcome] = []

            # 2. Worker Alpha claims all 3 pending records at t=1100 (lease duration 500ms -> expires at 1600)
            batch_1 = worker_alpha.claim(
                now_ms=1_100,
                lease_duration_ms=500,
                batch_size=10,
            )
            by_id_1 = {rec.message_id: rec for rec in batch_1}

            # msg-order-101 experiences transient failure before send at t=1150 -> retry scheduled at 1150 + 1000 = 2150
            relay_steps.append(
                worker_alpha.deliver_claimed(by_id_1["msg-order-101"], now_ms=1_150)
            )

            # msg-invoice-202 is delivered to broker at t=1200, but worker-alpha loses ack before mark_published!
            stale_invoice_claim = by_id_1["msg-invoice-202"]
            relay_steps.append(
                worker_alpha.deliver_claimed(stale_invoice_claim, now_ms=1_200)
            )

            # msg-webhook-303 experiences attempt 1 failure at t=1250 -> retry scheduled at 1250 + 1000 = 2250
            relay_steps.append(
                worker_alpha.deliver_claimed(by_id_1["msg-webhook-303"], now_ms=1_250)
            )

            # 3. At t=1600, worker-alpha's lease on msg-invoice-202 has expired (1600 >= 1600).
            # Worker Beta claims eligible records -> reclaims msg-invoice-202 with higher fencing_token.
            batch_2 = worker_beta.claim(
                now_ms=1_600,
                lease_duration_ms=500,
                batch_size=10,
            )
            assert len(batch_2) == 1 and batch_2[0].message_id == "msg-invoice-202"
            reclaimed_invoice = batch_2[0]

            # At t=1650, stale worker-alpha wakes up and attempts mark_published with its old fencing_token.
            stale_fencing_rejected = False
            stale_rejection_code: str | None = None
            try:
                store.mark_published(
                    message_id="msg-invoice-202",
                    worker_id="worker-alpha",
                    fencing_token=stale_invoice_claim.fencing_token,
                    now_ms=1_650,
                )
            except StaleFencingTokenError as exc:
                stale_fencing_rejected = True
                stale_rejection_code = exc.error_code

            # At t=1700, worker-beta delivers msg-invoice-202 (2nd raw broker delivery) and commits mark_published.
            relay_steps.append(
                worker_beta.deliver_claimed(reclaimed_invoice, now_ms=1_700)
            )

            # 4. At t=2150, msg-order-101 backoff elapses; worker-alpha claims and publishes it at t=2200.
            batch_3 = worker_alpha.claim(
                now_ms=2_150,
                lease_duration_ms=500,
                batch_size=10,
            )
            assert len(batch_3) == 1 and batch_3[0].message_id == "msg-order-101"
            relay_steps.append(
                worker_alpha.deliver_claimed(batch_3[0], now_ms=2_200)
            )

            # 5. At t=2250, msg-webhook-303 backoff elapses; worker-beta claims and fails attempt 2/2 -> dead_letter.
            batch_4 = worker_beta.claim(
                now_ms=2_250,
                lease_duration_ms=500,
                batch_size=10,
            )
            assert len(batch_4) == 1 and batch_4[0].message_id == "msg-webhook-303"
            relay_steps.append(
                worker_beta.deliver_claimed(batch_4[0], now_ms=2_300)
            )

            # Verify all scenario invariants
            invoice_deliveries = broker.list_deliveries("msg-invoice-202")
            duplicate_payloads_identical = (
                len(invoice_deliveries) == 2
                and invoice_deliveries[0].message_id == "msg-invoice-202"
                and invoice_deliveries[1].message_id == "msg-invoice-202"
                and invoice_deliveries[0].payload_json == invoice_deliveries[1].payload_json
                and invoice_deliveries[0].fencing_token < invoice_deliveries[1].fencing_token
            )
            final_order = store.get_outbox_record("msg-order-101")
            final_invoice = store.get_outbox_record("msg-invoice-202")
            final_webhook = store.get_outbox_record("msg-webhook-303")
            assert final_order is not None
            assert final_invoice is not None
            assert final_webhook is not None

            invariants = {
                "atomic_rollback_on_collision": atomic_rollback_verified,
                "stale_worker_fenced": stale_fencing_rejected
                and stale_rejection_code == "stale_fencing_token",
                "ack_loss_duplicate_emitted": duplicate_payloads_identical,
                "invoice_raw_delivery_count": inbox.raw_delivery_count("msg-invoice-202"),
                "invoice_deduplicated_count": inbox.deduplicated_count("msg-invoice-202"),
                "total_raw_broker_deliveries": inbox.raw_delivery_count(),
                "total_deduplicated_inbox_records": inbox.deduplicated_count(),
                "retry_exhausted_to_dead_letter": (
                    final_webhook.state == OutboxState.DEAD_LETTER
                    and final_webhook.attempt_count == final_webhook.max_attempts == 2
                    and final_webhook.next_attempt_at_ms is None
                ),
                "all_invariants_passed": False,
            }
            invariants["all_invariants_passed"] = bool(
                invariants["atomic_rollback_on_collision"]
                and invariants["stale_worker_fenced"]
                and invariants["ack_loss_duplicate_emitted"]
                and invariants["invoice_raw_delivery_count"] == 2
                and invariants["invoice_deduplicated_count"] == 1
                and invariants["total_raw_broker_deliveries"] == 3
                and invariants["total_deduplicated_inbox_records"] == 2
                and invariants["retry_exhausted_to_dead_letter"]
            )
            if not invariants["all_invariants_passed"]:
                raise AssertionError(f"Demo scenario invariant check failed: {invariants}")

            return {
                "scenario": "transactional-outbox-at-least-once-lab",
                "version": "0.1.0",
                "storage_mode": "sqlite_temp_file",
                "final_clock_ms": store.get_clock_ms(),
                "last_fencing_token": store.get_last_fencing_token(),
                "invariants": invariants,
                "domain_records": [d.to_dict() for d in store.list_domain_records()],
                "outbox_records": [r.to_dict() for r in store.list_outbox_records()],
                "relay_steps": [s.to_dict() for s in relay_steps],
                "broker_deliveries": [d.to_dict() for d in broker.list_deliveries()],
                "consumer_inbox": [i.to_dict() for i in inbox.list_inbox_records()],
                "audit_log": [a.to_dict() for a in store.list_audit_log()],
            }


def format_demo_text(report: dict[str, Any]) -> str:
    """Render a concise human-readable report from the demo scenario dictionary."""
    inv = report["invariants"]
    lines = [
        f"Transactional Outbox Lab v{report['version']} - Multi-Worker Simulation",
        "=" * 72,
        f"Final Logical Clock (ms): {report['final_clock_ms']}",
        f"Highest Fencing Token:    {report['last_fencing_token']}",
        f"All Invariants Passed:    {inv['all_invariants_passed']}",
        "",
        "Key Invariants:",
        f"  - Atomic rollback on outbox collision: {inv['atomic_rollback_on_collision']}",
        f"  - Stale worker fenced after expiry:    {inv['stale_worker_fenced']}",
        f"  - Ack-loss duplicate payload match:    {inv['ack_loss_duplicate_emitted']}",
        f"  - msg-invoice-202 deliveries:          raw={inv['invoice_raw_delivery_count']}, deduplicated={inv['invoice_deduplicated_count']}",
        f"  - Retry exhaustion to dead_letter:     {inv['retry_exhausted_to_dead_letter']}",
        "",
        "Final Outbox Records:",
    ]
    for rec in report["outbox_records"]:
        lines.append(
            f"  * {rec['message_id']} [{rec['event_type']}] -> state={rec['state']} "
            f"attempts={rec['attempt_count']}/{rec['max_attempts']} "
            f"token={rec['fencing_token']}"
        )
    lines.append("")
    lines.append("Audit Log Transitions:")
    for entry in report["audit_log"]:
        worker = entry["worker_id"] or "producer"
        from_st = entry["from_state"] or "-"
        lines.append(
            f"  #{entry['audit_seq']:02d} t={entry['recorded_at_ms']}ms "
            f"{entry['message_id']} ({worker}) {entry['action']}: "
            f"{from_st} -> {entry['to_state']} [token={entry['fencing_token']}]"
        )
    return "\n".join(lines)
