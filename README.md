# transactional-outbox-lab

**Development version 0.1.0** · [Lofrano Tecnologia](https://lofranotecnologia.com.br) · [Repository](https://github.com/martinlofranodeoliveira/transactional-outbox-lab) · [Português](README.pt-BR.md)

A Python 3.13 standard-library SQLite library and offline laboratory for the **Transactional Outbox** pattern. It demonstrates atomic producer writes, deterministic retries, leased worker takeover, fencing and duplicate delivery after post-send acknowledgment loss. There is no runtime service, model, API, telemetry or network dependency.

## Install, test and demonstrate

From the repository root, Python 3.13 can run everything without installation:

```bash
python3 -m unittest discover -s tests -v
python3 -m outbox_lab demo
python3 -m outbox_lab demo --format text
```

Optional package installation into a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install .
.venv/bin/python -m outbox_lab demo
```

Installation uses the setuptools build dependency declared in `pyproject.toml`; obtaining build tooling may require network access. Tests and the demo run offline directly from the source tree. The installed console command is `outbox-lab demo`.

The demo uses a temporary SQLite file, fixed synthetic identifiers and fixed logical ticks. Default output is a complete deterministic JSON report with domain/outbox snapshots, relay steps, raw deliveries, deduplicated inbox records, bounded audit entries and checked invariants. Human-readable output begins:

```text
Transactional Outbox Lab v0.1.0 - Multi-Worker Simulation
========================================================================
Final Logical Clock (ms): 2300
Highest Fencing Token:    6
All Invariants Passed:    True
```

The invoice message has two identical deliveries but one deduplicated inbox entry. The final order is published after one transient failure; the webhook is dead-lettered on its second committed failure.

## Small library example

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from outbox_lab import DomainMutation, OutboxDraft, OutboxStore

with TemporaryDirectory() as directory:
    with OutboxStore(Path(directory) / "example.sqlite3") as store:
        store.commit_domain_and_outbox(
            DomainMutation("task", "task-1", 1, {"status": "queued"}),
            OutboxDraft("message-1", "task.queued", {"task_id": "task-1"}),
            now_ms=100,
        )
        claim = store.claim_batch(
            worker_id="worker-a", now_ms=100,
            lease_duration_ms=50, batch_size=1,
        )[0]
        # An application would send the immutable event outside the transaction.
        result = store.mark_published(
            message_id=claim.message_id, worker_id="worker-a",
            fencing_token=claim.fencing_token, now_ms=110,
        )
        print(result.state.value)  # published
```

Use one caller-supplied logical integer millisecond domain per database. Equal timestamps are allowed. Never mix UTC epoch values with process monotonic origins. The store persists a nondecreasing clock across connections and reopenings; rejected semantic operations also preserve the observed time. In particular, an expired acknowledgment cannot be retried using an earlier tick. A lease is already expired at `now_ms == lease_expires_at_ms`.

`attempt_count` counts committed failures plus a committed success, **not claims or uncertain sends**. A crash or lost acknowledgment consumes no attempt, so recovery can send the same message repeatedly. `max_attempts` is not a bound on raw deliveries. See the [precise contract](docs/contract.md).

## Why this design?

Domain state and an immutable event are written together under `BEGIN IMMEDIATE`, removing the local dual-write gap. Network work never occurs inside a database transaction. Tokens and unexpired leases protect all worker database changes. A short `check_lease` preflight refreshes the relay's snapshot and persists send-time observations, but cannot fence an external broker atomically.

A worker can send successfully and crash before committing `mark_published`. Recovery must resend. The included in-memory consumer demonstrates deduplication by `message_id` and rejects reuse of an identifier with different event contents. This is not exactly-once delivery.

## Limits and failure handling

- This is a bounded architectural lab, **not a production broker, distributed relay service or CDC daemon**. Eventual delivery requires continued polling and a working transport; terminal dead letters are not delivered automatically.
- SQLite serializes writers. With the default zero busy timeout, contention raises `StoreBusyError` immediately; the caller decides when to retry. Storage failures do not guarantee clock advancement.
- Payload/state JSON is limited to 16 KiB, depth 16, 256 entries per container and 4,096 characters per value string. Serialization enforces the byte budget during traversal, including shared-subtree expansion. This representation is not a cross-language canonical JSON standard.
- Claims and listing pages are limited to 500 rows. Audit retention defaults to 500 rows per inserting connection, configurable up to 10,000. Broker/inbox histories and fault-plan maps have explicit capacities.
- Domain and outbox history remain on disk until the database owner manages retention; there is no automatic deletion, migration framework, daemon or durable consumer inbox. Offset pagination is not a snapshot under concurrent changes.
- Connections are thread-owned. Use separate connections per worker and coordinate callers on the same logical clock. Direct database modification is trusted administration, not a supported adversarial boundary.
- Audits contain identifiers, transitions and symbolic codes, not payloads or free-text reasons. Reasons are stored in the outbox for diagnosis; callers must not put secrets or personal data in identifiers, codes or reasons. Demo reports intentionally include synthetic payloads.

## Documentation

- [Contract / Contrato](docs/contract.md)
- [Design / Arquitetura](docs/design.md)
- [Contributing](CONTRIBUTING.md), [Security](SECURITY.md), [Changelog](CHANGELOG.md)

MIT license, copyright 2026 Lofrano Tecnologia. General contact: [official homepage](https://lofranotecnologia.com.br).
