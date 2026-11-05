# Development 0.1.0 contract / Contrato de desenvolvimento 0.1.0

## English

### Time, transactions and errors

All timed store calls require `now_ms`, an exact Python `int` in `0..9223372036854775807`; booleans, integer subclasses and floats are rejected. Use one logical millisecond origin per database, never mixed UTC and process-monotonic origins. There are no default clock reads or sleeps.

Every timed call acquires `BEGIN IMMEDIATE`, verifies `now_ms >= outbox_meta.clock_ms` and advances that persisted clock. Equal ticks are allowed. Operation effects run under a savepoint within the same write lock. A semantic rejection rolls back those effects but commits the observed clock before raising; worker fencing/expiry/state rejections also append bounded metadata-only audit entries. This avoids an unlocked rollback/reopen window. Failed expired acknowledgments therefore cannot be resurrected by an earlier tick on any connection.

Structural input validation and payload freezing happen before the transaction and do not observe time. Busy/locked databases, failed commits, I/O failures, missing/corrupt schema and externally installed transaction-rollback triggers do not guarantee clock advancement. They do not constitute successful worker transitions. Ordinary integrity failures such as `RAISE(ABORT)` roll back operation effects and retain the clock. No application callback is accepted or executed inside a transaction. The private transaction context is not an extension API.

Exceptions derive from `OutboxError` and expose stable `error_code` values:

| Exception | Code |
|---|---|
| `OutboxValidationError` | `validation_error` |
| `ClockRegressionError` | `clock_regression` |
| `DuplicateMessageError` | `duplicate_message_id` |
| `DomainVersionConflictError` | `domain_version_conflict` |
| `OutboxConstraintError` | `constraint_violation` |
| `MessageNotFoundError` | `message_not_found` |
| `StaleFencingTokenError` | `stale_fencing_token` |
| `LeaseExpiredError` | `lease_expired` |
| `InvalidStateTransitionError` | `invalid_state_transition` |
| `StoreBusyError` | `store_busy` |

SQLite primary BUSY/LOCKED result codes, including extended codes, map to `store_busy`; other database failures map to `constraint_violation`. Messages are explanatory, not stable identifiers. Raw SQLite diagnostics are not public messages. `close()` is idempotent; use after close raises `constraint_violation`. A connection is owned by its creating thread. Different workers use independent connections. Constructor schema initialization uses an explicit write transaction; `initialize_schema=False` requires an already initialized compatible database. There is no migration framework.

### Immutable data and limits

`DomainMutation(aggregate_type, aggregate_id, version, state)` and `OutboxDraft(message_id, event_type, payload, max_attempts=3)` are frozen draft containers. Their caller-owned nested values are copied into validated JSON before acquiring a transaction. Callers must not concurrently mutate an input during a call. Custom Python mappings may execute caller code during pre-transaction traversal; this API is not a sandbox for executable Python objects.

State and payload roots must be mappings. Supported values: mappings, lists/tuples, exact strings, booleans, null, finite floats and signed 64-bit integers. Keys are nonempty strings up to 128 characters. Limits: 16,384 serialized UTF-8 bytes, depth 16 (root depth 1; each child value adds 1), 256 items per container, 4,096 characters per value string. Cycles and unpaired Unicode surrogates are rejected. The byte budget is enforced incrementally, so shared subtree expansion cannot allocate an exponentially large serialized result. Output sorts keys, uses compact separators, preserves Unicode and rejects nonfinite numbers. Float formatting is Python JSON formatting, not a broad canonicalization standard.

Returned record JSON strings are immutable snapshots; `.state`/`.payload` views use mapping proxies and tuples. `to_dict()` returns a separate mutable representation. SQL triggers prevent updates to event identity, aggregate identity/version, type, payload, maximum attempts and creation time. Lifecycle fields change through store methods. Direct database administration can bypass protections and is outside this contract.

ASCII identifiers use `[A-Za-z0-9_.:/-]`: 1..64 characters for aggregate type/worker ID and 1..128 for aggregate ID/message ID/event type. Symbolic error codes use `[a-z0-9_.-]`, 1..64 characters; trailing newlines are invalid. Error reasons have at most 1,024 input characters, normalize whitespace to single spaces, contain 1..160 normalized characters and reject remaining control, format and surrogate characters. Reasons are stored in outbox rows, never copied into audit entries. Callers are responsible for keeping diagnostics free of sensitive data.

### Producer and worker API

- `commit_domain_and_outbox(domain, outbox, *, now_ms)` returns `(DomainRecord, OutboxRecord)`. The aggregate starts at version 1 and advances by exactly 1. It writes domain state first and inserts one pending event in the same local transaction. Duplicate IDs, insertion constraints and audit-insertion constraints roll back the entire domain/event mutation. Existing aggregate updates also roll back. The logical clock is separate from those operation effects.
- `claim_batch(*, worker_id, now_ms, lease_duration_ms, batch_size=10)` returns at most `batch_size` snapshots. Eligible: pending with `next_attempt_at_ms <= now_ms`, or leased with `lease_expires_at_ms <= now_ms`. Order: `(created_at_ms, message_id)` ascending using SQLite's default binary text ordering. Each claim gets the next globally increasing integer token and expiry `now_ms + lease_duration_ms`. Batch size is 1..500; duration is 1..31,536,000,000 ms. Token or timestamp overflow rejects the whole batch without partial allocation. Committed tokens are never reused.
- `mark_published(*, message_id, worker_id, fencing_token, now_ms)` commits success and increments the attempt counter once.
- `record_failure(*, message_id, worker_id, fencing_token, now_ms, error_code, error_reason, base_backoff_ms=1000, max_backoff_ms=60000)` increments the counter once. If the new count equals `max_attempts`, it transitions directly to `dead_letter`, sets diagnostic metadata and `dead_lettered_at_ms`, and schedules no retry. Otherwise it returns to pending with `next_attempt_at_ms = now_ms + min(max_backoff_ms, base_backoff_ms * 2**(new_count - 1))`. Both backoff parameters are integers in 0..31,536,000,000 and maximum must be at least base. Zero delay is permitted. Overflow leaves the original lease/counter untouched while retaining observed time.
- `renew_lease(*, message_id, worker_id, fencing_token, now_ms, lease_duration_ms)` sets expiry to `max(current_expiry, now_ms + lease_duration_ms)`. It never shortens a lease, increments a token or consumes an attempt.
- `check_lease(*, message_id, worker_id, fencing_token, now_ms)` persists observed time and returns a fresh authoritative snapshot without renewal or attempt consumption. The relay uses this before transport work; it does not make external publication atomic with SQLite.

All four worker operations validate, in order after structural validation: persisted clock; message existence; token equality; leased state; owner equality; strict `now_ms < lease_expires_at_ms`. A mismatched token or owner is `stale_fencing_token`. A current token on a terminal/pending record is `invalid_state_transition`. Exact expiry is `lease_expired`, even without takeover. Mutating worker SQL additionally predicates on state, token, owner and unexpired lease.

`attempt_count` counts **committed outcomes**: failures and the eventual success. Claims, renewals, preflight checks, crashes and uncertain post-send acknowledgments do not increment it. `max_attempts` is 1..100 and limits committed failed outcomes, not raw transmissions. Under repeated crashes there can be more deliveries than `max_attempts`.

### Lifecycle

| State | Owner/expiry | Next attempt | Terminal timestamp | Attempt count |
|---|---|---|---|---|
| pending | null | non-null | none | below maximum |
| leased | non-null | non-null | none | below maximum |
| published | null | null | published timestamp | 1..maximum |
| dead_letter | null | null | dead-letter timestamp | equals maximum |

Pending rows start with token/count 0 and `next_attempt_at_ms = created_at_ms`. Terminal rows retain their last token. Successful publication retains any previous failure diagnostic as historical metadata. There is no terminal reset, automatic requeue or deletion API.

### Reads, retention and fixtures

`get_domain_record(type, id)` and `get_outbox_record(id)` return a snapshot or `None`. Clock/token getters require valid store metadata. `list_domain_records(*, limit=500, offset=0)` orders by aggregate type/ID; `list_outbox_records(*, state=None, limit=500, offset=0)` orders by creation time/ID and validates its optional enum/string filter. Limits are 1..500 and offsets 0..INT64_MAX. Each call is a separate read; pagination is not a concurrent snapshot. `list_audit_log()` returns at most 10,000 retained entries in sequence order.

`max_audit_rows` is 1..10,000, default 500. Each inserting connection prunes the shared audit table to its configured limit, in the same transaction. Connections with different limits can retain different tail sizes; use the same setting for a uniform policy. Reopening with a smaller limit does not prune until insertion. Audit records contain transition metadata and codes, not payloads, reasons or tracebacks. Domain/event storage itself is not automatically pruned.

Offline fixtures are single-threaded and memory-only. `RecordingBroker` and `DeduplicatingConsumerInbox` each accept `max_deliveries=1000` in 1..10,000; capacity exhaustion rejects without dropping historical records. The inbox counts accepted raw deliveries and applies one immutable event per message ID. Reuse with differing aggregate/type/payload raises `constraint_violation`, without changing either fixture's counts. A duplicate returns `False`; a first application returns `True`. Inbox time cannot regress for the same message. This is a reference projection, not a durable transactional consumer.

`FaultInjectionPlan(max_entries=1000)` separately bounds its rules and message dispatch-counter maps to 1..10,000 entries. `configure(message_id, ordinal, FaultAction, ...)` uses exact positive integer ordinals up to INT64_MAX and validates action/diagnostics. Unconfigured dispatches use `NONE`. `FAIL_BEFORE_SEND` records failure without sending. `DROP_ACK_AFTER_SEND` sends but deliberately omits local acknowledgment. `RelayWorker.deliver_claimed(record, *, now_ms)` checks and refreshes the store snapshot before consuming a fault rule or sending. Validation errors do not send. Transport fixture errors propagate and leave the lease for caller handling or expiry. Preflight cannot prevent a concurrent takeover after it commits; external sends can still duplicate.

### CLI

`python3 -m outbox_lab demo [--format json|text]` runs the fixed temporary-database scenario. Default JSON includes the complete synthetic trace and exact duplicate checks. Exit codes: 0 success/help, 2 argument error, 1 handled store/I/O/invariant failure. Operational stderr is a symbolic error and generic message; no exception contents or payloads are printed there. There is no arbitrary database-path CLI input.

## Português

### Tempo e atomicidade

O contrato de desenvolvimento 0.1.0 usa um único domínio lógico de milissegundos por banco, fornecido explicitamente em `now_ms`, inteiro Python exato entre 0 e INT64_MAX. Não misture UTC com origens monotônicas. Ticks iguais são aceitos; retrocessos geram `clock_regression` entre conexões e reaberturas.

Cada operação temporal usa `BEGIN IMMEDIATE`. O relógio avança antes de um savepoint que delimita os efeitos da operação. Uma rejeição semântica desfaz esses efeitos e confirma o relógio ainda sob o mesmo lock. Uma confirmação rejeitada por expiração nunca pode ser recuperada com tempo anterior. Entradas estruturalmente inválidas são rejeitadas antes da transação. Contenção, falha de commit/I/O, corrupção de esquema e triggers administrativos que desfazem a transação inteira não garantem avanço. Restrições comuns com `RAISE(ABORT)` preservam o relógio. Não há callbacks de aplicação em transações.

`commit_domain_and_outbox` grava domínio e evento na mesma transação; colisão de ID, restrição no evento ou na auditoria desfaz ambas as escritas, inclusive atualização de agregado existente. Versões começam em 1 e avançam exatamente uma unidade. `close()` é idempotente. Conexões pertencem à thread criadora; workers usam conexões independentes. Não há framework de migração.

### Workers e tentativas

`claim_batch` seleciona pending pronto ou leased expirado, ordenando por criação/ID. Cada registro recebe token global crescente e expiração `now_ms + duração`. Lotes têm 1..500 itens; duração máxima é 31.536.000.000 ms. Overflow rejeita todo o lote, sem tokens parcialmente alocados.

`mark_published`, `record_failure`, `renew_lease` e `check_lease` validam: relógio, existência, token, estado leased, proprietário e `now_ms < expiração`. Token/proprietário incorreto gera `stale_fencing_token`; estado incompatível com token atual gera `invalid_state_transition`; igualdade na expiração já gera `lease_expired`, mesmo sem retomada. Atualizações SQL repetem os predicados de fencing.

`attempt_count` conta somente resultados confirmados: falhas e eventual sucesso. Claims, renovações, pré-verificações, quedas e confirmações perdidas não consomem tentativas. `max_attempts`, entre 1 e 100, não limita entregas brutas. Em `record_failure`, a falha número máximo termina imediatamente em dead letter, sem próxima tentativa. Antes disso, o atraso é `min(max_backoff_ms, base_backoff_ms * 2**(contagem - 1))`; atraso zero é permitido. Overflow mantém o lease/contador anteriores e preserva o tempo observado. `renew_lease` nunca encurta o lease nem troca token. `check_lease` persiste tempo e retorna snapshot atualizado, sem renovar.

Pending/leased têm próxima tentativa não nula e contagem abaixo do máximo. Published/dead_letter não têm lease nem próxima tentativa; possuem apenas o respectivo timestamp terminal. Published tem contagem entre 1 e máximo; dead_letter tem contagem igual ao máximo. Um sucesso mantém o diagnóstico histórico da última falha. Não há reset ou requeue automático.

### Dados, limites e erros

Payload/estado são mappings com valores JSON, listas/tuplas, floats finitos e inteiros signed64. São congelados antes da transação: 16.384 bytes UTF-8, profundidade 16 contando a raiz como 1, até 256 itens por contêiner, chaves não vazias até 128 caracteres e strings de valor até 4.096. Ciclos e surrogates isolados são rejeitados. O orçamento de bytes é aplicado durante a travessia. Views retornadas são profundamente imutáveis; `to_dict()` produz cópia separada. O formato ordena chaves e usa JSON compacto do Python, sem alegar padronização canônica entre linguagens. Triggers bloqueiam alterações nos campos imutáveis de eventos; administração direta do banco é confiável e fica fora desta fronteira.

Identificadores ASCII usam letras, dígitos e `_.:/-`, até 64 caracteres para tipo de agregado/worker e 128 para os demais. Códigos usam minúsculas, dígitos e `_.-`, até 64. Razões aceitam até 1.024 caracteres de entrada, normalizam espaços e ficam entre 1 e 160 caracteres, sem controles/formatação/surrogates restantes. Não coloque dados sensíveis nesses campos. Razões ficam na outbox, não na auditoria.

As classes/códigos da tabela em inglês são estáveis. Mensagens não são identificadores estáveis. SQLite BUSY/LOCKED gera `store_busy`; demais falhas de banco geram `constraint_violation`, sem diagnóstico bruto na mensagem pública.

Listagens de domínio/outbox usam `limit=500, offset=0`, limite máximo 500 e ordenação determinística; filtros de estado são validados. Paginação não é snapshot concorrente. Auditoria retorna no máximo 10.000 entradas. Cada inserção limita a tabela compartilhada a `max_audit_rows` da conexão: padrão 500, faixa 1..10.000. Use o mesmo valor entre conexões. A auditoria contém metadados/códigos, nunca payload, razão livre ou traceback. O histórico de domínio/outbox em disco não é removido automaticamente.

Fixtures são single-thread, em memória. Broker e inbox limitam entregas aceitas por `max_deliveries`, padrão 1.000, máximo 10.000, sem descartar histórico silenciosamente. A inbox aplica um evento por ID e conta entregas brutas; conteúdo diferente sob o mesmo ID gera `constraint_violation` sem alterar contadores. Duplicata retorna `False`; primeira aplicação retorna `True`. O tempo de um mesmo ID não pode retroceder.

O plano de falhas limita separadamente regras e contadores por `max_entries`, padrão 1.000, máximo 10.000. Ordinais são inteiros exatos positivos até INT64_MAX. `FAIL_BEFORE_SEND` registra falha sem envio; `DROP_ACK_AFTER_SEND` envia sem confirmação local. O relay valida o lease e busca conteúdo autoritativo antes do envio. Uma retomada ainda pode ocorrer após essa pré-verificação: ela não constitui fencing externo. Falhas de fixture propagam e mantêm o lease para tratamento ou expiração. A inbox não é um consumidor transacional durável.

`python3 -m outbox_lab demo [--format json|text]` executa o cenário sintético temporário. JSON é o padrão. Códigos de saída: 0 sucesso/ajuda, 2 argumentos inválidos, 1 falha tratada de store/I/O/invariante. Diagnósticos operacionais em stderr não incluem payloads nem conteúdo de exceções.


CLI writes and flushes are checked before success. Output or diagnostic failures remain operational exit 1; diagnostics are best effort if stderr is unavailable. A failed write can leave partial output.

A CLI verifica escrita/flush antes do sucesso. Falhas de saída/diagnóstico mantêm saída operacional 1; diagnósticos são uma tentativa se stderr estiver indisponível. Escrita interrompida pode deixar saída parcial.
