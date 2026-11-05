# Changelog / Histórico

## Unreleased — development 0.1.0 / desenvolvimento 0.1.0

### English

- Atomic SQLite domain/outbox writes with explicit `BEGIN IMMEDIATE`, optimistic aggregate versions and immutable event fields.
- Persisted caller-supplied logical millisecond clock, deterministic batch leases, global fencing tokens and strict expiry checks for all worker operations.
- Savepoint-based semantic rollback retains observed time under the same write lock, including failed expired acknowledgments and insertion constraints.
- Deterministic exponential retries and exact terminal dead-letter exhaustion; committed-outcome attempt semantics documented separately from raw deliveries.
- Idempotent connection closure, structured SQLite errors, timestamp/token exhaustion checks and non-shortening lease renewal.
- Incrementally bounded JSON serialization, Unicode and full-string identifier validation, immutable returned views, bounded listing pages and metadata-only audit retention.
- Relay preflight refresh, bounded fault/transport/inbox fixtures and rejection of message-ID reuse with different contents.
- Offline JSON/text multi-worker demo showing transient failure, lost acknowledgment, takeover, duplicate delivery and downstream deduplication.
- Regression tests for atomic rollback, clock/lease boundaries, independent-connection/process contention, forged snapshots, capacities and diagnostic redaction.

### Português

- Escrita atômica SQLite de domínio/outbox com `BEGIN IMMEDIATE`, versões otimistas e campos imutáveis de evento.
- Relógio lógico explícito persistido, leases determinísticos, tokens globais e expiração estrita em todas as operações de worker.
- Rollback semântico com savepoint preserva o tempo sob o mesmo lock, inclusive após confirmação expirada e restrições de inserção.
- Backoff exponencial determinístico e dead letter na tentativa exata; resultados confirmados são distintos de entregas brutas.
- Fechamento idempotente, erros SQLite estruturados, verificação de overflow e renovação sem encurtamento.
- JSON limitado durante a serialização, validação Unicode/identificadores completos, views imutáveis, paginação limitada e retenção de auditoria somente com metadados.
- Pré-verificação do relay, fixtures limitadas e rejeição de conteúdo diferente sob o mesmo ID.
- Demo offline JSON/texto com falha transitória, perda de confirmação, retomada, duplicata e deduplicação.
- Regressões de atomicidade, relógio/leases, contenção entre conexões/processos, snapshots adulterados, capacidades e diagnósticos sem conteúdo de exceções.

- Check CLI output/diagnostic flush and keep failed OS streams from being retried at interpreter shutdown.
- Verifica flush da saída/diagnósticos e evita nova tentativa em streams já falhos no encerramento.

* Qualify actual linked SQLite 3.51.3+ and provide a checksum-pinned SQLite 3.54.1 runtime build; preserve journal and transaction semantics.
