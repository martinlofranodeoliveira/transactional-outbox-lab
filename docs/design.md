# Design and failure model / Arquitetura e modelo de falhas

## English

### Producer boundary

This lab solves a local producer coordination problem, not a distributed transaction. The aggregate's current state and immutable event envelope share one SQLite file and one explicit `BEGIN IMMEDIATE` transaction. A failed event or audit insertion cannot leave a domain-only update. Optimistic aggregate versions prevent silent lost updates. Event records preserve aggregate version even as current domain state advances.

The connection is thread-owned; independent workers open independent connections. SQLite serializes writers. Default timeout zero exposes contention instead of retrying or sleeping invisibly. Application callers coordinate the single logical clock and retry busy operations explicitly.

### Clock and rollback structure

After taking the write lock, an operation verifies and advances the persisted clock, then creates a savepoint for its effects. Semantic rejection rolls back the savepoint, optionally records a fenced rejection, and commits the clock while still holding that lock. This is deliberately not a second best-effort clock transaction: another connection cannot intervene between effect rollback and clock persistence. Failed expired acknowledgment at tick 11 followed by attempted acknowledgment at tick 9 is rejected across connections and reopenings.

Storage/commit failures cannot promise a durable clock observation. Structural validation is performed before lock acquisition. Serialization and caller mapping traversal happen there too; application code must not mutate inputs concurrently. No network, arbitrary callback, sleep or clock default occurs in a transaction.

### At-least-once, not exactly-once

The deterministic demo stages three events. Worker alpha leases all three at tick 1100, with expiry 1600 and tokens 1, 2 and 3. The order fails before send and schedules retry 2150. The invoice reaches the broker at 1200, but the injected fault omits `mark_published`. The webhook fails before send and schedules retry 2250.

Worker beta reclaims the invoice at exactly 1600 with token 4. Alpha's delayed acknowledgment at 1650 is fenced. Beta sends the identical event at 1700 and commits success. The reference inbox reports two raw deliveries and one applied event. The order later publishes with token 5; the webhook's second committed failure dead-letters with token 6. Final logical time is 2300.

A relay preflight (`check_lease`) refreshes authoritative contents and rejects already stale or expired work before sending. It also persists the tick on the lost-ack path. It cannot prevent takeover after that transaction ends or retract a transport frame. All local worker mutations require current token, owner, leased state and an unexpired lease. An external transport still needs its own semantics; downstream effects must be idempotent.

Attempts count committed outcomes, not raw sends. Counting claims instead could exhaust a message solely through crashes before sending. This design deliberately permits repeated uncertain sends; it does not provide a raw-delivery ceiling or unconditional eventual delivery. Dead letters are terminal diagnostic records, not successful delivery.

### Bounded resources and trust

Incremental JSON serialization has a 16 KiB budget plus depth, item and string limits; it stops early even for heavily shared input graphs. Returned views cannot mutate stored payloads. Database triggers guard immutable event columns. Query pages and claim batches are bounded. Audits retain a bounded tail; fixture histories and fault maps fail explicitly at capacity rather than growing indefinitely or silently losing evidence.

The SQLite file can grow with domain and event history. The owner must manage disk capacity and retention. There is no cleanup daemon, schema migration, CDC stream, real broker adapter or durable inbox. Offset pages can shift under concurrent changes. Fixture objects are single-threaded. Direct SQL administration and executable custom Python mappings are trusted code, not sandboxed inputs.

Audit rows contain only identifiers, transitions and symbolic codes. Free-text reasons remain in outbox diagnostic fields; they are normalized, not secret-redacted. Callers must use nonsensitive diagnostic content. CLI failure messages do not include exception text. Demo reports intentionally expose the synthetic example data.

### Regression approach

Tests retain the original atomicity, expired-ack clock, retry, demo and contention scenarios. Additional adversarial cases cover rollback of existing aggregate updates, failure during audit insertion, exact expiry for every worker operation, process takeover fencing, token/timestamp exhaustion, non-shortening renewal, Unicode/regex boundaries, incremental shared-graph limits, forged relay snapshots, fixture identity collisions and capacity failures. Process contention uses synchronization events and timeout-zero SQLite connections, never sleeps. Test expectations include fixed observable outcomes rather than only recomputing results using implementation helpers.

### Primary references

- Python 3.13 sqlite3: <https://docs.python.org/3.13/library/sqlite3.html>
- SQLite transaction language: <https://www.sqlite.org/lang_transaction.html>
- Python unittest: <https://docs.python.org/3.13/library/unittest.html>

## Português

### Fronteira e transações

O laboratório coordena o produtor local, não uma transação distribuída. Estado atual do agregado e evento imutável compartilham arquivo SQLite e transação explícita `BEGIN IMMEDIATE`. Falha na inserção de evento ou auditoria não pode deixar atualização isolada de domínio. Versões otimistas evitam sobrescritas silenciosas. Cada conexão pertence à sua thread; workers independentes abrem conexões próprias. SQLite serializa escritores e timeout padrão zero expõe contenção sem repetição ou espera implícita.

Sob o lock, a operação verifica/avança o relógio persistido e cria um savepoint para seus efeitos. Rejeições semânticas desfazem esses efeitos e confirmam o relógio sem liberar o lock entre as etapas. Assim, confirmação expirada em 11 não pode ser repetida em 9, mesmo após reabertura. Falhas de armazenamento/commit não garantem persistência do tempo. Validação e serialização ocorrem antes da transação, sem callbacks arbitrários, rede, sleeps ou relógio implícito dentro dela.

### Duplicatas e tentativas

Na demo, alpha recebe tokens 1, 2 e 3 em 1100, com expiração 1600. Order falha e agenda 2150; invoice chega ao broker em 1200 e perde a confirmação; webhook falha e agenda 2250. Beta retoma invoice exatamente em 1600 com token 4. A confirmação atrasada de alpha é rejeitada; beta reenvia em 1700. A inbox registra duas entregas brutas e uma aplicação. Order publica com token 5; webhook termina em dead letter com token 6. O relógio final é 2300.

`check_lease` evita envio de trabalho já obsoleto e busca o conteúdo autoritativo, persistindo o tick também na perda de ack. Não impede retomada após a pré-verificação nem desfaz envio externo. Todas as mutações locais exigem token, proprietário, estado leased e lease não expirado. Consumidores precisam de efeitos idempotentes.

Tentativas contam resultados confirmados, não envios brutos. Claims consumindo tentativas poderiam esgotar uma mensagem antes de qualquer envio. A escolha permite reenvios incertos repetidos; não estabelece limite de entregas brutas nem entrega eventual incondicional. Dead letter é diagnóstico terminal, não entrega concluída.

### Limites, confiança e testes

Serialização incremental limita bytes, profundidade, itens e strings, inclusive para subárvores compartilhadas. Views são imutáveis e triggers protegem colunas de eventos. Claims, páginas, auditoria, históricos de fixtures e mapas de falhas possuem limites explícitos. Entretanto, o arquivo de domínio/outbox cresce: retenção e capacidade de disco são responsabilidade da administração. Não há migrações, daemon, CDC, adaptador de broker real ou inbox durável. Paginação pode mudar sob concorrência. Fixtures são single-thread. SQL administrativo e mappings Python executáveis são código confiável, não entradas isoladas em sandbox.

Auditoria usa metadados/códigos, nunca razões livres ou payloads. Razões da outbox são normalizadas, não redigidas como segredos; use conteúdo não sensível. Falhas da CLI não imprimem texto de exceções. A demo mostra os dados sintéticos intencionalmente.

Os testes preservam atomicidade, regressão após confirmação expirada, retries, demo e contenção. Casos adversariais acrescentam atualização de agregado desfeita, falha de auditoria, expiração de todas as operações, fencing após processo independente, overflow, renovação sem encurtamento, Unicode/regex, grafos compartilhados, snapshots adulterados e limites/conflitos de fixtures. Contenção usa eventos de sincronização e conexões SQLite com timeout zero, sem sleeps. As referências primárias estão listadas acima.
