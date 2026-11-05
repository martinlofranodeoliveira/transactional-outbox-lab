# transactional-outbox-lab

**Versão de desenvolvimento 0.1.0** · [Lofrano Tecnologia](https://lofranotecnologia.com.br) · [Repositório](https://github.com/martinlofranodeoliveira/transactional-outbox-lab) · [English](README.md)

Biblioteca e laboratório offline de **Transactional Outbox** em Python 3.13 com SQLite 3.51.3+ realmente vinculado e somente biblioteca padrão. Demonstra escrita atômica no produtor, retentativas determinísticas, retomada de leases, fencing e entrega duplicada após perda da confirmação pós-envio. Não depende de serviços, modelos ou APIs em execução e não utiliza telemetria nem rede.

## Instalação, testes e demonstração

Na raiz do repositório, execute sem instalar o pacote:

```bash
python3 -m unittest discover -s tests -v
python3 -m outbox_lab demo
python3 -m outbox_lab demo --format text
```

Instalação opcional em ambiente virtual:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install .
.venv/bin/python -m outbox_lab demo
```

A instalação usa a dependência de construção setuptools declarada em `pyproject.toml`; obter essa ferramenta pode exigir rede. Os testes e a demo funcionam offline diretamente nos fontes. O comando instalado é `outbox-lab demo`.

A demo usa um arquivo SQLite temporário, identificadores sintéticos e ticks fixos. A saída padrão é um relatório JSON determinístico completo com estados, passos do relay, entregas brutas, inbox deduplicadora, auditoria limitada e invariantes verificadas. A saída legível começa com:

```text
Transactional Outbox Lab v0.1.0 - Multi-Worker Simulation
========================================================================
Final Logical Clock (ms): 2300
Highest Fencing Token:    6
All Invariants Passed:    True
```

A mensagem de invoice tem duas entregas idênticas e um registro deduplicado. A mensagem de order é publicada após uma falha transitória; a de webhook termina em dead letter na segunda falha confirmada localmente.

## Exemplo pequeno

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from outbox_lab import DomainMutation, OutboxDraft, OutboxStore

with TemporaryDirectory() as directory:
    with OutboxStore(Path(directory) / "exemplo.sqlite3") as store:
        store.commit_domain_and_outbox(
            DomainMutation("task", "task-1", 1, {"status": "queued"}),
            OutboxDraft("message-1", "task.queued", {"task_id": "task-1"}),
            now_ms=100,
        )
        claim = store.claim_batch(
            worker_id="worker-a", now_ms=100,
            lease_duration_ms=50, batch_size=1,
        )[0]
        # Em uma integração, o envio ocorre fora da transação SQLite.
        result = store.mark_published(
            message_id=claim.message_id, worker_id="worker-a",
            fencing_token=claim.fencing_token, now_ms=110,
        )
        print(result.state.value)  # published
```

Use um único domínio lógico de milissegundos inteiros explícitos por banco. Timestamps iguais são permitidos. Não misture época UTC com origens monotônicas de processos. O relógio persistido não pode retroceder entre conexões ou reaberturas; rejeições semânticas também preservam o tempo observado. Uma confirmação expirada não pode ser repetida com um tick anterior. O lease já está expirado quando `now_ms == lease_expires_at_ms`.

`attempt_count` conta falhas confirmadas no banco e um eventual sucesso confirmado, **não claims ou envios incertos**. Uma queda ou perda de confirmação não consome tentativa; a recuperação pode reenviar repetidamente. `max_attempts` não limita o número de entregas brutas. Consulte o [contrato preciso](docs/contract.md).

## Justificativa do projeto

O domínio e um evento imutável são gravados juntos com `BEGIN IMMEDIATE`, eliminando a janela de escrita dupla local. Nenhuma chamada de transporte acontece em transação. Tokens e leases válidos protegem todas as mudanças de worker no banco. A pré-verificação curta `check_lease` atualiza o snapshot do relay e persiste o tempo do envio, mas não pode aplicar fencing atômico a um broker externo.

Se o worker enviar e cair antes do commit de `mark_published`, a recuperação precisa reenviar. O consumidor em memória demonstra deduplicação por `message_id` e rejeita reutilização do identificador com conteúdo diferente. Isso não é entrega exatamente uma vez.

## Limites e tratamento de falhas

- Laboratório arquitetural delimitado, **não um broker de produção, serviço distribuído de relay ou daemon CDC**. Entrega eventual depende de polling contínuo e transporte funcional. Dead letters não são entregues automaticamente.
- O SQLite serializa escritores. Com timeout padrão zero, contenção gera `StoreBusyError` imediatamente; o chamador decide quando repetir. Falhas de armazenamento não garantem avanço do relógio.
- JSON de payload/estado: até 16 KiB, profundidade 16, 256 itens por contêiner e 4.096 caracteres por string de valor. O limite de bytes é aplicado durante a travessia, inclusive na expansão de subárvores compartilhadas. Não se trata de um padrão de JSON canônico entre linguagens.
- Claims e páginas de listagem têm até 500 linhas. Cada conexão limita a auditoria ao inserir: padrão 500, configurável até 10.000. Históricos do broker/inbox e mapas de falhas também têm capacidades explícitas.
- O histórico de domínio e outbox permanece em disco até a administração definir retenção. Não há exclusão automática, migrações, daemon ou inbox durável. Paginação por offset não é um snapshot sob alterações concorrentes.
- Cada conexão pertence à sua thread. Use conexões independentes por worker e coordene o mesmo relógio lógico. Modificação direta do banco é administração confiável, não uma fronteira adversarial suportada.
- Auditoria contém identificadores, transições e códigos simbólicos, não payloads ou razões livres. Razões ficam na outbox para diagnóstico; não coloque segredos ou dados pessoais em identificadores, códigos ou razões. O relatório da demo inclui intencionalmente os payloads sintéticos.

## Documentação

- [Contrato / Contract](docs/contract.md)
- [Arquitetura / Design](docs/design.md)
- [Contribuição](CONTRIBUTING.md), [Segurança](SECURITY.md), [Histórico](CHANGELOG.md)

Licença MIT, copyright 2026 Lofrano Tecnologia. Contato geral: [site oficial](https://lofranotecnologia.com.br).

## Runtime SQLite

Use a [compilação com checksum e qualificação por SQL](docs/sqlite-runtime.md). A aplicação exige SQLite 3.51.3+ realmente vinculado; a imagem fornecida usa 3.54.1. Compile com `docker build -t transactional-outbox-lab-sqlite .` e execute os comandos documentados de teste/demo com `docker run --rm --network none transactional-outbox-lab-sqlite <argv>`. Instalações nativas devem fornecer SQLite na faixa suportada.
