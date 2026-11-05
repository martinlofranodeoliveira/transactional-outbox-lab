# Contributing / Guia de Contribuição

## English

Thank you for your interest in contributing to `transactional-outbox-lab` by [Lofrano Tecnologia](https://lofranotecnologia.com.br).

### Development Workflow

1. Ensure Python `3.13` is installed.
2. Keep the runtime dependency-free (Python standard library only).
3. Do not introduce `time.sleep`, wall-clock defaults, or nondeterministic behavior into the store, relay harness, or tests.
4. Run the full test suite and deterministic demo before opening a pull request:

```bash
python3 -m unittest discover -s tests -v
python3 -m outbox_lab demo
python3 -m outbox_lab demo --format text
```

---

## Português

Obrigado pelo interesse em contribuir com o `transactional-outbox-lab` da [Lofrano Tecnologia](https://lofranotecnologia.com.br).

### Fluxo de Desenvolvimento

1. Utilize Python `3.13`.
2. Mantenha o projeto sem dependências externas de execução (apenas biblioteca padrão do Python).
3. Não introduza `time.sleep`, relógio de parede implícito ou comportamento não-determinístico no store, no relay ou nos testes.
4. Execute a suíte completa de testes e a demonstração determinística antes de abrir um pull request:

```bash
python3 -m unittest discover -s tests -v
python3 -m outbox_lab demo
python3 -m outbox_lab demo --format text
```
