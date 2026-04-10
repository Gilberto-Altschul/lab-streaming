# Lab Streaming

Projeto de streaming com Python, Kafka e MongoDB, integrado com Databricks para processamento e análise.

## Estrutura recomendada

- `src/` — código principal da aplicação
  - `src/client/` — componentes cliente e scripts de produção de eventos
  - `src/server/` — conectores e utilitários de backend (Kafka/MongoDB)
  - `src/common/` — módulos compartilhados e modelos
- `tests/` — testes unitários e de integração
- `Notebooks/databricks/` — notebooks Databricks para ETL e análise
- `.vscode/` — configurações locais do VS Code
- `databricks.yml` — configuração do Databricks bundle
- `.env` — variáveis de ambiente locais (não commitadas)

## Dependências

Instale o ambiente virtual e as dependências:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Variáveis de ambiente

Crie um arquivo `.env` com as variáveis necessárias:

```text
KAFKA_BOOTSTRAP=
KAFKA_API_KEY=
KAFKA_API_SECRET=
KAFKA_TOPIC=orders.events
MONGO_URI=
```

## Como rodar

- Enviar eventos Kafka:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python producer_orders.py
  ```

- Testar conexão Kafka:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python test-kafka.py
  ```

- Testar conexão MongoDB:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python test_mongo.py
  ```

## Integração com Databricks

O arquivo `databricks.yml` já configura o bundle para o workspace de desenvolvimento. Use a extensão Databricks do VS Code para deploy e execução em notebooks.

### Boas práticas

- Separe código de ingestão (`producer`, `client/server`) do código de processamento e notebooks.
- Use `src/` para módulos reutilizáveis e `tests/` para validação automática.
- Commite apenas código, não credenciais ou arquivos de ambiente.
- Use branches de feature e Pull Requests para revisão de código.

## Próximos passos sugeridos

1. Mover scripts existentes para `src/` e criar módulos claros `client/`, `server/` e `common/`.
2. Implementar testes com `pytest`.
3. Documentar no `README.md` como rodar local e como conectar ao Databricks.
4. Configurar GitHub Actions para testes automáticos.
