# Lab Streaming

Projeto de streaming com Python, Kafka e MongoDB, integrado com Databricks para processamento e análise.

## Estrutura recomendada

- `src/` — código principal da aplicação
  - `src/client/` — componentes cliente e scripts de produção de eventos
  - `src/server/` — conectores e utilitários de backend (Kafka/MongoDB)
  - `src/common/` — módulos compartilhados e modelos
- `tests/` — testes unitários e de integração
- `notebooks/databricks/` — notebooks Databricks para ETL e análise
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

Para a opção 1, crie um arquivo local `.env` copiando o modelo:

```powershell
copy .env.example .env
```

Preencha as credenciais:

```text
KAFKA_BOOTSTRAP=
KAFKA_API_KEY=
KAFKA_API_SECRET=
KAFKA_TOPIC=orders.events
MONGO_URI=
```

No VS Code, o arquivo `.env` é carregado automaticamente pelo ambiente Python configurado em `.vscode/settings.json`.

> Importante: não commit o arquivo `.env` no Git. Ele já está listado em `.gitignore`.

## Como rodar

- Enviar eventos Kafka:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python producer_orders.py
  ```

- Rodar consumidor Kafka → MongoDB:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  python -m src.server.consumer
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

### Conectar Databricks no VS Code

1. Abra o painel de extensões (`Ctrl+Shift+X`) e instale as recomendações do projeto.
2. Abra o Command Palette (`Ctrl+Shift+P`).
3. Execute `Databricks: Configure Workspace`.
4. Informe o `Host` do seu workspace e o token pessoal (PAT).
5. Use o comando `Databricks: Deploy current file to Databricks` para enviar notebooks ou arquivos Python.

No modo local, notebooks também podem carregar as variáveis do `.env` se você instalar e usar `python-dotenv`.

### Fluxo recomendado

- Edite localmente em `src/` e versionamento no Git.
- Faça `git add`, `git commit` e `git push origin main` normalmente.
- Mantenha os notebooks em `notebooks/databricks/` sincronizados com o workspace Databricks.
- Prefira notebooks em formato `.py` com `# COMMAND ----------` para facilitar diffs e versionamento.
- Use `notebooks/databricks/streaming_example.py` como exemplo de notebook versionado.

### Versionando notebooks com Git

- Garanta que os notebooks estejam sob controle de versão, não apenas no workspace Databricks.
- Faça commit das alterações do notebook local antes de fazer deploy no Databricks.
- Se editar diretamente no Databricks, sempre exporte ou sincronize o notebook de volta ao arquivo local.
- O arquivo `.gitattributes` no projeto já adiciona suporte básico para `.ipynb` e garante que `Notebooks/databricks/` seja tratado como texto.
- Não armazene credenciais diretamente no notebook. Use variáveis de ambiente locais ou `dbutils.secrets` no Databricks.

### Notebook existente

O notebook `notebooks/notebook-kafka.ipynb` já pode ser usado. Ele foi atualizado para carregar credenciais de:

- variáveis de ambiente (`KAFKA_BOOTSTRAP`, `KAFKA_API_KEY`, `KAFKA_API_SECRET`)
- ou `dbutils.secrets` no Databricks

Além disso, há um notebook Databricks em `notebooks/databricks/kafka_to_delta.py` que consome Kafka e grava os eventos em Delta Lake.

### Como executar módulos Python

No terminal do projeto, use:

```powershell
python -m src.client.producer
python -m src.server.kafka_health
python -m src.server.mongo_health
```

### Executar localmente com Spark + Delta

Se quiser rodar o pipeline Kafka → Delta localmente, use o script:

```powershell
.\.venv\Scripts\Activate.ps1
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,io.delta:delta-spark_2.12:3.0.0 src/server/kafka_delta_local.py
```

#### Instalação Windows para Spark + Delta

No Windows, o Delta pode precisar de binários nativos do Hadoop (como `winutils.exe` e `hadoop.dll`) para acessar diretórios locais.

1. Instale uma build compatível de Hadoop 3.x para Windows.
2. Coloque os binários em `C:\hadoop\bin` ou em outro diretório de sua escolha.
3. Defina as variáveis de ambiente:

```powershell
setx HADOOP_HOME "C:\hadoop"
setx PATH "$env:PATH;C:\hadoop\bin"
```

4. Verifique em um novo terminal:

```powershell
echo $env:HADOOP_HOME
where.exe winutils.exe
```

5. Execute o `spark-submit` novamente.

Se preferir, use o helper PowerShell em `scripts\setup-hadoop-windows.ps1` para validar a instalação.

Antes de executar, defina as credenciais no `.env` ou no ambiente:

```text
KAFKA_BOOTSTRAP=
KAFKA_API_KEY=
KAFKA_API_SECRET=
KAFKA_TOPIC=orders.events
DELTA_PATH=file:///tmp/kafka_orders_delta
DELTA_CHECKPOINT=file:///tmp/kafka_orders_delta_checkpoint
```

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
