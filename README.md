# 🚀 IH Ingestion Generator

Ferramenta em Go para gerar automaticamente:

- Kafka Connect **source connectors** (Debezium SQL Server)
- Kafka Connect **sink connectors** (Snowflake)
- **Jobs** de criação de tabelas no Snowflake (tabela `_INGEST`, tabela final e STAGE)

A partir de:

- Metadados reais das tabelas no SQL Server (INFORMATION_SCHEMA)
- Um arquivo declarativo `ingestion.yaml`
- Variáveis de ambiente (`.env` em dev, Secrets em prod)

## 🧭 Diagrama de arquitetura

```mermaid
flowchart TD
    SQL[SQL Server
    Tabelas e metadados] -->|INFORMATION_SCHEMA| CLI[Ingestion CLI]
    INGESTION[Arquivo declarativo
    ingestion.yaml] --> CLI
    ENV[Variáveis de ambiente
    (.env ou secrets)] --> CLI
    CLI -->|Geração de artefatos| GEN[Conectores e jobs]
    GEN --> SRC[Kafka Connect
    Debezium SQL Server (source)]
    SRC --> KAFKA[(Kafka tópicos)]
    KAFKA --> SNK[Kafka Connect
    Snowflake (sink)]
    SNK --> STAGE[Snowflake
    Tabela STAGE]
    STAGE --> INGEST[_INGEST]
    INGEST --> FINAL[Tabela final]
```

---

## 🔧 Como rodar em modo single (tabela única)

```bash
cp .env.example .env   # ajustar valores

go run ./cmd/ingestion-cli \
  -schema dbo \
  -table Clientes \
  -out ./out

```

## 🗒️ Checklist de configuração

- **SQL Server:** usuário com permissão de leitura em `INFORMATION_SCHEMA` para coletar metadados das tabelas.
- **Kafka Connect Debezium:** cluster acessível a partir do ambiente onde o CLI será executado; `BOOTSTRAP_SERVERS` e `CONNECT_URL` definidos.
- **Snowflake:** warehouse, database e schema já criados; role com permissão de criação de tabelas e stage.
- **Arquivos de ambiente:** copie o `.env.example` e preencha variáveis obrigatórias (conexão SQL Server, Kafka, Snowflake, tópico). Em produção, use secrets/variáveis do runner em vez de `.env`.
- **Go 1.21+** instalado para rodar o CLI localmente.

## 🧾 Exemplo mínimo de `ingestion.yaml`

```yaml
# Define um pipeline simples para uma tabela
pipeline:
  name: clientes_pipeline
  source:
    schema: dbo
    table: Clientes
  sink:
    snowflake:
      database: ANALYTICS
      schema: LANDING
      table: CLIENTES

# Opcional: múltiplas tabelas podem ser listadas
additional_tables:
  - schema: dbo
    table: Pedidos
  - schema: dbo
    table: Produtos
```

## 🧰 Rodando em lote (múltiplas tabelas)

```bash
go run ./cmd/ingestion-cli \
  -config ./ingestion.yaml \
  -out ./out
```

O CLI lê o `ingestion.yaml` e gera conectores para cada tabela listada, além dos artefatos de stage/final. Os nomes de tópicos e tabelas de destino seguem as configurações do arquivo.

## 🗂️ Artefatos gerados

- **Conector Debezium (source)**: JSON para criação no Kafka Connect, configurando captura de mudanças no SQL Server.
- **Conector Snowflake (sink)**: JSON para ingestão dos tópicos no stage Snowflake.
- **Jobs de tabelas**: scripts para criar `_INGEST`, tabela final e STAGE com colunas alinhadas ao schema real.
- **Log de execução**: arquivos informando tabelas encontradas, colunas ignoradas e validações feitas durante o parsing do `ingestion.yaml`.

## 🛠️ Dicas e troubleshooting

- Certifique-se de que a porta do SQL Server esteja acessível e que a variável `SQLSERVER_PORT` corresponda ao ambiente.
- Caso o Debezium não veja novas mudanças, verifique permissões de CDC na tabela origem.
- Erros ao criar tabelas no Snowflake geralmente estão ligados a role/warehouse incorreto; revise `SNOWFLAKE_ROLE` e `SNOWFLAKE_WAREHOUSE`.
- Para depurar, execute com `LOG_LEVEL=debug` no `.env` e inspecione os arquivos de saída em `./out`.