# 🚀 IH Ingestion Generator

Ferramenta em Go para gerar automaticamente:

- Kafka Connect **source connectors** (Debezium SQL Server)
- Kafka Connect **sink connectors** (Snowflake)
- **Jobs** de criação de tabelas no Snowflake (tabela `_INGEST`, tabela final e STAGE)

A partir de:

- Metadados reais das tabelas no SQL Server (INFORMATION_SCHEMA)
- Um arquivo declarativo `ingestion.yaml`
- Variáveis de ambiente (`.env` em dev, Secrets em prod)

---

## 🔧 Como rodar em modo single (tabela única)

```bash
cp .env.example .env   # ajustar valores

go run ./cmd/ingestion-cli \
  -schema dbo \
  -table Clientes \
  -out ./out
