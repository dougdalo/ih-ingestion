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

```

## 🖥️ UI simples para editar .env e ingestion.yaml

Para experimentar uma interface web mínima (HTMX) que salva os arquivos direto do navegador:

```bash
# caminhos padrão: .env e config/ingestion.yaml
go run ./cmd/config-ui -addr :8080
```

Depois acesse http://localhost:8080 e edite os dois arquivos em texto plano. O `ingestion.yaml` é validado com a mesma regra usada pelo CLI antes de salvar.

Use as flags `-env` e `-config` se quiser apontar para outros caminhos.
