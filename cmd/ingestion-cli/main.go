// cmd/ingestion-cli/main.go
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/joho/godotenv"

	"ih-ingestion/internal/config"
	"ih-ingestion/internal/generator"
	"ih-ingestion/internal/model"
	"ih-ingestion/internal/sqlserver"
	"ih-ingestion/internal/templates"
)

func main() {
	// Carrega .env se existir
	_ = godotenv.Load()

	// Flags
	configPath := flag.String("config", "", "caminho para arquivo YAML de ingestão (vários bancos/tabelas)")
	schema := flag.String("schema", "dbo", "schema da tabela de origem (modo single)")
	table := flag.String("table", "", "nome da tabela de origem (modo single)")
	group := flag.String("group", "grupo1", "nome do grupo de tabelas para o source")
	mode := flag.String("mode", "online", "modo: online ou batch")
	size := flag.String("size", "m", "tamanho: p/m/g")
	outDir := flag.String("out", ".", "diretório de saída para os YAMLs")
	dryRun := flag.Bool("dry-run", false, "se verdadeiro, não grava arquivos, apenas mostra o que seria gerado")

	flag.Parse()

	// Modo multi-banco/multi-tabela via YAML
	if *configPath != "" {
		log.Printf("Iniciando modo config: configPath=%s group=%s mode=%s size=%s outDir=%s dryRun=%v",
			*configPath, *group, *mode, *size, *outDir, *dryRun)

		if err := runFromConfig(*configPath, *group, *mode, *size, *outDir, *dryRun); err != nil {
			log.Fatalf("erro no modo config: %v", err)
		}
		return
	}

	// Modo single (tabela única)
	if *table == "" {
		log.Fatal("flag -table é obrigatória quando -config não é informado")
	}

	log.Printf("Iniciando modo single: schema=%s table=%s group=%s mode=%s size=%s outDir=%s dryRun=%v",
		*schema, *table, *group, *mode, *size, *outDir, *dryRun)

	if err := runSingleTable(*schema, *table, *group, *mode, *size, *outDir, *dryRun); err != nil {
		log.Fatalf("erro no modo single: %v", err)
	}
}

// Modo antigo / single: usa SQLSERVER_HOST/USER/PASSWORD/DATABASE
func runSingleTable(schema, table, group, mode, size, outDir string, dryRun bool) error {
	db, dbName, err := sqlserver.NewFromEnv()
	if err != nil {
		return fmt.Errorf("conectando no SQL Server: %w", err)
	}
	defer db.Close()

	cols, err := sqlserver.LoadColumns(db, schema, table)
	if err != nil {
		return fmt.Errorf("lendo colunas: %w", err)
	}

	businessDDL := sqlserver.BuildBusinessColumnsDDL(cols)

	clusterName := config.GetEnvOrDefault("CONNECT_CLUSTER_NAME", "inthub-prd")

	dbNameLower := strings.ToLower(dbName)
	dbNameUpper := strings.ToUpper(dbName)
	schemaLower := strings.ToLower(schema)
	tableUpper := strings.ToUpper(table)
	tableLower := strings.ToLower(table)

	// Source
	sourceName := fmt.Sprintf(
		"source-debeziumsqlserver-%s-%s-%s-%s-%s",
		dbNameLower, schemaLower, group, mode, size,
	)

	topicPrefix := fmt.Sprintf(
		"source_debeziumsqlserver_%s_%s_%s_%s_%s",
		dbNameLower, schemaLower, group, mode, size,
	)

	schemaHistoryTopic := "sh_" + topicPrefix

	host, err := config.RequireEnv("SQLSERVER_HOST")
	if err != nil {
		return fmt.Errorf("SQLSERVER_HOST não configurado: %w", err)
	}
	port := config.GetEnvOrDefault("SQLSERVER_PORT", "1433")
	dbSecret := config.GetEnvOrDefault("SQLSERVER_SECRET_NAME", "sqlserver-origem-sqlcrmp")
	shBootstrap := config.GetEnvOrDefault("SCHEMA_HISTORY_BOOTSTRAP_SERVERS", "kafka01:9092,kafka02:9092,kafka03:9092")

	sourceCfg := model.SourceConfig{
		Name:                          sourceName,
		ClusterName:                   clusterName,
		DatabaseHost:                  host,
		DatabasePort:                  port,
		DatabaseSecret:                dbSecret,
		DatabaseNameUpper:             dbNameUpper,
		TopicPrefix:                   topicPrefix,
		TableIncludeList:              fmt.Sprintf("%s.%s", schema, table),
		SchemaHistoryBootstrapServers: shBootstrap,
		SchemaHistoryTopic:            schemaHistoryTopic,
	}

	// Sink / Snowflake
	snowJdbc := config.GetEnvOrDefault(
		"SNOWFLAKE_JDBC_URL",
		"jdbc:snowflake://seuaccount.snowflakecomputing.com?schema=CRMB001D&db=LZ_SQL_IH_PRD&warehouse=WH_IH_PROD&CLIENT_SESSION_KEEP_ALIVE=TRUE&tracing=WARNING",
	)
	snowUserSecret := config.GetEnvOrDefault("SNOWFLAKE_USER_SECRET", "snowflake-creds")
	snowPassSecret := config.GetEnvOrDefault("SNOWFLAKE_PASSWORD_SECRET", "snowflake-creds")
	logicalDB := config.GetEnvOrDefault("SNOWFLAKE_DB_LOGICAL", "lz-sql-ih-prd")

	topicName := fmt.Sprintf(
		"%s.%s.%s.%s",
		topicPrefix, dbNameUpper, strings.ToUpper(schema), tableUpper,
	)

	sinkName := fmt.Sprintf(
		"sink-jdbcsnowflake-%s-%s-%s-%s-%s-%s",
		logicalDB,
		dbNameLower,
		tableLower,
		mode,
		size,
		"v1",
	)

	sinkCfg := model.SinkConfig{
		Name:                    sinkName,
		ClusterName:             clusterName,
		TopicName:               topicName,
		SnowflakeURL:            snowJdbc,
		SnowflakeUserSecret:     snowUserSecret,
		SnowflakePasswordSecret: snowPassSecret,
		Stage:                   tableUpper,
		Table:                   tableUpper,
		Schema:                  dbNameUpper,
	}

	// Job Snowflake
	jobName := fmt.Sprintf("lz-sql-ih-%s-%s-v1", dbNameLower, tableLower)
	sqlConfigMapName := fmt.Sprintf("lz-sql-ih-%s-%s-sql", dbNameLower, tableLower)

	connCfgMap := config.GetEnvOrDefault("SNOWFLAKE_CONN_CONFIGMAP", "lz-sql-ih-connection")
	role := config.GetEnvOrDefault("SNOWFLAKE_ROLE", "SNFLK_INTEGRATION_HUB_ROLE")
	sfDatabase := config.GetEnvOrDefault("SNOWFLAKE_DATABASE", "LZ_SQL_IH")

	jobCfg := model.SnowflakeJobConfig{
		JobName:             jobName,
		ConnectionConfigMap: connCfgMap,
		SqlConfigMapName:    sqlConfigMapName,
		Role:                role,
		Database:            sfDatabase,
		Schema:              dbNameUpper,
		TableIngest:         fmt.Sprintf("%s_INGEST", tableUpper),
		TableFinal:          tableUpper,
		StageName:           tableUpper,
		BusinessColumnsDDL:  businessDDL,
	}

	// Paths
	srcPath := fmt.Sprintf("%s/source-%s-%s.yaml", outDir, dbNameLower, tableLower)
	sinkPath := fmt.Sprintf("%s/sink-%s-%s.yaml", outDir, dbNameLower, tableLower)
	jobPath := fmt.Sprintf("%s/job-snowflake-%s-%s.yaml", outDir, dbNameLower, tableLower)

	log.Printf("[single] DB=%s Schema=%s Table=%s", dbNameUpper, schema, tableUpper)
	log.Printf("[single] source=%s -> %s", sourceName, srcPath)
	log.Printf("[single] sink  =%s -> %s", sinkName, sinkPath)
	log.Printf("[single] job   =%s -> %s", jobName, jobPath)

	if dryRun {
		log.Printf("[single] DRY-RUN ativo: nenhum arquivo foi gravado.")
		return nil
	}

	// Render
	if err := generator.RenderToFile(templates.SourceTemplate, sourceCfg, srcPath); err != nil {
		return fmt.Errorf("gerando source: %w", err)
	}
	if err := generator.RenderToFile(templates.SinkTemplate, sinkCfg, sinkPath); err != nil {
		return fmt.Errorf("gerando sink: %w", err)
	}
	if err := generator.RenderToFile(templates.SnowflakeJobTemplate, jobCfg, jobPath); err != nil {
		return fmt.Errorf("gerando job: %w", err)
	}

	log.Printf("Arquivos gerados em %s (modo single)", outDir)
	return nil
}

// Modo YAML: vários bancos/tabelas via ingestion.yaml
func runFromConfig(configPath, group, mode, size, outDir string, dryRun bool) error {
	cfgYaml, err := config.LoadIngestionConfig(configPath)
	if err != nil {
		return fmt.Errorf("carregando config YAML: %w", err)
	}

	if err := config.ValidateIngestionConfig(cfgYaml); err != nil {
		return fmt.Errorf("ingestion.yaml inválido: %w", err)
	}

	if err := config.ValidateEnvForAliases(cfgYaml); err != nil {
		return fmt.Errorf("validação de envs: %w", err)
	}

	clusterName := config.GetEnvOrDefault("CONNECT_CLUSTER_NAME", "inthub-prd")
	snowJdbc := config.GetEnvOrDefault(
		"SNOWFLAKE_JDBC_URL",
		"jdbc:snowflake://seuaccount.snowflakecomputing.com?schema=CRMB001D&db=LZ_SQL_IH_PRD&warehouse=WH_IH_PROD&CLIENT_SESSION_KEEP_ALIVE=TRUE&tracing=WARNING",
	)
	snowUserSecret := config.GetEnvOrDefault("SNOWFLAKE_USER_SECRET", "snowflake-creds")
	snowPassSecret := config.GetEnvOrDefault("SNOWFLAKE_PASSWORD_SECRET", "snowflake-creds")
	logicalDB := config.GetEnvOrDefault("SNOWFLAKE_DB_LOGICAL", "lz-sql-ih-prd")

	connCfgMap := config.GetEnvOrDefault("SNOWFLAKE_CONN_CONFIGMAP", "lz-sql-ih-connection")
	role := config.GetEnvOrDefault("SNOWFLAKE_ROLE", "SNFLK_INTEGRATION_HUB_ROLE")
	sfDatabase := config.GetEnvOrDefault("SNOWFLAKE_DATABASE", "LZ_SQL_IH")
	shBootstrap := config.GetEnvOrDefault("SCHEMA_HISTORY_BOOTSTRAP_SERVERS", "kafka01:9092,kafka02:9092,kafka03:9092")

	totalTables := 0

	for _, srv := range cfgYaml.SqlServers {
		dbNameLower := strings.ToLower(srv.Database)
		dbNameUpper := strings.ToUpper(srv.Database)

		log.Printf("[alias=%s] database=%s schemaDefault=%s tables=%d",
			srv.Alias, dbNameUpper, srv.Schema, len(srv.Tables))

		// Conecta por alias
		db, err := sqlserver.NewFromAlias(srv.Alias, srv.Database)
		if err != nil {
			return fmt.Errorf("conectando alias %s: %w", srv.Alias, err)
		}

		for _, t := range srv.Tables {
			totalTables++

			tableName := t.Name
			tableUpper := strings.ToUpper(tableName)
			tableLower := strings.ToLower(tableName)

			schema := srv.Schema
			if strings.TrimSpace(t.Schema) != "" {
				schema = t.Schema
			}
			schemaLower := strings.ToLower(schema)

			cols, err := sqlserver.LoadColumns(db, schema, tableName)
			if err != nil {
				db.Close()
				return fmt.Errorf("lendo colunas %s.%s (%s): %w", schema, tableName, srv.Alias, err)
			}

			businessDDL := sqlserver.BuildBusinessColumnsDDL(cols)

			// Source
			sourceName := fmt.Sprintf(
				"source-debeziumsqlserver-%s-%s-%s-%s-%s",
				dbNameLower, schemaLower, group, mode, size,
			)

			topicPrefix := fmt.Sprintf(
				"source_debeziumsqlserver_%s_%s_%s_%s_%s",
				dbNameLower, schemaLower, group, mode, size,
			)

			schemaHistoryTopic := "sh_" + topicPrefix

			upperAlias := strings.ToUpper(srv.Alias)
			hostEnvName := fmt.Sprintf("SQLSERVER_%s_HOST", upperAlias)
			portEnvName := fmt.Sprintf("SQLSERVER_%s_PORT", upperAlias)

			host, err := config.RequireEnv(hostEnvName)
			if err != nil {
				db.Close()
				return fmt.Errorf("%s não configurado: %w", hostEnvName, err)
			}
			port := config.GetEnvOrDefault(portEnvName, "1433")

			sourceCfg := model.SourceConfig{
				Name:                          sourceName,
				ClusterName:                   clusterName,
				DatabaseHost:                  host,
				DatabasePort:                  port,
				DatabaseSecret:                srv.SecretName, // secret específico desse banco
				DatabaseNameUpper:             dbNameUpper,
				TopicPrefix:                   topicPrefix,
				TableIncludeList:              fmt.Sprintf("%s.%s", schema, tableName),
				SchemaHistoryBootstrapServers: shBootstrap,
				SchemaHistoryTopic:            schemaHistoryTopic,
			}

			// Sink
			topicName := fmt.Sprintf(
				"%s.%s.%s.%s",
				topicPrefix, dbNameUpper, strings.ToUpper(schema), tableUpper,
			)

			sinkName := fmt.Sprintf(
				"sink-jdbcsnowflake-%s-%s-%s-%s-%s-%s",
				logicalDB,
				dbNameLower,
				tableLower,
				mode,
				size,
				"v1",
			)

			sinkCfg := model.SinkConfig{
				Name:                    sinkName,
				ClusterName:             clusterName,
				TopicName:               topicName,
				SnowflakeURL:            snowJdbc,
				SnowflakeUserSecret:     snowUserSecret,
				SnowflakePasswordSecret: snowPassSecret,
				Stage:                   tableUpper,
				Table:                   tableUpper,
				Schema:                  dbNameUpper,
			}

			// Job Snowflake
			jobName := fmt.Sprintf("lz-sql-ih-%s-%s-v1", dbNameLower, tableLower)
			sqlConfigMapName := fmt.Sprintf("lz-sql-ih-%s-%s-sql", dbNameLower, tableLower)

			jobCfg := model.SnowflakeJobConfig{
				JobName:             jobName,
				ConnectionConfigMap: connCfgMap,
				SqlConfigMapName:    sqlConfigMapName,
				Role:                role,
				Database:            sfDatabase,
				Schema:              dbNameUpper,
				TableIngest:         fmt.Sprintf("%s_INGEST", tableUpper),
				TableFinal:          tableUpper,
				StageName:           tableUpper,
				BusinessColumnsDDL:  businessDDL,
			}

			srcPath := fmt.Sprintf("%s/source-%s-%s.yaml", outDir, dbNameLower, tableLower)
			sinkPath := fmt.Sprintf("%s/sink-%s-%s.yaml", outDir, dbNameLower, tableLower)
			jobPath := fmt.Sprintf("%s/job-snowflake-%s-%s.yaml", outDir, dbNameLower, tableLower)

			logPrefix := fmt.Sprintf("[alias=%s db=%s schema=%s table=%s]", srv.Alias, dbNameUpper, schema, tableUpper)
			log.Printf("%s source=%s sink=%s job=%s", logPrefix, sourceName, sinkName, jobName)

			if dryRun {
				log.Printf("%s DRY-RUN: arquivos seriam %s, %s, %s", logPrefix, srcPath, sinkPath, jobPath)
				continue
			}

			if err := generator.RenderToFile(templates.SourceTemplate, sourceCfg, srcPath); err != nil {
				db.Close()
				return fmt.Errorf("gerando source (%s.%s): %w", schema, tableName, err)
			}
			if err := generator.RenderToFile(templates.SinkTemplate, sinkCfg, sinkPath); err != nil {
				db.Close()
				return fmt.Errorf("gerando sink (%s.%s): %w", schema, tableName, err)
			}
			if err := generator.RenderToFile(templates.SnowflakeJobTemplate, jobCfg, jobPath); err != nil {
				db.Close()
				return fmt.Errorf("gerando job (%s.%s): %w", schema, tableName, err)
			}
		}

		db.Close()
	}

	if dryRun {
		log.Printf("DRY-RUN concluído. Tabelas processadas (sem gerar arquivos): %d", totalTables)
	} else {
		log.Printf("Arquivos gerados em %s (modo config, vários bancos/tabelas). Tabelas processadas: %d", outDir, totalTables)
	}

	return nil
}
