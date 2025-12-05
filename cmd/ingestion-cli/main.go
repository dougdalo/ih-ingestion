package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/joho/godotenv"

	"ih-ingestion/internal/config"
	"ih-ingestion/internal/generator"
	"ih-ingestion/internal/gitops"
	"ih-ingestion/internal/model"
	"ih-ingestion/internal/sqlserver"
	"ih-ingestion/internal/templates"
)

type tableMeta struct {
	Name        string
	Schema      string
	RowCount    int64
	BusinessDDL string
}

type sourceGroup struct {
	Tables    []tableMeta
	TotalRows int64
}

func main() {
	// Descobre o diretório do executável
	execPath, err := os.Executable()
	if err != nil {
		log.Fatalf("erro ao obter caminho do executável: %v", err)
	}
	execDir := filepath.Dir(execPath)

	// Carrega .env da pasta do executável (se existir)
	envPath := filepath.Join(execDir, ".env")
	_ = godotenv.Load(envPath)

	// Flags
	configFlag := flag.String("config", "", "caminho para arquivo YAML de ingestão (vários bancos/tabelas). Se vazio, tenta ingestion.yaml ao lado do binário")
	schema := flag.String("schema", "dbo", "schema da tabela de origem (modo single)")
	table := flag.String("table", "", "nome da tabela de origem (modo single)")
	group := flag.String("group", "grupo1", "nome lógico do grupo/wave de tabelas")
	mode := flag.String("mode", "online", "modo: online ou batch")
	size := flag.String("size", "m", "tamanho: p/m/g")
	outDirFlag := flag.String("out", "./out", "diretório de saída para os YAMLs (se GitOps estiver habilitado, é relativo à raiz do repo)")
	dryRun := flag.Bool("dry-run", false, "se verdadeiro, não grava arquivos nem faz git push; apenas mostra o que seria feito")

	maxTablesPerSource := flag.Int("max-tables-per-source", 0, "máximo de tabelas por source connector (0 = ilimitado, pode ser sobrescrito por alias no YAML)")
	maxRowsPerSource := flag.Int64("max-rows-per-source", 0, "máximo de linhas totais por source connector (0 = ignorar rowcount, pode ser sobrescrito por alias no YAML)")

	flag.Parse()

	// Resolve configPath: flag > ingestion.yaml ao lado do binário
	finalConfigPath := *configFlag
	if finalConfigPath == "" {
		candidate := filepath.Join(execDir, "ingestion.yaml")
		if _, err := os.Stat(candidate); err == nil {
			finalConfigPath = candidate
		}
	}

	// Carrega config GitOps (se existir)
	gitCfg, err := gitops.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("erro carregando configuração de GitOps: %v", err)
	}
	gitEnabled := gitCfg != nil

	// ---------------------
	// MODO CONFIG (wave)
	// ---------------------
	if finalConfigPath != "" {
		var outBaseDir string
		var repoPath, branchName string

		if gitEnabled && !*dryRun {
			// Prepara repo Git (clone/update + branch)
			repoPath, branchName, err = gitops.PrepareRepo(gitCfg, execDir, *group)
			if err != nil {
				log.Fatalf("erro preparando repositório GitOps: %v", err)
			}

			// outDir relativo à raiz do repo (se não for absoluto)
			if filepath.IsAbs(*outDirFlag) {
				outBaseDir = *outDirFlag
			} else {
				outBaseDir = filepath.Join(repoPath, *outDirFlag)
			}
		} else {
			// Sem GitOps: outDir relativo à pasta do executável (se não for absoluto)
			if filepath.IsAbs(*outDirFlag) {
				outBaseDir = *outDirFlag
			} else {
				outBaseDir = filepath.Join(execDir, *outDirFlag)
			}
		}

		log.Printf(
			"Iniciando modo config: configPath=%s group=%s mode=%s size=%s outDir=%s dryRun=%v maxTablesPerSource(flag)=%d maxRowsPerSource(flag)=%d gitEnabled=%v",
			finalConfigPath, *group, *mode, *size, outBaseDir, *dryRun, *maxTablesPerSource, *maxRowsPerSource, gitEnabled,
		)

		if err := runFromConfig(finalConfigPath, *group, *mode, *size, outBaseDir, *dryRun, *maxTablesPerSource, *maxRowsPerSource); err != nil {
			log.Fatalf("erro no modo config: %v", err)
		}

		// Se GitOps estiver habilitado e não for dry-run, faz commit/push
		if gitEnabled && !*dryRun {
			msg := fmt.Sprintf("Ingestion wave %s", *group)
			if err := gitops.CommitAndPush(repoPath, branchName, msg); err != nil {
				log.Fatalf("erro ao fazer commit/push GitOps: %v", err)
			}
			log.Printf("GitOps concluído com sucesso. Branch: %s", branchName)
		}

		return
	}

	// ---------------------
	// MODO SINGLE
	// ---------------------
	if *table == "" {
		log.Fatal("flag -table é obrigatória quando -config não é informado e não foi encontrado ingestion.yaml ao lado do binário")
	}

	// outDir relativo à pasta do executável (se não for absoluto)
	outBaseDir := *outDirFlag
	if !filepath.IsAbs(outBaseDir) {
		outBaseDir = filepath.Join(execDir, outBaseDir)
	}

	log.Printf("Iniciando modo single: schema=%s table=%s group=%s mode=%s size=%s outDir=%s dryRun=%v",
		*schema, *table, *group, *mode, *size, outBaseDir, *dryRun)

	if err := runSingleTable(*schema, *table, *group, *mode, *size, outBaseDir, *dryRun); err != nil {
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

// Modo YAML: vários bancos/tabelas via ingestion.yaml, com agrupamento em sources
func runFromConfig(configPath, group, mode, size, outDir string, dryRun bool, maxTablesPerSourceFlag int, maxRowsPerSourceFlag int64) error {
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
	totalSources := 0

	for _, srv := range cfgYaml.SqlServers {
		dbNameLower := strings.ToLower(srv.Database)
		dbNameUpper := strings.ToUpper(srv.Database)

		// limites efetivos (YAML > flag > 0)
		effMaxTables := maxTablesPerSourceFlag
		if srv.MaxTablesPerSource > 0 {
			effMaxTables = srv.MaxTablesPerSource
		}
		effMaxRows := maxRowsPerSourceFlag
		if srv.MaxRowsPerSource > 0 {
			effMaxRows = srv.MaxRowsPerSource
		}

		log.Printf("[alias=%s] database=%s schemaDefault=%s tables=%d maxTables=%d maxRows=%d",
			srv.Alias, dbNameUpper, srv.Schema, len(srv.Tables), effMaxTables, effMaxRows)

		// Conecta por alias
		db, err := sqlserver.NewFromAlias(srv.Alias, srv.Database)
		if err != nil {
			return fmt.Errorf("conectando alias %s: %w", srv.Alias, err)
		}

		// Monta metadados de cada tabela (DDL + rowcount)
		var metas []tableMeta
		for _, t := range srv.Tables {
			schema := srv.Schema
			if strings.TrimSpace(t.Schema) != "" {
				schema = strings.TrimSpace(t.Schema)
			}

			cols, err := sqlserver.LoadColumns(db, schema, t.Name)
			if err != nil {
				db.Close()
				return fmt.Errorf("lendo colunas %s.%s (%s): %w", schema, t.Name, srv.Alias, err)
			}
			businessDDL := sqlserver.BuildBusinessColumnsDDL(cols)

			var rowCount int64
			if effMaxRows > 0 {
				rowCount, err = sqlserver.GetTableRowCount(db, schema, t.Name)
				if err != nil {
					db.Close()
					return fmt.Errorf("obtendo rowcount de %s.%s (%s): %w", schema, t.Name, srv.Alias, err)
				}
			}

			metas = append(metas, tableMeta{
				Name:        t.Name,
				Schema:      schema,
				RowCount:    rowCount,
				BusinessDDL: businessDDL,
			})
			totalTables++
		}

		groups := groupTablesIntoSources(metas, effMaxTables, effMaxRows)
		log.Printf("[alias=%s] grupos de source criados: %d (maxTables=%d, maxRows=%d)",
			srv.Alias, len(groups), effMaxTables, effMaxRows)

		dbDefaultSchemaLower := strings.ToLower(strings.TrimSpace(srv.Schema))
		if dbDefaultSchemaLower == "" {
			dbDefaultSchemaLower = "dbo"
		}

		for gi, g := range groups {
			totalSources++
			groupIndex := gi + 1

			// Naming base pra esse grupo
			sourceName := fmt.Sprintf(
				"source-debeziumsqlserver-%s-%s-%s-%s-%s-%03d",
				dbNameLower, dbDefaultSchemaLower, group, mode, size, groupIndex,
			)

			topicPrefix := fmt.Sprintf(
				"source_debeziumsqlserver_%s_%s_%s_%s_%s",
				dbNameLower, dbDefaultSchemaLower, group, mode, size,
			)

			schemaHistoryTopic := fmt.Sprintf("sh_%s_%03d", topicPrefix, groupIndex)

			upperAlias := strings.ToUpper(srv.Alias)
			hostEnvName := fmt.Sprintf("SQLSERVER_%s_HOST", upperAlias)
			portEnvName := fmt.Sprintf("SQLSERVER_%s_PORT", upperAlias)

			host, err := config.RequireEnv(hostEnvName)
			if err != nil {
				db.Close()
				return fmt.Errorf("%s não configurado: %w", hostEnvName, err)
			}
			port := config.GetEnvOrDefault(portEnvName, "1433")

			// table.include.list com todas as tabelas desse grupo
			includeParts := make([]string, 0, len(g.Tables))
			for _, tm := range g.Tables {
				includeParts = append(includeParts, fmt.Sprintf("%s.%s", tm.Schema, tm.Name))
			}
			tableIncludeList := strings.Join(includeParts, ",")

			sourceCfg := model.SourceConfig{
				Name:                          sourceName,
				ClusterName:                   clusterName,
				DatabaseHost:                  host,
				DatabasePort:                  port,
				DatabaseSecret:                srv.SecretName,
				DatabaseNameUpper:             dbNameUpper,
				TopicPrefix:                   topicPrefix,
				TableIncludeList:              tableIncludeList,
				SchemaHistoryBootstrapServers: shBootstrap,
				SchemaHistoryTopic:            schemaHistoryTopic,
			}

			srcPath := fmt.Sprintf("%s/source-%s-%s-%03d.yaml", outDir, dbNameLower, dbDefaultSchemaLower, groupIndex)

			logPrefix := fmt.Sprintf("[alias=%s grp=%02d db=%s]", srv.Alias, groupIndex, dbNameUpper)
			log.Printf("%s source=%s (tables=%d, totalRows=%d) -> %s",
				logPrefix, sourceName, len(g.Tables), g.TotalRows, srcPath)
			for _, tm := range g.Tables {
				log.Printf("%s   table=%s.%s rows=%d", logPrefix, tm.Schema, strings.ToUpper(tm.Name), tm.RowCount)
			}

			if !dryRun {
				if err := generator.RenderToFile(templates.SourceTemplate, sourceCfg, srcPath); err != nil {
					db.Close()
					return fmt.Errorf("gerando source group %d (%s): %w", groupIndex, srv.Alias, err)
				}
			} else {
				log.Printf("%s DRY-RUN: source NÃO gravado (apenas preview)", logPrefix)
			}

			// Agora sinks e jobs por tabela (1:1)
			for _, tm := range g.Tables {
				schema := tm.Schema
				tableUpper := strings.ToUpper(tm.Name)
				tableLower := strings.ToLower(tm.Name)

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
					BusinessColumnsDDL:  tm.BusinessDDL,
				}

				sinkPath := fmt.Sprintf("%s/sink-%s-%s.yaml", outDir, dbNameLower, tableLower)
				jobPath := fmt.Sprintf("%s/job-snowflake-%s-%s.yaml", outDir, dbNameLower, tableLower)

				log.Printf("%s sink=%s job=%s table=%s.%s -> %s , %s",
					logPrefix, sinkName, jobName, schema, tableUpper, sinkPath, jobPath)

				if dryRun {
					log.Printf("%s DRY-RUN: sink/job NÃO gravados (apenas preview)", logPrefix)
					continue
				}

				if err := generator.RenderToFile(templates.SinkTemplate, sinkCfg, sinkPath); err != nil {
					db.Close()
					return fmt.Errorf("gerando sink (%s.%s): %w", schema, tm.Name, err)
				}
				if err := generator.RenderToFile(templates.SnowflakeJobTemplate, jobCfg, jobPath); err != nil {
					db.Close()
					return fmt.Errorf("gerando job (%s.%s): %w", schema, tm.Name, err)
				}
			}
		}

		db.Close()
	}

	if dryRun {
		log.Printf("DRY-RUN concluído. Sources simulados: %d | Tabelas processadas: %d", totalSources, totalTables)
	} else {
		log.Printf("Arquivos gerados em %s (modo config). Sources: %d | Tabelas: %d", outDir, totalSources, totalTables)
	}

	return nil
}

// Agrupa as tabelas em grupos (cada grupo vira 1 source connector)
func groupTablesIntoSources(tables []tableMeta, maxTables int, maxRows int64) []sourceGroup {
	if len(tables) == 0 {
		return nil
	}

	// Sem limites: tudo em um único source
	if maxTables <= 0 && maxRows <= 0 {
		g := sourceGroup{}
		for _, t := range tables {
			g.Tables = append(g.Tables, t)
			g.TotalRows += t.RowCount
		}
		return []sourceGroup{g}
	}

	// Ordena por rowcount desc (tabelas maiores primeiro)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].RowCount > tables[j].RowCount
	})

	var groups []sourceGroup

	for _, t := range tables {
		placed := false

		for gi := range groups {
			// limite de tabelas
			if maxTables > 0 && len(groups[gi].Tables) >= maxTables {
				continue
			}
			// limite de linhas
			if maxRows > 0 && groups[gi].TotalRows+t.RowCount > maxRows {
				continue
			}

			groups[gi].Tables = append(groups[gi].Tables, t)
			groups[gi].TotalRows += t.RowCount
			placed = true
			break
		}

		// se não coube em nenhum grupo existente, cria um novo
		if !placed {
			g := sourceGroup{
				Tables:    []tableMeta{t},
				TotalRows: t.RowCount,
			}
			groups = append(groups, g)
		}
	}

	return groups
}
