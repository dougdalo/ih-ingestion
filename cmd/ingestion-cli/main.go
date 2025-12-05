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
	"ih-ingestion/internal/kustomize"
	"ih-ingestion/internal/model"
	"ih-ingestion/internal/repo"
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
	group := flag.String("group", "grupo1", "nome lógico do grupo/wave de tabelas (ex: grupo1)")
	mode := flag.String("mode", "online", "modo: online ou batch (usado em nomes de connectors/arquivos)")
	size := flag.String("size", "m", "tamanho: p/m/g (usado em nomes de connectors/arquivos)")
	outDirFlag := flag.String("out", "./apps", "no modo GitOps: subpasta apps/ dentro do repo. No modo local: pasta base onde serão criadas source/sink/jobs.")
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

	// IH_GITOPS_ENABLED controla explicitamente o modo
	gitopsFlag := strings.ToLower(strings.TrimSpace(os.Getenv("IH_GITOPS_ENABLED")))
	var gitEnabled bool

	switch gitopsFlag {
	case "true", "1", "yes", "y":
		if gitCfg == nil {
			log.Fatalf("IH_GITOPS_ENABLED=true mas GIT_REPO_URL não está configurado (GitOps não pode ser usado)")
		}
		gitEnabled = true
	case "false", "0", "no", "n":
		gitEnabled = false
	default:
		// modo automático: se há GIT_REPO_URL, usa GitOps; senão, modo local/out
		gitEnabled = (gitCfg != nil)
	}

	// ---------------------
	// MODO CONFIG (waves)
	// ---------------------
	if finalConfigPath != "" {
		var baseDir string
		var repoPath, branchName string

		if gitEnabled && !*dryRun {
			// Prepara repo Git (clone/update + branch)
			repoPath, branchName, err = gitops.PrepareRepo(gitCfg, execDir, *group)
			if err != nil {
				log.Fatalf("erro preparando repositório GitOps: %v", err)
			}

			// baseDir relativo à raiz do repo (se não for absoluto)
			if filepath.IsAbs(*outDirFlag) {
				baseDir = *outDirFlag
			} else {
				baseDir = filepath.Join(repoPath, *outDirFlag) // normalmente repo/apps
			}
		} else {
			// Sem GitOps: baseDir relativo à pasta do executável (se não for absoluto)
			if filepath.IsAbs(*outDirFlag) {
				baseDir = *outDirFlag
			} else {
				baseDir = filepath.Join(execDir, *outDirFlag) // ex: ./out
			}
		}

		log.Printf(
			"Iniciando modo config: configPath=%s group=%s mode=%s size=%s baseDir=%s dryRun=%v maxTablesPerSource(flag)=%d maxRowsPerSource(flag)=%d gitEnabled=%v",
			finalConfigPath, *group, *mode, *size, baseDir, *dryRun, *maxTablesPerSource, *maxRowsPerSource, gitEnabled,
		)

		if err := runFromConfig(finalConfigPath, *group, *mode, *size, baseDir, *dryRun, *maxTablesPerSource, *maxRowsPerSource, gitEnabled); err != nil {
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
// baseDir:
//   - no GitOps: caminho da pasta apps dentro do repo
//   - no modo local: pasta base (ex: ./out)
func runFromConfig(
	configPath, group, mode, size, baseDir string,
	dryRun bool,
	maxTablesPerSourceFlag int,
	maxRowsPerSourceFlag int64,
	useArgoLayout bool,
) error {
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

	envName := config.GetEnvOrDefault("IH_ENV", "production")

	layout := repo.NewLayout(baseDir, envName, "debeziumsqlserver", logicalDB, useArgoLayout)

	// 🔒 Segurança de layout:
	// - GitOps (ArgoStyle=true): roots DEVEM existir (apps/<...>), senão erro
	// - Local/out (ArgoStyle=false): roots são criados se não existirem
	if layout.ArgoStyle {
		rootMap := map[string]string{
			"sourceRoot": layout.SourceRoot(),
			"sinkRoot":   layout.SinkRoot(),
			"jobRoot":    layout.JobRoot(),
		}

		for name, p := range rootMap {
			fi, err := os.Stat(p)
			if err != nil {
				return fmt.Errorf("layout inválido: diretório base %s não encontrado (%s): %w", name, p, err)
			}
			if !fi.IsDir() {
				return fmt.Errorf("layout inválido: %s existe mas não é diretório: %s", name, p)
			}
		}
	} else if !dryRun {
		// modo local: garante que as raízes existam
		for _, root := range []string{layout.SourceRoot(), layout.SinkRoot(), layout.JobRoot()} {
			if err := os.MkdirAll(root, 0o755); err != nil {
				return fmt.Errorf("criando diretório base %s: %w", root, err)
			}
		}
	}

	totalTables := 0
	totalSources := 0

	for _, srv := range cfgYaml.SqlServers {
		dbNameLower := strings.ToLower(srv.Database)
		dbNameUpper := strings.ToUpper(srv.Database)

		// limites efetivos (YAML > flag)
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
			schemaName := srv.Schema
			if strings.TrimSpace(t.Schema) != "" {
				schemaName = strings.TrimSpace(t.Schema)
			}

			cols, err := sqlserver.LoadColumns(db, schemaName, t.Name)
			if err != nil {
				db.Close()
				return fmt.Errorf("lendo colunas %s.%s (%s): %w", schemaName, t.Name, srv.Alias, err)
			}
			businessDDL := sqlserver.BuildBusinessColumnsDDL(cols)

			var rowCount int64
			if effMaxRows > 0 {
				rowCount, err = sqlserver.GetTableRowCount(db, schemaName, t.Name)
				if err != nil {
					db.Close()
					return fmt.Errorf("obtendo rowcount de %s.%s (%s): %w", schemaName, t.Name, srv.Alias, err)
				}
			}

			metas = append(metas, tableMeta{
				Name:        t.Name,
				Schema:      schemaName,
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

		// diretórios reais (por banco), com base no layout
		sourceDir := layout.SourceDBDir(dbNameLower, dbDefaultSchemaLower)
		sinkDir := layout.SinkDBDir(dbNameLower)
		jobDir := layout.JobDBDir(dbNameLower)

		if !dryRun {
			// Aqui MkdirAll só cria a pasta do banco (bkbl001d, crmb001d, etc),
			// pois os roots já foram validados/criados antes.
			if err := os.MkdirAll(sourceDir, 0o755); err != nil {
				db.Close()
				return fmt.Errorf("criando sourceDir %s: %w", sourceDir, err)
			}
			if err := os.MkdirAll(sinkDir, 0o755); err != nil {
				db.Close()
				return fmt.Errorf("criando sinkDir %s: %w", sinkDir, err)
			}
			if err := os.MkdirAll(jobDir, 0o755); err != nil {
				db.Close()
				return fmt.Errorf("criando jobDir %s: %w", jobDir, err)
			}
		}

		sourceKustomFiles := []string{}
		sinkKustomFiles := []string{}
		jobKustomFiles := []string{}

		for gi, g := range groups {
			totalSources++
			groupIndex := gi + 1

			// Nome do arquivo source dentro da pasta do banco
			// Ex: grupo1-online-m-001.yaml
			sourceFileName := fmt.Sprintf("%s-%s-%s-%03d.yaml", group, mode, size, groupIndex)
			srcPath := filepath.Join(sourceDir, sourceFileName)

			// Nome lógico do connector source
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

			sourceKustomFiles = append(sourceKustomFiles, sourceFileName)

			// sinks + jobs por tabela
			for _, tm := range g.Tables {
				schemaName := tm.Schema
				tableUpper := strings.ToUpper(tm.Name)
				tableLower := strings.ToLower(tm.Name)

				topicName := fmt.Sprintf(
					"%s.%s.%s.%s",
					topicPrefix, dbNameUpper, strings.ToUpper(schemaName), tableUpper,
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

				// Exemplo: bkbl001d-clientes-online-m.yaml
				sinkFileName := fmt.Sprintf("%s-%s-%s-%s.yaml", dbNameLower, tableLower, mode, size)
				sinkPath := filepath.Join(sinkDir, sinkFileName)

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
				// Exemplo: bkbl001d-clientes.yaml
				jobFileName := fmt.Sprintf("%s-%s.yaml", dbNameLower, tableLower)
				jobPath := filepath.Join(jobDir, jobFileName)

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

				log.Printf("%s sink=%s job=%s table=%s.%s -> %s , %s",
					logPrefix, sinkName, jobName, schemaName, tableUpper, sinkPath, jobPath)

				if dryRun {
					log.Printf("%s DRY-RUN: sink/job NÃO gravados (apenas preview)", logPrefix)
				} else {
					if err := generator.RenderToFile(templates.SinkTemplate, sinkCfg, sinkPath); err != nil {
						db.Close()
						return fmt.Errorf("gerando sink (%s.%s): %w", schemaName, tm.Name, err)
					}
					if err := generator.RenderToFile(templates.SnowflakeJobTemplate, jobCfg, jobPath); err != nil {
						db.Close()
						return fmt.Errorf("gerando job (%s.%s): %w", schemaName, tm.Name, err)
					}
				}

				sinkKustomFiles = append(sinkKustomFiles, sinkFileName)
				jobKustomFiles = append(jobKustomFiles, jobFileName)
			}
		}

		if !dryRun {
			// Source com namespace strimzi
			if err := kustomize.UpdateKustomization(sourceDir, sourceKustomFiles, "strimzi"); err != nil {
				db.Close()
				return fmt.Errorf("atualizando kustomization do source em %s: %w", sourceDir, err)
			}
			// Sink e Jobs sem namespace (como nos teus exemplos)
			if err := kustomize.UpdateKustomization(sinkDir, sinkKustomFiles, ""); err != nil {
				db.Close()
				return fmt.Errorf("atualizando kustomization do sink em %s: %w", sinkDir, err)
			}
			if err := kustomize.UpdateKustomization(jobDir, jobKustomFiles, ""); err != nil {
				db.Close()
				return fmt.Errorf("atualizando kustomization dos jobs em %s: %w", jobDir, err)
			}
		} else {
			log.Printf("[alias=%s] DRY-RUN: kustomization.yaml NÃO atualizado. sourceDir=%s sinkDir=%s jobDir=%s",
				srv.Alias, sourceDir, sinkDir, jobDir)
		}

		db.Close()
	}

	if dryRun {
		log.Printf("DRY-RUN concluído. Sources simulados: %d | Tabelas processadas: %d", totalSources, totalTables)
	} else {
		log.Printf("Arquivos gerados sob baseDir=%s (modo config). Sources: %d | Tabelas: %d", baseDir, totalSources, totalTables)
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
