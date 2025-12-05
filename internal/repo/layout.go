package repo

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Layout conhece a estrutura de diretórios do repo ArgoCD/Strimzi/Jobs
// ou de uma pasta "out" local, dependendo de ArgoStyle.
type Layout struct {
	BaseDir          string // apps/ (no GitOps) ou out/ (no modo local)
	Env              string // production | homolog | development
	SourceProvider   string // ex: "debeziumsqlserver"
	SnowflakeLogical string // ex: "lz-sql-ih-prd"
	ArgoStyle        bool   // true = estrutura Argo real; false = estrutura local/out
}

// baseDir: caminho absoluto da base (apps ou out)
// env: production / homolog / development
// sourceProvider: "debeziumsqlserver", "debeziumoracle", etc.
// snowflakeLogical: ex "lz-sql-ih-prd"
// argoStyle: true → usa apps/strimzi_conectores/... ; false → usa out/source|sink|jobs/...
func NewLayout(baseDir, env, sourceProvider, snowflakeLogical string, argoStyle bool) Layout {
	return Layout{
		BaseDir:          baseDir,
		Env:              env,
		SourceProvider:   sourceProvider,
		SnowflakeLogical: snowflakeLogical,
		ArgoStyle:        argoStyle,
	}
}

// ==== ROOTS (ponto de ancoragem) ====

// SourceRoot:
//
//   - Modo GitOps (ArgoStyle=true):
//     apps/strimzi_conectores/envs/<env>/source/debeziumsqlserver
//
//   - Modo local/out (ArgoStyle=false):
//     out/source/debeziumsqlserver
func (l Layout) SourceRoot() string {
	if l.ArgoStyle {
		return filepath.Join(
			l.BaseDir,
			"strimzi_conectores",
			"envs",
			l.Env,
			"source",
			l.SourceProvider,
		)
	}

	return filepath.Join(
		l.BaseDir,
		"source",
		l.SourceProvider,
	)
}

// SinkRoot:
//
//   - GitOps:
//     apps/strimzi_conectores/envs/<env>/sink/jobsnowflake/<lz-sql-ih-prd>
//
//   - Local:
//     out/sink/jobsnowflake/<lz-sql-ih-prd>
func (l Layout) SinkRoot() string {
	if l.ArgoStyle {
		return filepath.Join(
			l.BaseDir,
			"strimzi_conectores",
			"envs",
			l.Env,
			"sink",
			"jobsnowflake",
			l.SnowflakeLogical,
		)
	}

	return filepath.Join(
		l.BaseDir,
		"sink",
		"jobsnowflake",
		l.SnowflakeLogical,
	)
}

// JobRoot:
//
//   - GitOps:
//     apps/jobs/snowflake_envs/<env>/<lz-sql-ih-prd>
//
//   - Local:
//     out/jobs/snowflake_envs/<env>/<lz-sql-ih-prd>
func (l Layout) JobRoot() string {
	return filepath.Join(
		l.BaseDir,
		"jobs",
		"snowflake_envs",
		l.Env,
		l.SnowflakeLogical,
	)
}

// ==== DIRS POR BANCO (podem ser criados se não existirem) ====

// Diretório do SOURCE Debezium SQL Server para um banco+schema:
//
//   - GitOps:
//     apps/strimzi_conectores/envs/<env>/source/debeziumsqlserver/<db>_<schema>/
//
//   - Local:
//     out/source/debeziumsqlserver/<db>_<schema>/
func (l Layout) SourceDBDir(dbNameLower, schemaLower string) string {
	dbSlug := strings.ToLower(dbNameLower)
	schemaSlug := strings.ToLower(schemaLower)
	subdir := fmt.Sprintf("%s_%s", dbSlug, schemaSlug)

	return filepath.Join(
		l.SourceRoot(),
		subdir,
	)
}

// Diretório dos SINKS Snowflake (connectors) para um banco:
//
//   - GitOps:
//     apps/strimzi_conectores/envs/<env>/sink/jobsnowflake/<lz-sql-ih-prd>/<db>/
//
//   - Local:
//     out/sink/jobsnowflake/<lz-sql-ih-prd>/<db>/
func (l Layout) SinkDBDir(dbNameLower string) string {
	dbSlug := strings.ToLower(dbNameLower)

	return filepath.Join(
		l.SinkRoot(),
		dbSlug,
	)
}

// Diretório dos JOBS Snowflake para um banco:
//
//   - GitOps:
//     apps/jobs/snowflake_envs/<env>/<lz-sql-ih-prd>/<db>/
//
//   - Local:
//     out/jobs/snowflake_envs/<env>/<lz-sql-ih-prd>/<db>/
func (l Layout) JobDBDir(dbNameLower string) string {
	dbSlug := strings.ToLower(dbNameLower)

	return filepath.Join(
		l.JobRoot(),
		dbSlug,
	)
}
