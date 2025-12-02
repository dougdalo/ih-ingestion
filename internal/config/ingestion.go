package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type TableEntry struct {
	Name   string `yaml:"name"`
	Schema string `yaml:"schema,omitempty"`
}

type SqlServerEntry struct {
	Alias      string       `yaml:"alias"`
	Database   string       `yaml:"database"`
	Schema     string       `yaml:"schema"`     // schema default
	SecretName string       `yaml:"secretName"` // nome do secret usado no connector
	Tables     []TableEntry `yaml:"tables"`
}

type IngestionConfig struct {
	SqlServers []SqlServerEntry `yaml:"sqlservers"`
}

func LoadIngestionConfig(path string) (*IngestionConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg IngestionConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Valida estrutura lógica do YAML (alias, databases, schemas, tabelas duplicadas etc.)
func ValidateIngestionConfig(cfg *IngestionConfig) error {
	var problems []string

	if len(cfg.SqlServers) == 0 {
		problems = append(problems, "nenhum sqlserver definido em sqlservers")
	}

	seenAliases := map[string]bool{}
	seenTables := map[string]bool{} // alias|database|schema|table

	for i, srv := range cfg.SqlServers {
		ctx := fmt.Sprintf("sqlservers[%d] (alias=%s)", i, srv.Alias)

		alias := strings.TrimSpace(srv.Alias)
		if alias == "" {
			problems = append(problems, ctx+": alias vazio")
		} else {
			upperAlias := strings.ToUpper(alias)
			if seenAliases[upperAlias] {
				problems = append(problems, fmt.Sprintf("%s: alias duplicado %q", ctx, alias))
			} else {
				seenAliases[upperAlias] = true
			}
		}

		if strings.TrimSpace(srv.Database) == "" {
			problems = append(problems, ctx+": database vazio")
		}

		if strings.TrimSpace(srv.SecretName) == "" {
			problems = append(problems, ctx+": secretName vazio")
		}

		if len(srv.Tables) == 0 {
			problems = append(problems, ctx+": nenhuma tabela configurada em tables")
		}

		defaultSchema := "dbo"
		if strings.TrimSpace(srv.Schema) != "" {
			defaultSchema = strings.TrimSpace(srv.Schema)
		}

		for j, t := range srv.Tables {
			if strings.TrimSpace(t.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s.tables[%d]: name vazio", ctx, j))
				continue
			}
			schema := strings.TrimSpace(t.Schema)
			if schema == "" {
				schema = defaultSchema
			}

			key := fmt.Sprintf("%s|%s|%s|%s",
				strings.ToUpper(alias),
				strings.ToUpper(srv.Database),
				strings.ToUpper(schema),
				strings.ToUpper(t.Name),
			)
			if seenTables[key] {
				problems = append(problems,
					fmt.Sprintf("%s.tables[%d]: tabela duplicada %s.%s no mesmo alias/database", ctx, j, schema, t.Name))
			} else {
				seenTables[key] = true
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("ingestion.yaml inválido:\n- %s", strings.Join(problems, "\n- "))
	}

	return nil
}

// Valida se existem envs mínimas para cada alias declarado no YAML
func ValidateEnvForAliases(cfg *IngestionConfig) error {
	var problems []string

	for _, srv := range cfg.SqlServers {
		alias := strings.TrimSpace(srv.Alias)
		if alias == "" {
			continue
		}
		upper := strings.ToUpper(alias)

		keys := []string{
			"SQLSERVER_" + upper + "_HOST",
			"SQLSERVER_" + upper + "_USER",
			"SQLSERVER_" + upper + "_PASSWORD",
		}

		for _, k := range keys {
			if os.Getenv(k) == "" {
				problems = append(problems, fmt.Sprintf("%s não definida (alias=%s)", k, srv.Alias))
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("variáveis de ambiente ausentes:\n- %s", strings.Join(problems, "\n- "))
	}

	return nil
}
