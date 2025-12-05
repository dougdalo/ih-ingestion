package sqlserver

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/microsoft/go-mssqldb"

	"ih-ingestion/internal/config"
	"ih-ingestion/internal/model"
)

// MODO UNICO – ainda funciona (único banco via SQLSERVER_HOST/...)
// Usado quando você NÃO passa -config e trabalha com uma tabela só.
func NewFromEnv() (*sql.DB, string, error) {
	host, err := config.RequireEnv("SQLSERVER_HOST")
	if err != nil {
		return nil, "", err
	}
	user, err := config.RequireEnv("SQLSERVER_USER")
	if err != nil {
		return nil, "", err
	}
	password, err := config.RequireEnv("SQLSERVER_PASSWORD")
	if err != nil {
		return nil, "", err
	}
	dbName, err := config.RequireEnv("SQLSERVER_DATABASE")
	if err != nil {
		return nil, "", err
	}
	port := config.GetEnvOrDefault("SQLSERVER_PORT", "1433")

	query := url.Values{}
	query.Add("database", dbName)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, password),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: query.Encode(),
	}

	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		return nil, "", err
	}
	if err := db.Ping(); err != nil {
		return nil, "", err
	}

	return db, dbName, nil
}

// conexão por alias (SQLSERVER_{ALIAS}_HOST/USER/PASSWORD)
func NewFromAlias(alias, database string) (*sql.DB, error) {
	upper := strings.ToUpper(alias)

	host, err := config.RequireEnv("SQLSERVER_" + upper + "_HOST")
	if err != nil {
		return nil, err
	}
	user, err := config.RequireEnv("SQLSERVER_" + upper + "_USER")
	if err != nil {
		return nil, err
	}
	password, err := config.RequireEnv("SQLSERVER_" + upper + "_PASSWORD")
	if err != nil {
		return nil, err
	}
	port := config.GetEnvOrDefault("SQLSERVER_"+upper+"_PORT", "1433")

	query := url.Values{}
	query.Add("database", database)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, password),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: query.Encode(),
	}

	db, err := sql.Open("sqlserver", u.String())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func LoadColumns(db *sql.DB, schema, table string) ([]model.ColumnInfo, error) {
	const q = `
SELECT
  COLUMN_NAME,
  DATA_TYPE,
  IS_NULLABLE,
  CHARACTER_MAXIMUM_LENGTH,
  NUMERIC_PRECISION,
  NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`
	rows, err := db.Query(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []model.ColumnInfo
	for rows.Next() {
		var c model.ColumnInfo
		if err := rows.Scan(
			&c.Name,
			&c.DataType,
			&c.IsNullable,
			&c.CharMaxLength,
			&c.NumericPrecision,
			&c.NumericScale,
		); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("nenhuma coluna encontrada para %s.%s", schema, table)
	}

	return cols, nil
}

func GetTableRowCount(db *sql.DB, schema, table string) (int64, error) {
	const q = `
SELECT
  SUM(p.row_count) AS row_count
FROM sys.dm_db_partition_stats AS p
JOIN sys.tables t   ON p.object_id = t.object_id
JOIN sys.schemas s  ON t.schema_id = s.schema_id
WHERE p.index_id IN (0,1)
  AND s.name = @p1
  AND t.name = @p2;
`
	var rowCount sql.NullInt64
	if err := db.QueryRow(q, schema, table).Scan(&rowCount); err != nil {
		return 0, err
	}
	if !rowCount.Valid {
		return 0, nil
	}
	return rowCount.Int64, nil
}

func mapToSnowflakeType(c model.ColumnInfo) string {
	t := strings.ToLower(c.DataType)

	switch t {
	case "int", "bigint", "smallint", "tinyint":
		return "INT"
	case "decimal", "numeric":
		if c.NumericPrecision.Valid && c.NumericScale.Valid {
			return fmt.Sprintf("NUMBER(%d,%d)", c.NumericPrecision.Int64, c.NumericScale.Int64)
		}
		return "NUMBER"
	case "float", "real":
		return "FLOAT"
	case "bit":
		return "BOOLEAN"
	case "datetime", "datetime2", "smalldatetime", "datetimeoffset", "date", "time":
		return "TIMESTAMP_NTZ"
	case "char", "nchar", "varchar", "nvarchar":
		if c.CharMaxLength.Valid && c.CharMaxLength.Int64 > 0 {
			return fmt.Sprintf("VARCHAR(%d)", c.CharMaxLength.Int64)
		}
		return "VARCHAR"
	default:
		return "VARCHAR"
	}
}

func BuildBusinessColumnsDDL(cols []model.ColumnInfo) string {
	var b strings.Builder

	for _, c := range cols {
		sfType := mapToSnowflakeType(c)
		nullStr := "NOT NULL"
		if strings.EqualFold(c.IsNullable, "YES") {
			nullStr = "NULL"
		}
		fmt.Fprintf(&b, "      %s %s %s,\n", c.Name, sfType, nullStr)
	}

	return b.String()
}
