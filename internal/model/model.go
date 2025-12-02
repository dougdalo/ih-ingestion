package model

import "database/sql"

type ColumnInfo struct {
	Name             string
	DataType         string
	IsNullable       string
	CharMaxLength    sql.NullInt64
	NumericPrecision sql.NullInt64
	NumericScale     sql.NullInt64
}

type SourceConfig struct {
	Name                          string
	ClusterName                   string
	DatabaseHost                  string
	DatabasePort                  string
	DatabaseSecret                string
	DatabaseNameUpper             string
	TopicPrefix                   string
	TableIncludeList              string
	SchemaHistoryBootstrapServers string
	SchemaHistoryTopic            string
}

type SinkConfig struct {
	Name                    string
	ClusterName             string
	TopicName               string
	SnowflakeURL            string
	SnowflakeUserSecret     string
	SnowflakePasswordSecret string
	Stage                   string
	Table                   string
	Schema                  string
}

type SnowflakeJobConfig struct {
	JobName             string
	ConnectionConfigMap string
	SqlConfigMapName    string
	Role                string
	Database            string
	Schema              string
	TableIngest         string
	TableFinal          string
	StageName           string
	BusinessColumnsDDL  string
}
