package templates

import "text/template"

var SourceTemplate = template.Must(template.New("source").Parse(`
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  # source-debeziumsqlserver-{database}-{schema}-{grupo}-{online/batch}-{p/m/g}
  name: {{ .Name }}
  labels:
    strimzi.io/cluster: {{ .ClusterName }}
spec:
  autoRestart:
    enabled: true
  class: io.debezium.connector.sqlserver.SqlServerConnector
  tasksMax: 1
  config:
    # Conexão com SQL Server
    database.hostname: "{{ .DatabaseHost }}"
    database.port: "{{ .DatabasePort }}"
    database.user: "${secrets:{{ .DatabaseSecret }}:user}"
    database.password: "${secrets:{{ .DatabaseSecret }}:password}"
    database.names: "{{ .DatabaseNameUpper }}"
    database.encrypt: false

    # Tópicos e tabelas
    topic.prefix: "{{ .TopicPrefix }}"
    table.include.list: "{{ .TableIncludeList }}"

    # Regras de tipos / tombstones
    decimal.handling.mode: "string"
    tombstones.on.delete: false

    # Schema history interno do Debezium
    schema.history.internal.kafka.bootstrap.servers: "{{ .SchemaHistoryBootstrapServers }}"
    schema.history.internal.kafka.topic: "{{ .SchemaHistoryTopic }}"

    # Converters (Avro) + Schema Registry
    value.converter: "io.confluent.connect.avro.AvroConverter"
    key.converter: "io.confluent.connect.avro.AvroConverter"
    key.converter.schemas.enable: "false"
    value.converter.schemas.enable: "true"
    key.converter.schema.registry.url: "{{ .SchemaRegistryURL }}"
    value.converter.schema.registry.url: "{{ .SchemaRegistryURL }}"

    # Modo de snapshot e leitura
    data.query.mode: direct
    snapshot.mode: "when_needed"
    snapshot.locking.mode: none
    snapshot.isolation.mode: read_committed
    snapshot.max.threads: 5
`[1:]))
