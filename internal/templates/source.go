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
    database.hostname: "{{ .DatabaseHost }}"
    database.port: "{{ .DatabasePort }}"
    database.user: "${secrets:{{ .DatabaseSecret }}:user}"
    database.password: "${secrets:{{ .DatabaseSecret }}:password}"
    database.names: "{{ .DatabaseNameUpper }}"
    database.encrypt: false

    topic.prefix: "{{ .TopicPrefix }}"
    table.include.list: "{{ .TableIncludeList }}"

    decimal.handling.mode: "string"
    tombstones.on.delete: false

    schema.history.internal.kafka.bootstrap.servers: {{ .SchemaHistoryBootstrapServers }}
    schema.history.internal.kafka.topic: "{{ .SchemaHistoryTopic }}"

    value.converter: "org.apache.kafka.connect.json.JsonConverter"
    key.converter: "org.apache.kafka.connect.json.JsonConverter"
    key.converter.schemas.enable: "false"
    value.converter.schemas.enable: "true"

    data.query.mode: direct
    snapshot.locking.mode: none
    snapshot.isolation.mode: read_committed
    snapshot.max.threads: 5
`[1:]))
