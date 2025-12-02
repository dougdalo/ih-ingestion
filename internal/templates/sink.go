package templates

import "text/template"

var SinkTemplate = template.Must(template.New("sink").Parse(`
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  # sink-jdbcsnowflake-{database}-{schema}-{tabela}-{batch/online}-{agrupamento}-{p/m/g}
  name: {{ .Name }}
  labels:
    strimzi.io/cluster: {{ .ClusterName }}
spec:
  autoRestart:
    enabled: true
  class: br.com.datastreambrasil.v3.SnowflakeSinkConnector
  tasksMax: 1
  config:
    topics: "{{ .TopicName }}"
    url: "{{ .SnowflakeURL }}"
    user: "${secrets:{{ .SnowflakeUserSecret }}:username}"
    password: "${secrets:{{ .SnowflakePasswordSecret }}:password}"
    stage: "{{ .Stage }}"
    table: "{{ .Table }}"
    schema: "{{ .Schema }}"

    key.converter: "io.confluent.connect.avro.AvroConverter"
    key.converter.schema.registry.url: "http://schema-registry-ih.kafka-admin:8081"
    key.converter.schemas.enable: true
    value.converter: "io.confluent.connect.avro.AvroConverter"
    value.converter.schema.registry.url: "http://schema-registry-ih.kafka-admin:8081"
    value.converter.schemas.enable: true
`[1:]))
