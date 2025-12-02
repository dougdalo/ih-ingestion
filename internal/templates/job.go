package templates

import "text/template"

var SnowflakeJobTemplate = template.Must(template.New("job").Parse(`
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .JobName }}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: ubuntu
          image: ubuntu:20.04
          securityContext:
            runAsUser: 0
            privileged: true
          command: ["bin/bash", "-c"]
          volumeMounts:
            - name: cfg
              mountPath: /tmp/cfg
              readOnly: true
            - name: sql
              mountPath: /tmp/sql/script.sql
              subPath: script.sql
              readOnly: true
          args:
            - |
              apt-get update -y
              apt-get install -y curl unzip

              curl -o /tmp/snowsql-linux_x86_64.bash https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.3/linux_x86_64/snowsql-1.3.2-linux_x86_64.bash
              SNOWSQL_DEST=~/bin SNOWSQL_LOGIN_SHELL=~/.bashrc bash /tmp/snowsql-linux_x86_64.bash

              /root/bin/snowsql --config /tmp/cfg/snowsql.config --connection custom --filename /tmp/sql/script.sql
      volumes:
        - name: cfg
          configMap:
            name: {{ .ConnectionConfigMap }}
        - name: sql
          configMap:
            name: {{ .SqlConfigMapName }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .SqlConfigMapName }}
data:
  script.sql: |
    USE ROLE {{ .Role }};
    USE DATABASE {{ .Database }};

    CREATE SCHEMA IF NOT EXISTS {{ .Schema }};
    USE SCHEMA {{ .Schema }};

    DROP TABLE IF EXISTS {{ .TableIngest }};
    DROP TABLE IF EXISTS {{ .TableFinal }};

    CREATE TABLE IF NOT EXISTS {{ .TableIngest }} (
{{ .BusinessColumnsDDL }}      IH_TOPIC VARCHAR(255) NOT NULL,
      IH_PARTITION INT NOT NULL,
      IH_OFFSET INT NOT NULL,
      IH_OP VARCHAR(1) NOT NULL,
      IH_DATETIME TIMESTAMP_NTZ NOT NULL,
      IH_BLOCKID VARCHAR(40) NOT NULL,
      constraint pkey PRIMARY KEY (IH_TOPIC, IH_PARTITION, IH_OFFSET)
    );

    CREATE TABLE IF NOT EXISTS {{ .TableFinal }} (
{{ .BusinessColumnsDDL }}    );

    CREATE OR REPLACE STAGE {{ .StageName }}
      FILE_FORMAT = (
        TYPE = 'CSV',
        FIELD_OPTIONALLY_ENCLOSED_BY = '"',
        SKIP_HEADER = 0,
        FIELD_DELIMITER = ';',
        NULL_IF = ('\\N', 'NULL')
      );
`[1:]))
