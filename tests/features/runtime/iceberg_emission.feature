Feature: Iceberg emission
  Scenario Outline: Iceberg catalog and object storage resolve through the node DNS fixture
    Given MQTT is running
    And Iceberg dependencies are running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "dns_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/dns_notifications_{{test_id}}" with columns
      """
      user_id I64
      """
    And the Iceberg endpoints are published under fixture DNS
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-iceberg-dns-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'iceberg_dns_in_{{test_id}}' MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_dns_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_dns_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'dns_notifications_{{test_id}}' NAMESPACE '{{domain}}'
      LOCATION 's3://nervix-iceberg/tables/dns_notifications_{{test_id}}'
      CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "iceberg_dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the Iceberg table "dns_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":42}
      """
    And within "5s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      data file target: 512MiB
      """
    And the DNS fixture eventually receives Iceberg catalog and object-store questions

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Iceberg emitter accepts GCS object storage configuration
    Given GCS is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE GCS CLIENT gcs_main
        CONFIG {
          'service_path' = '{{gcs_addr}}',
          'no_auth' = true
        };

      CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };

      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO gcs_main TABLE 'gcs_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 'gs://nervix-iceberg/tables/gcs_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      SHOW CREATE EMITTER iceberg_notifications;
      """
    Then the last command output contains
      """
      TO gcs_main TABLE 'gcs_notifications_{{test_id}}'
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: An Iceberg GCS client whose no_auth is false signs its object requests
    Given HTTP receiver "gcs_catalog" is running
    And HTTP receiver "gcs_objects" is running
    And HTTP receiver "gcs_catalog" answers requests for "/v1/config" with 'respond 200; header Content-Type: application/json; body {"defaults":{},"overrides":{}}'
    And HTTP receiver "gcs_catalog" answers requests for "/v1/namespaces/{{domain}}/tables/events" with 'respond 200; header Content-Type: application/json; body {"metadata-location":"gs://nervix-gcs/tables/events/metadata/00000-6f0a3c0e-0d5b-4f0e-9a43-3a1c2b4d5e6f.metadata.json","metadata":{"format-version":2,"table-uuid":"6f0a3c0e-0d5b-4f0e-9a43-3a1c2b4d5e6f","location":"gs://nervix-gcs/tables/events","last-sequence-number":0,"last-updated-ms":1767225600000,"last-column-id":1,"current-schema-id":0,"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"user_id","required":false,"type":"long"}]}],"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"last-partition-id":999,"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"properties":{},"current-snapshot-id":-1,"refs":{},"snapshots":[],"snapshot-log":[],"metadata-log":[]},"config":{}}'
    And HTTP receiver "gcs_catalog" answers unscripted requests with "respond 503"
    And HTTP receiver "gcs_objects" answers unscripted requests with 'respond 200; header Content-Type: application/json; body {"name":"object","bucket":"nervix-gcs","size":"0"}'
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-gcs-{{test_id}}.example.com;
      CREATE ENDPOINT notification_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_input
        FROM notification_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE GCS CLIENT gcs_main CONFIG {
        'service_path' = '{{http_receiver.gcs_objects}}',
        'token' = 'fixture-token',
        'no_auth' = false
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{http_receiver.gcs_catalog}}'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_output FROM notifications
        TO gcs_main TABLE 'events' NAMESPACE '{{domain}}'
        LOCATION 'gs://nervix-gcs/tables/events'
        CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 5s
        VALUES { 'user_id' = input.user_id }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "iceberg_output" on the leader node contains
      """
      status: OK
      """
    When http payload is posted to host "iceberg-gcs-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":43}
      """
    Then HTTP receiver "gcs_objects" eventually receives at least 1 request
    And HTTP receiver "gcs_objects" request 1 has header "authorization" with value "Bearer fixture-token"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_execution_time
  Scenario Outline: Iceberg VALUES errors reject only the poison record before staging
    Given MQTT is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "values_error_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/values_error_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      clock_class STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        denominator I64,
        action STRING
      );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        error_operation STRING,
        source_user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        denominator integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-iceberg-values-error-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'iceberg_values_error_in_{{test_id}}' MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TIMESTAMP NOW
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
      CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
      CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications
      FROM notifications
      TO s3_main TABLE 'values_error_notifications_{{test_id}}' NAMESPACE '{{domain}}'
      LOCATION 's3://nervix-iceberg/tables/values_error_notifications_{{test_id}}'
      CATALOG iceberg_catalog
      COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES {
        'user_id' = input.user_id / input.denominator,
        'action' = LOWER(input.action),
        'clock_class' = CASE
          WHEN now() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN 'historical'
          ELSE 'physical'
        END
      } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          error_operation = error.operation,
          source_user_id = input.user_id
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    When these MQTT messages are rapidly published to topic "iceberg_values_error_in_{{test_id}}"
      """
      {"user_id":10,"denominator":2,"action":"HEALTHY_A"}
      {"user_id":11,"denominator":0,"action":"POISON"}
      {"user_id":12,"denominator":3,"action":"HEALTHY_B"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "error_code":"evaluation","error_operation":"values","source_user_id":11
      """
    And the relay subscription does not receive a payload within "1s"
    And the Iceberg table "values_error_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":5,"action":"healthy_a","clock_class":"historical"}
      """
    And the Iceberg table "values_error_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":4,"action":"healthy_b","clock_class":"historical"}
      """
    And the Iceberg table "values_error_notifications_{{test_id}}" does not contain a row within "1s"
      """
      {"action":"poison"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Iceberg emitter accepts Azure Blob object storage configuration
    Given Azure Blob is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE AZURE_BLOB CLIENT azure_main
        CONFIG {
          'account_name' = 'devstoreaccount1',
          'account_key' = 'local-key'
        };

      CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };

      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO azure_main TABLE 'azure_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 'wasb://nervix-iceberg@input.input.input.input.net/tables/azure_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      SHOW CREATE EMITTER iceberg_notifications;
      """
    Then the last command output contains
      """
      TO azure_main TABLE 'azure_notifications_{{test_id}}'
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter flushes a disk-backed batch to S3
    Given MQTT is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      created_at DATETIME
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING,
        created_at DATETIME
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string,
        created_at string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification
        ENCODE created_at AS RFC3339;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action, 'created_at' = input.created_at } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "iceberg_notifications_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN","created_at":"2026-06-04T00:00:00.123456Z"}
      """
    Then the Iceberg table "notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":42,"action":"OPEN"}
      """
    When MQTT message is published to topic "iceberg_notifications_in_{{test_id}}"
      """
      {"user_id":42,"action":"CLOSE","created_at":"2026-06-04T00:00:01.123456Z"}
      """
    Then the Iceberg table "notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":42,"action":"CLOSE"}
      """
    And the Iceberg table "notifications_{{test_id}}" metadata does not contain "timestamptz_ns"
    And the object storage path "s3://nervix-iceberg/catalogs/notifications_{{test_id}}.catalog.json" does not exist

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: An Iceberg emitter that declares BATCH commits every row at its data-file and commit boundaries
    Given Iceberg dependencies are running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "batched_{{test_id}}" exists at "s3://nervix-iceberg/tables/batched_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-batch-{{test_id}}.example.com;
      CREATE ENDPOINT notification_ingress
      ON edge
      PATH '/notifications'
      TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_source
      FROM notification_ingress MODE NO ACK
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
      CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
      CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'batched_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/batched_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "iceberg-batch-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":41,"action":"OPEN"}
      """
    And http payload is posted to host "iceberg-batch-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42,"action":"UPDATE"}
      """
    And http payload is posted to host "iceberg-batch-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":43,"action":"CLOSE"}
      """
    Then the Iceberg table "batched_{{test_id}}" eventually contains a row
      """
      {"user_id":41,"action":"OPEN"}
      """
    And the Iceberg table "batched_{{test_id}}" eventually contains a row
      """
      {"user_id":42,"action":"UPDATE"}
      """
    And the Iceberg table "batched_{{test_id}}" eventually contains a row
      """
      {"user_id":43,"action":"CLOSE"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @iceberg_batch_bounds @deloxide_stress
  Scenario Outline: Iceberg BATCH MAX MESSAGES rolls one produced row batch into bounded data files
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "row_bounded_{{test_id}}" exists at "s3://nervix-iceberg/tables/row_bounded_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR notifications_in
        FROM SCHEMA notification
          MODE ACK SEQUENTIAL ACK TIMEOUT 1m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications
        TO s3_main TABLE 'row_bounded_{{test_id}}' NAMESPACE '{{domain}}'
        LOCATION 's3://nervix-iceberg/tables/row_bounded_{{test_id}}'
        CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'user_id' = input.user_id, 'action' = input.action }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to node "node-1"
    When client "app" opens producer "input" on ingestor "notifications_in" expecting fields "user_id I64, action STRING"
    And producer "input" submits batch "rows" with rows
      | user_id | action |
      | 41      | open   |
      | 42      | update |
      | 43      | close  |
      | 44      | done   |
    Then within "30s" the Iceberg table "row_bounded_{{test_id}}" has 2 data files holding 4 rows, each with at most 2 rows and 1048576 bytes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @iceberg_batch_bounds @deloxide_stress
  Scenario Outline: Iceberg BATCH MAX SIZE rolls Parquet files and rejects an oversized row
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "byte_bounded_{{test_id}}" exists at "s3://nervix-iceberg/tables/byte_bounded_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE SCHEMA emitter_error (error_code STRING, error_operation STRING, source_user_id I64);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR notifications_in
        FROM SCHEMA notification
          MODE ACK SEQUENTIAL ACK TIMEOUT 1m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications
        TO s3_main TABLE 'byte_bounded_{{test_id}}' NAMESPACE '{{domain}}'
        LOCATION 's3://nervix-iceberg/tables/byte_bounded_{{test_id}}'
        CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'user_id' = input.user_id, 'action' = input.action }
        BATCH MAX MESSAGES 10 MAX SIZE 3KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR SEND TO emitter_errors
          SET error_code = error.code,
              error_operation = error.operation,
              source_user_id = input.user_id
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    And client "app" is connected to node "node-1"
    When client "app" opens producer "input" on ingestor "notifications_in" expecting fields "user_id I64, action STRING"
    # One source batch has two incompressible 1800-byte values and one 8192-byte value.
    # Each of the first two fits a 3 KiB Parquet file, while both together and the last alone do not.
    And producer "input" submits Iceberg file-bound batch "rows"
    Then within "30s" the Iceberg table "byte_bounded_{{test_id}}" has 2 data files holding 2 rows, each with at most 10 rows and 3072 bytes
    And within "30s" the relay subscription receives a payload
      """
      "error_code":"validation","error_operation":"encode","source_user_id":43
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Iceberg emitter commits nested datetime lists at microsecond precision
    Given MQTT is running
    And Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "nested_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/nested_notifications_{{test_id}}" with columns
      """
      user_id I64
      observed <table_type>
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64, observed <nested_type>);
      CREATE CODEC notification_codec FROM JSON TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-iceberg-nested-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'iceberg_nested_in_{{test_id}}' MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications INHERIT ALL UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'nested_notifications_{{test_id}}' NAMESPACE '{{domain}}'
      LOCATION 's3://nervix-iceberg/tables/nested_notifications_{{test_id}}'
      CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'observed' = input.observed } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "iceberg_nested_in_{{test_id}}"
      """
      {"user_id":42,"observed":[["1970-01-01T00:00:00.000001234Z","1969-12-31T23:59:59.999999999Z"],["1970-01-01T00:00:01.000001999Z"]]}
      """
    Then the Iceberg table "nested_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":42,"observed":[[1,-1],[1000001]]}
      """

    Examples:
      | cluster_size | table_type           | nested_type        |
      | 1            | LIST<LIST<DATETIME>> | VEC<VEC<DATETIME>> |
      | 3            | LIST<LIST<DATETIME>> | VEC<VEC<DATETIME>> |

  Scenario Outline: Iceberg emitter requires an existing catalog table
    Given MQTT is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-missing-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_missing_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE DETACHED ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'missing_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/missing_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "iceberg_missing_notifications_in_{{test_id}}"
      """
      {"user_id":47,"action":"MISSING"}
      """
    Then within "5s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      failed to initialize Iceberg table
      """
    And the last command output contains
      """
      missing_notifications_{{test_id}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter refuses a mapped column the table cannot write at open
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "incompatible_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/incompatible_notifications_{{test_id}}" with columns
      """
      user_id I64
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id STRING);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE DETACHED ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'incompatible_notifications_{{test_id}}' NAMESPACE '{{domain}}'
      LOCATION 's3://nervix-iceberg/tables/incompatible_notifications_{{test_id}}'
      CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "5s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      mapped column 'user_id' has Iceberg type string, but the table requires long
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An Iceberg emitter writes each mapped column into the table column of its name and null into each optional column VALUES leaves out
    Given MQTT is running
    And Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "by_name_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/by_name_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      note STRING
      attempts I32
      readings <readings_column_type>
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64, action STRING, attempts U16, readings <readings_type>);
      CREATE CODEC notification_codec FROM JSON TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-iceberg-by-name-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'iceberg_by_name_in_{{test_id}}' MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications INHERIT ALL UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'by_name_notifications_{{test_id}}' NAMESPACE '{{domain}}'
      LOCATION 's3://nervix-iceberg/tables/by_name_notifications_{{test_id}}'
      CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
      VALUES { 'readings' = input.readings, 'action' = input.action, 'attempts' = input.attempts, 'user_id' = input.user_id }
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "iceberg_by_name_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN","attempts":3,"readings":[7,8]}
      """
    Then the Iceberg table "by_name_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":42,"action":"OPEN","note":null,"attempts":3,"readings":[7,8]}
      """

    Examples:
      | cluster_size | readings_column_type | readings_type |
      | 1            | LIST<I64>            | ARRAY<I64, 2> |
      | 3            | LIST<I64>            | ARRAY<I64, 2> |

  Scenario Outline: An Iceberg emitter refuses at open a required table column that VALUES could leave without a value
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "required_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/required_notifications_{{test_id}}" with columns
      """
      user_id I64 REQUIRED
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id <user_id_type>, action STRING);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE DETACHED ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'required_notifications_{{test_id}}' NAMESPACE '{{domain}}'
      LOCATION 's3://nervix-iceberg/tables/required_notifications_{{test_id}}'
      CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
      VALUES { <values> }
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      <refusal>
      """

    Examples:
      | cluster_size | user_id_type | values                                             | refusal                                                                                         |
      | 1            | I64          | 'action' = input.action                            | table column 'user_id' is required, but VALUES does not map it                                  |
      | 3            | I64          | 'action' = input.action                            | table column 'user_id' is required, but VALUES does not map it                                  |
      | 1            | I64 OPTIONAL | 'user_id' = input.user_id, 'action' = input.action | table column 'user_id' is required, but the VALUES expression mapped into it can produce a null |
      | 3            | I64 OPTIONAL | 'user_id' = input.user_id, 'action' = input.action | table column 'user_id' is required, but the VALUES expression mapped into it can produce a null |

  Scenario Outline: Iceberg emitters use explicitly provisioned catalog tables
    Given MQTT is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "namespace_notifications_a_{{test_id}}" exists at "s3://nervix-iceberg/tables/namespace_notifications_a_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And Iceberg table "namespace_notifications_b_{{test_id}}" exists at "s3://nervix-iceberg/tables/namespace_notifications_b_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And Iceberg table "namespace_notifications_c_{{test_id}}" exists at "s3://nervix-iceberg/tables/namespace_notifications_c_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And Iceberg table "namespace_notifications_d_{{test_id}}" exists at "s3://nervix-iceberg/tables/namespace_notifications_d_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-namespace-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_namespace_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE ICEBERG TABLE EMITTER iceberg_notifications_a FROM notifications TO s3_main TABLE 'namespace_notifications_a_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/namespace_notifications_a_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ICEBERG TABLE EMITTER iceberg_notifications_b FROM notifications TO s3_main TABLE 'namespace_notifications_b_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/namespace_notifications_b_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ICEBERG TABLE EMITTER iceberg_notifications_c FROM notifications TO s3_main TABLE 'namespace_notifications_c_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/namespace_notifications_c_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ICEBERG TABLE EMITTER iceberg_notifications_d FROM notifications TO s3_main TABLE 'namespace_notifications_d_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/namespace_notifications_d_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "iceberg_namespace_notifications_in_{{test_id}}"
      """
      {"user_id":46,"action":"NAMESPACE"}
      """
    Then the Iceberg table "namespace_notifications_a_{{test_id}}" eventually contains a row
      """
      {"user_id":46,"action":"NAMESPACE"}
      """
    And the Iceberg table "namespace_notifications_b_{{test_id}}" eventually contains a row
      """
      {"user_id":46,"action":"NAMESPACE"}
      """
    And the Iceberg table "namespace_notifications_c_{{test_id}}" eventually contains a row
      """
      {"user_id":46,"action":"NAMESPACE"}
      """
    And the Iceberg table "namespace_notifications_d_{{test_id}}" eventually contains a row
      """
      {"user_id":46,"action":"NAMESPACE"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter stages batches under the configured temp directory
    Given MQTT is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And temporary files use a custom temp directory
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "temp_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/temp_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-temp-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_temp_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'temp_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/temp_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 10s MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "iceberg_temp_notifications_in_{{test_id}}"
      """
      {"user_id":44,"action":"STAGED"}
      """
    Then the temp directory eventually contains an Iceberg Arrow IPC staged batch
    And the temp directory does not contain an Iceberg Parquet staged batch
    And the Iceberg table "temp_notifications_{{test_id}}" does not contain a row within "500ms"
      """
      {"user_id":44,"action":"STAGED"}
      """
    Then the Iceberg table "temp_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":44,"action":"STAGED"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  @planned-graceful-handoff
  Scenario Outline: Iceberg emitter commits staged IPC batches during graceful shutdown
    Given MQTT is running
    And Iceberg dependencies are running
    Given graceful shutdown drain is enabled
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And temporary files use a custom temp directory
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "shutdown_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/shutdown_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-shutdown-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_shutdown_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'shutdown_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/shutdown_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 1h MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "iceberg_shutdown_notifications_in_{{test_id}}"
      """
      {"user_id":46,"action":"SHUTDOWN_STAGED"}
      """
    Then the temp directory eventually contains an Iceberg Arrow IPC staged batch
    And the Iceberg table "shutdown_notifications_{{test_id}}" does not contain a row within "500ms"
      """
      {"user_id":46,"action":"SHUTDOWN_STAGED"}
      """
    When all nodes are gracefully stopped
    Then the Iceberg table "shutdown_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":46,"action":"SHUTDOWN_STAGED"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter reports a REST catalog failure while opening its table
    Given MQTT is running
    And Iceberg dependencies are running
    And HTTP receiver "broken_catalog" is running
    And HTTP receiver "broken_catalog" answers unscripted requests with "respond 503"
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-init-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_init_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{http_receiver.broken_catalog}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE DETACHED ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'init_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/init_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "iceberg_init_notifications_in_{{test_id}}"
      """
      {"user_id":45,"action":"INIT"}
      """
    Then within "5s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      transient error: failed to initialize Iceberg table {{domain}}.init_notifications_{{test_id}}:
      """
    And the last command output contains
      """
      503 Service Unavailable
      """
    And HTTP receiver "broken_catalog" eventually receives at least 1 request

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter holds ACK until the append succeeds
    Given MQTT is running
    And Iceberg dependencies are running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "ack_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/ack_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the Iceberg endpoints are published under fixture DNS
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-iceberg-ack-{{test_id}}'
        };
        CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_dns_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_dns_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'iceberg_ack_notifications_in_{{test_id}}' SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'ack_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/ack_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "iceberg_notifications" enters stall mode
    And MQTT QoS 1 message is published to topic "iceberg_ack_notifications_in_{{test_id}}"
      """
      {"user_id":43,"action":"FAULTED"}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And the relay subscription does not receive a payload within "1s"
    And the Iceberg table "ack_notifications_{{test_id}}" does not contain a row within "500ms"
      """
      {"user_id":43,"action":"FAULTED"}
      """
    When emitter "iceberg_notifications" leaves stall mode
    Then the Iceberg table "ack_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":43,"action":"FAULTED"}
      """
    And the DNS fixture eventually receives Iceberg catalog and object-store questions

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  @iceberg_commit_memory @deloxide_stress
  Scenario Outline: An Iceberg commit the bulk memory class has no room for keeps its row and commits once the room is there
    Given Iceberg dependencies are running
    And temporary files use a custom temp directory
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "memory_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/memory_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer, action string);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-memory-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
      CREATE ICEBERG TABLE EMITTER iceberg_memory FROM notifications TO s3_main TABLE 'memory_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/memory_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 30s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "iceberg-memory-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":61,"action":"held"}
      """
    Then the temp directory eventually contains an Iceberg Arrow IPC staged batch
    # The row is staged and its commit comes due 30 seconds later. Loading the staged file charges
    # every byte of it, and the commit takes that charge only when the class has room for it now.
    When bulk memory on every node keeps only "64" bytes free
    Then within "90s" DESCRIBE EMITTER "iceberg_memory" on the leader node reports an Iceberg commit waiting for bulk memory, whose staged bytes are saved as placeholder "commit_bytes"
    And the Iceberg table "memory_notifications_{{test_id}}" does not contain a row within "1s"
      """
      {"user_id":61,"action":"held"}
      """
    When bulk memory on every node is released
    Then the Iceberg table "memory_notifications_{{test_id}}" eventually contains a row
      """
      {"user_id":61,"action":"held"}
      """
    And within "10s" DESCRIBE EMITTER "iceberg_memory" on the leader node reports "bytes_total" "sent" relay "notifications" totalling "{{commit_bytes}}"
    And within "30s" every node reserves only the bulk memory the scenario holds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @iceberg_refused_append
  Scenario Outline: An Iceberg commit its catalog refused retries what it began with, and rows staged during its backoff are acknowledged only once a catalog commit of their own holds them
    Given Iceberg dependencies are running
    And temporary files use a custom temp directory
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "refused_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/refused_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the Iceberg REST catalog is reached through a gate that holds every commit to table "refused_notifications_{{test_id}}"
    # Each row is a branch of its own, which the emitter collects apart and publishes one branch at a
    # time. Every published batch reaches its flush and commit boundaries at once, so a refused
    # commit is retried by the emitter's retry wake, which first stages what was collected meanwhile.
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE SCHEMA user_key (user_id I64);
      CREATE BRANCH by_user SCHEMA user_key TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_user;
      CREATE CLIENT PRODUCER INGESTOR notifications_in
        FROM SCHEMA notification
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 10m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          BRANCHED BY by_user SET user_id = message.user_id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_catalog_gate}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
      CREATE ICEBERG TABLE EMITTER iceberg_refused FROM notifications
        COLLECT FOR 5s
        TO s3_main TABLE 'refused_notifications_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/refused_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 1h MAX SIZE 1B MODE ACK RETRY POLICY BACKOFF 10s MAX 10s VALUES { 'user_id' = input.user_id, 'action' = input.action } FLUSH EACH 1h MAX BATCH SIZE 1B
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to node "node-1"
    When client "app" opens producer "input" on ingestor "notifications_in" expecting fields "user_id I64, action STRING"
    And producer "input" submits batch "rows" with rows
      | user_id | action |
      | 71      | first  |
      | 72      | second |
    Then within "60s" the Iceberg catalog gate holds a commit
    When the Iceberg catalog gate refuses the commit it holds
    Then within "30s" DESCRIBE EMITTER "iceberg_refused" on the leader node contains
      """
      transient error: failed to commit Iceberg staged batches
      """
    And the last command output contains
      """
      reconnect backoff: 10s
      """
    # The retry comes no sooner than its backoff, and it staged the other branch's row before it
    # commits again: the file the refused commit began with and the newer one are both staged.
    And within "120s" the Iceberg catalog gate holds a commit no sooner than "10s" after it refused one
    And the temp directory holds 2 Iceberg Arrow IPC staged batches
    And batch "rows" remains pending for "1s"
    # The retried commit appends only what it began with. The newer row waits for a commit of its
    # own, and the batch that carries it stays unacknowledged until the catalog holds that row too.
    When the Iceberg catalog gate passes the commit it holds
    Then within "60s" the Iceberg catalog gate holds a commit
    And the Iceberg table "refused_notifications_{{test_id}}" holds 1 rows
    And batch "rows" remains pending for "1s"
    When the Iceberg catalog gate passes every commit
    Then batch "rows" completes
    And within "60s" the Iceberg table "refused_notifications_{{test_id}}" holds exactly these rows
      """
      {"user_id":71,"action":"first"}
      {"user_id":72,"action":"second"}
      """
    And within "30s" DESCRIBE EMITTER "iceberg_refused" on the leader node reports "messages_total" "sent" relay "notifications" totalling "2"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @iceberg_commit_whole_class
  Scenario Outline: An Iceberg staged file larger than the bulk memory class commits only once the whole class is free
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "oversized_messages_{{test_id}}" exists at "s3://nervix-iceberg/tables/oversized_messages_{{test_id}}" with columns
      """
      value I64
      message STRING
      """
    # Each payload reaches the emitter as a relay batch of its own, below the 32 MiB a relay frame
    # carries between nodes. The emitter collects both on its own node into one input batch, which
    # it stages as one file larger than the 32 MiB bulk memory class.
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA large_message (value I64, message STRING);
      CREATE WIRE JSON SCHEMA large_message_wire MODE STRICT (value integer, message string);
      CREATE CODEC large_message_codec FROM WIRE JSON SCHEMA large_message_wire TO SCHEMA large_message;
      CREATE RELAY large_messages SCHEMA large_message UNBRANCHED;
      CREATE VHOST edge iceberg-oversized-{{test_id}}.example.com;
      CREATE ENDPOINT large_messages_endpoint ON edge PATH '/messages' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_large_messages
        FROM large_messages_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 64MiB DECODE USING large_message_codec
        TO large_messages INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
      CREATE ICEBERG TABLE EMITTER iceberg_oversized FROM large_messages
        COLLECT FOR 1h MAX BATCH SIZE 33MiB
        TO s3_main TABLE 'oversized_messages_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/oversized_messages_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 1h MAX SIZE 1B MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'value' = input.value, 'message' = input.message } FLUSH EACH 100ms MAX BATCH SIZE 64MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # The scenario holds half of every node's bulk memory class before any row arrives, so the
    # commit can read the staged file only once the scenario gives that half back.
    When bulk memory on every node keeps only "16777216" bytes free
    And http payload with value 81 and a 17825792 byte message is posted to host "iceberg-oversized-{{test_id}}.example.com" path "/messages"
    And http payload with value 82 and a 17825792 byte message is posted to host "iceberg-oversized-{{test_id}}.example.com" path "/messages"
    Then within "120s" DESCRIBE EMITTER "iceberg_oversized" on the leader node reports an Iceberg commit waiting for the whole bulk memory class, whose staged file bytes are saved as placeholder "staged_bytes"
    And the Iceberg table "oversized_messages_{{test_id}}" holds 0 rows
    When bulk memory on every node is released
    Then within "120s" the Iceberg table "oversized_messages_{{test_id}}" holds 2 rows
    And within "30s" DESCRIBE EMITTER "iceberg_oversized" on the leader node reports "bytes_total" "sent" relay "large_messages" totalling "{{staged_bytes}}"
    And within "30s" every node reserves only the bulk memory the scenario holds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
