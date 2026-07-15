Feature: Iceberg emission
  Scenario Outline: Iceberg emitter accepts GCS object storage configuration
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT gcs_main
        TYPE GCS
        CONFIG {
          'service_path' = 'http://127.0.0.1:4443',
          'no_auth' = true
        };

      CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };

      CREATE EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON GCS gcs_main TABLE gcs_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 'gs://nervix-iceberg/tables/gcs_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;

      SHOW CREATE EMITTER iceberg_notifications;
      """
    Then the last command output contains
      """
      TO ICEBERG ON GCS gcs_main TABLE gcs_notifications_{{test_id}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter accepts Azure Blob object storage configuration
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT azure_main
        TYPE AZURE_BLOB
        CONFIG {
          'account_name' = 'devstoreaccount1',
          'account_key' = 'local-key'
        };

      CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };

      CREATE EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON AZURE_BLOB azure_main TABLE azure_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 'wasb://nervix-iceberg@devstoreaccount1.blob.core.windows.net/tables/azure_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;

      SHOW CREATE EMITTER iceberg_notifications;
      """
    Then the last command output contains
      """
      TO ICEBERG ON AZURE_BLOB azure_main TABLE azure_notifications_{{test_id}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter flushes a disk-backed batch to S3
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      created_at DATETIME
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING,
        created_at DATETIME
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
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
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action,
          'created_at' = notifications.created_at
        }
        LOCATION 's3://nervix-iceberg/tables/notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
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

  Scenario Outline: Iceberg emitter requires an existing catalog table
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-missing-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_missing_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE DETACHED EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE missing_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/missing_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
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

  Scenario Outline: Iceberg emitters use explicitly provisioned catalog tables
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-namespace-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_namespace_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE EMITTER iceberg_notifications_a
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE namespace_notifications_a_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/namespace_notifications_a_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
        CREATE EMITTER iceberg_notifications_b
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE namespace_notifications_b_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/namespace_notifications_b_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
        CREATE EMITTER iceberg_notifications_c
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE namespace_notifications_c_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/namespace_notifications_c_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
        CREATE EMITTER iceberg_notifications_d
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE namespace_notifications_d_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/namespace_notifications_d_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
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
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And temporary files use a custom temp directory
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "temp_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/temp_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-temp-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_temp_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE temp_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/temp_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 10s MAX SIZE 512MiB;
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

  Scenario Outline: Iceberg emitter commits staged IPC batches during graceful shutdown
    Given graceful shutdown drain is enabled
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And temporary files use a custom temp directory
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "shutdown_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/shutdown_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-shutdown-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_shutdown_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE shutdown_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/shutdown_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 1h MAX SIZE 512MiB;
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

  Scenario Outline: Iceberg emitter reports initialization errors instead of a half-initialized sink
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-init-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_init_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE DETACHED EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE init_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 'http://nervix-iceberg/tables/init_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
        START;
      """
    And MQTT message is published to topic "iceberg_init_notifications_in_{{test_id}}"
      """
      {"user_id":45,"action":"INIT"}
      """
    Then within "5s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      table location 'http://nervix-iceberg/tables/init_notifications_{{test_id}}' must use s3://
      """
    And the last command output does not contain
      """
      Iceberg sink client is not initialized
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Iceberg emitter holds ACK until the append succeeds
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "ack_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/ack_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-iceberg-ack-{{test_id}}'
        };
        CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9900',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
        CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = 'http://127.0.0.1:8181',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC iceberg_ack_notifications_in_{{test_id}}
        SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE EMITTER iceberg_notifications
        FROM notifications
        TO ICEBERG ON S3 s3_main TABLE ack_notifications_{{test_id}}
        VALUES {
          'user_id' = notifications.user_id,
          'action' = notifications.action
        }
        LOCATION 's3://nervix-iceberg/tables/ack_notifications_{{test_id}}'
        CATALOG iceberg_catalog
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        COMMIT EACH 100ms MAX SIZE 1MiB;
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

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
