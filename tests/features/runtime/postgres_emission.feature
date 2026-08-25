Feature: Postgres emission
  Scenario Outline: Postgres emitter inserts mapped rows from a relay
    Given MQTT is running
    And Postgres is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "notifications_pg_out_{{test_id}}" exists
    When these NSPL commands are executed
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
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-postgres-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC postgres_notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT postgres_client
        TYPE POSTGRES
        CONFIG {
          'addr' = '{{postgres_addr}}'
        };
        CREATE EMITTER to_pg FROM notifications TO POSTGRES postgres_client INSERT TO TABLE notifications_pg_out_{{test_id}} VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) } WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "to_pg" enters stall mode
    Then within "10s" repeatedly publishing MQTT message to topic "postgres_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "5s" DESCRIBE EMITTER "to_pg" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "to_pg" leaves fault mode
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":42,"postgres_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Postgres emitter handles insert conflicts with <conflict_action>
    Given MQTT is running
    And Postgres is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "notifications_pg_conflict_{{test_id}}" with primary key exists
    When these NSPL commands are executed
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
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-postgres-conflict-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC postgres_conflict_notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT postgres_client
        TYPE POSTGRES
        CONFIG {
          'addr' = '{{postgres_addr}}'
        };
        CREATE EMITTER to_pg FROM notifications TO POSTGRES postgres_client INSERT TO TABLE notifications_pg_conflict_{{test_id}} VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) } ON CONFLICT <conflict_target> <conflict_action> WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "postgres_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "10s" repeatedly publishing MQTT message to topic "postgres_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"CLOSE"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":42,"postgres_action":"<expected_action>"}
      """

    Examples:
      | cluster_size | replica_count | conflict_target      | conflict_action | expected_action |
      | 1            | 0             | ("postgres_user_id") | DO UPDATE       | close           |
      | 3            | 0             | ("postgres_user_id") | DO UPDATE       | close           |
      | 3            | 1             | ("postgres_user_id") | DO UPDATE       | close           |
      | 1            | 0             | ("postgres_user_id") | DO NOTHING      | open            |
      | 3            | 0             | ("postgres_user_id") | DO NOTHING      | open            |
      | 3            | 1             | ("postgres_user_id") | DO NOTHING      | open            |
      | 1            | 0             |                      | DO NOTHING      | open            |
      | 3            | 0             |                      | DO NOTHING      | open            |

  @database_emitter_modes @poison_isolation
  Scenario Outline: Postgres poison isolation delivers healthy rows and routes only the rejected record
    Given MQTT is running
    And Postgres is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "poison_pg_{{test_id}}" rejecting poison actions exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        source_user_id I64,
        source_action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE CLIENT mqtt_ingress
      TYPE MQTT
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-postgres-poison-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC postgres_poison_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT postgres_client
      TYPE POSTGRES
      CONFIG {
        'addr' = '{{postgres_addr}}'
      };
      CREATE EMITTER to_pg
      FROM notifications
      TO POSTGRES postgres_client INSERT TO TABLE poison_pg_{{test_id}}
      VALUES {
        "postgres_user_id" = input.user_id,
        "postgres_now" = NOW() AS STRING,
        "postgres_action" = LOWER(input.action)
      }
      WITH MAX BATCH 10
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          source_user_id = input.user_id,
          source_action = input.action
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    When these MQTT messages are rapidly published to topic "postgres_poison_in_{{test_id}}"
      """
      {"user_id":1,"action":"HEALTHY_A"}
      {"user_id":2,"action":"POISON"}
      {"user_id":3,"action":"HEALTHY_B"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "source_action":"POISON","source_user_id":2
      """
    And the relay subscription does not receive a payload within "1s"
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":1,"postgres_action":"healthy_a"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":3,"postgres_action":"healthy_b"}
      """
    And the Postgres table eventually contains exactly 2 rows

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @database_emitter_modes @values_error_isolation
  Scenario Outline: Postgres VALUES errors reject only the affected record
    Given MQTT is running
    And Postgres is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "values_error_pg_{{test_id}}" exists
    When these NSPL commands are executed
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
      CREATE CLIENT mqtt_ingress
      TYPE MQTT
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-postgres-values-error-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC postgres_values_error_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT postgres_client
      TYPE POSTGRES
      CONFIG {
        'addr' = '{{postgres_addr}}'
      };
      CREATE EMITTER to_pg
      FROM notifications
      TO POSTGRES postgres_client INSERT TO TABLE values_error_pg_{{test_id}}
      VALUES {
        "postgres_user_id" = input.user_id / input.denominator,
        "postgres_now" = NOW() AS STRING,
        "postgres_action" = LOWER(input.action)
      }
      WITH MAX BATCH 10
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          error_operation = error.operation,
          source_user_id = input.user_id
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    When these MQTT messages are rapidly published to topic "postgres_values_error_in_{{test_id}}"
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
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":5,"postgres_action":"healthy_a"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":4,"postgres_action":"healthy_b"}
      """
    And the Postgres table eventually contains exactly 2 rows

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @database_emitter_modes @max_batch @postgres_max_batch
  Scenario Outline: Postgres WITH MAX BATCH splits one oversized flush into multiple inserts
    Given MQTT is running
    And Postgres is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "batch_pg_{{test_id}}" recording insert statement sizes exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT mqtt_ingress TYPE MQTT CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-postgres-batch-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC postgres_batch_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT postgres_client TYPE POSTGRES CONFIG {
        'addr' = '{{postgres_addr}}'
      };
      CREATE EMITTER to_pg
      FROM notifications
      TO POSTGRES postgres_client INSERT TO TABLE batch_pg_{{test_id}}
      VALUES { "postgres_user_id" = input.user_id }
      WITH MAX BATCH 2
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When 5 JSON messages with user id 42 are rapidly published to "MQTT" input "postgres_batch_in_{{test_id}}"
    Then the Postgres table eventually contains 5 rows across at least 3 inserts of at most 2 rows

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
