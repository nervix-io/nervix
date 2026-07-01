Feature: Postgres emission
  Scenario Outline: Postgres emitter inserts mapped rows from a relay
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
          'client_id' = 'nervix-cucumber-postgres-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC postgres_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT postgres_client
        TYPE POSTGRES
        CONFIG {
          'addr' = 'host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres'
        };
        CREATE EMITTER to_pg
        FROM notifications
        TO POSTGRES postgres_client INSERT TO TABLE notifications_pg_out_{{test_id}}
        VALUES {
          "postgres_user_id" = notifications.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(notifications.action)
        }
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        SUBSCRIBE SESSION TO notifications;
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
          'client_id' = 'nervix-cucumber-postgres-conflict-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC postgres_conflict_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT postgres_client
        TYPE POSTGRES
        CONFIG {
          'addr' = 'host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres'
        };
        CREATE EMITTER to_pg
        FROM notifications
        TO POSTGRES postgres_client INSERT TO TABLE notifications_pg_conflict_{{test_id}}
        VALUES {
          "postgres_user_id" = notifications.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(notifications.action)
        }
        ON CONFLICT <conflict_target> <conflict_action>
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        SUBSCRIBE SESSION TO notifications;
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
