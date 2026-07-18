Feature: MySQL emission
  Scenario Outline: MySQL emitter inserts mapped rows from a relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "notifications_mysql_out_{{test_id}}" exists
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
          'client_id' = 'nervix-cucumber-mysql-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }

        FROM MQTT mqtt_ingress
        TOPIC mysql_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE CLIENT mysql_client
        TYPE MYSQL
        CONFIG {
          'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
        };
        CREATE EMITTER to_mysql
        FROM notifications
        TO MYSQL mysql_client INSERT TO TABLE notifications_mysql_out_{{test_id}}
        VALUES {
          "mysql_user_id" = notifications.user_id,
          "mysql_now" = NOW() AS STRING,
          "mysql_action" = LOWER(notifications.action)
        }
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "to_mysql" enters stall mode
    Then within "10s" repeatedly publishing MQTT message to topic "mysql_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "5s" DESCRIBE EMITTER "to_mysql" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "to_mysql" leaves fault mode
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":42,"mysql_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: MySQL emitter handles insert conflicts with <conflict_action>
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "notifications_mysql_conflict_{{test_id}}" with primary key exists
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
          'client_id' = 'nervix-cucumber-mysql-conflict-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }

        FROM MQTT mqtt_ingress
        TOPIC mysql_conflict_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE CLIENT mysql_client
        TYPE MYSQL
        CONFIG {
          'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
        };
        CREATE EMITTER to_mysql
        FROM notifications
        TO MYSQL mysql_client INSERT TO TABLE notifications_mysql_conflict_{{test_id}}
        VALUES {
          "mysql_user_id" = notifications.user_id,
          "mysql_now" = NOW() AS STRING,
          "mysql_action" = LOWER(notifications.action)
        }
        ON CONFLICT <conflict_action>
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "mysql_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "10s" repeatedly publishing MQTT message to topic "mysql_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"CLOSE"}
      """
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":42,"mysql_action":"<expected_action>"}
      """

    Examples:
      | cluster_size | replica_count | conflict_action | expected_action |
      | 1            | 0             | DO UPDATE       | close           |
      | 3            | 0             | DO UPDATE       | close           |
      | 3            | 1             | DO UPDATE       | close           |
      | 1            | 0             | DO NOTHING      | open            |
      | 3            | 0             | DO NOTHING      | open            |
      | 3            | 1             | DO NOTHING      | open            |
