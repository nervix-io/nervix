Feature: MongoDB emission
  Scenario Outline: MongoDB emitter inserts mapped documents from a relay
    Given runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "notifications_mongodb_out_{{test_id}}" exists
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
          'client_id' = 'nervix-cucumber-mongodb-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }

        FROM MQTT mqtt_ingress
        TOPIC mongodb_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE CLIENT mongodb_client
        TYPE MONGODB
        CONFIG {
          'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
          'database' = 'nervix'
        };
        CREATE EMITTER to_mongodb
        FROM notifications
        TO MONGODB mongodb_client INSERT TO COLLECTION notifications_mongodb_out_{{test_id}}
        VALUES {
          "mongodb_user_id" = notifications.user_id,
          "mongodb_now" = NOW() AS STRING,
          "mongodb_action" = LOWER(notifications.action)
        }
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "to_mongodb" enters stall mode
    Then within "10s" repeatedly publishing MQTT message to topic "mongodb_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "5s" DESCRIBE EMITTER "to_mongodb" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "to_mongodb" leaves fault mode
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":42,"mongodb_action":"open"}
      """

    Examples:
      | nodes | replicas |
      | 1     | 0        |
      | 3     | 0        |
      | 3     | 1        |

  Scenario Outline: MongoDB emitter handles insert conflicts with <conflict_action>
    Given runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "notifications_mongodb_conflict_{{test_id}}" with unique user id exists
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
          'client_id' = 'nervix-cucumber-mongodb-conflict-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }

        FROM MQTT mqtt_ingress
        TOPIC mongodb_conflict_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE CLIENT mongodb_client
        TYPE MONGODB
        CONFIG {
          'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
          'database' = 'nervix'
        };
        CREATE EMITTER to_mongodb
        FROM notifications
        TO MONGODB mongodb_client INSERT TO COLLECTION notifications_mongodb_conflict_{{test_id}}
        VALUES {
          "mongodb_user_id" = notifications.user_id,
          "mongodb_now" = NOW() AS STRING,
          "mongodb_action" = LOWER(notifications.action)
        }
        ON CONFLICT ("mongodb_user_id") <conflict_action>
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "mongodb_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "10s" repeatedly publishing MQTT message to topic "mongodb_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"CLOSE"}
      """
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":42,"mongodb_action":"<expected_action>"}
      """

    Examples:
      | nodes | replicas | conflict_action | expected_action |
      | 1     | 0        | DO UPDATE       | close           |
      | 3     | 0        | DO UPDATE       | close           |
      | 3     | 1        | DO UPDATE       | close           |
      | 1     | 0        | DO NOTHING      | open            |
      | 3     | 0        | DO NOTHING      | open            |
      | 3     | 1        | DO NOTHING      | open            |
