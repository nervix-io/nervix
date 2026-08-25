Feature: MongoDB emission
  Scenario Outline: MongoDB emitter inserts mapped documents from a relay
    Given MQTT is running
    And MongoDB is running
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
          'client_id' = 'nervix-cucumber-mongodb-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC mongodb_notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT mongodb_client
        TYPE MONGODB
        CONFIG {
          'addr' = '{{mongodb_addr}}',
          'database' = 'nervix'
        };
        CREATE EMITTER to_mongodb FROM notifications TO MONGODB mongodb_client INSERT TO COLLECTION notifications_mongodb_out_{{test_id}} VALUES { "mongodb_user_id" = input.user_id, "mongodb_now" = NOW() AS STRING, "mongodb_action" = LOWER(input.action) } WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given MQTT is running
    And MongoDB is running
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
          'client_id' = 'nervix-cucumber-mongodb-conflict-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC mongodb_conflict_notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT mongodb_client
        TYPE MONGODB
        CONFIG {
          'addr' = '{{mongodb_addr}}',
          'database' = 'nervix'
        };
        CREATE EMITTER to_mongodb FROM notifications TO MONGODB mongodb_client INSERT TO COLLECTION notifications_mongodb_conflict_{{test_id}} VALUES { "mongodb_user_id" = input.user_id, "mongodb_now" = NOW() AS STRING, "mongodb_action" = LOWER(input.action) } ON CONFLICT ("mongodb_user_id") <conflict_action> WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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

  @database_emitter_modes @poison_isolation
  Scenario Outline: MongoDB poison isolation delivers healthy documents and routes only the rejected record
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "poison_mongodb_{{test_id}}" rejecting poison actions exists
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
        'client_id' = 'nervix-cucumber-mongodb-poison-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC mongodb_poison_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT mongodb_client
      TYPE MONGODB
      CONFIG {
        'addr' = '{{mongodb_addr}}',
        'database' = 'nervix'
      };
      CREATE EMITTER to_mongodb
      FROM notifications
      TO MONGODB mongodb_client INSERT TO COLLECTION poison_mongodb_{{test_id}}
      VALUES {
        "mongodb_user_id" = input.user_id,
        "mongodb_now" = NOW() AS STRING,
        "mongodb_action" = LOWER(input.action)
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
    When these MQTT messages are rapidly published to topic "mongodb_poison_in_{{test_id}}"
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
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":1,"mongodb_action":"healthy_a"}
      """
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":3,"mongodb_action":"healthy_b"}
      """
    And the MongoDB collection eventually contains exactly 2 documents

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @database_emitter_modes @max_batch @mongodb_max_batch
  Scenario Outline: MongoDB WITH MAX BATCH splits one oversized flush into multiple inserts
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "batch_mongodb_{{test_id}}" recording insert command sizes exists
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
        'client_id' = 'nervix-cucumber-mongodb-batch-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC mongodb_batch_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT mongodb_client TYPE MONGODB CONFIG {
        'addr' = '{{mongodb_addr}}',
        'database' = 'nervix'
      };
      CREATE EMITTER to_mongodb
      FROM notifications
      TO MONGODB mongodb_client INSERT TO COLLECTION batch_mongodb_{{test_id}}
      VALUES { "mongodb_user_id" = input.user_id }
      WITH MAX BATCH 2
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When 5 JSON messages with user id 42 are rapidly published to "MQTT" input "mongodb_batch_in_{{test_id}}"
    Then the MongoDB collection eventually contains 5 documents across at least 3 inserts of at most 2 documents

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
