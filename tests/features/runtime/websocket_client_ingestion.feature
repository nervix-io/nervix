Feature: Websocket client ingestion
  Scenario Outline: Websocket client ingestor connects to a remote endpoint and delivers a JSON payload
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS BRANCH by_ws_notifications BY user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = 'ws://127.0.0.1:18080/ws/{{test_id}}'
        };

      CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      """
    And these NSPL commands are executed
      """
      CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "10s" the observed broker receives payloads
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Websocket client ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "ws_notifications" enters fault mode
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_ws_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = 'ws://127.0.0.1:18080/ws/{{test_id}}'
        };
        CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "ws_notifications" leaves fault mode
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And within "5s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
