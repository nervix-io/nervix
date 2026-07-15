Feature: Redis ingestion
  Scenario Outline: Redis ingestor delivers JSON payloads to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
        CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_redis_notifications;
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE INGESTOR redis_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_redis_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM REDIS PUBSUB redis_main
        CHANNEL notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Redis message is published to channel "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Redis ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "redis_notifications" enters fault mode
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
        CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_redis_notifications;
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE INGESTOR redis_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_redis_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM REDIS PUBSUB redis_main
        CHANNEL notifications_reconnect_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "redis_notifications" leaves fault mode
    And Redis message is published to channel "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
