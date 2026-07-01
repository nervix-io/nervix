Feature: NATS ingestion
  Scenario Outline: NATS ingestor delivers JSON payloads to a subscribed relay
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
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = 'nats://127.0.0.1:4222'
        };
        CREATE INGESTOR nats_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_nats_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM NATS nats_main
        SUBJECT notifications_{{test_id}}
        QUEUE GROUP nats_notifications_group_{{test_id}}
        INSTANCES <instances>
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    And NATS message is published to subject "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    And NATS message is published to subject "notifications_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "10s" the relay subscription receives payloads
      """
      "user_id":42
      "user_id":43
      """

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 2         | 1             |

  Scenario Outline: NATS ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "nats_notifications" enters fault mode
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
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = 'nats://127.0.0.1:4222'
        };
        CREATE INGESTOR nats_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_nats_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM NATS nats_main
        SUBJECT notifications_reconnect_{{test_id}}
        QUEUE GROUP nats_notifications_reconnect_group_{{test_id}}
        INSTANCES 1
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "nats_notifications" leaves fault mode
    Then within "5s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: -
      """
    And within "10s" repeatedly publishing NATS message to subject "notifications_reconnect_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":43}
      """
    And the last relay subscription payload contains key fragment '{"user_id":43}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
