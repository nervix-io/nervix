Feature: Pulsar ingestion
  Scenario Outline: Pulsar ingestor delivers JSON payloads to a subscribed relay
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT pulsar_main
        TYPE PULSAR
        CONFIG {
          'addr' = 'pulsar://127.0.0.1:6650'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_pulsar_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR pulsar_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_pulsar_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM PULSAR pulsar_main
        TOPIC notifications_{{test_id}}
        SUBSCRIPTION nervix_cucumber_{{test_id}}
        INSTANCES <instances>
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When Pulsar message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 1         | 0             |
      | 3            | 2         | 0             |
      | 3            | 1         | 1             |
      | 3            | 2         | 1             |

  Scenario Outline: Pulsar ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "pulsar_notifications" enters fault mode
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT pulsar_main
        TYPE PULSAR
        CONFIG {
          'addr' = 'pulsar://127.0.0.1:6650'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS BRANCH by_pulsar_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR pulsar_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_pulsar_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM PULSAR pulsar_main
        TOPIC notifications_reconnect_{{test_id}}
        SUBSCRIPTION nervix_cucumber_reconnect_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "pulsar_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "pulsar_notifications" leaves fault mode
    And Pulsar message is published to topic "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "pulsar_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
