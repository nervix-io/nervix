Feature: Pulsar ingestion
  Scenario Outline: Pulsar ingestor delivers JSON payloads to a subscribed relay
    Given Pulsar is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_pulsar_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_notifications;
        CREATE CLIENT pulsar_main
        TYPE PULSAR
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
        CREATE PULSAR TOPIC INGESTOR pulsar_notifications
        FROM pulsar_main TOPIC 'notifications_{{test_id}}' SUBSCRIPTION 'nervix_cucumber_{{test_id}}' INSTANCES <instances> MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
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

  Scenario Outline: A Pulsar ingestor delivers every payload it reads under MODE <delivery>
    Given Pulsar is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT pulsar_main
      TYPE PULSAR
      CONFIG {
        'addr' = '{{pulsar_addr}}'
      };
      CREATE PULSAR TOPIC INGESTOR pulsar_notifications
      FROM pulsar_main TOPIC 'notifications_modes_{{test_id}}' SUBSCRIPTION 'nervix_cucumber_modes_{{test_id}}' MODE <mode>
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    When Pulsar message is published to topic "notifications_modes_{{test_id}}"
      """
      {"user_id":41}
      """
    And Pulsar message is published to topic "notifications_modes_{{test_id}}"
      """
      {"user_id":42}
      """
    And Pulsar message is published to topic "notifications_modes_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":41
      "user_id":42
      "user_id":43
      """

    Examples:
      | cluster_size | delivery     | mode                                                                                     |
      | 1            | NO ACK       | NO ACK                                                                                   |
      | 3            | NO ACK       | NO ACK                                                                                   |
      | 1            | ACK PARALLEL | ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |
      | 3            | ACK PARALLEL | ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |

  Scenario Outline: Pulsar ingestor reports transient source failures and recovers
    Given Pulsar is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "pulsar_notifications" enters fault mode
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_pulsar_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_notifications;
        CREATE CLIENT pulsar_main
        TYPE PULSAR
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
        CREATE PULSAR TOPIC INGESTOR pulsar_notifications
        FROM pulsar_main TOPIC 'notifications_reconnect_{{test_id}}' SUBSCRIPTION 'nervix_cucumber_reconnect_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
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
