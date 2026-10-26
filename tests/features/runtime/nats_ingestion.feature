Feature: NATS ingestion
  Scenario Outline: NATS ingestor delivers JSON payloads to a subscribed relay
    Given NATS is running
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
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_{{test_id}}' QUEUE GROUP 'nats_notifications_group_{{test_id}}' INSTANCES <instances> MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_nats_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
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
    Given NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "nats_notifications" enters fault mode
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
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_reconnect_{{test_id}}' QUEUE GROUP 'nats_notifications_reconnect_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_nats_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "nats_notifications" leaves fault mode
    Then within "10s" repeatedly publishing NATS message to subject "notifications_reconnect_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":43}
      """
    And the last relay subscription payload contains key fragment '{"user_id":43}'
    And within "5s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # The ingestor reads through an endpoint the scenario controls, so the NATS server can go down for
  # this ingestor alone while the other scenarios keep sharing it. A lost connection ends the
  # subscription and the host reopens it on its reconnect cadence: DESCRIBE INGESTOR shows the failed
  # reconnects and a growing delay. A successful subscription makes the source ready; the previous
  # failure and backoff remain until the next accepted batch proves progress.
  @nats_ingestor_lost_connection
  Scenario Outline: NATS ingestor reports a lost server connection and reconnects on its reconnect cadence
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
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
      CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_controlled_addr}}'
        };
      CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_lost_{{test_id}}' QUEUE GROUP 'nats_notifications_lost_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      ready: true
      """
    When NATS message is published to subject "notifications_lost_{{test_id}}"
      """
      {"user_id":41}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":41
      """
    When the controlled NATS endpoint goes down
    Then within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: failed to connect NATS client
      """
    And within "60s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      reconnect backoff: 8s
      """
    When the controlled NATS endpoint forwards everything
    Then within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      ready: true
      """
    And within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: failed to connect NATS client
      """
    When NATS message is published to subject "notifications_lost_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":43
      """
    And within "30s" DESCRIBE INGESTOR "nats_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
