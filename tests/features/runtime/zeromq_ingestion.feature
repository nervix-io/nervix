Feature: ZeroMQ ingestion
  Scenario Outline: ZeroMQ ingestor delivers JSON payloads to a subscribed relay
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
        CREATE IF NOT EXISTS BRANCH by_zeromq_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_zeromq_notifications;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };
        CREATE ZEROMQ PULL INGESTOR zeromq_notifications
        FROM zeromq_main MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_zeromq_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And ZeroMQ message is published
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

  Scenario Outline: ZeroMQ ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "zeromq_notifications" enters fault mode
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
        CREATE IF NOT EXISTS BRANCH by_zeromq_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_zeromq_notifications;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };
        CREATE ZEROMQ PULL INGESTOR zeromq_notifications
        FROM zeromq_main MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_zeromq_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "zeromq_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "zeromq_notifications" leaves fault mode
    And ZeroMQ message is published
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "zeromq_notifications" on the leader node contains
      """
      transient error: -
      """
    # A ZeroMQ source keeps no transport position, so its reconnect discards nothing for the
    # broker to redeliver.
    And within "5s" DESCRIBE INGESTOR "zeromq_notifications" on the leader node contains
      """
      nervix_ingestor_transport_redeliveries_total: 0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: ZeroMQ ingestor renders its client address over a mounted resource
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zeromq_feed_dir" containing
      """
      {
        "feed.txt": "ZeroMQ notifications feed\n"
      }
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE zeromq_feed;
      UPLOAD RESOURCE zeromq_feed VERSION '{{zeromq_feed_dir}}';
      """
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
        CREATE IF NOT EXISTS BRANCH by_zeromq_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_zeromq_notifications;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        MOUNT zeromq_feed VERSION 1
        CONFIG {
          'addr' = '{% if zeromq_feed %}{{zeromq_ingest_addr}}{% endif %}',
          'bind' = 'true'
        };
        CREATE ZEROMQ PULL INGESTOR zeromq_notifications
        FROM zeromq_main MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_zeromq_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "30s" DESCRIBE INGESTOR "zeromq_notifications" on the leader node contains
      """
      ready: true
      """
    When ZeroMQ message is published
      """
      {"user_id":44}
      """
    Then the relay subscription receives a payload
      """
      "user_id":44
      """
    And the last relay subscription payload contains key fragment '{"user_id":44}'
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RESOURCE zeromq_feed;
      """
    Then the last command output contains
      """
      usages:
      - kind=client name=zeromq_main version=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
