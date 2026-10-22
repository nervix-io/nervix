Feature: Websocket client ingestion
  Scenario Outline: Websocket client ingestor connects to a remote endpoint and delivers a JSON payload
    Given Kafka is running
    And the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the WebSocket mock endpoints are published under fixture DNS
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
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
      CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;

      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE WEBSOCKETS CLIENT ws_main
        CONFIG {
          'endpoint' = '{{mock_ws_dns_addr}}/ws/{{test_id}}'
        };

      CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And these Plexus commands are executed
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
        FROM ws_main MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_ws_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "10s" the observed broker receives payloads
      """
      {"user_id":42}
      """
    And the DNS fixture eventually receives a question for "websocket.nervix.test"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Websocket client ingestor reports transient source failures and recovers
    Given the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "ws_notifications" enters fault mode
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
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE WEBSOCKETS CLIENT ws_main
        CONFIG {
          'endpoint' = '{{mock_ws_addr}}/ws/{{test_id}}'
        };
        CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
        FROM ws_main MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_ws_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
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

  @websocket_source_lifecycle @deloxide_stress
  Scenario Outline: Dropping a WebSocket ingestor sends its peer a Close frame on <cluster_size> nodes
    Given a WebSocket lifecycle peer closes its first 0 connections
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE WEBSOCKETS CLIENT ws_main CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
        FROM ws_main MODE NO ACK ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When these Plexus statements are executed on the leader node
      """
      DROP INGESTOR ws_notifications;
      """
    Then the WebSocket lifecycle peer receives a Close frame

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @websocket_source_lifecycle @deloxide_stress
  Scenario Outline: A repeatedly closing WebSocket peer is reconnected with growing delays on <cluster_size> nodes
    Given a WebSocket lifecycle peer closes its first 3 connections
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE WEBSOCKETS CLIENT ws_main CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
        FROM ws_main MODE NO ACK ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then the WebSocket lifecycle peer observes growing reconnect delays
    And within "10s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      transient error: WebSocket peer closed with code 1013: fixture restart
      """
    And within "10s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      reconnect backoff: 1s
      """
    When the WebSocket lifecycle peer sends its payload
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And within "10s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @websocket_source_lifecycle @deloxide_stress
  Scenario: Moving a WebSocket ingestor closes its former session gracefully
    Given a WebSocket lifecycle peer closes its first 0 connections
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE WEBSOCKETS CLIENT ws_main CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
        FROM ws_main MODE NO ACK ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "ws_notifications" is saved as placeholder "original_ws_owner"
    When these Plexus statements are executed on the leader node
      """
      DRAIN NODE {{original_ws_owner}};
      """
    Then the WebSocket lifecycle peer receives a Close frame
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
