Feature: Server ingestor scheduling
  Scenario: HTTP endpoint ingestors accept traffic on every live node
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }

        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    Then node "node-1" eventually forwards http traffic for host "http-{{test_id}}.example.com" path "/ingest" to the observed broker
      """
      {"user_id":401}
      """
    And node "node-2" eventually forwards http traffic for host "http-{{test_id}}.example.com" path "/ingest" to the observed broker
      """
      {"user_id":402}
      """
    And node "node-3" eventually forwards http traffic for host "http-{{test_id}}.example.com" path "/ingest" to the observed broker
      """
      {"user_id":403}
      """

  Scenario: Websocket endpoint ingestors accept traffic on every live node
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE VHOST edge ws-{{test_id}}.example.com;
        CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
        CREATE INGESTOR ws_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }

        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    Then node "node-1" eventually forwards websocket traffic for host "ws-{{test_id}}.example.com" path "/ws" to the observed broker
      """
      {"user_id":501}
      """
    And node "node-2" eventually forwards websocket traffic for host "ws-{{test_id}}.example.com" path "/ws" to the observed broker
      """
      {"user_id":502}
      """
    And node "node-3" eventually forwards websocket traffic for host "ws-{{test_id}}.example.com" path "/ws" to the observed broker
      """
      {"user_id":503}
      """

  Scenario: HTTP endpoint ingestors follow node stop and rejoin
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }

        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    Then node "node-3" eventually forwards http traffic for host "http-{{test_id}}.example.com" path "/ingest" to the observed broker
      """
      {"user_id":601}
      """
    When node "node-3" is stopped
    And http payload is posted to node "node-3" with host "http-{{test_id}}.example.com" path "/ingest" and fails
      """
      {"user_id":62}
      """
    When node "node-3" is started
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually forwards http traffic for host "http-{{test_id}}.example.com" path "/ingest" to the observed broker
      """
      {"user_id":602}
      """

  Scenario: Websocket endpoint ingestors follow node stop and rejoin
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE VHOST edge ws-{{test_id}}.example.com;
        CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
        CREATE INGESTOR ws_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }

        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    Then node "node-3" eventually forwards websocket traffic for host "ws-{{test_id}}.example.com" path "/ws" to the observed broker
      """
      {"user_id":701}
      """
    When node "node-3" is stopped
    And websocket message is published to node "node-3" host "ws-{{test_id}}.example.com" path "/ws" and fails
      """
      {"user_id":72}
      """
    When node "node-3" is started
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually forwards websocket traffic for host "ws-{{test_id}}.example.com" path "/ws" to the observed broker
      """
      {"user_id":702}
      """
