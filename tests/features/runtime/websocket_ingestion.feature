Feature: Websocket endpoint ingestion
  Scenario Outline: Websocket endpoint ingestor delivers JSON payloads to a subscribed relay
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
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE VHOST edge ws-{{test_id}}.example.com;
        CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
        CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    And websocket message is published to host "ws-{{test_id}}.example.com" path "/ws"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Secure websocket endpoint ingestor delivers JSON payloads to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "tls_bundle" for hosts "ws-{{test_id}}.example.com"
    When these NSPL commands are executed
      """
      CREATE RESOURCE tls_bundle;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE tls_bundle VERSION "{{tls_bundle}}";
      """
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
      CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
      CREATE VHOST edge ws-{{test_id}}.example.com WITH TLS tls_bundle;
      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
      CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ws_notifications_endpoint
        MODE NO_ACK SEQUENTIAL
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And secure websocket message is published to host "ws-{{test_id}}.example.com" path "/ws" using CA from resource directory "tls_bundle"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Websocket endpoint signaling buffers schema payloads until the handshake completes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        seq I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL binance_style_subscribe
        ON CONNECT
        SEND BODY '{"method":"SUBSCRIBE","params":["btcusdt@aggTrade"],"id":1}',
                  '{"method":"SUBSCRIBE","params":["btcusdc@aggTrade"],"id":2}'
        WAIT BODY '{"id":1,"result":null}', '{"id":2,"result":null}' TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL binance_style_subscribe;

      CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ws_notifications_endpoint
        MODE NO_ACK SEQUENTIAL
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And websocket text frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"method":"SUBSCRIBE","params":["btcusdt@aggTrade"],"id":1}
      EXPECT {"method":"SUBSCRIBE","params":["btcusdc@aggTrade"],"id":2}
      SEND {"seq":1}
      SEND {"id":1,"result":null}
      SEND {"seq":2}
      SEND {"id":2,"result":null}
      SEND {"seq":3}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      "seq":2
      "seq":3
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
