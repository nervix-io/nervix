Feature: Websocket signaling protocols
  Scenario Outline: Websocket endpoint signaling matches out-of-order acknowledgements with JAQ despite extra dynamic fields
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL binance_style_subscribe
        FROM JSON
        ON CONNECT
        SEND JAQ '{method: "SUBSCRIBE", params: ["btcusdt@aggTrade"], id: 1}',
                 '{method: "SUBSCRIBE", params: ["btcusdc@aggTrade"], id: 2}'
        WAIT JAQ '.id == 1 and .result == null',
                 '.id == 2 and .result == null'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL binance_style_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"method": "SUBSCRIBE", "params": ["btcusdt@aggTrade"], "id": 1}
      EXPECT {"method": "SUBSCRIBE", "params": ["btcusdc@aggTrade"], "id": 2}
      SEND {"seq":1}
      SEND {"id":2,"result":null,"conn_id":"1f0c7b2e","ts":1712000001}
      SEND {"seq":2}
      SEND {"id":1,"result":null,"conn_id":"1f0c7b2e","ts":1712000000}
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

  Scenario Outline: Websocket endpoint signaling rejects the session when a FAIL JAQ matcher fires
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL rejecting_subscribe
        FROM JSON
        ON CONNECT
        SEND JAQ '{method: "SUBSCRIBE", id: 1}'
        WAIT JAQ '.id == 1 and .result == null'
        FAIL JAQ '.error'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL rejecting_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"method": "SUBSCRIBE", "id": 1}
      SEND {"seq":9}
      SEND {"error":"subscription denied","id":1}
      EXPECT CLOSE
      """
    Then the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling exchanges RAW text frames
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL raw_handshake
        FROM RAW
        ON CONNECT
        SEND JAQ '"HELLO"'
        WAIT JAQ '. == "WELCOME"'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL raw_handshake;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT HELLO
      SEND {"seq":1}
      SEND WELCOME
      SEND {"seq":2}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      "seq":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling completes a protobuf handshake from a compiled resource
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "signaling.proto": "syntax = \"proto3\";\npackage nervix.test;\n\nmessage Subscribe {\n  uint32 id = 1;\n}\n\nmessage Ack {\n  uint32 id = 1;\n}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE RESOURCE proto_bundle;
      """
    And these NSPL commands are executed
      """
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        seq I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL protobuf_subscribe
        FROM PROTOBUF USING RESOURCE proto_bundle VERSION 1
          CONFIG {'file' = 'signaling.proto', 'include' = '.'}
          SEND MESSAGE 'nervix.test.Subscribe'
          WAIT MESSAGE 'nervix.test.Ack'
        ON CONNECT
        SEND JAQ '{id: 1}'
        WAIT JAQ '.id == 1'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL protobuf_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT BASE64 CAE=
      SEND BASE64 CAE=
      SEND {"seq":1}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling carries captured state into a later phase
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL authenticated_subscribe
        FROM JSON
        ON CONNECT
        SEND JAQ '{op: "auth", key: "public"}'
        WAIT JAQ '.op == "auth" and .success' CAPTURE '{token: .data.token}'
        SEND JAQ '{op: "subscribe", token: $state.token, id: 1}'
        WAIT JAQ '.id == 1 and .success'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL authenticated_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"op": "auth", "key": "public"}
      SEND {"op":"auth","success":true,"data":{"token":"tok-7f3a"}}
      EXPECT {"op": "subscribe", "token": "tok-7f3a", "id": 1}
      SEND {"id":1,"success":true}
      SEND {"seq":1}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling withholds a later phase until the current one is acknowledged
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL sequenced_subscribe
        FROM JSON
        ON CONNECT
        SEND JAQ '{op: "first"}'
        WAIT JAQ '.acked == "first"'
        SEND JAQ '{op: "second"}'
        WAIT JAQ '.acked == "second"'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL sequenced_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"op": "first"}
      EXPECT SILENCE 500ms
      SEND {"acked":"first"}
      EXPECT {"op": "second"}
      SEND {"acked":"second"}
      SEND {"seq":1}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling answers a server challenge before sending anything
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL challenge_response
        FROM JSON
        ON CONNECT
        WAIT JAQ '.challenge' CAPTURE '{nonce: .challenge}'
        SEND JAQ '{op: "answer", nonce: $state.nonce}'
        WAIT JAQ '.accepted'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL challenge_response;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT SILENCE 500ms
      SEND {"challenge":"n-42"}
      EXPECT {"op": "answer", "nonce": "n-42"}
      SEND {"accepted":true}
      SEND {"seq":1}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling passes data through once its ACCEPT DATA matcher is satisfied
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL passthrough_subscribe
        FROM JSON
        ON CONNECT
        SEND JAQ '{op: "subscribe"}', '{op: "subscribe_more"}'
        WAIT JAQ '.subscribed' ACCEPT DATA
        WAIT JAQ '.subscribed_more'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL passthrough_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"op": "subscribe"}
      EXPECT {"op": "subscribe_more"}
      SEND {"seq":1}
      SEND {"subscribed":true}
      SEND {"seq":2}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "seq":1
      "seq":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Websocket endpoint signaling withholds held data when ACCEPT DATA is never reached
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE SIGNALING PROTOCOL buffered_subscribe
        FROM JSON
        ON CONNECT
        SEND JAQ '{op: "subscribe"}'
        WAIT JAQ '.subscribed'
        TIMEOUT 5s;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL buffered_subscribe;

      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}.example.com" path "/ws"
      """
      EXPECT {"op": "subscribe"}
      SEND {"seq":1}
      """
    Then the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
