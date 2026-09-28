Feature: WebSocket client name resolution

  Scenario Outline: WebSocket clients reconnect through a changed DNS answer
    Given the HTTP mock server is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the WebSocket endpoint "{{mock_ws_addr}}" is forwarded as "websocket.nervix.test" from fixture addresses "127.0.5.1,127.0.5.2,127.0.5.4"
    And the DNS fixture answers "websocket.nervix.test" with addresses "127.0.5.3,127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE SIGNALING PROTOCOL initial_frame
        FORMAT JSON
        ON CONNECT
        SEND JAQ '{op: "subscribe"}'
        WAIT JAQ '.user_id == 42' ACCEPT DATA
        TIMEOUT 5s;
      CREATE CLIENT ws_main TYPE WEBSOCKETS WITH SIGNALING PROTOCOL initial_frame CONFIG {
        'endpoint' = '{{websocket_forwarded_addr}}/ws/{{test_id}}'
      };
      CREATE INGESTOR ws_notifications
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      ready: true
      """
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the websocket client test server sends a payload
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":43}
      """
    When the DNS fixture answers "websocket.nervix.test" with "no addresses"
    And the TCP forwarder at "127.0.5.1" stops
    Then within "20s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      ready: false
      """
    When the DNS fixture answers "websocket.nervix.test" with addresses "127.0.5.2"
    Then the TCP forwarder at "127.0.5.2" eventually accepts a connection
    And within "10s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      ready: true
      """
    When the websocket client test server sends a payload
      """
      {"user_id":44}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":44}
      """
    When the DNS fixture answers "websocket.nervix.test" with "<outage_answer>"
    And the TCP forwarder at "127.0.5.2" stops
    Then within "35s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      <lookup_failure>
      """
    When the DNS fixture answers "websocket.nervix.test" with addresses "127.0.5.4"
    Then the TCP forwarder at "127.0.5.4" eventually accepts a connection
    And within "10s" DESCRIBE INGESTOR "ws_notifications" on the leader node contains
      """
      ready: true
      """
    When the websocket client test server sends a payload
      """
      {"user_id":45}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":45}
      """

    Examples:
      | cluster_size | outage_answer  | lookup_failure                           |
      | 1            | name not found | the name does not exist                  |
      | 3            | silence        | resolving 'websocket.nervix.test' failed |
