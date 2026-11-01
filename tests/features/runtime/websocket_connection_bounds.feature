Feature: Outbound WebSocket connection bounds
  Scenario Outline: A WebSocket connection reports a silent peer through idle_timeout_ms on <cluster_size> nodes
    Given a WebSocket bounds peer blackholes
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = '{{websocket_peer}}', 'idle_timeout_ms' = '250' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "socket_events" on the leader node contains
      """
      idle_timeout_ms 250ms
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSocket handshake releases retained frames in order after completing on <cluster_size> nodes
    Given a WebSocket bounds peer negotiates
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE SIGNALING PROTOCOL bounded_handshake FORMAT JSON ON CONNECT
        SEND JAQ '{op: "subscribe"}'
        WAIT JAQ '.open' ACCEPT DATA WAIT JAQ '.ready' TIMEOUT 30s;
      CREATE WEBSOCKETS CLIENT socket WITH SIGNALING PROTOCOL bounded_handshake
        CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    Then within "60s" the relay subscription receives payloads in order
      """
      "user_id":0
      "user_id":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A full WebSocket handshake handoff backpressures until the signaling timeout on <cluster_size> nodes
    Given a WebSocket bounds peer floods
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE SIGNALING PROTOCOL bounded_handshake FORMAT JSON ON CONNECT
        SEND JAQ '{op: "subscribe"}'
        WAIT JAQ '.open' ACCEPT DATA WAIT JAQ '.ready' TIMEOUT 1s;
      CREATE WEBSOCKETS CLIENT socket WITH SIGNALING PROTOCOL bounded_handshake
        CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "socket_events" on the leader node contains
      """
      signaling timed out after 1s; unsatisfied WAIT JAQ matchers: [.ready]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A silent WebSocket peer is reconnected after three ping intervals on <cluster_size> nodes
    Given a WebSocket bounds peer stalls
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = '{{websocket_peer}}', 'ping_interval_ms' = '100' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the WebSocket bounds peer observes a reconnect after the idle bound
    And the relay subscription receives a payload
      """
      "user_id":43
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSocket ping interval paces client pings once signaling completes on <cluster_size> nodes
    Given a WebSocket ping peer holds signaling open for 1500ms
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE SIGNALING PROTOCOL held_handshake FORMAT JSON ON CONNECT
        SEND JAQ '{op: "subscribe"}'
        WAIT JAQ '.ready' TIMEOUT 30s;
      CREATE WEBSOCKETS CLIENT socket WITH SIGNALING PROTOCOL held_handshake
        CONFIG { 'endpoint' = '{{websocket_peer}}', 'ping_interval_ms' = '500', 'idle_timeout_ms' = '30000' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the WebSocket ping peer receives 3 pings at least 500ms apart once signaling completes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
