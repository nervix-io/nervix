Feature: WebSockets connection declarations
  Scenario Outline: A WebSockets connection sends literal upgrade headers and exposes response headers and frame types
    Given a WebSocket declaration peer verifies upgrade headers and sends text and binary frames
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE SCHEMA received_event ( user_id I64, frame_type STRING OPTIONAL, upgraded STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA received_event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket HEADERS { 'authorization' = 'Bearer fixture' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL SET frame_type = metadata.frame_type, upgraded = read_header('x-upgrade')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    When the WebSocket lifecycle peer sends its payload
    Then the relay subscription receives a payload
      """
      "frame_type":"text"
      """
    And the last relay subscription payload contains '{"frame_type":"text","upgraded":"ready","user_id":42}'
    Then the relay subscription receives a payload
      """
      "frame_type":"binary"
      """
    And the last relay subscription payload contains '{"frame_type":"binary","upgraded":"ready","user_id":43}'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @websocket_declaration_lifecycle @deloxide_stress
  Scenario Outline: A buffered WebSockets connection preserves upgrade headers and frame types through a codec change
    Given entity gate deadline is configured as "30s"
    And a WebSocket declaration peer verifies upgrade headers and sends text and binary frames
    And the WebSocket declaration peer observes a cancelled opening handshake
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE SCHEMA received_event ( user_id I64, frame_type STRING OPTIONAL, upgraded STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC next_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA received_event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket HEADERS { 'authorization' = 'Bearer fixture' }
        MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST DECODE USING event_codec
        TO received INHERIT ALL SET frame_type = metadata.frame_type, upgraded = read_header('x-upgrade')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "socket_events" is saved as placeholder "ws_owner"
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands begin executing in the background
      """
      ALTER WEBSOCKETS CONNECTION INGESTOR socket_events SET DECODE USING next_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "10s" node "{{ws_owner}}" eventually reports describe ingestor "socket_events" as "quiesce state: entity hold"
    When the WebSocket lifecycle peer sends its payload
    Then within "10s" node "{{ws_owner}}" eventually reports describe ingestor "socket_events" as "nervix_ingestor_quiesce_buffered_records: 2"
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    Then the relay subscription receives a payload
      """
      "frame_type":"text"
      """
    And the last relay subscription payload contains '{"frame_type":"text","upgraded":"ready","user_id":42}'
    Then the relay subscription receives a payload
      """
      "frame_type":"binary"
      """
    And the last relay subscription payload contains '{"frame_type":"binary","upgraded":"ready","user_id":43}'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSockets connection offers subprotocols and reads the one its peer selects on <cluster_size> nodes
    Given a WebSocket subprotocol peer selects "v1.events"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE SCHEMA received_event ( user_id I64, subprotocol STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA received_event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket HEADERS { 'Sec-WebSocket-Protocol' = 'v2.events, v1.events', 'authorization' = 'Bearer fixture' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL SET subprotocol = read_header('sec-websocket-protocol')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    Then the WebSocket subprotocol peer receives the offer "v2.events,v1.events"
    And the relay subscription receives a payload
      """
      "subprotocol":"v1.events"
      """
    And the last relay subscription payload contains '{"subprotocol":"v1.events","user_id":42}'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSockets connection fails when its peer <answer> after an offer of <offer> on <cluster_size> nodes
    Given a WebSocket subprotocol peer <answer>
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = '{{websocket_peer}}' };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket <headers>
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE INGESTOR "socket_events" on the leader node contains
      """
      transient error: <failure>
      """

    Examples:
      | cluster_size | answer                 | offer               | headers                                                       | failure                                                                 |
      | 1            | selects "v3.events"    | v2.events,v1.events | HEADERS { 'sec-websocket-protocol' = 'v2.events, v1.events' } | WebSocket peer selected a subprotocol the upgrade did not offer         |
      | 3            | selects "v3.events"    | v2.events,v1.events | HEADERS { 'sec-websocket-protocol' = 'v2.events, v1.events' } | WebSocket peer selected a subprotocol the upgrade did not offer         |
      | 1            | selects no subprotocol | v2.events,v1.events | HEADERS { 'sec-websocket-protocol' = 'v2.events, v1.events' } | WebSocket peer selected none of the subprotocols the upgrade offered    |
      | 3            | selects no subprotocol | v2.events,v1.events | HEADERS { 'sec-websocket-protocol' = 'v2.events, v1.events' } | WebSocket peer selected none of the subprotocols the upgrade offered    |
      | 1            | selects "v1.events"    | nothing             |                                                               | WebSocket peer selected a subprotocol although the upgrade offered none |
      | 3            | selects "v1.events"    | nothing             |                                                               | WebSocket peer selected a subprotocol although the upgrade offered none |

  Scenario Outline: A WebSockets client refuses configuration it cannot use before opening a connection
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "client 'socket' configuration is invalid in domain '{{domain}}': unknown client config key 'unexpected'"
      """
      CREATE WEBSOCKETS CLIENT socket CONFIG {
        'endpoint' = 'ws://websocket.nervix.test/events',
        'unexpected' = 'value'
      };
      """
    When these Plexus statements fail with "client 'socket' configuration is invalid in domain '{{domain}}': client config key 'tls_ca_file' requires 'endpoint' to use wss"
      """
      CREATE WEBSOCKETS CLIENT socket CONFIG {
        'endpoint' = 'ws://websocket.nervix.test/events',
        'tls_ca_file' = '/etc/nervix/ca.pem'
      };
      """
    When these Plexus statements fail with "WEBSOCKETS client 'socket' in domain '{{domain}}' cannot use its CONFIG: 'endpoint' must not have a fragment"
      """
      CREATE WEBSOCKETS CLIENT socket CONFIG {
        'endpoint' = 'ws://websocket.nervix.test/events#latest'
      };
      """
    When these Plexus statements fail with "WEBSOCKETS client 'socket' in domain '{{domain}}' cannot use its CONFIG: 'handshake_buffer_capacity' must be at most 65535"
      """
      CREATE WEBSOCKETS CLIENT socket CONFIG {
        'endpoint' = 'ws://websocket.nervix.test/events',
        'handshake_buffer_capacity' = '65536'
      };
      """

    Given the leader node is configured with these Plexus statements
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG { 'endpoint' = 'ws://websocket.nervix.test/events' };
      """
    When these Plexus commands are executed
      """
      DESCRIBE CLIENT socket;
      """
    Then the last command output contains
      """
      endpoint: ws://websocket.nervix.test/events
      handshake_buffer_capacity: 16 (default)
      max_frame_size: 16777216 (default)
      max_message_size: 67108864 (default)
      """
    And the last command output does not contain
      """
      idle_timeout_ms
      """
    When these Plexus statements fail with "ingestor 'socket_events' in domain '{{domain}}' cannot use its HEADERS: upgrade header 'Host' is invalid or reserved"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events FROM socket HEADERS { 'Host' = 'example.test' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'socket_events' in domain '{{domain}}' cannot use its HEADERS: upgrade header 'x-event' is set more than once"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events FROM socket HEADERS { 'X-Event' = 'one', 'x-event' = 'two' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'socket_events' in domain '{{domain}}' cannot use its HEADERS: upgrade header 'Sec-WebSocket-Key' is invalid or reserved"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events FROM socket HEADERS { 'Sec-WebSocket-Key' = 'dGhlIHNhbXBsZSBub25jZQ==' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'socket_events' in domain '{{domain}}' cannot use its HEADERS: upgrade header 'sec-websocket-protocol' element 1 is not a subprotocol token"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events FROM socket HEADERS { 'Sec-WebSocket-Protocol' = 'v1 events' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'socket_events' in domain '{{domain}}' cannot use its HEADERS: upgrade header 'sec-websocket-protocol' has an empty element 2"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events FROM socket HEADERS { 'sec-websocket-protocol' = 'v1.events,,v2.events' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'socket_events' in domain '{{domain}}' cannot use its HEADERS: upgrade header 'sec-websocket-protocol' element 2 repeats an earlier subprotocol"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events FROM socket HEADERS { 'sec-websocket-protocol' = 'chat, chat' }
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
