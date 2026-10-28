Feature: Published endpoint route lifetimes
  Scenario Outline: Tearing down one domain keeps another domain's intake on the same HTTP route
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN intake_{{test_id}};
      CREATE UNPACED DOMAIN intake_peer_{{test_id}};
      """
    And the active domain is "intake_{{test_id}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge shared-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR event_source
        FROM ingress MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And the active domain is "intake_peer_{{test_id}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge shared-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR event_source
        FROM ingress MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    When http payload is posted to node "<request_node>" with host "shared-{{test_id}}.example.com" path "/events"
      """
      {"user_id":41}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":41}
      """
    When the active session targets domain "intake_{{test_id}}"
    And these Plexus commands are executed on the active session
      """
      STOP;
      """
    And the active session targets domain "intake_peer_{{test_id}}"
    And http payload is posted to node "<request_node>" with host "shared-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | request_node |
      | 1            | node-1       |
      | 3            | node-3       |

  Scenario Outline: A WebSocket connection keeps its intake lifetime across source replacement
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge lifetime-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE WEBSOCKETS;
      CREATE WEBSOCKETS ENDPOINT INGESTOR event_source
        FROM ingress MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And an endpoint websocket is opened on node "<request_node>" host "lifetime-{{test_id}}.example.com" path "/events"
    And a payload is sent on the endpoint websocket
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    When these Plexus commands are executed on the leader node
      """
      STOP;
      START;
      """
    And a payload is sent on the endpoint websocket
      """
      {"user_id":43}
      """
    Then the endpoint websocket closes with retry code 1013
    When an endpoint websocket is opened on node "<request_node>" host "lifetime-{{test_id}}.example.com" path "/events"
    And a payload is sent on the endpoint websocket
      """
      {"user_id":44}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":44}
      """

    Examples:
      | cluster_size | request_node |
      | 1            | node-1       |
      | 3            | node-3       |

  # A node that restarted without a quorum to catch up through has installed no routes yet, so it
  # cannot tell a route the cluster binds from one no domain declares. It refuses every endpoint
  # request as temporarily unavailable until it installs its revision, never as a missing route.
  # Only three nodes can hold a restarted node before installation, so the scenario is topology
  # specific.
  Scenario: A restarted node refuses endpoint requests as temporarily unavailable until it installs its routes
    Given a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge restart-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR event_source
        FROM ingress MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-3" eventually accepts http traffic for host "restart-{{test_id}}.example.com" path "/events"
      """
      {"user_id":41}
      """
    When node "node-1" is stopped
    And node "node-2" is stopped
    And node "node-3" is stopped
    And node "node-3" is started without waiting for it to catch up
    And endpoint request "uninstalled" is posted on node "node-3" host "restart-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42}
      """
    Then endpoint request "uninstalled" answers HTTP 503
    When node "node-1" is started
    Then node "node-3" eventually accepts http traffic for host "restart-{{test_id}}.example.com" path "/events"
      """
      {"user_id":43}
      """
    When endpoint request "unrouted" is posted on node "node-3" host "restart-{{test_id}}.example.com" path "/missing"
      """
      {"user_id":44}
      """
    Then endpoint request "unrouted" answers HTTP 404
