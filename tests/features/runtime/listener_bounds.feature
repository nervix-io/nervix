Feature: Declared public listener bounds
  A node bounds the connections and input its listeners retain and reports refusals without
  stopping the listener or consuming the input as a record.

  Scenario Outline: Ordinary fragmented HTTP input within its bounds is admitted on <cluster_size> nodes
    Given node-1 starts with execution-intake-memory-bytes = 4MiB
    And node-1 starts with http-body-bytes = 1MiB
    # Small read buffers keep the up-front read scratch, four read buffers, small beside the pool.
    And node-1 starts with listener-buffer-bytes = 8KiB
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/ingest';
      CREATE HTTP ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And node "node-1" admits an ordinary fragmented HTTP body for host "bounded-{{test_id}}.example.com"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP input arriving in small network pieces within its bounds is admitted on <cluster_size> nodes with <framing> framing
    Given node-1 starts with execution-intake-memory-bytes = 4MiB
    And node-1 starts with http-body-bytes = 1MiB
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/ingest';
      CREATE HTTP ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And node "node-1" admits an HTTP body arriving in 512 byte pieces with <framing> framing for host "bounded-{{test_id}}.example.com"

    Examples:
      | cluster_size | framing |
      | 1            | chunked |
      | 3            | chunked |
      | 1            | length  |
      | 3            | length  |

  Scenario Outline: A half-open gRPC session is reaped by transport keepalive on <cluster_size> nodes
    Given node-1 starts with grpc-keepalive-interval = 500ms
    And node-1 starts with grpc-keepalive-timeout = 2s
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" releases a gRPC session whose peer stops reading within "30s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Default HTTP framing bounds refuse an oversized chunked trailer on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/ingest';
      CREATE HTTP ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And node "node-1" refuses an oversized chunked trailer for host "bounded-{{test_id}}.example.com"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A public listener waits its declared pause after an accept failure on <cluster_size> nodes
    Given node-1 starts with listener-accept-retry-delay = 500ms
    And a <cluster_size> node nervix cluster is started
    Then the <listener> listener on node "node-1" waits at least "500ms" after an accept failure

    Examples:
      | cluster_size | listener      |
      | 1            | http          |
      | 3            | http          |
      | 1            | console       |
      | 3            | console       |
      | 1            | observability |
      | 3            | observability |

  Scenario Outline: Slow HTTP headers end at the declared timeout on <cluster_size> nodes
    Given node-1 starts with listener-header-timeout = 500ms
    And node-1 starts with listener-idle-timeout = 10s
    And a <cluster_size> node nervix cluster is started
    Then the <listener> listener on node "node-1" refuses unfinished headers after "500ms"

    Examples:
      | cluster_size | listener      |
      | 1            | http          |
      | 3            | http          |
      | 1            | console       |
      | 3            | console       |
      | 1            | observability |
      | 3            | observability |

  Scenario Outline: HTTP connections beyond the declared capacity are refused and capacity returns on <cluster_size> nodes
    Given node-1 starts with listener-max-connections = 2
    And node-1 starts with listener-idle-timeout = 120s
    And a <cluster_size> node nervix cluster is started
    Then the <listener> listener on node "node-1" admits two connections and refuses a third with status 503

    Examples:
      | cluster_size | listener      |
      | 1            | console       |
      | 3            | console       |
      | 1            | observability |
      | 3            | observability |

  Scenario Outline: Default HTTP body admission refuses input above its byte bound on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/ingest';
      CREATE HTTP ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And node "node-1" refuses a 33554433 byte HTTP payload for host "bounded-{{test_id}}.example.com" with status 413 using length framing

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Unacceptable HTTP bodies are refused before decode on <cluster_size> nodes
    Given node-1 starts with http-body-bytes = 1KiB
    And node-1 starts with http-body-timeout = 500ms
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/ingest';
      CREATE HTTP ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_bodies TO events;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And node "node-1" refuses a 2048 byte HTTP payload for host "bounded-{{test_id}}.example.com" with status <status> using <framing> framing
    When http payload is posted to host "bounded-{{test_id}}.example.com" path "/ingest"
      """
      {"value":"still serving"}
      """
    Then the relay subscription receives a payload
      """
      {"value":"still serving"}
      """

    Examples:
      | cluster_size | framing | status |
      | 1            | length  | 413    |
      | 3            | length  | 413    |
      | 1            | chunked | 413    |
      | 3            | chunked | 413    |
      | 1            | stalled | 408    |
      | 3            | stalled | 408    |

  Scenario Outline: An idle public connection ends at its declared deadline on <cluster_size> nodes
    Given node-1 starts with listener-idle-timeout = 500ms
    And a <cluster_size> node nervix cluster is started
    Then the <listener> listener on node "node-1" closes an idle connection after "500ms"

    Examples:
      | cluster_size | listener      |
      | 1            | http          |
      | 3            | http          |
      | 1            | console       |
      | 3            | console       |
      | 1            | observability |
      | 3            | observability |

  Scenario Outline: A stalled console TLS handshake ends at its declared deadline on <cluster_size> nodes
    Given the web console listener is configured for HTTPS
    And node-1 starts with listener-tls-timeout = 500ms
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" closes an unfinished console TLS handshake after "500ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Header input beyond the declared byte bound is refused on <cluster_size> nodes
    Given node-1 starts with listener-header-bytes = 16KiB
    And a <cluster_size> node nervix cluster is started
    Then the <listener> listener on node "node-1" refuses 32768 header bytes with status 431

    Examples:
      | cluster_size | listener      |
      | 1            | http          |
      | 3            | http          |
      | 1            | console       |
      | 3            | console       |
      | 1            | observability |
      | 3            | observability |

  Scenario Outline: A stalled gRPC TLS handshake ends at its declared deadline on <cluster_size> nodes
    Given client grpc transport is configured with mode "https"
    And node-1 starts with listener-tls-timeout = 500ms
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" closes an unfinished gRPC TLS handshake after "500ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A node publishes its resolved listener bounds through consensus on <cluster_size> nodes
    Given node-1 starts with listener-max-connections = 17
    And node-1 starts with listener-header-timeout = 2s
    And node-1 starts with listener-accept-retry-delay = 500ms
    And node-1 starts with http-body-bytes = 1MiB
    And node-1 starts with http-body-copy-bytes = 32KiB
    And node-1 starts with http-refusal-drain-timeout = 750ms
    And node-1 starts with grpc-concurrency = 7
    And node-1 starts with grpc-max-streams = 9
    And a <cluster_size> node nervix cluster is started
    Then node "node-<cluster_size>" eventually describes committed capabilities for node "node-1"
    When these Plexus commands are executed through the client on node "node-<cluster_size>"
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      max_connections: 17
      """
    And the last command output contains
      """
      header_timeout: 2s
      """
    And the last command output contains
      """
      accept_retry_delay: 500ms
      """
    And the last command output contains
      """
      http_body_bytes: 1048576
      """
    And the last command output contains
      """
      http_body_copy_bytes: 32768
      """
    And the last command output contains
      """
      http_refusal_drain_timeout: 750ms
      """
    And the last command output contains
      """
      grpc_concurrency: 7
      """
    And the last command output contains
      """
      grpc_max_streams: 9
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Retained HTTP input growth waits for room another request holds and completes once it frees on <cluster_size> nodes
    Given node-1 starts with execution-intake-memory-bytes = 4MiB
    And node-1 starts with http-body-bytes = 1MiB
    And node-1 starts with http-body-timeout = 120s
    And node-1 starts with listener-idle-timeout = 120s
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/ingest';
      CREATE HTTP ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_bodies TO events;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And node "node-1" observability metric "nervix_execution_memory_capacity_bytes" with labels eventually equals 4194304
      """
      nervix_execution_memory_capacity_bytes{class="intake"}
      """
    And node "node-1" holds retained HTTP input growth for host "bounded-{{test_id}}.example.com" until other input frees room
    When http payload is posted to node "node-1" with host "bounded-{{test_id}}.example.com" path "/ingest"
      """
      {"value":"after-capacity-wait"}
      """
    Then the relay subscription receives a payload
      """
      "value":"after-capacity-wait"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSocket endpoint closes only the session that exceeds its <limit> bound on <cluster_size> nodes
    Given node-1 starts with websocket-frame-bytes = 16B
    And node-1 starts with websocket-message-bytes = 32B
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/ws';
      CREATE WEBSOCKETS ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_events TO events;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    When an endpoint websocket is opened on node "node-1" host "bounded-{{test_id}}.example.com" path "/ws"
    Then a second WebSocket on host "bounded-{{test_id}}.example.com" path "/ws" closes without a Close frame for an oversized <limit>
    When a payload is sent on the endpoint websocket
      """
      {"value":"ok"}
      """
    Then the relay subscription receives a payload
      """
      {"value":"ok"}
      """

    Examples:
      | cluster_size | limit   |
      | 1            | frame   |
      | 3            | frame   |
      | 1            | message |
      | 3            | message |

  Scenario Outline: A WebSocket upgrade keeps the listener idle deadline on <cluster_size> nodes
    Given node-1 starts with listener-idle-timeout = 2s
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge bounded-{{test_id}}.example.com;
      CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/ws';
      CREATE WEBSOCKETS ENDPOINT INGESTOR intake_ingestor FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "intake_ingestor" as "ready: true"
    And a WebSocket on host "bounded-{{test_id}}.example.com" path "/ws" closes without a Close frame after "2s" of silence

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
