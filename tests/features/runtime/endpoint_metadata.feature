Feature: Endpoint request metadata
  An HTTP ENDPOINT ingestor reads the admitted request's method, path, query, host and the address
  of the peer that sent it as metadata.method, metadata.path, metadata.query, metadata.host and
  metadata.remote_addr. A WEBSOCKETS ENDPOINT ingestor reads each data frame's kind, the
  connection's peer and the path its upgrade request named as metadata.frame_type,
  metadata.remote_addr and metadata.path. Endpoints serve on every node, so each request and frame
  is read on the node that received it.

  Scenario Outline: An HTTP endpoint ingestor routes requests by their metadata and reads every field
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA request_event (
        seq I64,
        route STRING,
        method STRING OPTIONAL,
        path STRING OPTIONAL,
        query STRING OPTIONAL,
        host STRING OPTIONAL,
        remote_addr STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY tenant_requests SCHEMA request_event UNBRANCHED;
      CREATE RELAY other_requests SCHEMA request_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-metadata.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      """
    When these Plexus statements are executed
      """
      CREATE HTTP ENDPOINT INGESTOR requests_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO tenant_requests
          INHERIT ALL
          SET route = 'tenant', method = metadata.method, path = metadata.path,
              query = metadata.query, host = metadata.host, remote_addr = metadata.remote_addr
          WHERE metadata.method = 'POST' AND metadata.path = '/events'
            AND metadata.query = 'tenant=acme&region=eu'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        TO other_requests
          INHERIT ALL
          SET route = 'other', method = metadata.method, path = metadata.path,
              query = coalesce(metadata.query, 'absent'), host = metadata.host,
              remote_addr = metadata.remote_addr
          WHERE is_null(metadata.query)
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION tenant_subscription TO tenant_requests;
      CREATE SUBSCRIPTION other_subscription TO other_requests;
      START;
      """
    And http payload is posted to node "<intake_node>" with host "http-{{test_id}}-metadata.example.com" path "/events?tenant=acme&region=eu"
      """
      {"seq":1}
      """
    And http payload is posted to node "<intake_node>" with host "HTTP-{{test_id}}-METADATA.Example.COM:8080" path "/events"
      """
      {"seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "route":"tenant" | "method":"POST" | "path":"/events" | "query":"tenant=acme&region=eu" | "host":"http-{{test_id}}-metadata.example.com" | "remote_addr":"127.0.0.1:
      "seq":2 | "route":"other" | "method":"POST" | "path":"/events" | "query":"absent" | "host":"http-{{test_id}}-metadata.example.com" | "remote_addr":"127.0.0.1:
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-1      |
      | 3            | node-3      |

  Scenario Outline: An HTTP endpoint ingestor reads a header value that is UTF-8 but not ASCII
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA tenant_event ( seq I64, tenant STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY tenant_events SCHEMA tenant_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-header.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      """
    When these Plexus statements are executed
      """
      CREATE HTTP ENDPOINT INGESTOR tenants_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO tenant_events INHERIT ALL SET tenant = read_header('x-tenant')
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION tenant_subscription TO tenant_events;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-header.example.com" path "/events" and header "x-tenant" value "Zürich ✓"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "tenant":"Zürich ✓"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An HTTP endpoint ingestor drops and counts a header value that is not UTF-8
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA tenant_event ( seq I64, tenant STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY tenant_events SCHEMA tenant_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-invalid-header.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      """
    When these Plexus statements are executed
      """
      CREATE HTTP ENDPOINT INGESTOR tenants_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO tenant_events INHERIT ALL SET tenant = coalesce(read_header('x-tenant'), 'dropped')
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION tenant_subscription TO tenant_events;
      START;
      """
    And http payload is posted to node "<intake_node>" with host "http-{{test_id}}-invalid-header.example.com" path "/events" and header "x-tenant" with base64 bytes "WvxyaWNo"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "tenant":"dropped"
      """
    And within "30s" node "<intake_node>" observability metric "nervix_ingestor_header_values_dropped_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="tenants_in"
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-2      |

  Scenario Outline: An acknowledged HTTP endpoint delivers the request's metadata before answering
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA request_event (
        seq I64,
        method STRING OPTIONAL,
        path STRING OPTIONAL,
        query STRING OPTIONAL,
        host STRING OPTIONAL,
        remote_addr STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY requests SCHEMA request_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-acknowledged.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      """
    When these Plexus statements are executed
      """
      CREATE HTTP ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 5s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO requests
          INHERIT ALL
          SET method = metadata.method, path = metadata.path, query = metadata.query,
              host = metadata.host, remote_addr = metadata.remote_addr
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION request_subscription TO requests;
      START;
      """
    And endpoint request "direct" is posted on node "<intake_node>" host "http-{{test_id}}-acknowledged.example.com" path "/events?page=2"
      """
      {"seq":7}
      """
    Then endpoint request "direct" answers HTTP 200 with "completed" cause "none"
    And within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":7 | "method":"POST" | "path":"/events" | "query":"page=2" | "host":"http-{{test_id}}-acknowledged.example.com" | "remote_addr":"127.0.0.1:
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-2      |

  Scenario Outline: A WebSocket endpoint ingestor routes frames by their type and reads its connection's metadata
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA frame_event (
        seq I64,
        route STRING,
        frame_type STRING OPTIONAL,
        path STRING OPTIONAL,
        remote_addr STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY text_frames SCHEMA frame_event UNBRANCHED;
      CREATE RELAY binary_frames SCHEMA frame_event UNBRANCHED;
      CREATE VHOST edge ws-{{test_id}}-metadata.example.com;
      CREATE SIGNALING PROTOCOL greeting FORMAT JSON
        ON CONNECT ACCEPT DATA SEND JAQ '{"ready":true}' WAIT JAQ '.ready == true' TIMEOUT 30s;
      CREATE ENDPOINT stream ON edge PATH '/stream'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL greeting;
      """
    When these Plexus statements are executed
      """
      CREATE WEBSOCKETS ENDPOINT INGESTOR frames_in
        FROM stream MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO text_frames
          INHERIT ALL
          SET route = 'text', frame_type = metadata.frame_type, path = metadata.path,
              remote_addr = metadata.remote_addr
          WHERE metadata.frame_type = 'text'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        TO binary_frames
          INHERIT ALL
          SET route = 'binary', frame_type = metadata.frame_type, path = metadata.path,
              remote_addr = metadata.remote_addr
          WHERE metadata.frame_type = 'binary'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION text_subscription TO text_frames;
      CREATE SUBSCRIPTION binary_subscription TO binary_frames;
      START;
      """
    And websocket frames are exchanged with host "ws-{{test_id}}-metadata.example.com" path "/stream"
      """
      EXPECT {"ready": true}
      SEND {"seq":1}
      SEND BASE64 eyJzZXEiOjJ9
      SEND {"ready":true}
      SEND {"seq":3}
      SEND BASE64 eyJzZXEiOjR9
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "route":"text" | "frame_type":"text" | "path":"/stream" | "remote_addr":"127.0.0.1:
      "seq":2 | "route":"binary" | "frame_type":"binary" | "path":"/stream" | "remote_addr":"127.0.0.1:
      "seq":3 | "route":"text" | "frame_type":"text" | "path":"/stream" | "remote_addr":"127.0.0.1:
      "seq":4 | "route":"binary" | "frame_type":"binary" | "path":"/stream" | "remote_addr":"127.0.0.1:
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @endpoint_metadata_buffer @deloxide_stress
  Scenario Outline: An HTTP endpoint replays a buffered request with the metadata it arrived with
    Given entity gate deadline is configured as "30s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA request_event (
        seq I64,
        method STRING OPTIONAL,
        path STRING OPTIONAL,
        query STRING OPTIONAL,
        host STRING OPTIONAL,
        remote_addr STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC event_codec_v2 FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY requests SCHEMA request_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-buffered.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR buffered_in
        FROM intake MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO requests
          INHERIT ALL
          SET method = metadata.method, path = metadata.path, query = metadata.query,
              host = metadata.host, remote_addr = metadata.remote_addr
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION request_subscription TO requests;
      START;
      """
    And these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR buffered_in SET DECODE USING event_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "5s" node "node-1" eventually reports describe ingestor "buffered_in" as "quiesce state: entity hold"
    When http payload is posted to node "node-1" with host "http-{{test_id}}-buffered.example.com" path "/events?replayed=yes"
      """
      {"seq":5}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "buffered_in" as "nervix_ingestor_quiesce_buffered_records: 1"
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":5 | "method":"POST" | "path":"/events" | "query":"replayed=yes" | "host":"http-{{test_id}}-buffered.example.com" | "remote_addr":"127.0.0.1:
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
