Feature: Endpoint request outcomes

  An HTTP ENDPOINT or WEBSOCKETS ENDPOINT ingestor counts every request and frame it answers once,
  in nervix_ingestor_endpoint_requests_total, under the outcome it reached on the node that
  received it.

  Scenario Outline: An HTTP endpoint counts each request once by its outcome on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge requests-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR measured FROM intake MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "measured" as "ready: true"
    When endpoint request "first" is posted on node "node-1" host "requests-{{test_id}}.example.com" path "/events"
      """
      {"user_id":1}
      """
    Then endpoint request "first" answers HTTP 202
    When endpoint request "second" is posted on node "node-1" host "requests-{{test_id}}.example.com" path "/events"
      """
      {"user_id":2}
      """
    Then endpoint request "second" answers HTTP 202
    When endpoint request "third" is posted on node "node-1" host "requests-{{test_id}}.example.com" path "/events"
      """
      {"user_id":3}
      """
    Then endpoint request "third" answers HTTP 202
    When endpoint request "undecodable" is posted on node "node-1" host "requests-{{test_id}}.example.com" path "/events"
      """
      {"user_id":"three"}
      """
    Then endpoint request "undecodable" answers HTTP 202
    And within "30s" the relay subscription receives payloads
      """
      {"user_id":1}
      {"user_id":2}
      {"user_id":3}
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 3
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="HTTP"
      entity="ENDPOINT"
      outcome="accepted"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="HTTP"
      entity="ENDPOINT"
      outcome="decode_rejected"
      """
    And node "node-1" observability metric "nervix_ingestor_decode_rejections_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="HTTP"
      entity="ENDPOINT"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 0
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="HTTP"
      entity="ENDPOINT"
      outcome="processing_failed"
      """
    And within "30s" node "node-1" eventually reports describe ingestor "measured" as 'entity="ENDPOINT",outcome="accepted"} total=3'
    And within "30s" node "node-1" eventually reports describe ingestor "measured" as 'entity="ENDPOINT",outcome="decode_rejected"} total=1'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A quiesced ingestor counts each request of a route with two bindings once on <cluster_size> nodes
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
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC event_codec_v2 FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge quiesce-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR held FROM intake MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR open FROM intake MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "open" as "ready: true"
    When these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR held SET DECODE USING event_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "5s" node "node-1" eventually reports describe ingestor "held" as "quiesce state: entity hold"
    And within "5s" node "node-1" eventually reports describe ingestor "open" as "quiesce state: none"
    When endpoint request "first" is posted on node "node-1" host "quiesce-{{test_id}}.example.com" path "/events"
      """
      {"user_id":1}
      """
    Then endpoint request "first" answers HTTP 202
    When endpoint request "second" is posted on node "node-1" host "quiesce-{{test_id}}.example.com" path "/events"
      """
      {"user_id":2}
      """
    Then endpoint request "second" answers HTTP 202
    And node "node-1" observability metric "nervix_ingestor_quiesce_rejected_total" with labels eventually equals 2
      """
      domain="{{domain}}"
      ingestor="held"
      """
    And within "5s" node "node-1" eventually reports describe ingestor "held" as "nervix_ingestor_quiesce_rejected_total: 2"
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 2
      """
      domain="{{domain}}"
      ingestor="open"
      outcome="accepted"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 2
      """
      domain="{{domain}}"
      ingestor="held"
      outcome="refused"
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An acknowledged HTTP endpoint counts each request once by its outcome on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events SET value = 100 / message.value UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER output FROM events TO SCHEMA event
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "acknowledged" as "ready: true"
    Given client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "output" expecting fields "value I64"
    And endpoint request "first" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":4}
      """
    And consumer "output" reads output batch "held"
    And endpoint request "overflow" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":5}
      """
    Then endpoint request "overflow" answers HTTP 503 with "not_admitted" cause "busy"
    When output batch "held" is acknowledged
    Then endpoint request "first" answers HTTP 200 with "completed" cause "none"
    When endpoint request "second" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":10}
      """
    And consumer "output" reads output batch "refused"
    And output batch "refused" is rejected because "application refused"
    Then endpoint request "second" answers HTTP 503 with "processing_failed" cause "rejected"
    When endpoint request "undecodable" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":"bad"}
      """
    Then endpoint request "undecodable" answers HTTP 400 with "processing_failed" cause "rejected"
    When endpoint request "unroutable" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":0}
      """
    Then endpoint request "unroutable" answers HTTP 400 with "processing_failed" cause "rejected"
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      outcome="accepted"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      outcome="busy"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      outcome="processing_failed"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      outcome="decode_rejected"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      outcome="refused"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 0
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      outcome="not_admitted"
      """
    And node "node-1" observability metric "nervix_ingestor_decode_rejections_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="acknowledged"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An endpoint counts a request the extension workers cannot take as not admitted in <mode> on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge unfolding-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR measured FROM intake <mode>
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "measured" as "ready: true"
    When extension execution is saturated on every node
    And endpoint request "refused" is posted on node "node-1" host "unfolding-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42}
      """
    Then endpoint request "refused" answers HTTP 503
    And node "node-1" observability metric "nervix_ingestor_unfolding_refused_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      outcome="not_admitted"
      """
    When extension execution is released on every node
    And endpoint request "accepted" is posted on node "node-1" host "unfolding-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42}
      """
    Then endpoint request "accepted" answers HTTP <accepted_status>
    And within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      outcome="accepted"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      outcome="not_admitted"
      """

    Examples:
      | cluster_size | mode                           | accepted_status |
      | 1            | MODE NO ACK                    | 202             |
      | 3            | MODE NO ACK                    | 202             |
      | 1            | MODE ACK MAX 8 ACK TIMEOUT 30s | 200             |
      | 3            | MODE ACK MAX 8 ACK TIMEOUT 30s | 200             |

  Scenario Outline: A WebSocket endpoint counts each frame once by its outcome on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge frames-{{test_id}}.example.com;
      CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/frames';
      CREATE WEBSOCKETS ENDPOINT INGESTOR measured FROM intake MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    Then within "30s" node "node-1" eventually reports describe ingestor "measured" as "ready: true"
    When an endpoint websocket is opened on node "node-1" host "frames-{{test_id}}.example.com" path "/frames"
    And a payload is sent on the endpoint websocket
      """
      {"user_id":1}
      """
    And a payload is sent on the endpoint websocket
      """
      {"user_id":"two"}
      """
    And a payload is sent on the endpoint websocket
      """
      {"user_id":3}
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"user_id":1}
      {"user_id":3}
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 2
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="WEBSOCKETS"
      entity="ENDPOINT"
      outcome="accepted"
      """
    And node "node-1" observability metric "nervix_ingestor_endpoint_requests_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="WEBSOCKETS"
      entity="ENDPOINT"
      outcome="decode_rejected"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
