Feature: Acknowledged endpoint intake

  Scenario Outline: An acknowledged HTTP endpoint delivers its request before answering
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      """
    When these Plexus statements are executed
      """
      CREATE HTTP ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 5s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And endpoint request "direct" is posted on node "<intake_node>" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    Then endpoint request "direct" answers HTTP 200 with "completed" cause "none"
    And the relay subscription receives a payload
      """
      {"value":42}
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-1      |
      | 3            | node-2      |
      | 3            | node-3      |

  @endpoint_ack @deadlock_diagnostics @deloxide_stress_endpoint_ack
  Scenario Outline: An HTTP response follows the attached consumer's <decision> decision
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER output FROM events TO CLIENT SCHEMA event
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When these Plexus statements are executed
      """
      RELOCATE EMITTER output ONTO NODE <emitter_node> IGNORE PREFERENCES;
      """
    When client "app" opens consumer "output" on emitter "output" expecting fields "value I64"
    And endpoint request "first" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    And consumer "output" reads output batch "held"
    And endpoint request "overflow" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":43}
      """
    Then endpoint request "overflow" answers HTTP 503 with "not_admitted" cause "busy"
    And endpoint request "overflow" asks to retry after "1"
    When the endpoint response boundary is recorded
    And output batch "held" is <settlement>
    Then endpoint request "first" answers HTTP <status> with "<outcome>" cause "<cause>"
    And endpoint request "first" answers after the recorded response boundary

    Examples:
      | cluster_size | emitter_node | decision | settlement                             | status | outcome           | cause    |
      | 1            | node-1       | ACK      | acknowledged                           | 200    | completed         | none     |
      | 3            | node-3       | ACK      | acknowledged                           | 200    | completed         | none     |
      | 1            | node-1       | REJECT   | rejected because "application refused" | 503    | processing_failed | rejected |
      | 3            | node-3       | REJECT   | rejected because "application refused" | 503    | processing_failed | rejected |

  @endpoint_ack
  Scenario Outline: An HTTP endpoint reports a <refusal> refusal using the client outcome vocabulary
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events SET value = 100 / message.value UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When endpoint request "refused" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      <payload>
      """
    Then endpoint request "refused" answers HTTP 400 with "processing_failed" cause "rejected"

    Examples:
      | cluster_size | refusal | payload         |
      | 1            | codec   | {"value":"bad"} |
      | 3            | codec   | {"value":"bad"} |
      | 1            | route   | {"value":0}     |
      | 3            | route   | {"value":0}     |

  @endpoint_ack @deadlock_diagnostics @deloxide_stress_endpoint_ack
  Scenario Outline: A held HTTP response ends when <ending>
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And graceful shutdown drain is enabled
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 1 ACK TIMEOUT <ack_timeout>
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER output FROM events TO CLIENT SCHEMA event
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "output" expecting fields "value I64"
    And endpoint request "held" is posted on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    And consumer "output" reads output batch "held"
    And <event>
    Then endpoint request "held" answers HTTP 503 with "<outcome>" cause "<cause>"
    And endpoint request "held" asks to retry after "1"

    Examples:
      | cluster_size | ending               | ack_timeout | event                                      | outcome           | cause       |
      | 1            | its ACK time expires | 1s          | the endpoint response boundary is recorded | processing_failed | ack_timeout |
      | 3            | its ACK time expires | 1s          | the endpoint response boundary is recorded | processing_failed | ack_timeout |
      | 1            | its node stops       | 5m          | node "node-1" is gracefully stopped        | outcome_unknown   | interrupted |
      | 3            | its node stops       | 5m          | node "node-1" is gracefully stopped        | outcome_unknown   | interrupted |

  @endpoint_ack
  Scenario Outline: A WebSocket endpoint acknowledges each <result> frame during and after signaling
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-{{test_id}}.example.com;
      CREATE SIGNALING PROTOCOL ack_frames FORMAT JSON
        ON CONNECT ACCEPT DATA SEND JAQ '{"ready":true}' WAIT JAQ '.ready == true' TIMEOUT 30s;
      CREATE ENDPOINT intake ON edge PATH '/events'
        TYPE WEBSOCKETS WITH SIGNALING PROTOCOL ack_frames;
      CREATE WEBSOCKETS ENDPOINT INGESTOR acknowledged
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 5s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events SET value = 100 / message.value UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When an endpoint websocket is opened on node "node-1" host "ack-{{test_id}}.example.com" path "/events"
    Then the endpoint websocket receives a JSON frame
      """
      {"ready":true}
      """
    When a payload is sent on the endpoint websocket
      """
      <payload>
      """
    Then the endpoint websocket acknowledges with "<outcome>" cause "<cause>"
    When a payload is sent on the endpoint websocket
      """
      {"ready":true}
      """
    And a payload is sent on the endpoint websocket
      """
      <payload>
      """
    Then the endpoint websocket acknowledges with "<outcome>" cause "<cause>"

    Examples:
      | cluster_size | result        | payload         | outcome           | cause    |
      | 1            | completed     | {"value":42}    | completed         | none     |
      | 3            | completed     | {"value":42}    | completed         | none     |
      | 1            | codec-refused | {"value":"bad"} | processing_failed | rejected |
      | 3            | codec-refused | {"value":"bad"} | processing_failed | rejected |
      | 1            | route-refused | {"value":0}     | processing_failed | rejected |
      | 3            | route-refused | {"value":0}     | processing_failed | rejected |

  @endpoint_ack @deadlock_diagnostics @deloxide_stress_endpoint_ack
  Scenario Outline: Shared endpoint intake reaches both consumers before either acknowledgement
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY first_events SCHEMA event UNBRANCHED;
      CREATE RELAY second_events SCHEMA event UNBRANCHED;
      CREATE VHOST edge fanout-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR first_source
        FROM intake MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO first_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR second_source
        FROM intake MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO second_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER first_output FROM first_events TO CLIENT SCHEMA event
        MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER second_output FROM second_events TO CLIENT SCHEMA event
        MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "first" on emitter "first_output" expecting fields "value I64"
    And client "app" opens consumer "second" on emitter "second_output" expecting fields "value I64"
    And endpoint request "fanout" is posted on node "node-1" host "fanout-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    And consumer "first" reads output batch "first_batch"
    And consumer "second" reads output batch "second_batch"
    And output batch "first_batch" is acknowledged
    And output batch "second_batch" is acknowledged
    Then endpoint request "fanout" answers HTTP 200 with "completed" cause "none"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
