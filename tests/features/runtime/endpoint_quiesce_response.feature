Feature: Endpoint responses during quiesce

  Scenario Outline: An acknowledged HTTP endpoint holds requests through an alteration in arrival order
    Given entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC next_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge paused-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR incoming
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 20s
        ON QUIESCE BUFFER MAX SIZE 30B RESPOND AFTER RESUME DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR incoming SET DECODE USING next_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "quiesce state: entity hold"
    When endpoint request "first" is posted on node "<intake_node>" host "paused-{{test_id}}.example.com" path "/events"
      """
      {"value":41}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When endpoint request "second" is posted on node "<intake_node>" host "paused-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 2"
    And within "5s" node "<intake_node>" observability metric "nervix_ingestor_endpoint_pause_waiting_requests" with labels eventually equals 2
      """
      domain="{{domain}}"
      ingestor="incoming"
      """
    And within "5s" node "<intake_node>" observability metric "nervix_ingestor_endpoint_pause_waiting_bytes" with labels eventually equals 28
      """
      domain="{{domain}}"
      ingestor="incoming"
      """
    When endpoint request "overflow" is posted on node "<intake_node>" host "paused-{{test_id}}.example.com" path "/events"
      """
      {"value":43}
      """
    Then endpoint request "overflow" answers HTTP 503 with "not_admitted" cause "busy"
    And endpoint request "overflow" supplies no retry delay
    And within "5s" node "<intake_node>" observability metric "nervix_ingestor_endpoint_pause_refusals_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="incoming"
      cause="buffer_full"
      """
    When the endpoint response boundary is recorded
    And the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And endpoint request "first" answers HTTP 200 with "completed" cause "none"
    And endpoint request "first" answers after the recorded response boundary
    And endpoint request "second" answers HTTP 200 with "completed" cause "none"
    And endpoint request "second" answers after the recorded response boundary
    And the relay subscription receives a payload
      """
      {"value":41}
      """
    And the relay subscription receives a payload
      """
      {"value":42}
      """
    And within "5s" node "<intake_node>" observability metric "nervix_ingestor_endpoint_pause_waiting_requests" with labels eventually equals 0
      """
      domain="{{domain}}"
      ingestor="incoming"
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-2      |

  Scenario Outline: A domain stop discards requests that were waiting for its pause to end
    Given an HTTPS licensing test service allows one ephemeral activation
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge stop-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR incoming
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 2m
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND AFTER RESUME DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    Then the committed license decision is saved as placeholder "first_activation"
    And the ephemeral expiry of license placeholder "first_activation" is saved as placeholder "expired_at"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    Then the current leader node is saved as placeholder "leader"
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "paused.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When endpoint request "waiting" is posted on node "<intake_node>" host "stop-{{test_id}}.example.com" path "/events"
      """
      {"value":41}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When endpoint request "also_waiting" is posted on node "<intake_node>" host "stop-{{test_id}}.example.com" path "/events"
      """
      {"value":43}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 2"
    When both licensing clocks pass the "ephemeral" deadline
    Then node "<intake_node>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And endpoint request "waiting" answers HTTP 503 with "not_admitted" cause "suspended"
    And endpoint request "also_waiting" answers HTTP 503 with "not_admitted" cause "suspended"
    And within "30s" DESCRIBE DOMAIN on node "<intake_node>" contains
      """
      discarded with the ingestor session: <intake_node>: ingestor 'incoming': payloads an ingestor's pause buffer retained (2)
      """
    And the licensing service marks placeholder "first_activation" expired and frees its slot
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    And endpoint request "after_start" is posted on node "<intake_node>" host "stop-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    Then endpoint request "after_start" answers HTTP 200 with "completed" cause "none"
    And the relay subscription receives a payload
      """
      {"value":42}
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-2      |

  Scenario Outline: An acknowledged endpoint cannot be changed to answer before its records enter the graph
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ack-mode-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR incoming FROM intake MODE ACK MAX 2 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      ALTER HTTP ENDPOINT INGESTOR incoming SET QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE;
      """
    Then the last command error contains
      """
      HTTP ENDPOINT ingestors do not support ON QUIESCE BUFFER RESPOND IMMEDIATE with MODE ACK
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER HTTP ENDPOINT INGESTOR incoming SET QUIESCE BUFFER MAX SIZE 1MiB RESPOND AFTER RESUME;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An unacknowledged HTTP endpoint <timing> keeps its buffered body and refuses overflow
    Given entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC next_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge unacked-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR incoming FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 20B RESPOND <clause> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR incoming SET DECODE USING next_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When endpoint request "kept" is posted on node "<intake_node>" host "unacked-{{test_id}}.example.com" path "/events"
      """
      {"value":41}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    And <before>
    When endpoint request "overflow" is posted on node "<intake_node>" host "unacked-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    Then endpoint request "overflow" answers HTTP 503
    When the endpoint response boundary is recorded
    And the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And endpoint request "kept" answers HTTP 202
    And endpoint request "kept" answers <boundary> the recorded response boundary
    And the relay subscription receives a payload
      """
      {"value":41}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | intake_node | timing               | clause       | before                                                                                                    | boundary |
      | 1            | node-1      | answers immediately  | IMMEDIATE    | endpoint request "kept" answers HTTP 202                                                                  | before   |
      | 3            | node-2      | answers immediately  | IMMEDIATE    | endpoint request "kept" answers HTTP 202                                                                  | before   |
      | 1            | node-1      | answers after resume | AFTER RESUME | within "5s" node "node-1" eventually reports describe ingestor "incoming" as "quiesce state: entity hold" | after    |
      | 3            | node-2      | answers after resume | AFTER RESUME | within "5s" node "node-2" eventually reports describe ingestor "incoming" as "quiesce state: entity hold" | after    |

  Scenario Outline: A held acknowledged HTTP request leaves the pause buffer when <reason>
    Given entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC next_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge abandoned-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR incoming
        FROM intake MODE ACK MAX 1 ACK TIMEOUT <deadline>
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND AFTER RESUME DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands begin executing in the background
      """
      ALTER HTTP ENDPOINT INGESTOR incoming SET DECODE USING next_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When endpoint request "waiting" is posted on node "<intake_node>" host "abandoned-{{test_id}}.example.com" path "/events"
      """
      {"value":41}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    And <abandonment>
    Then within "10s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 0"
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    When endpoint request "retry" is posted on node "<intake_node>" host "abandoned-{{test_id}}.example.com" path "/events"
      """
      {"value":42}
      """
    Then endpoint request "retry" answers HTTP 200 with "completed" cause "none"
    And the relay subscription receives a payload
      """
      {"value":42}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | intake_node | deadline | reason                 | abandonment                                                                       |
      | 1            | node-1      | 8s       | its deadline passes    | endpoint request "waiting" answers HTTP 503 with "not_admitted" cause "suspended" |
      | 3            | node-2      | 8s       | its deadline passes    | endpoint request "waiting" answers HTTP 503 with "not_admitted" cause "suspended" |
      | 1            | node-1      | 1m       | its client disconnects | endpoint request "waiting" disconnects                                            |
      | 3            | node-2      | 1m       | its client disconnects | endpoint request "waiting" disconnects                                            |

  @endpoint_pause_websocket @deloxide_stress_endpoint_ack
  Scenario Outline: A second WebSocket frame closes only its connection while the first is held
    Given entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC next_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ws-pause-{{test_id}}.example.com;
      CREATE SIGNALING PROTOCOL ws_ack FORMAT JSON
        ON CONNECT ACCEPT DATA SEND JAQ '{"ready":true}' WAIT JAQ '.ready == true' TIMEOUT 30s;
      CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/events' WITH SIGNALING PROTOCOL ws_ack;
      CREATE WEBSOCKETS ENDPOINT INGESTOR incoming
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 20s
        ON QUIESCE BUFFER MAX SIZE 64B RESPOND AFTER RESUME DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When endpoint websocket "first" is opened on node "<intake_node>" host "ws-pause-{{test_id}}.example.com" path "/events"
    And endpoint websocket "other" is opened on node "<intake_node>" host "ws-pause-{{test_id}}.example.com" path "/events"
    And endpoint websocket "oversized" is opened on node "<intake_node>" host "ws-pause-{{test_id}}.example.com" path "/events"
    And endpoint websocket "leaving" is opened on node "<intake_node>" host "ws-pause-{{test_id}}.example.com" path "/events"
    Then endpoint websocket "first" receives a JSON frame
      """
      {"ready":true}
      """
    And endpoint websocket "other" receives a JSON frame
      """
      {"ready":true}
      """
    And endpoint websocket "oversized" receives a JSON frame
      """
      {"ready":true}
      """
    And endpoint websocket "leaving" receives a JSON frame
      """
      {"ready":true}
      """
    When a payload is sent on endpoint websocket "other"
      """
      {"ready":true}
      """
    And a payload is sent on endpoint websocket "oversized"
      """
      {"ready":true}
      """
    And these Plexus commands begin executing in the background
      """
      ALTER WEBSOCKETS ENDPOINT INGESTOR incoming SET DECODE USING next_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When a payload is sent on endpoint websocket "first"
      """
      {"value":41}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When a payload is sent on endpoint websocket "other"
      """
      {"value":42}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 2"
    When a payload is sent on endpoint websocket "first"
      """
      {"value":43}
      """
    Then endpoint websocket "first" closes with retry code 1013
    And within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When a payload is sent on endpoint websocket "oversized"
      """
      {"value":44,"padding":"012345678901234567890123456789012345678901234567890123456789"}
      """
    Then endpoint websocket "oversized" closes with retry code 1013
    And within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When a payload is sent on endpoint websocket "leaving"
      """
      {"value":45}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 2"
    When endpoint websocket "leaving" disconnects
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And endpoint websocket "other" acknowledges with "completed" cause "none"
    And the relay subscription receives a payload
      """
      {"value":42}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-2      |

  Scenario Outline: An unacknowledged WebSocket keeps the buffered frame when a later frame overflows
    Given entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC next_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ws-buffer-{{test_id}}.example.com;
      CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/events';
      CREATE WEBSOCKETS ENDPOINT INGESTOR incoming FROM intake MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 20B RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When an endpoint websocket is opened on node "<intake_node>" host "ws-buffer-{{test_id}}.example.com" path "/events"
    And these Plexus commands begin executing in the background
      """
      ALTER WEBSOCKETS ENDPOINT INGESTOR incoming SET DECODE USING next_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When a payload is sent on the endpoint websocket
      """
      {"value":41}
      """
    Then within "5s" node "<intake_node>" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When a payload is sent on the endpoint websocket
      """
      {"value":42}
      """
    Then the endpoint websocket closes with retry code 1013
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And the relay subscription receives a payload
      """
      {"value":41}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-2      |
