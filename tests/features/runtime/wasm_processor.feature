Feature: WASM processor runtime behavior
  Scenario Outline: WASM enrichment references unchanged input columns inside concrete branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_reference_enricher;
      UPLOAD RESOURCE wasm_reference_enricher VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        value I32,
        tenant STRING,
        message STRING,
        occurred_at DATETIME
      );
      CREATE SCHEMA enriched_event (
        value I32,
        tenant STRING,
        message STRING,
        occurred_at DATETIME,
        bucket STRING
      );
      CREATE STRICT WIRE JSON SCHEMA event_wire (
        value integer,
        tenant string,
        message string,
        occurred_at string
      );
      CREATE CODEC event_codec
        FROM WIRE JSON SCHEMA event_wire
        TO SCHEMA event
        ENCODE occurred_at AS RFC3339;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY enriched_events SCHEMA enriched_event BRANCHED BY by_tenant;
      CREATE VHOST edge wasm-reference-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        TO raw_events
        DECODE USING event_codec
        BRANCHED BY by_tenant VALUES { tenant = raw_events.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR enrich_even_rows
        USING RESOURCE wasm_reference_enricher VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_events
        TO enriched_events
        BRANCHED BY by_tenant
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO enriched_events;
      START;
      """
    When http payload is posted to host "wasm-reference-{{test_id}}.example.com" path "/events"
      """
      {"value":11,"tenant":"alpha","message":"alpha-first","occurred_at":"2026-07-13T01:02:03Z"}
      """
    And http payload is posted to host "wasm-reference-{{test_id}}.example.com" path "/events"
      """
      {"value":21,"tenant":"beta","message":"beta-first","occurred_at":"2026-07-13T02:03:04Z"}
      """
    And http payload is posted to host "wasm-reference-{{test_id}}.example.com" path "/events"
      """
      {"value":12,"tenant":"alpha","message":"alpha-second","occurred_at":"2026-07-13T03:04:05Z"}
      """
    And http payload is posted to host "wasm-reference-{{test_id}}.example.com" path "/events"
      """
      {"value":22,"tenant":"beta","message":"beta-second","occurred_at":"2026-07-13T04:05:06Z"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      "bucket":"EVEN","message":"alpha-second","occurred_at":"2026-07-13T03:04:05+00:00","tenant":"alpha","value":12
      "bucket":"EVEN","message":"beta-second","occurred_at":"2026-07-13T04:05:06+00:00","tenant":"beta","value":22
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor filters records inside each concrete branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """


      CREATE SCHEMA metric (
        value I32
      );

      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        value integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/metrics'
        TYPE HTTP;

      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;

      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then the relay subscription receives a payload
      """
      "value":2
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      wasm processor: filter_even_rows
      """
    And the last command output contains
      """
      ABI serialization: FlatBuffers
      """
    And the last command output contains
      """
      persistent state: true
      """
    And the last command output contains
      """
      replicated state: true
      """
    And the last command output contains
      """
      state structures: 1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor shares one generated column across branched output routes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_route_filter;
      UPLOAD RESOURCE wasm_route_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric_input (
        value I32,
        tenant STRING
      );

      CREATE SCHEMA metric (
        value I32,
        tenant STRING,
        bucket STRING
      );

      CREATE SCHEMA audited_metric (
        value I32,
        tenant STRING,
        classification STRING
      );

      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        value integer,
        tenant string
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric_input;

      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;

      CREATE RELAY raw_metrics SCHEMA metric_input BRANCHED BY by_tenant;
      CREATE RELAY enriched_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY audited_metrics SCHEMA audited_metric BRANCHED BY by_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/metrics'
        TYPE HTTP;

      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        BRANCHED BY by_tenant VALUES { tenant = raw_metrics.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_route_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO enriched_metrics
        TO audited_metrics
        BRANCHED BY by_tenant
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;

      SUBSCRIBE SESSION TO enriched_metrics;
      SUBSCRIBE SESSION TO audited_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"alpha"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"beta"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"tenant":"alpha"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"tenant":"beta"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      "bucket":"EVEN","tenant":"alpha","value":2
      "bucket":"EVEN","tenant":"beta","value":2
      "classification":"EVEN","tenant":"alpha","value":2
      "classification":"EVEN","tenant":"beta","value":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor applies FILTER WHERE before guest execution and TO WHERE after guest output
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_route_timing_filter;
      UPLOAD RESOURCE wasm_route_timing_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        value I32
      );

      CREATE SCHEMA routed_metric (
        value I32,
        bucket STRING
      );

      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        value integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY selected_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY routed_metrics SCHEMA routed_metric UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/metrics'
        TYPE HTTP;

      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_route_timing_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics FILTER WHERE raw_metrics.value != 10 AS I32
        TO selected_metrics
        TO routed_metrics
          SET routed_metrics.bucket = lower(routed_metrics.bucket)
          WHERE routed_metrics.value != 4 AS I32
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;

      SUBSCRIBE SESSION TO routed_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":10}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then the relay subscription receives a payload
      """
      "value":2
      """
    And the last relay subscription payload contains
      """
      "bucket":"even"
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":6}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":8}
      """
    Then the relay subscription receives a payload
      """
      "value":8
      """
    And the last relay subscription payload contains
      """
      "bucket":"even"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor restores guest state after cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter_restart;
      UPLOAD RESOURCE wasm_filter_restart VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """


      CREATE SCHEMA metric (
        value I32
      );

      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        value integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/metrics'
        TYPE HTTP;

      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_filter_restart VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;

      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO filtered_metrics;
      """
    Then within "10s" repeatedly posting http payload to host "http-{{test_id}}.example.com" path "/metrics" yields a relay subscription payload
      """
      {"value":2}
      """
    And the last relay subscription payload contains
      """
      "value":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: WASM processor receives referenced input columns from a guest timeout
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_timeout_filter;
      UPLOAD RESOURCE wasm_timeout_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_timeout_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    Then within "10s" repeatedly posting http payload to host "http-{{test_id}}.example.com" path "/metrics" yields a relay subscription payload
      """
      {"value":2}
      """
    And the last relay subscription payload contains
      """
      "value":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Go WASM processor guest runs through the runtime
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "go" WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_go_filter;
      UPLOAD RESOURCE wasm_go_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_go_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then the relay subscription receives a payload
      """
      "value":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |

  Scenario Outline: Rust and Go WASM processors run in sequence
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "rust" example WASM processor resource directory "rust_wasm_processor"
    And node "node-1" has "go" example WASM processor resource directory "go_wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE rust_wasm_filter;
      UPLOAD RESOURCE rust_wasm_filter VERSION '{{rust_wasm_processor}}';
      CREATE RESOURCE go_wasm_filter;
      UPLOAD RESOURCE go_wasm_filter VERSION '{{go_wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY rust_filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY go_filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR rust_filter_even_rows
        USING RESOURCE rust_wasm_filter VERSION 1
        FILE 'nervix_wasm_processor_rust_guest.wasm'
        FROM raw_metrics
        TO rust_filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      CREATE WASM PROCESSOR go_filter_even_rows
        USING RESOURCE go_wasm_filter VERSION 1
        FILE 'nervix_wasm_processor_go_guest.wasm'
        FROM rust_filtered_metrics
        TO go_filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO rust_filtered_metrics;
      SUBSCRIBE SESSION TO go_filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then the relay subscription receives a payload
      """
      "value":2
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "value":4
      """
    And within "5s" the relay subscription receives a payload
      """
      "value":4
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Rust and Go WASM processors emit one final value for four one-row ingests
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "rust" example WASM processor resource directory "rust_wasm_processor"
    And node "node-1" has "go" example WASM processor resource directory "go_wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE rust_wasm_filter;
      UPLOAD RESOURCE rust_wasm_filter VERSION '{{rust_wasm_processor}}';
      CREATE RESOURCE go_wasm_filter;
      UPLOAD RESOURCE go_wasm_filter VERSION '{{go_wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY rust_filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY go_filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR rust_filter_even_rows
        USING RESOURCE rust_wasm_filter VERSION 1
        FILE 'nervix_wasm_processor_rust_guest.wasm'
        FROM raw_metrics
        TO rust_filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      CREATE WASM PROCESSOR go_filter_even_rows
        USING RESOURCE go_wasm_filter VERSION 1
        FILE 'nervix_wasm_processor_go_guest.wasm'
        FROM rust_filtered_metrics
        TO go_filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO go_filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "value":4
      """
    And the relay subscription does not receive a payload within "1500ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Rust WASM processor references a time-flushed input batch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "rust" example WASM processor resource directory "rust_wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE rust_wasm_filter;
      UPLOAD RESOURCE rust_wasm_filter VERSION '{{rust_wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY rust_filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH EACH 2s MAX BATCH SIZE 100kb
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR rust_filter_even_rows
        USING RESOURCE rust_wasm_filter VERSION 1
        FILE 'nervix_wasm_processor_rust_guest.wasm'
        FROM raw_metrics
        TO rust_filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO rust_filtered_metrics;
      START;
      """
    When 100 sequential metric http payloads are posted to host "http-{{test_id}}.example.com" path "/metrics"
    Then within "60s" the relay subscription receives a payload
      """
      "value":100
      """
    And the relay subscription does not receive a payload within "1500ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor routes guest-reported message errors
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter_message_error;
      UPLOAD RESOURCE wasm_filter_message_error VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32 );
      CREATE SCHEMA error_record (
        error_message STRING,
        failed_node STRING,
        failed_record STRING
      );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY error_stream SCHEMA error_record UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_filter_message_error VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR DLQ error_stream SET error_message = message_error.message, failed_node = message_error.node, failed_record = message_error.record
        ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO error_stream;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":-100}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "error_message":"guest message error for value -100"
      """
    And the last relay subscription payload contains
      """
      "failed_node":"filter_even_rows"
      """
    And the last relay subscription payload contains
      """
      "failed_record":"{\"value\":-100}"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor handles guest global errors outside the ack sidecar
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter_global_error;
      UPLOAD RESOURCE wasm_filter_global_error VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_filter_global_error VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":-200}
      """
    Then within "10s" the active session observes a server error
    And the last server error contains
      """
      wasm guest reported global error: guest global error for value -200
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor reports guest error state as a global error
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter_error_state;
      UPLOAD RESOURCE wasm_filter_error_state VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_filter_error_state VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":-300}
      """
    Then within "10s" the active session observes a server error
    And the last server error contains
      """
      wasm guest reported global error: guest error state for value -300
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: WASM processor traps are handled as global errors
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has trapping WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_trapping_filter;
      UPLOAD RESOURCE wasm_trapping_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_trapping_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    Then within "10s" the active session observes a server error
    And the last server error contains
      """
      failed to call wasm export 'nervix_process_batch'
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Invalid WASM processor module prevents runtime startup
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has invalid WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_invalid_filter;
      UPLOAD RESOURCE wasm_invalid_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands fail with "failed to compile wasm processor 'filter_even_rows'"
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_invalid_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario: Malformed WASM processor output reports a runtime error
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has malformed-output WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_malformed_filter;
      UPLOAD RESOURCE wasm_malformed_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA metric ( value I32 );
      CREATE STRICT WIRE JSON SCHEMA metric_wire ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows
        USING RESOURCE wasm_malformed_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        FROM raw_metrics
        TO filtered_metrics
        UNBRANCHED
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SUBSCRIBE SESSION TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    Then within "10s" the active session observes a server error
