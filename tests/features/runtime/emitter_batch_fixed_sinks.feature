Feature: Fixed-format emitter batch envelopes
  Syslog frames, Sentry events, and OTLP exports carry their source members in the container
  defined by each protocol. The optional batch policy bounds the complete encoded unit.

  @emitter_batch_fixed_sinks @sentry_batch_event
  Scenario Outline: Sentry accepts one event whose extra field preserves every batch member
    Given Sentry is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Sentry endpoint is published under fixture DNS
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA error_event ( message STRING, environment STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA error_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC sentry_batch_codec FROM JSON TO SCHEMA error_event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{message: .message, environment: .environment}'
          ON EMITTING BATCH '{message: "batched errors", environment: .[0].environment, extra: {records: .}}';
      CREATE RELAY errors SCHEMA error_event UNBRANCHED;
      CREATE VHOST edge sentry-{{test_id}}.example.com;
      CREATE ENDPOINT errors_endpoint ON edge PATH '/errors' TYPE HTTP;
      CREATE INGESTOR http_errors
        FROM ENDPOINT errors_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO errors INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT sentry_main TYPE SENTRY CONFIG {
        'dsn' = '{{sentry_dns_dsn}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER batched_sentry FROM errors
        TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING sentry_batch_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And sink client for emitter "batched_sentry" enters unavailable fault mode
    When http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      <payload>
      """
    Then within "5s" DESCRIBE EMITTER "batched_sentry" on the leader node contains
      """
      transient error: sink fault injector returned an unavailable client
      """
    And sink client for emitter "batched_sentry" leaves fault mode
    Then Sentry eventually receives an event
      """
      {"message":"batched errors","environment":"{{test_id}}","extra":{"records":<payload>}}
      """

    Examples:
      | cluster_size | payload                                                                                            |
      | 1            | [{"message":"first","environment":"{{test_id}}"},{"message":"second","environment":"{{test_id}}"}] |
      | 3            | [{"message":"first","environment":"{{test_id}}"},{"message":"second","environment":"{{test_id}}"}] |
      | 1            | [{"message":"singleton","environment":"{{test_id}}"}]                                              |
      | 3            | [{"message":"singleton","environment":"{{test_id}}"}]                                              |

  @emitter_batch_fixed_sinks @sentry_native_limit
  Scenario Outline: Sentry rejects a batched event above its decompressed native limit
    Given Sentry is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Sentry endpoint is published under fixture DNS
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA error_event ( message STRING );
      CREATE SCHEMA rejected_event ( error_message STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA error_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE CODEC sentry_batch_codec FROM JSON TO SCHEMA error_event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{message: .message}'
          ON EMITTING BATCH '{message: "batched errors", extra: {records: .}}';
      CREATE RELAY errors SCHEMA error_event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge sentry-{{test_id}}.example.com;
      CREATE ENDPOINT errors_endpoint ON edge PATH '/errors' TYPE HTTP;
      CREATE INGESTOR http_errors
        FROM ENDPOINT errors_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO errors INHERIT ALL SET message = repeat(input.message, 1000000)
          UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT sentry_main TYPE SENTRY CONFIG {
        'dsn' = '{{sentry_dns_dsn}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER batched_sentry FROM errors
        TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING sentry_batch_codec
        INHERIT ALL
        BATCH MAX MESSAGES 2 MAX SIZE 2MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      {"message":"x"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      encoded Sentry event | maximum is 1000000
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_fixed_sinks @syslog_batch_frame
  Scenario Outline: A Syslog UDP receiver accepts a single frame containing its source messages
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, hostname STRING OPTIONAL, message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE VHOST edge syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_main TYPE SYSLOG CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_emit_addr}}'
      };
      CREATE EMITTER batched_syslog FROM events
        TO SYSLOG syslog_main MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      [{"facility":4,"severity":2,"hostname":"app-01","message":"first"},{"facility":4,"severity":2,"hostname":"app-01","message":"second"}]
      """
    Then the observed Syslog UDP endpoint receives a payload
      """
      ["{{syslog_pri}}1 - app-01 - - - - first","{{syslog_pri}}1 - app-01 - - - - second"]
      """
    When http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      [{"facility":4,"severity":2,"hostname":"app-01","message":"third"},{"facility":4,"severity":3,"hostname":"app-01","message":"fourth"}]
      """
    Then the observed Syslog UDP endpoint receives a payload
      """
      ["{{syslog_pri}}1 - app-01 - - - - third"]
      """
    And the observed Syslog UDP endpoint receives a payload
      """
      35>1 - app-01 - - - - fourth"]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_fixed_sinks @syslog_udp_native_limit
  Scenario Outline: A Syslog UDP emitter rejects a batched frame above the datagram limit
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, hostname STRING OPTIONAL, message STRING
      );
      CREATE SCHEMA rejected_event ( error_message STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL SET message = repeat(input.message, 65500)
          UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_main TYPE SYSLOG CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_emit_addr}}'
      };
      CREATE EMITTER batched_syslog FROM events
        TO SYSLOG syslog_main MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        BATCH MAX MESSAGES 2 MAX SIZE 128KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      {"facility":4,"severity":2,"hostname":"app-01","message":"x"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      encoded Syslog UDP payload | maximum is 65507
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_fixed_sinks @syslog_batch_stream
  Scenario Outline: A Syslog TCP receiver sees one batched frame with the declared framing
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, hostname STRING OPTIONAL, message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY received_events SCHEMA syslog_event UNBRANCHED;
      CREATE VHOST edge syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_listener TYPE SYSLOG CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_ingest_addr}}',
        'max_message_size' = '4096'
      };
      CREATE INGESTOR syslog_intake
        FROM SYSLOG syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND DECODE USING syslog_codec
        TO received_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_main TYPE SYSLOG CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_ingest_addr}}',
        'framing' = '<framing>'
      };
      CREATE EMITTER batched_syslog FROM events
        TO SYSLOG syslog_main MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_events_subscription TO received_events;
      START;
      """
    And http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      [{"facility":4,"severity":2,"hostname":"app-01","message":"first"},{"facility":4,"severity":2,"hostname":"app-01","message":"second"}]
      """
    Then the relay subscription receives a payload
      """
      first
      """
    And the last relay subscription payload contains
      """
      second
      """

    Examples:
      | cluster_size | framing         |
      | 1            | octet-counting  |
      | 3            | octet-counting  |
      | 1            | non-transparent |
      | 3            | non-transparent |

  @emitter_batch_fixed_sinks @syslog_batch_tls
  Scenario Outline: A Syslog TLS receiver sees one batched RFC 5425 frame
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these NSPL commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, hostname STRING OPTIONAL, message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY received_events SCHEMA syslog_event UNBRANCHED;
      CREATE VHOST edge syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_listener TYPE SYSLOG
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls', 'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_ca_file' = '{{ syslog_tls }}/ca.crt'
        };
      CREATE INGESTOR syslog_intake
        FROM SYSLOG syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND DECODE USING syslog_codec
        TO received_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_main TYPE SYSLOG
        MOUNT syslog_tls VERSION 1
        CONFIG {
          'protocol' = 'tls', 'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_ca_file' = '{{ syslog_tls }}/ca.crt'
        };
      CREATE EMITTER batched_syslog FROM events
        TO SYSLOG syslog_main MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_events_subscription TO received_events;
      START;
      """
    And http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      [{"facility":4,"severity":2,"hostname":"app-01","message":"first"},{"facility":4,"severity":2,"hostname":"app-01","message":"second"}]
      """
    Then the relay subscription receives a payload
      """
      first
      """
    And the last relay subscription payload contains
      """
      second
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_fixed_sinks @otel_batch_limit
  Scenario Outline: An OTEL export rejects a singleton larger than its encoded request limit
    Given OpenTelemetry Collector is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, body STRING );
      CREATE SCHEMA rejected_event ( seq I64, error_message STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge otel-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT otel_main TYPE OTEL CONFIG {
        'endpoint' = '<endpoint>',
        'protocol' = '<protocol>',
        'timeout_ms' = 5000
      };
      CREATE EMITTER accepted_otel FROM events
        TO OTEL otel_main LOGS
          VALUES { 'time' = now(), 'body' = input.body }
          ATTRIBUTES { 'event.seq' = input.seq }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'batch-test' VERSION '1.0'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        BATCH MAX MESSAGES 2 MAX SIZE 1KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER accepted_otel_traces FROM events
        TO OTEL otel_main TRACES
          VALUES {
            'trace_id' = '00112233445566778899aabbccddeeff',
            'span_id' = '0011223344556677',
            'name' = input.body,
            'kind' = 'INTERNAL',
            'start_time' = now(),
            'end_time' = now(),
            'status_code' = 'OK'
          }
          ATTRIBUTES { 'event.marker' = input.body }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'batch-test' VERSION '1.0'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        BATCH MAX MESSAGES 2 MAX SIZE 1KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER accepted_otel_metrics FROM events
        TO OTEL otel_main
          METRIC 'nervix.test.batch.count' UNIT '1' DESCRIPTION 'Cucumber batch count'
          SUM MONOTONIC DELTA
          VALUES { 'time' = now(), 'start_time' = now(), 'value' = input.seq }
          ATTRIBUTES { 'event.marker' = input.body }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'batch-test' VERSION '1.0'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        BATCH MAX MESSAGES 2 MAX SIZE 1KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER limited_otel FROM events
        TO OTEL otel_main LOGS
          VALUES { 'time' = now(), 'body' = input.body }
          ATTRIBUTES { 'event.seq' = input.seq }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'batch-test' VERSION '1.0'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        BATCH MAX MESSAGES 2 MAX SIZE 1B
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq, error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "otel-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"body":"batch-accepted-{{test_id}}-first"},{"seq":2,"body":"batch-accepted-{{test_id}}-second"},{"seq":3,"body":"batch-accepted-{{test_id}}-third"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | OTEL export request
      "seq":2 | OTEL export request
      "seq":3 | OTEL export request
      """
    And OpenTelemetry Collector eventually contains "batch-accepted-{{test_id}}-first"
    And OpenTelemetry Collector eventually contains "batch-accepted-{{test_id}}-second"
    And OpenTelemetry Collector eventually contains "batch-accepted-{{test_id}}-third"
    And OpenTelemetry Collector receives a two-member "logs" export followed by a one-member export
      """
      batch-accepted-{{test_id}}-first
      batch-accepted-{{test_id}}-second
      batch-accepted-{{test_id}}-third
      """
    And OpenTelemetry Collector receives a two-member "traces" export followed by a one-member export
      """
      batch-accepted-{{test_id}}-first
      batch-accepted-{{test_id}}-second
      batch-accepted-{{test_id}}-third
      """
    And OpenTelemetry Collector receives a two-member "metrics" export followed by a one-member export
      """
      batch-accepted-{{test_id}}-first
      batch-accepted-{{test_id}}-second
      batch-accepted-{{test_id}}-third
      """

    Examples:
      | cluster_size | endpoint                     | protocol      |
      | 1            | {{otel_collector_grpc_addr}} | grpc          |
      | 3            | {{otel_collector_grpc_addr}} | grpc          |
      | 1            | {{otel_collector_http_addr}} | http/protobuf |
      | 3            | {{otel_collector_http_addr}} | http/protobuf |
