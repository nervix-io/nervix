Feature: OTEL emission
  Scenario Outline: OTEL log and trace emitters export typed relay records over OTLP gRPC
    Given OpenTelemetry Collector is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA audit_event (
        event_ts DATETIME,
        level STRING,
        level_num I32,
        message STRING,
        trace_id STRING,
        span_id STRING,
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA audit_event_wire MODE STRICT (
        event_ts string,
        level string,
        level_num integer,
        message string,
        trace_id string,
        span_id string,
        user_id integer,
        action string
      );
      CREATE CODEC audit_event_codec
      FROM WIRE JSON SCHEMA audit_event_wire
      TO SCHEMA audit_event
      ENCODE event_ts AS RFC3339;
      CREATE RELAY audit_events SCHEMA audit_event UNBRANCHED;
      CREATE VHOST edge otel-{{test_id}}.example.com;
      CREATE ENDPOINT audit_ingress
      ON edge
      PATH '/audit'
      TYPE HTTP;
      CREATE INGESTOR audit_source
      FROM ENDPOINT audit_ingress MODE NO_ACK SEQUENTIAL
      DECODE USING audit_event_codec
      TO audit_events
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT otel_main
      TYPE OTEL
      CONFIG {
        'endpoint' = '{{otel_collector_grpc_addr}}',
        'protocol' = 'grpc',
        'timeout_ms' = 5000
      };
      CREATE EMITTER audit_to_otel
      FROM audit_events
      TO OTEL otel_main LOGS
      VALUES {
        'time' = input.event_ts,
        'severity_text' = input.level,
        'severity_number' = input.level_num,
        'body' = input.message,
        'trace_id' = input.trace_id,
        'span_id' = input.span_id
      }
      ATTRIBUTES {
        'user.id' = input.user_id,
        'audit.action' = input.action
      }
      RESOURCE {
        'service.name' = 'nervix-cucumber',
        'deployment.environment.name' = '{{test_id}}'
      }
      SCOPE 'nervix/audit' VERSION '1.0'
      MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE EMITTER audit_trace_to_otel
      FROM audit_events
      TO OTEL otel_main TRACES
      VALUES {
        'trace_id' = input.trace_id,
        'span_id' = input.span_id,
        'name' = 'otel-trace-{{test_id}}',
        'kind' = 'INTERNAL',
        'start_time' = input.event_ts,
        'end_time' = input.event_ts,
        'status_code' = 'OK'
      }
      ATTRIBUTES {
        'user.id' = input.user_id,
        'audit.action' = input.action
      }
      RESOURCE {
        'service.name' = 'nervix-cucumber',
        'deployment.environment.name' = '{{test_id}}'
      }
      SCOPE 'nervix/audit' VERSION '1.0'
      MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And OTEL client for emitter "audit_to_otel" enters unavailable fault mode
    And http payload is posted to host "otel-{{test_id}}.example.com" path "/audit"
      """
      {
        "event_ts":"2026-08-24T12:34:56Z",
        "level":"INFO",
        "level_num":9,
        "message":"otel-log-{{test_id}}",
        "trace_id":"00112233445566778899aabbccddeeff",
        "span_id":"0011223344556677",
        "user_id":42,
        "action":"checkout"
      }
      """
    Then within "5s" DESCRIBE EMITTER "audit_to_otel" on the leader node contains
      """
      transient error: OTEL client fault injector returned gRPC UNAVAILABLE
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And OTEL client for emitter "audit_to_otel" leaves fault mode
    Then OpenTelemetry Collector eventually contains "otel-log-{{test_id}}"
    And OpenTelemetry Collector eventually contains "otel-trace-{{test_id}}"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: OTEL metric emitters export over OTLP HTTP protobuf
    Given OpenTelemetry Collector is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA request_count (
        window_start DATETIME,
        window_end DATETIME,
        value I64,
        route STRING
      );
      CREATE WIRE JSON SCHEMA request_count_wire MODE STRICT (
        window_start string,
        window_end string,
        value integer,
        route string
      );
      CREATE CODEC request_count_codec
      FROM WIRE JSON SCHEMA request_count_wire
      TO SCHEMA request_count
      ENCODE window_start AS RFC3339,
             window_end AS RFC3339;
      CREATE RELAY request_counts SCHEMA request_count UNBRANCHED;
      CREATE VHOST edge otel-metric-{{test_id}}.example.com;
      CREATE ENDPOINT metric_ingress
      ON edge
      PATH '/metric'
      TYPE HTTP;
      CREATE INGESTOR metric_source
      FROM ENDPOINT metric_ingress MODE NO_ACK SEQUENTIAL
      DECODE USING request_count_codec
      TO request_counts
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT otel_http
      TYPE OTEL
      CONFIG {
        'endpoint' = '{{otel_collector_http_addr}}',
        'protocol' = 'http/protobuf',
        'compression' = 'gzip',
        'timeout_ms' = 5000
      };
      CREATE EMITTER request_count_to_otel
      FROM request_counts
      TO OTEL otel_http
      METRIC 'nervix.test.request.count' UNIT '1' DESCRIPTION 'Cucumber request count'
      SUM MONOTONIC DELTA
      VALUES {
        'time' = input.window_end,
        'start_time' = input.window_start,
        'value' = input.value
      }
      ATTRIBUTES { 'http.route' = input.route }
      RESOURCE { 'service.name' = 'nervix-cucumber' }
      SCOPE 'nervix/metrics' VERSION '1.0'
      MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "otel-metric-{{test_id}}.example.com" path "/metric"
      """
      {
        "window_start":"2026-08-24T12:34:00Z",
        "window_end":"2026-08-24T12:35:00Z",
        "value":17,
        "route":"/checkout"
      }
      """
    Then OpenTelemetry Collector eventually contains "nervix.test.request.count"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
