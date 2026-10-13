Feature: OTEL DNS recovery reproduction
  @domain_execution_time
  Scenario Outline: OTEL DNS recovery reproduction
    Given OpenTelemetry Collector is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the OTEL gRPC endpoint is published under fixture DNS
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
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
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING audit_event_codec
      TIMESTAMP NOW
      TO audit_events
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT otel_main
      TYPE OTEL
      CONFIG {
        'endpoint' = '{{otel_collector_grpc_dns_addr}}',
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
        'body' = CASE
          WHEN now() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN input.message
          ELSE 'physical-time'
        END,
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
        'name' = CASE
          WHEN now() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN 'otel-trace-{{test_id}}'
          ELSE 'physical-time-trace'
        END,
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
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    When the DNS fixture answers "otel-grpc.nervix.test" with "name not found"
    And sink client for emitter "audit_to_otel" enters unavailable fault mode
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
      transient error: sink fault injector returned an unavailable client
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    When sink client for emitter "audit_to_otel" leaves fault mode
    Then within "10s" DESCRIBE EMITTER "audit_to_otel" on the leader node contains
      """
      transient error: OTEL gRPC export ended without an answer from the receiver
      """
    And the DNS fixture eventually receives a question for "otel-grpc.nervix.test"
    When the DNS fixture answers "otel-grpc.nervix.test" with addresses "127.0.0.1"
    Then OpenTelemetry Collector eventually contains "otel-log-{{test_id}}"
    And OpenTelemetry Collector eventually contains "otel-trace-{{test_id}}"
    And the DNS fixture eventually receives a question for "otel-grpc.nervix.test"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
      | 1            | 0             |
