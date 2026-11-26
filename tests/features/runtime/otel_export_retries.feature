Feature: OTEL Export requests through unknown outcomes
  An OTEL emitter prepares each OTLP Export request once: its records, resource, scope and observed
  timestamps, within the limits of its batching clause. A request whose outcome the emitter did not
  learn, because the receiver closed the connection before it answered or the request timed out, is
  sent again byte for byte with the same records, after the requests the receiver answered, over
  both OTLP transports. A request the receiver refused routes every record it carried through ON
  MESSAGE ERROR with one shared reference and is never sent again.

  @otel_export_retries @emitter_payload_observations
  Scenario Outline: An OTEL Export request whose answer was lost is sent again byte for byte
    Given OTLP receiver "collector" is running for "<protocol>"
    # The receiver accepts the first request, refuses the second, and closes the connection after
    # reading the third without answering it. Later requests are accepted.
    And OTLP receiver "collector" answers with
      """
      accept
      reject
      lose response
      """
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, body STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_reference STRING,
        error_code STRING,
        operation STRING,
        external_code STRING OPTIONAL
      );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge otel-retry-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE OTEL CLIENT otel_main CONFIG {
        'endpoint' = '{{otlp_receiver.collector}}',
        'protocol' = '<protocol>',
        'headers' = 'authorization=Bearer%20token%2Cpart%3D1%25%2B+{{test_id}}',
        'compression' = 'gzip',
        'timeout_ms' = 5000
      };
      CREATE OTEL LOGS EMITTER retried_logs FROM events
        TO otel_main
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s VALUES { 'time' = now(), 'body' = input.body }
          ATTRIBUTES { 'event.seq' = input.seq }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'retry-test' VERSION '1.0' BATCH MAX MESSAGES 2 MAX SIZE 1KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_reference = error.reference,
              error_code = error.class,
              operation = error.operation,
              external_code = error.external_code
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "otel-retry-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"body":"first-{{test_id}}"},{"seq":2,"body":"second-{{test_id}}"},{"seq":3,"body":"third-{{test_id}}"},{"seq":4,"body":"fourth-{{test_id}}"},{"seq":5,"body":"fifth-{{test_id}}"}]
      """
    # The retry sends the third request exactly as it was first sent: the same records, resource,
    # scope and observed timestamps, gzip-compressed the same way. The accepted first request and
    # the refused second request are not sent again.
    Then OTLP receiver "collector" eventually receives at least 4 export requests
    And every export request at OTLP receiver "collector" has header "authorization" equal to "Bearer token,part=1%++{{test_id}}"
    And OTLP receiver "collector" request 4 repeats request 3
    And OTLP receiver "collector" captured these log export requests
      """
      first-{{test_id}} | second-{{test_id}}
      third-{{test_id}} | fourth-{{test_id}}
      fifth-{{test_id}}
      fifth-{{test_id}}
      """
    And within "30s" the relay subscription receives payloads containing all fragments that share one "error_reference"
      """
      "seq":3 | "error_code":"refused" | "operation":"publish" | "external_code":"<external_code>"
      "seq":4 | "error_code":"refused" | "operation":"publish" | "external_code":"<external_code>"
      """
    And OTLP receiver "collector" has captured exactly 4 export requests
    And within "30s" DESCRIBE EMITTER "retried_logs" on the leader node contains
      """
      batch container: OTLP Export request
      """
    And within "30s" DESCRIBE EMITTER "retried_logs" on the leader node contains
      """
      payloads: 3 source_messages=5
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    And node "{{metrics_owner}}" observability metric "nervix_emitter_payload_encoded_bytes_total" with labels eventually reaches at least 1
      """
      emitter="retried_logs"
      """
    And node "{{metrics_owner}}" observability metric "nervix_emitter_work_messages" with labels eventually equals 0
      """
      emitter="retried_logs"
      state="prepared"
      """

    # The refusal carries the status code of the transport in use: gRPC INVALID_ARGUMENT is 3, and
    # an HTTP refusal is 400 Bad Request.
    Examples:
      | cluster_size | protocol      | external_code |
      | 1            | grpc          | 3             |
      | 3            | grpc          | 3             |
      | 1            | http/protobuf | 400           |
      | 3            | http/protobuf | 400           |

  @otel_export_retries
  Scenario Outline: An OTEL <signal> Export request the receiver refuses routes its record with the status code it answered
    Given OTLP receiver "collector" is running for "<protocol>"
    And OTLP receiver "collector" answers with
      """
      reject
      """
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        external_code STRING OPTIONAL
      );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge otel-refusal-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE OTEL CLIENT otel_main CONFIG {
        'endpoint' = '{{otlp_receiver.collector}}',
        'protocol' = '<protocol>',
        'timeout_ms' = 5000
      };
      CREATE OTEL <signal> EMITTER refused FROM events
        TO otel_main
          <metric>
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s VALUES { <values> }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'refusal-test' VERSION '1.0'
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.class,
              operation = error.operation,
              external_code = error.external_code
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "otel-refusal-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "error_code":"refused" | "operation":"publish" | "external_code":"<external_code>"
      """
    And OTLP receiver "collector" has captured exactly 1 export requests

    # The refusal carries the status code of the transport in use: gRPC INVALID_ARGUMENT is 3, and
    # an HTTP refusal is 400 Bad Request.
    Examples:
      | cluster_size | protocol      | signal  | metric                                                                     | values                                                                                                                                                                                   | external_code |
      | 1            | grpc          | TRACES  |                                                                            | 'trace_id' = '0123456789abcdef0123456789abcdef', 'span_id' = '0123456789abcdef', 'name' = 'refused', 'kind' = 'INTERNAL', 'start_time' = now(), 'end_time' = now(), 'status_code' = 'OK' | 3             |
      | 3            | grpc          | TRACES  |                                                                            | 'trace_id' = '0123456789abcdef0123456789abcdef', 'span_id' = '0123456789abcdef', 'name' = 'refused', 'kind' = 'INTERNAL', 'start_time' = now(), 'end_time' = now(), 'status_code' = 'OK' | 3             |
      | 1            | http/protobuf | TRACES  |                                                                            | 'trace_id' = '0123456789abcdef0123456789abcdef', 'span_id' = '0123456789abcdef', 'name' = 'refused', 'kind' = 'INTERNAL', 'start_time' = now(), 'end_time' = now(), 'status_code' = 'OK' | 400           |
      | 3            | http/protobuf | TRACES  |                                                                            | 'trace_id' = '0123456789abcdef0123456789abcdef', 'span_id' = '0123456789abcdef', 'name' = 'refused', 'kind' = 'INTERNAL', 'start_time' = now(), 'end_time' = now(), 'status_code' = 'OK' | 400           |
      | 1            | grpc          | METRICS | METRIC 'nervix.test.refused' UNIT '1' DESCRIPTION 'Cucumber refusal' GAUGE | 'time' = now(), 'value' = input.seq                                                                                                                                                      | 3             |
      | 3            | grpc          | METRICS | METRIC 'nervix.test.refused' UNIT '1' DESCRIPTION 'Cucumber refusal' GAUGE | 'time' = now(), 'value' = input.seq                                                                                                                                                      | 3             |
      | 1            | http/protobuf | METRICS | METRIC 'nervix.test.refused' UNIT '1' DESCRIPTION 'Cucumber refusal' GAUGE | 'time' = now(), 'value' = input.seq                                                                                                                                                      | 400           |
      | 3            | http/protobuf | METRICS | METRIC 'nervix.test.refused' UNIT '1' DESCRIPTION 'Cucumber refusal' GAUGE | 'time' = now(), 'value' = input.seq                                                                                                                                                      | 400           |

  @otel_export_retries
  Scenario Outline: An OTEL Export request that timed out is sent again byte for byte
    Given OTLP receiver "collector" is running for "<protocol>"
    # The receiver reads the first request and never answers it, so the client's one-second
    # request timeout ends the attempt. Later requests are accepted.
    And OTLP receiver "collector" answers with
      """
      hold response
      """
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, body STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge otel-timeout-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE OTEL CLIENT otel_main CONFIG {
        'endpoint' = '{{otlp_receiver.collector}}',
        'protocol' = '<protocol>',
        'timeout_ms' = 1000
      };
      CREATE OTEL LOGS EMITTER timed_out_logs FROM events
        TO otel_main
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s VALUES { 'time' = now(), 'body' = input.body }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'timeout-test' VERSION '1.0' FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "otel-timeout-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"body":"first-{{test_id}}"},{"seq":2,"body":"second-{{test_id}}"}]
      """
    # Without the batching clause the buffered batch is one request. The retry after the timeout
    # sends it again exactly as it was first sent, observed timestamps included.
    Then OTLP receiver "collector" eventually receives at least 2 export requests
    And OTLP receiver "collector" request 2 repeats request 1
    And OTLP receiver "collector" captured these log export requests
      """
      first-{{test_id}} | second-{{test_id}}
      first-{{test_id}} | second-{{test_id}}
      """

    Examples:
      | cluster_size | protocol      |
      | 1            | grpc          |
      | 3            | grpc          |
      | 1            | http/protobuf |
      | 3            | http/protobuf |
