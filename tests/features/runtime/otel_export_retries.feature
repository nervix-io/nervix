Feature: OTEL Export requests through unknown outcomes
  An OTEL emitter prepares each OTLP Export request once: its records, resource, scope and observed
  timestamps, within the limits of its batching clause. A request whose outcome the emitter did not
  learn, because the receiver closed the connection before it answered or the request timed out, is
  sent again byte for byte with the same records, after the requests the receiver answered, over
  both OTLP transports. A request the receiver refused routes every record it carried through ON
  MESSAGE ERROR with one shared reference and is never sent again.

  @otel_export_retries
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, body STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_reference STRING,
        error_code STRING,
        operation STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge otel-retry-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT otel_main TYPE OTEL CONFIG {
        'endpoint' = '{{otlp_receiver.collector}}',
        'protocol' = '<protocol>',
        'compression' = 'gzip',
        'timeout_ms' = 5000
      };
      CREATE EMITTER retried_logs FROM events
        TO OTEL otel_main LOGS
          VALUES { 'time' = now(), 'body' = input.body }
          ATTRIBUTES { 'event.seq' = input.seq }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'retry-test' VERSION '1.0'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        BATCH MAX MESSAGES 2 MAX SIZE 1KiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_reference = error.reference,
              error_code = error.code,
              operation = error.operation
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
      "seq":3 | "error_code":"external" | "operation":"publish"
      "seq":4 | "error_code":"external" | "operation":"publish"
      """
    And OTLP receiver "collector" has captured exactly 4 export requests

    Examples:
      | cluster_size | protocol      |
      | 1            | grpc          |
      | 3            | grpc          |
      | 1            | http/protobuf |
      | 3            | http/protobuf |

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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, body STRING );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge otel-timeout-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT otel_main TYPE OTEL CONFIG {
        'endpoint' = '{{otlp_receiver.collector}}',
        'protocol' = '<protocol>',
        'timeout_ms' = 1000
      };
      CREATE EMITTER timed_out_logs FROM events
        TO OTEL otel_main LOGS
          VALUES { 'time' = now(), 'body' = input.body }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
          SCOPE 'timeout-test' VERSION '1.0'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
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
