Feature: An emitter's retry policy gives up after GIVE UP AFTER
  An emitter retries the work its destination did not take, paced by its RETRY POLICY. Without
  GIVE UP AFTER it retries until the destination takes the work. With it, the emitter stops retrying
  a unit of work once that much time has passed since the unit's first failed attempt: the rows the
  unit still holds fail with an `unavailable` error of the `publish` operation, and the emitter's
  error handling receives them. Rows the destination already took are never failed. A destination
  that refuses the writes as it is configured is retried like an outage and shown as misconfigured
  while it lasts.

  Scenario Outline: An HTTP emitter that cannot reach its destination for longer than GIVE UP AFTER fails exactly the rows it did not deliver
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/events/delivered-{{test_id}}" with "respond 200"
    And HTTP receiver "api" answers unscripted requests with "respond 503"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (id STRING);
      CREATE SCHEMA failed_event (
        id STRING, error_code STRING, operation STRING, message STRING,
        external_code STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY failed_events SCHEMA failed_event UNBRANCHED;
      CREATE VHOST edge give-up-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER published FROM events
        TO api METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 3s WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id,
              error_code = error.class,
              operation = error.operation,
              message = error.message,
              external_code = error.external_code
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_events_subscription TO failed_events;
      START;
      """
    And http payload is posted to host "give-up-{{test_id}}.example.com" path "/events"
      """
      {"id":"delivered-{{test_id}}"}
      """
    Then HTTP receiver "api" eventually captures one request that is
      """
      POST /events/delivered-{{test_id}}
      """
    When http payload is posted to host "give-up-{{test_id}}.example.com" path "/events"
      """
      {"id":"stranded-{{test_id}}"}
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "id":"stranded-{{test_id}}" | "error_code":"unavailable" | "operation":"publish" | "message":"gave up after 3s of retries: | "external_code":"503"
      """
    And the relay subscription does not receive a payload within "5s"
    And HTTP receiver "api" eventually captures requests that are
      """
      POST /events/stranded-{{test_id}}
      """
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      mode: ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 3s
      """
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      give up in: -
      """
    And the last command output owner is saved as placeholder "metrics_owner"
    And node "{{metrics_owner}}" observability metric "nervix_emitter_give_ups_total" with labels eventually reaches at least 1
      """
      emitter="published"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An outage shorter than GIVE UP AFTER delivers every row and fails none
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 503
      respond 503
      respond 503
      respond 200
      """
    And HTTP receiver "api" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (id STRING);
      CREATE SCHEMA failed_event (id STRING, error_code STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY failed_events SCHEMA failed_event UNBRANCHED;
      CREATE VHOST edge give-up-short-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER published FROM events
        TO api METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 5m WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, error_code = error.class
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_events_subscription TO failed_events;
      START;
      """
    And http payload is posted to host "give-up-short-{{test_id}}.example.com" path "/events"
      """
      {"id":"retried-{{test_id}}"}
      """
    Then HTTP receiver "api" eventually receives at least 4 requests
    And HTTP receiver "api" request 4 repeats request 1
    And the relay subscription does not receive a payload within "5s"
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An emitter whose GIVE UP AFTER is 0s never retries a row its destination did not take
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 503
      """
    And HTTP receiver "api" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (id STRING);
      CREATE SCHEMA failed_event (id STRING, error_code STRING, operation STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY failed_events SCHEMA failed_event UNBRANCHED;
      CREATE VHOST edge give-up-never-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER published FROM events
        TO api METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 0s WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_events_subscription TO failed_events;
      START;
      """
    And http payload is posted to host "give-up-never-{{test_id}}.example.com" path "/events"
      """
      {"id":"once-{{test_id}}"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"once-{{test_id}}" | "error_code":"unavailable" | "operation":"publish"
      """
    When http payload is posted to host "give-up-never-{{test_id}}.example.com" path "/events"
      """
      {"id":"after-{{test_id}}"}
      """
    Then HTTP receiver "api" eventually captures one request that is
      """
      POST /events/after-{{test_id}}
      """
    And HTTP receiver "api" has captured exactly 2 requests
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A destination that refuses writes as it is configured is retried, shown as misconfigured and delivers once repaired
    Given OTLP receiver "collector" is running for "<protocol>"
    And OTLP receiver "collector" answers with
      """
      forbidden
      forbidden
      forbidden
      forbidden
      forbidden
      forbidden
      """
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (seq I64, body STRING);
      CREATE SCHEMA failed_event (seq I64, error_code STRING);
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY failed_events SCHEMA failed_event UNBRANCHED;
      CREATE VHOST edge give-up-setup-{{test_id}}.example.com;
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
      CREATE OTEL LOGS EMITTER repaired_logs FROM events
        TO otel_main
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1s VALUES { 'time' = now(), 'body' = input.body }
          RESOURCE { 'service.name' = 'nervix-cucumber' }
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET seq = input.seq, error_code = error.class
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_events_subscription TO failed_events;
      START;
      """
    And http payload is posted to host "give-up-setup-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"body":"held-{{test_id}}"}]
      """
    Then within "30s" DESCRIBE EMITTER "repaired_logs" on the leader node contains
      """
      misconfigured: <refusal>
      """
    And OTLP receiver "collector" eventually receives at least 7 export requests
    And OTLP receiver "collector" request 7 repeats request 1
    And the relay subscription does not receive a payload within "3s"
    And within "30s" DESCRIBE EMITTER "repaired_logs" on the leader node contains
      """
      misconfigured: -
      """

    Examples:
      | cluster_size | protocol      | refusal                                                      |
      | 1            | grpc          | OTEL gRPC export failed with non-retryable                   |
      | 3            | grpc          | OTEL gRPC export failed with non-retryable                   |
      | 1            | http/protobuf | OTEL HTTP export returned non-retryable status 403 Forbidden |
      | 3            | http/protobuf | OTEL HTTP export returned non-retryable status 403 Forbidden |
