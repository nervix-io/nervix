Feature: Domain execution time
  @domain_execution_time
  Scenario Outline: Every expression context observes its domain execution time
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA source_event (
        sequence I64
      );
      CREATE SCHEMA ingested_event (
        sequence I64,
        ingested_at DATETIME
      );
      CREATE SCHEMA processed_event (
        sequence I64,
        ingested_at DATETIME,
        processed_at DATETIME
      );
      CREATE WIRE JSON SCHEMA source_event_wire MODE STRICT (
        sequence integer
      );
      CREATE CODEC source_event_codec
        FROM WIRE JSON SCHEMA source_event_wire
        TO SCHEMA source_event;
      CREATE RELAY ingested_events SCHEMA ingested_event UNBRANCHED;
      CREATE RELAY processed_events SCHEMA processed_event UNBRANCHED;
      CREATE VHOST edge execution-time-{{test_id}}.example.com;
      CREATE ENDPOINT source_event_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE INGESTOR source_events
        FROM ENDPOINT source_event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_event_codec
        TIMESTAMP NOW
        FILTER WHERE now() < ('2001-01-01T00:00:00Z' AS DATETIME)
        TO ingested_events
          SET sequence = input.sequence,
              ingested_at = now()
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION process_events
        FROM ingested_events WHERE now() < ('2001-01-01T00:00:00Z' AS DATETIME)
        FILTER WHERE now() < ('2001-01-01T00:00:00Z' AS DATETIME)
        UNBRANCHED
        TO processed_events
          INHERIT ALL
          SET processed_at = now()
          WHERE now() < ('2001-01-01T00:00:00Z' AS DATETIME)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION processed_events_subscription TO processed_events
        WHERE now() < ('2001-01-01T00:00:00Z' AS DATETIME);
      """
    And these NSPL commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And http payload is posted to host "execution-time-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "sequence":1
      """
    And the last relay subscription payload field "ingested_at" is saved as timestamp placeholder "ingested_execution_time"
    And the last relay subscription payload field "processed_at" is saved as timestamp placeholder "processor_execution_time"
    And timestamp placeholder "ingested_execution_time" is before "2001-01-01T00:00:00Z"
    And timestamp placeholder "processor_execution_time" is before "2001-01-01T00:00:00Z"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_execution_time
  Scenario Outline: Generated message errors use the failing execution snapshot
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA calculation (
        id STRING,
        numerator I64,
        denominator I64
      );
      CREATE SCHEMA calculation_result (
        id STRING,
        result I64
      );
      CREATE SCHEMA calculation_error (
        id STRING,
        operation STRING,
        occurred_at DATETIME,
        handled_at DATETIME
      );
      CREATE WIRE JSON SCHEMA calculation_wire MODE STRICT (
        id string,
        numerator integer,
        denominator integer
      );
      CREATE CODEC calculation_codec
        FROM WIRE JSON SCHEMA calculation_wire
        TO SCHEMA calculation;
      CREATE RELAY calculations SCHEMA calculation UNBRANCHED;
      CREATE RELAY calculation_results SCHEMA calculation_result UNBRANCHED;
      CREATE RELAY calculation_errors SCHEMA calculation_error UNBRANCHED;
      CREATE VHOST edge execution-error-time-{{test_id}}.example.com;
      CREATE ENDPOINT calculation_endpoint
        ON edge
        PATH '/calculations'
        TYPE HTTP;
      CREATE INGESTOR calculation_source
        FROM ENDPOINT calculation_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING calculation_codec
        TIMESTAMP NOW
        TO calculations
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION calculate
        FROM calculations
        UNBRANCHED
        TO calculation_results
          SET id = input.id,
              result = input.numerator / input.denominator
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO calculation_errors
            SET id = input.id,
                operation = error.operation,
                occurred_at = error.occurred_at,
                handled_at = now();
      CREATE SUBSCRIPTION calculation_errors_subscription TO calculation_errors;
      """
    And these NSPL commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And http payload is posted to host "execution-error-time-{{test_id}}.example.com" path "/calculations"
      """
      {"id":"division-by-zero","numerator":1,"denominator":0}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "id":"division-by-zero"
      """
    And the last relay subscription payload field "occurred_at" is saved as timestamp placeholder "error_occurrence_time"
    And the last relay subscription payload field "handled_at" is saved as timestamp placeholder "error_handler_time"
    And timestamp placeholder "error_occurrence_time" is before "2001-01-01T00:00:00Z"
    And timestamp placeholder "error_handler_time" equals timestamp placeholder "error_occurrence_time"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_execution_time
  Scenario Outline: Generated errors WASM timeouts and telemetry timestamps use their assigned clock classes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has historical-time tokenless WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE historical_time_guest;
      UPLOAD RESOURCE historical_time_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 10.0;
      CREATE SCHEMA trigger_event (
        id I64
      );
      CREATE SCHEMA generated_event (
        value I64
      );
      CREATE SCHEMA generated_summary (
        total I64,
        observed_at DATETIME
      );
      CREATE WIRE JSON SCHEMA trigger_event_wire MODE STRICT (
        id integer
      );
      CREATE CODEC trigger_event_codec
        FROM WIRE JSON SCHEMA trigger_event_wire
        TO SCHEMA trigger_event;
      CREATE RELAY trigger_events SCHEMA trigger_event UNBRANCHED;
      CREATE RELAY generated_events SCHEMA generated_event UNBRANCHED;
      CREATE RELAY generated_summaries SCHEMA generated_summary UNBRANCHED;
      CREATE VHOST edge wasm-execution-time-{{test_id}}.example.com;
      CREATE ENDPOINT trigger_endpoint
        ON edge
        PATH '/trigger'
        TYPE HTTP;
      CREATE INGESTOR trigger_source
        FROM ENDPOINT trigger_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING trigger_event_codec
        TIMESTAMP NOW
        TO trigger_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR historical_time_output FROM trigger_events
        USING RESOURCE historical_time_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        UNBRANCHED
        TO generated_events
          SET value = value
          ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE WINDOW PROCESSOR summarize_generated FROM generated_events
        WIDTH 1s DURATION
        STEP 1s DURATION
        UNBRANCHED
        TO generated_summaries
          SET total = SUM(input.value), observed_at = now()
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION generated_summaries_subscription TO generated_summaries;
      """
    And http payload is posted to host "wasm-execution-time-{{test_id}}.example.com" path "/trigger"
      """
      {"id":1}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "total":42
      """
    And the last relay subscription payload field "observed_at" is saved as timestamp placeholder "wasm_execution_time"
    And timestamp placeholder "wasm_execution_time" is before "2001-01-01T00:00:00Z"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
