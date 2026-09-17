Feature: Datetime functions
  Scenario Outline: Datetime functions compute from event timestamps and the domain execution time
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        id STRING,
        occurred_at DATETIME,
        origin DATETIME,
        delay_ms I64,
        epoch_ms I64
      );
      CREATE SCHEMA reading_time (
        id STRING,
        hour_of_day I64,
        day_of_year I64,
        iso_week I64,
        day_start DATETIME,
        week_start DATETIME,
        quarter_hour DATETIME,
        delayed DATETIME,
        elapsed_seconds I64,
        unix_milliseconds I64,
        from_epoch DATETIME,
        execution_day DATETIME,
        execution_day_of_year I64
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        id string,
        occurred_at string,
        origin string,
        delay_ms integer,
        epoch_ms integer
      );
      CREATE CODEC reading_codec
        FROM WIRE JSON SCHEMA reading_wire
        TO SCHEMA reading
        ENCODE occurred_at AS RFC3339,
               origin AS RFC3339;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY reading_times SCHEMA reading_time UNBRANCHED;
      CREATE VHOST edge datetime-functions-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TIMESTAMP NOW
        TO readings
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION derive_reading_times
        FROM readings
        UNBRANCHED
        TO reading_times
          SET id = input.id,
              hour_of_day = date_part('hour', input.occurred_at),
              day_of_year = date_part('day_of_year', input.occurred_at),
              iso_week = date_part('iso_week', input.occurred_at),
              day_start = date_trunc('day', input.occurred_at),
              week_start = date_trunc('week', input.occurred_at),
              quarter_hour = date_bin('minute', 15, input.occurred_at, input.origin),
              delayed = date_add('millisecond', input.delay_ms, input.occurred_at),
              elapsed_seconds = date_diff('second', input.origin, input.occurred_at),
              unix_milliseconds = to_unix('millisecond', input.occurred_at),
              from_epoch = from_unix('millisecond', input.epoch_ms),
              execution_day = date_trunc('day', now()),
              execution_day_of_year = date_part('day_of_year', now())
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION reading_times_subscription TO reading_times;
      START AT '2000-02-29T12:00:00Z' TIME RATE <time_rate>;
      """
    And http payload is posted to node "node-1" with host "datetime-functions-{{test_id}}.example.com" path "/readings"
      """
      {"id":"leap-day","occurred_at":"2000-02-29T23:52:30.250Z","origin":"2000-02-29T00:05:00Z","delay_ms":450000,"epoch_ms":951868800000}
      """
    And http payload is posted to node "node-1" with host "datetime-functions-{{test_id}}.example.com" path "/readings"
      """
      {"id":"before-epoch","occurred_at":"1969-12-31T23:59:59.999999999Z","origin":"1970-01-01T00:00:00Z","delay_ms":-1,"epoch_ms":-1}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"leap-day" | "hour_of_day":23 | "day_of_year":60 | "iso_week":9 | "day_start":"2000-02-29T00:00:00+00:00" | "week_start":"2000-02-28T00:00:00+00:00" | "quarter_hour":"2000-02-29T23:50:00+00:00" | "delayed":"2000-03-01T00:00:00.250+00:00" | "elapsed_seconds":85650 | "unix_milliseconds":951868350250 | "from_epoch":"2000-03-01T00:00:00+00:00" | "execution_day":"2000-02-29T00:00:00+00:00" | "execution_day_of_year":60
      "id":"before-epoch" | "hour_of_day":23 | "day_of_year":365 | "iso_week":1 | "day_start":"1969-12-31T00:00:00+00:00" | "week_start":"1969-12-29T00:00:00+00:00" | "quarter_hour":"1969-12-31T23:45:00+00:00" | "delayed":"1969-12-31T23:59:59.998999999+00:00" | "elapsed_seconds":0 | "unix_milliseconds":-1 | "from_epoch":"1969-12-31T23:59:59.999+00:00" | "execution_day":"2000-02-29T00:00:00+00:00" | "execution_day_of_year":60
      """
    When these NSPL commands are executed on the leader node
      """
      STOP;
      START AT '1999-12-31T12:00:00Z' TIME RATE <time_rate>;
      """
    And http payload is posted to node "node-1" with host "datetime-functions-{{test_id}}.example.com" path "/readings"
      """
      {"id":"next-generation","occurred_at":"2000-01-01T00:00:00Z","origin":"2000-01-01T00:00:00Z","delay_ms":0,"epoch_ms":0}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"next-generation" | "execution_day":"1999-12-31T00:00:00+00:00" | "execution_day_of_year":365
      """

    Examples:
      | cluster_size | time_rate |
      | 1            | 0.01      |
      | 3            | 0.01      |
      | 1            | 100.0     |
      | 3            | 100.0     |

  Scenario Outline: Datetime results outside their types fail only their own messages
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA boundary_event (
        id STRING,
        occurred_at DATETIME,
        origin DATETIME,
        amount I64,
        count I64
      );
      CREATE SCHEMA boundary_result (
        id STRING,
        minute_start DATETIME,
        hour_bin DATETIME,
        shifted DATETIME,
        converted DATETIME,
        elapsed_nanoseconds I64
      );
      CREATE SCHEMA datetime_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC boundary_event_batch_codec
        FROM JSON
        TO SCHEMA boundary_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY boundary_events SCHEMA boundary_event UNBRANCHED;
      CREATE RELAY boundary_results SCHEMA boundary_result UNBRANCHED;
      CREATE RELAY datetime_errors SCHEMA datetime_error UNBRANCHED;
      CREATE VHOST edge datetime-boundaries-{{test_id}}.example.com;
      CREATE ENDPOINT boundary_ingress ON edge PATH '/boundaries' TYPE HTTP;
      CREATE INGESTOR boundary_source
        FROM ENDPOINT boundary_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING boundary_event_batch_codec
        TO boundary_events
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION derive_boundaries
        FROM boundary_events
        UNBRANCHED
        TO boundary_results
          SET id = input.id,
              minute_start = date_trunc('minute', input.occurred_at),
              hour_bin = date_bin('hour', 1, input.occurred_at, input.origin),
              shifted = date_add('day', input.amount, input.occurred_at),
              converted = from_unix('second', input.count),
              elapsed_nanoseconds = date_diff('nanosecond', input.origin, input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO datetime_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION boundary_results_subscription TO boundary_results;
      CREATE SUBSCRIPTION datetime_errors_subscription TO datetime_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "datetime-boundaries-{{test_id}}.example.com" path "/boundaries"
      """
      [{"id":"ordinary","occurred_at":"2024-02-29T12:40:00Z","origin":"2024-01-01T00:30:00Z","amount":1,"count":0},{"id":"minute-before-minimum","occurred_at":"1677-09-21T00:12:43.145224192Z","origin":"1677-09-21T00:12:43.145224192Z","amount":0,"count":0},{"id":"hour-bin-before-minimum","occurred_at":"1677-09-21T00:13:00Z","origin":"1677-09-21T01:30:00Z","amount":0,"count":0},{"id":"shifted-past-maximum","occurred_at":"2262-04-11T00:00:00Z","origin":"2262-04-11T00:00:00Z","amount":1,"count":0},{"id":"converted-past-maximum","occurred_at":"2024-02-29T12:40:00Z","origin":"2024-02-29T12:40:00Z","amount":0,"count":9223372037},{"id":"elapsed-past-maximum","occurred_at":"2262-04-11T23:47:16.854775807Z","origin":"1677-09-21T00:12:43.145224192Z","amount":0,"count":0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ordinary" | "minute_start":"2024-02-29T12:40:00+00:00" | "hour_bin":"2024-02-29T12:30:00+00:00" | "shifted":"2024-03-01T12:40:00+00:00" | "converted":"1970-01-01T00:00:00+00:00" | "elapsed_nanoseconds":5141400000000000
      "input_id":"minute-before-minimum" | overflow: date_trunc result is outside the DATETIME range
      "input_id":"hour-bin-before-minimum" | overflow: date_bin result is outside the DATETIME range
      "input_id":"shifted-past-maximum" | overflow: date_add result is outside the DATETIME range
      "input_id":"converted-past-maximum" | overflow: from_unix result is outside the DATETIME range
      "input_id":"elapsed-past-maximum" | overflow: date_diff result does not fit I64
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Datetime function arguments are validated when the statement is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        occurred_at DATETIME,
        unit STRING,
        amount F64
      );
      CREATE SCHEMA summary (
        value DATETIME
      );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY summaries SCHEMA summary UNBRANCHED;
      """
    When these NSPL commands fail with "function 'date_trunc' does not accept time unit 'month'"
      """
      CREATE JUNCTION truncate_to_month
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_trunc('month', input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'date_part' does not accept date part 'weekday'"
      """
      CREATE JUNCTION extract_weekday
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_part('weekday', input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'date_bin' requires its time unit to be a STRING literal"
      """
      CREATE JUNCTION bin_by_unit_field
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_bin(input.unit, 15, input.occurred_at, input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'date_bin' requires a positive width, found 0"
      """
      CREATE JUNCTION bin_without_width
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_bin('minute', 0, input.occurred_at, input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'from_unix' expects 2 arguments, found 1"
      """
      CREATE JUNCTION convert_without_unit
        FROM events
        UNBRANCHED
        TO summaries
          SET value = from_unix(input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'date_add' requires integer input, found Float64"
      """
      CREATE JUNCTION add_fractional_seconds
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_add('second', input.amount, input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
