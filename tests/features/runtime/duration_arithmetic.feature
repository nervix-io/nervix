Feature: Duration arithmetic
  Durations add, subtract and negate. A DURATION moves a DATETIME, and the difference of two
  DATETIME values is a DURATION. A duration converts to a whole number of a unit with its rounding
  written out, and a whole number of a unit converts to a duration. A result its type cannot hold
  fails its own message, and every other message is computed as usual.

  Scenario Outline: Durations add, subtract and negate, move a DATETIME and convert to whole units in each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA attempt (
        tenant STRING,
        id STRING,
        started_at DATETIME,
        finished_at DATETIME,
        backoff_ms I64
      );
      CREATE SCHEMA attempt_plan (
        tenant STRING,
        id STRING,
        took DURATION,
        backoff DURATION,
        total DURATION,
        spare DURATION,
        reversed DURATION,
        retry_at DATETIME,
        retry_at_from_backoff DATETIME,
        prepared_at DATETIME,
        grace DURATION,
        took_seconds_floor I64,
        took_seconds_ceil I64,
        took_seconds_round I64,
        spare_seconds_floor I64,
        spare_seconds_ceil I64,
        spare_seconds_round I64,
        retry_after_seconds I64,
        quarter_hour DATETIME
      );
      CREATE JAQ JSON CODEC attempt_codec TO SCHEMA attempt WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY attempts SCHEMA attempt BRANCHED BY by_tenant;
      CREATE RELAY attempt_plans SCHEMA attempt_plan BRANCHED BY by_tenant;
      CREATE VHOST edge duration-arithmetic-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/attempts';
      CREATE HTTP ENDPOINT INGESTOR attempt_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING attempt_codec
        TO attempts INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION plan_retries FROM attempts BRANCHED BY by_tenant
        TO attempt_plans
          SET tenant = input.tenant,
              id = input.id,
              took = input.finished_at - input.started_at,
              backoff = to_duration('millisecond', input.backoff_ms),
              total = output.took + output.backoff,
              spare = output.backoff - output.took,
              reversed = -output.took,
              retry_at = input.finished_at + output.backoff,
              retry_at_from_backoff = output.backoff + input.finished_at,
              prepared_at = input.started_at - 1h 30m,
              grace = 10s + 5s,
              took_seconds_floor = duration_floor('second', output.took),
              took_seconds_ceil = duration_ceil('second', output.took),
              took_seconds_round = duration_round('second', output.took),
              spare_seconds_floor = duration_floor('second', output.spare),
              spare_seconds_ceil = duration_ceil('second', output.spare),
              spare_seconds_round = duration_round('second', output.spare),
              retry_after_seconds = duration_ceil('second', output.backoff),
              quarter_hour = date_bin(15m, input.finished_at, input.started_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION attempt_plans_subscription TO attempt_plans;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-arithmetic-{{test_id}}.example.com" path "/attempts"
      """
      [{"tenant":"acme","id":"a1","started_at":"2024-02-29T12:00:00Z","finished_at":"2024-02-29T12:01:30.400Z","backoff_ms":2500},{"tenant":"beta","id":"b1","started_at":"2024-03-10T06:59:59.500Z","finished_at":"2024-03-10T07:00:00.500Z","backoff_ms":1500},{"tenant":"acme","id":"a2","started_at":"2024-02-29T12:00:00Z","finished_at":"2024-02-29T12:00:01Z","backoff_ms":500},{"tenant":"beta","id":"b2","started_at":"2024-03-10T07:14:59.500Z","finished_at":"2024-03-10T07:30:00Z","backoff_ms":60000}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "id":"a1" | "took":"1m 30s 400ms" | "backoff":"2s 500ms" | "total":"1m 32s 900ms" | "spare":"-1m 27s 900ms" | "reversed":"-1m 30s 400ms" | "retry_at":"2024-02-29T12:01:32.900+00:00" | "retry_at_from_backoff":"2024-02-29T12:01:32.900+00:00" | "prepared_at":"2024-02-29T10:30:00+00:00" | "grace":"15s" | "took_seconds_floor":90 | "took_seconds_ceil":91 | "took_seconds_round":90 | "spare_seconds_floor":-88 | "spare_seconds_ceil":-87 | "spare_seconds_round":-88 | "retry_after_seconds":3 | "quarter_hour":"2024-02-29T12:00:00+00:00"
      key={"tenant":"beta"} | "id":"b1" | "took":"1s" | "backoff":"1s 500ms" | "total":"2s 500ms" | "spare":"500ms" | "reversed":"-1s" | "retry_at":"2024-03-10T07:00:02+00:00" | "retry_at_from_backoff":"2024-03-10T07:00:02+00:00" | "prepared_at":"2024-03-10T05:29:59.500+00:00" | "grace":"15s" | "took_seconds_floor":1 | "took_seconds_ceil":1 | "took_seconds_round":1 | "spare_seconds_floor":0 | "spare_seconds_ceil":1 | "spare_seconds_round":1 | "retry_after_seconds":2 | "quarter_hour":"2024-03-10T06:59:59.500+00:00"
      key={"tenant":"acme"} | "id":"a2" | "took":"1s" | "backoff":"500ms" | "total":"1s 500ms" | "spare":"-500ms" | "reversed":"-1s" | "retry_at":"2024-02-29T12:00:01.500+00:00" | "retry_at_from_backoff":"2024-02-29T12:00:01.500+00:00" | "prepared_at":"2024-02-29T10:30:00+00:00" | "grace":"15s" | "took_seconds_floor":1 | "took_seconds_ceil":1 | "took_seconds_round":1 | "spare_seconds_floor":-1 | "spare_seconds_ceil":0 | "spare_seconds_round":-1 | "retry_after_seconds":1 | "quarter_hour":"2024-02-29T12:00:00+00:00"
      key={"tenant":"beta"} | "id":"b2" | "took":"15m 500ms" | "backoff":"1m" | "total":"16m 500ms" | "spare":"-14m 500ms" | "reversed":"-15m 500ms" | "retry_at":"2024-03-10T07:31:00+00:00" | "retry_at_from_backoff":"2024-03-10T07:31:00+00:00" | "prepared_at":"2024-03-10T05:44:59.500+00:00" | "grace":"15s" | "took_seconds_floor":900 | "took_seconds_ceil":901 | "took_seconds_round":901 | "spare_seconds_floor":-841 | "spare_seconds_ceil":-840 | "spare_seconds_round":-841 | "retry_after_seconds":60 | "quarter_hour":"2024-03-10T07:29:59.500+00:00"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A duration or DATETIME result outside its type fails only its own message
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA span_event (
        tenant STRING,
        id STRING,
        at DATETIME,
        other DATETIME,
        length_ns I64,
        count I64
      );
      CREATE SCHEMA span_result (
        tenant STRING,
        id STRING,
        length DURATION,
        days DURATION,
        negated DURATION,
        later DATETIME,
        earlier DATETIME,
        elapsed DURATION,
        shortened DURATION,
        doubled DURATION
      );
      CREATE SCHEMA span_error (tenant STRING, input_id STRING, error_message STRING);
      CREATE JAQ JSON CODEC span_codec TO SCHEMA span_event WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY span_events SCHEMA span_event BRANCHED BY by_tenant;
      CREATE RELAY span_results SCHEMA span_result BRANCHED BY by_tenant;
      CREATE RELAY span_errors SCHEMA span_error BRANCHED BY by_tenant;
      CREATE VHOST edge duration-boundaries-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/spans';
      CREATE HTTP ENDPOINT INGESTOR span_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING span_codec
        TO span_events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION measure_spans FROM span_events BRANCHED BY by_tenant
        TO span_results
          SET tenant = input.tenant,
              id = input.id,
              length = to_duration('nanosecond', input.length_ns),
              days = to_duration('day', input.count),
              negated = -output.length,
              later = input.at + output.length,
              earlier = input.at - output.length,
              elapsed = input.at - input.other,
              shortened = output.days - output.length,
              doubled = output.length + output.length
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO span_errors
          SET tenant = input.tenant,
              input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION span_results_subscription TO span_results;
      CREATE SUBSCRIPTION span_errors_subscription TO span_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-boundaries-{{test_id}}.example.com" path "/spans"
      """
      [{"tenant":"acme","id":"ordinary","at":"2024-02-29T12:40:00Z","other":"2024-02-29T12:00:00Z","length_ns":90000000000,"count":2},{"tenant":"beta","id":"days-past-maximum","at":"2024-02-29T12:40:00Z","other":"2024-02-29T12:40:00Z","length_ns":0,"count":106752},{"tenant":"acme","id":"negated-minimum","at":"1970-01-01T00:00:00Z","other":"1970-01-01T00:00:00Z","length_ns":-9223372036854775808,"count":0},{"tenant":"beta","id":"later-past-maximum","at":"2262-04-11T00:00:00Z","other":"2262-04-11T00:00:00Z","length_ns":86400000000000,"count":0},{"tenant":"acme","id":"earlier-before-minimum","at":"1677-09-21T00:12:43.145224192Z","other":"1677-09-21T00:12:43.145224192Z","length_ns":1,"count":0},{"tenant":"beta","id":"elapsed-past-maximum","at":"2262-04-11T23:47:16.854775807Z","other":"1677-09-21T00:12:43.145224192Z","length_ns":0,"count":0},{"tenant":"acme","id":"shortened-before-minimum","at":"1970-01-01T00:00:00Z","other":"1970-01-01T00:00:00Z","length_ns":9223372036854775807,"count":-106751},{"tenant":"beta","id":"doubled-past-maximum","at":"1970-01-01T00:00:00Z","other":"1970-01-01T00:00:00Z","length_ns":9223372036854775807,"count":0},{"tenant":"beta","id":"steady","at":"2024-03-10T07:00:00Z","other":"2024-03-10T06:00:00Z","length_ns":250000000,"count":-1}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "id":"ordinary" | "length":"1m 30s" | "days":"2d" | "negated":"-1m 30s" | "later":"2024-02-29T12:41:30+00:00" | "earlier":"2024-02-29T12:38:30+00:00" | "elapsed":"40m" | "shortened":"1d 23h 58m 30s" | "doubled":"3m"
      key={"tenant":"beta"} | "id":"steady" | "length":"250ms" | "days":"-1d" | "negated":"-250ms" | "later":"2024-03-10T07:00:00.250+00:00" | "earlier":"2024-03-10T06:59:59.750+00:00" | "elapsed":"1h" | "shortened":"-1d 250ms" | "doubled":"500ms"
      key={"tenant":"beta"} | "input_id":"days-past-maximum" | overflow: to_duration result does not fit a DURATION
      key={"tenant":"acme"} | "input_id":"negated-minimum" | overflow: duration negation result does not fit a DURATION
      key={"tenant":"beta"} | "input_id":"later-past-maximum" | overflow: datetime addition result is outside the DATETIME range
      key={"tenant":"acme"} | "input_id":"earlier-before-minimum" | overflow: datetime subtraction result is outside the DATETIME range
      key={"tenant":"beta"} | "input_id":"elapsed-past-maximum" | overflow: datetime subtraction result does not fit a DURATION
      key={"tenant":"acme"} | "input_id":"shortened-before-minimum" | overflow: duration subtraction result does not fit a DURATION
      key={"tenant":"beta"} | "input_id":"doubled-past-maximum" | overflow: duration addition result does not fit a DURATION
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: An operator or a function that duration arithmetic does not define is refused when it is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA timed (id STRING, wait DURATION, at DATETIME, count I64, ratio F64, unit STRING);
      CREATE SCHEMA timed_result (wait DURATION OPTIONAL, at DATETIME OPTIONAL, count I64 OPTIONAL);
      CREATE RELAY timed_events SCHEMA timed UNBRANCHED;
      CREATE RELAY timed_results SCHEMA timed_result UNBRANCHED;
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE JUNCTION check_arithmetic FROM timed_events UNBRANCHED TO timed_results SET <assignment> FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | assignment                                    | diagnostic                                                                                                                                  |
      | 1            | wait = input.wait * 2                         | operator Mul is not valid for Duration(Nanosecond) and Int64                                                                                |
      | 3            | wait = input.wait / input.wait                | operator Div is not valid for Duration(Nanosecond)                                                                                          |
      | 1            | at = input.at + input.at                      | operator Add is not valid for Timestamp(Nanosecond                                                                                          |
      | 3            | wait = input.wait - input.at                  | operator Sub is not valid for Duration(Nanosecond) and Timestamp(Nanosecond                                                                 |
      | 1            | at = input.count + input.at                   | operator Add is not valid for Int64 and Timestamp(Nanosecond                                                                                |
      | 3            | count = duration_floor('week', input.wait)    | function 'duration_floor' does not accept time unit 'week'; expected one of nanosecond, microsecond, millisecond, second, minute, hour, day |
      | 1            | count = duration_ceil(input.unit, input.wait) | function 'duration_ceil' requires its time unit to be a STRING literal                                                                      |
      | 3            | count = duration_round('second', input.count) | function 'duration_round' requires Duration input, found Int64                                                                              |
      | 1            | wait = to_duration('second', input.ratio)     | function 'to_duration' requires integer input, found Float64                                                                                |
      | 3            | wait = to_duration('month', input.count)      | function 'to_duration' does not accept time unit 'month'                                                                                    |
      | 1            | at = date_bin(0s, input.at, input.at)         | function 'date_bin' requires a positive width, found 0s                                                                                     |
      | 3            | at = date_bin(input.wait, input.at, input.at) | function 'date_bin' requires its width to be a DURATION literal                                                                             |
