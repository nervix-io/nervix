Feature: Calendar, time zone and formatted datetime functions
  Scenario Outline: Calendar functions follow local calendars and the domain execution time
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        id STRING,
        occurred_at DATETIME,
        origin DATETIME,
        months I64
      );
      CREATE SCHEMA local_event (
        id STRING,
        local_hour I64,
        local_hour_start DATETIME,
        local_day_start DATETIME,
        berlin_month_start DATETIME,
        utc_quarter_start DATETIME,
        local_next_day DATETIME,
        shifted DATETIME,
        local_days_elapsed I64,
        months_elapsed I64,
        local_text STRING,
        offset_text STRING,
        execution_date STRING,
        execution_month DATETIME
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        id string,
        occurred_at string,
        origin string,
        months integer
      );
      CREATE CODEC event_codec
        FROM WIRE JSON SCHEMA event_wire
        TO SCHEMA event
        ENCODE occurred_at AS RFC3339,
               origin AS RFC3339;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY local_events SCHEMA local_event UNBRANCHED;
      CREATE VHOST edge calendar-functions-{{test_id}}.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP NOW
        TO events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION localize_events
        FROM events
        UNBRANCHED
        TO local_events
          SET id = input.id,
              local_hour = date_part('hour', input.occurred_at, 'America/New_York'),
              local_hour_start = date_trunc('hour', input.occurred_at, 'America/New_York'),
              local_day_start = date_trunc('day', input.occurred_at, 'America/New_York'),
              berlin_month_start = date_trunc('month', input.occurred_at, 'Europe/Berlin'),
              utc_quarter_start = date_trunc('quarter', input.occurred_at),
              local_next_day = date_add('day', 1, input.occurred_at, 'America/New_York'),
              shifted = date_add('month', input.months, input.occurred_at),
              local_days_elapsed = date_diff('day', input.origin, input.occurred_at, 'America/New_York'),
              months_elapsed = date_diff('month', input.origin, input.occurred_at),
              local_text = format_datetime('%a %d %b %Y %H:%M:%S%.f %Z', input.occurred_at, 'America/New_York'),
              offset_text = format_datetime('%FT%T%:z', input.occurred_at, '+05:30'),
              execution_date = format_datetime('%F %Z', now(), 'Asia/Tokyo'),
              execution_month = date_trunc('month', now(), 'Pacific/Auckland')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION local_events_subscription TO local_events;
      START AT '2000-02-29T12:00:00Z' TIME RATE <time_rate>;
      """
    And http payload is posted to node "node-1" with host "calendar-functions-{{test_id}}.example.com" path "/events"
      """
      {"id":"leap-month-end","occurred_at":"2024-01-31T23:30:00Z","origin":"2023-12-31T23:30:00Z","months":1}
      """
    And http payload is posted to node "node-1" with host "calendar-functions-{{test_id}}.example.com" path "/events"
      """
      {"id":"spring-forward","occurred_at":"2024-03-10T11:00:00.25Z","origin":"2024-03-09T12:00:00Z","months":-1}
      """
    And http payload is posted to node "node-1" with host "calendar-functions-{{test_id}}.example.com" path "/events"
      """
      {"id":"fall-back","occurred_at":"2024-11-03T06:30:00Z","origin":"2024-11-02T05:30:00Z","months":12}
      """
    And http payload is posted to node "node-1" with host "calendar-functions-{{test_id}}.example.com" path "/events"
      """
      {"id":"local-mean-time","occurred_at":"1883-11-18T17:01:00Z","origin":"1883-11-17T17:01:00Z","months":0}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"leap-month-end" | "local_hour":18 | "local_hour_start":"2024-01-31T23:00:00+00:00" | "local_day_start":"2024-01-31T05:00:00+00:00" | "berlin_month_start":"2024-01-31T23:00:00+00:00" | "utc_quarter_start":"2024-01-01T00:00:00+00:00" | "local_next_day":"2024-02-01T23:30:00+00:00" | "shifted":"2024-02-29T23:30:00+00:00" | "local_days_elapsed":31 | "months_elapsed":1 | "local_text":"Wed 31 Jan 2024 18:30:00 EST" | "offset_text":"2024-02-01T05:00:00+05:30" | "execution_date":"2000-02-29 JST" | "execution_month":"2000-02-29T11:00:00+00:00"
      "id":"spring-forward" | "local_hour":7 | "local_hour_start":"2024-03-10T11:00:00+00:00" | "local_day_start":"2024-03-10T05:00:00+00:00" | "berlin_month_start":"2024-02-29T23:00:00+00:00" | "utc_quarter_start":"2024-01-01T00:00:00+00:00" | "local_next_day":"2024-03-11T11:00:00.250+00:00" | "shifted":"2024-02-10T11:00:00.250+00:00" | "local_days_elapsed":1 | "months_elapsed":0 | "local_text":"Sun 10 Mar 2024 07:00:00.25 EDT" | "offset_text":"2024-03-10T16:30:00+05:30" | "execution_date":"2000-02-29 JST" | "execution_month":"2000-02-29T11:00:00+00:00"
      "id":"fall-back" | "local_hour":1 | "local_hour_start":"2024-11-03T05:00:00+00:00" | "local_day_start":"2024-11-03T04:00:00+00:00" | "berlin_month_start":"2024-10-31T23:00:00+00:00" | "utc_quarter_start":"2024-10-01T00:00:00+00:00" | "local_next_day":"2024-11-04T06:30:00+00:00" | "shifted":"2025-11-03T06:30:00+00:00" | "local_days_elapsed":1 | "months_elapsed":0 | "local_text":"Sun 03 Nov 2024 01:30:00 EST" | "offset_text":"2024-11-03T12:00:00+05:30" | "execution_date":"2000-02-29 JST" | "execution_month":"2000-02-29T11:00:00+00:00"
      "id":"local-mean-time" | "local_hour":12 | "local_hour_start":"1883-11-18T16:56:02+00:00" | "local_day_start":"1883-11-18T04:56:02+00:00" | "berlin_month_start":"1883-10-31T23:06:32+00:00" | "utc_quarter_start":"1883-10-01T00:00:00+00:00" | "local_next_day":"1883-11-19T17:01:00+00:00" | "shifted":"1883-11-18T17:01:00+00:00" | "local_days_elapsed":0 | "months_elapsed":0 | "local_text":"Sun 18 Nov 1883 12:01:00 EST" | "offset_text":"1883-11-18T22:31:00+05:30" | "execution_date":"2000-02-29 JST" | "execution_month":"2000-02-29T11:00:00+00:00"
      """
    When these NSPL commands are executed on the leader node
      """
      STOP;
      START AT '1999-12-31T12:00:00Z' TIME RATE <time_rate>;
      """
    And http payload is posted to node "node-1" with host "calendar-functions-{{test_id}}.example.com" path "/events"
      """
      {"id":"next-generation","occurred_at":"2000-01-01T00:00:00Z","origin":"2000-01-01T00:00:00Z","months":0}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"next-generation" | "execution_date":"1999-12-31 JST" | "execution_month":"1999-12-31T11:00:00+00:00"
      """

    Examples:
      | cluster_size | time_rate |
      | 1            | 0.01      |
      | 3            | 0.01      |
      | 1            | 100.0     |
      | 3            | 100.0     |

  Scenario Outline: Parsing resolves local times explicitly and fails only the messages it cannot convert
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        id STRING,
        local_text STRING OPTIONAL,
        log_text STRING OPTIONAL
      );
      CREATE SCHEMA strict_reading (
        id STRING,
        local_time DATETIME OPTIONAL
      );
      CREATE SCHEMA later_reading (
        id STRING,
        later_time DATETIME OPTIONAL
      );
      CREATE SCHEMA logged_reading (
        id STRING,
        logged_at DATETIME OPTIONAL
      );
      CREATE SCHEMA parse_error (
        input_id STRING,
        route STRING,
        error_message STRING
      );
      CREATE CODEC reading_batch_codec
        FROM JSON
        TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY strict_readings SCHEMA strict_reading UNBRANCHED;
      CREATE RELAY later_readings SCHEMA later_reading UNBRANCHED;
      CREATE RELAY logged_readings SCHEMA logged_reading UNBRANCHED;
      CREATE RELAY parse_errors SCHEMA parse_error UNBRANCHED;
      CREATE VHOST edge datetime-parsing-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_batch_codec
        TO readings
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION parse_strict_local_times
        FROM readings
        UNBRANCHED
        TO strict_readings
          SET id = input.id,
              local_time = parse_datetime('%Y-%m-%d %H:%M:%S', input.local_text, 'America/New_York')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO parse_errors
          SET input_id = input.id,
              route = 'strict',
              error_message = error.message;
      CREATE JUNCTION parse_later_local_times
        FROM readings
        UNBRANCHED
        TO later_readings
          SET id = input.id,
              later_time = parse_datetime('%Y-%m-%d %H:%M:%S', input.local_text, 'America/New_York', 'later')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO parse_errors
          SET input_id = input.id,
              route = 'later',
              error_message = error.message;
      CREATE JUNCTION parse_logged_times
        FROM readings
        UNBRANCHED
        TO logged_readings
          SET id = input.id,
              logged_at = parse_datetime('%d/%b/%Y:%H:%M:%S %z', input.log_text)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO parse_errors
          SET input_id = input.id,
              route = 'logged',
              error_message = error.message;
      CREATE SUBSCRIPTION strict_readings_subscription TO strict_readings;
      CREATE SUBSCRIPTION later_readings_subscription TO later_readings;
      CREATE SUBSCRIPTION logged_readings_subscription TO logged_readings;
      CREATE SUBSCRIPTION parse_errors_subscription TO parse_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "datetime-parsing-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"summer","local_text":"2024-07-04 12:30:00","log_text":"04/Jul/2024:12:30:00 -0400"},{"id":"missing","local_text":null,"log_text":null},{"id":"wrong-separator","local_text":"2024/07/04 12:30:00","log_text":"04/Jul/2024:12:30:00 -04:00"},{"id":"february-twenty-ninth","local_text":"2023-02-29 00:00:00","log_text":"29/Feb/2023:00:00:00 +0000"},{"id":"hour-past-day","local_text":"2024-07-04 24:00:00","log_text":null},{"id":"skipped","local_text":"2024-03-10 02:30:00","log_text":null},{"id":"repeated","local_text":"2024-11-03 01:30:00","log_text":null},{"id":"trailing","local_text":"2024-07-04 12:30:00Z","log_text":null},{"id":"past-range","local_text":"2262-04-12 00:00:00","log_text":null}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"summer" | "local_time":"2024-07-04T16:30:00+00:00"
      {"id":"missing"}
      "id":"summer" | "later_time":"2024-07-04T16:30:00+00:00"
      {"id":"missing"}
      "id":"skipped" | "later_time":"2024-03-10T07:30:00+00:00"
      "id":"repeated" | "later_time":"2024-11-03T06:30:00+00:00"
      "id":"summer" | "logged_at":"2024-07-04T16:30:00+00:00"
      {"id":"missing"}
      "input_id":"wrong-separator" | "route":"strict" | cast_failed: parse_datetime input does not match its format at byte 4: expected '-'
      "input_id":"february-twenty-ninth" | "route":"strict" | cast_failed: parse_datetime date does not exist
      "input_id":"hour-past-day" | "route":"strict" | cast_failed: parse_datetime hour is out of range
      "input_id":"skipped" | "route":"strict" | invalid_argument: parse_datetime local time does not exist in America/New_York
      "input_id":"repeated" | "route":"strict" | invalid_argument: parse_datetime local time is ambiguous in America/New_York
      "input_id":"trailing" | "route":"strict" | cast_failed: parse_datetime input continues past its format at byte 19
      "input_id":"past-range" | "route":"strict" | overflow: parse_datetime result is outside the DATETIME range
      "input_id":"past-range" | "route":"later" | overflow: parse_datetime result is outside the DATETIME range
      "input_id":"wrong-separator" | "route":"logged" | cast_failed: parse_datetime input does not match its format at byte 21: expected '%z'
      "input_id":"february-twenty-ninth" | "route":"logged" | cast_failed: parse_datetime date does not exist
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Calendar, time zone and format arguments are validated when the statement is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        occurred_at DATETIME,
        text STRING,
        zone STRING
      );
      CREATE SCHEMA summary (
        value DATETIME,
        label STRING
      );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY summaries SCHEMA summary UNBRANCHED;
      """
    When these NSPL commands fail with "function 'date_trunc' does not accept time zone 'Mars/Olympus_Mons'"
      """
      CREATE JUNCTION truncate_on_mars
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_trunc('day', input.occurred_at, 'Mars/Olympus_Mons'),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'date_trunc' requires its time zone to be a STRING literal"
      """
      CREATE JUNCTION truncate_in_field_zone
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_trunc('day', input.occurred_at, input.zone),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'date_bin' does not accept time unit 'month'"
      """
      CREATE JUNCTION bin_by_month
        FROM events
        UNBRANCHED
        TO summaries
          SET value = date_bin('month', 1, input.occurred_at, input.occurred_at),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'format_datetime' does not accept format '%Y-%q': unknown directive '%q' at byte 3"
      """
      CREATE JUNCTION format_quarter
        FROM events
        UNBRANCHED
        TO summaries
          SET value = input.occurred_at,
              label = format_datetime('%Y-%q', input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "values can be up to 270 bytes long, longer than the 256-byte limit"
      """
      CREATE JUNCTION format_month_names
        FROM events
        UNBRANCHED
        TO summaries
          SET value = input.occurred_at,
              label = format_datetime('%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B%B', input.occurred_at)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'parse_datetime' does not accept format '%H:%M': it does not read a complete date"
      """
      CREATE JUNCTION parse_clock_time
        FROM events
        UNBRANCHED
        TO summaries
          SET value = parse_datetime('%H:%M', input.text, 'UTC'),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'parse_datetime' does not accept format '%d.%m.%y': unknown directive '%y' at byte 6"
      """
      CREATE JUNCTION parse_two_digit_year
        FROM events
        UNBRANCHED
        TO summaries
          SET value = parse_datetime('%d.%m.%y', input.text, 'UTC'),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'parse_datetime' format '%FT%T%:z' reads its UTC offset from the input, so it takes no time zone"
      """
      CREATE JUNCTION parse_offset_in_zone
        FROM events
        UNBRANCHED
        TO summaries
          SET value = parse_datetime('%FT%T%:z', input.text, 'UTC'),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'parse_datetime' format '%F %T' reads no UTC offset or Unix time from the input, so it requires a time zone"
      """
      CREATE JUNCTION parse_without_zone
        FROM events
        UNBRANCHED
        TO summaries
          SET value = parse_datetime('%F %T', input.text),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'parse_datetime' does not accept disambiguation 'first'; expected one of compatible, earlier, later, reject"
      """
      CREATE JUNCTION parse_first_occurrence
        FROM events
        UNBRANCHED
        TO summaries
          SET value = parse_datetime('%F %T', input.text, 'America/New_York', 'first'),
              label = input.text
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
