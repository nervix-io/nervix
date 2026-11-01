Feature: Plexus file formatting

  The formatter is an offline tool: it never starts a cluster or opens a session, so these
  scenarios exercise the executable directly rather than a one-node and three-node topology.

  Scenario: An unreadable Plexus file reports its UTF-8 decoding cause
    Given a Plexus file "pipeline.plexus" with invalid UTF-8
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 4
    And the last command error contains
      """
      invalid utf-8 sequence of 1 bytes from index 0
      """

  Scenario: The formatter help lists its formatting flags
    When the nervix-plexus-format help is requested
    Then the last command output contains
      """
      Usage: nervix-plexus-format [OPTIONS] <PATH>...
      """
    And the last command output contains
      """
      --check
      """
    And the last command output contains
      """
      --stdout
      """

  Scenario: An unformatted Plexus file is rewritten in place
    Given a Plexus file "pipeline.plexus" containing
      """
      use    demo  ;begin;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      USE demo;
      BEGIN;
      """

  Scenario: An already formatted file is left untouched
    Given a Plexus file "pipeline.plexus" containing
      """
      USE demo;
      BEGIN;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" is unchanged

  Scenario: Check mode reports an unformatted file without rewriting it
    Given a Plexus file "pipeline.plexus" containing
      """
      use    demo  ;
      """
    When nervix-plexus-format checks the Plexus file "pipeline.plexus"
    Then the formatter exits with code 1
    And the last command output contains
      """
      pipeline.plexus
      """
    And the Plexus file "pipeline.plexus" is unchanged

  Scenario: Check mode accepts an already formatted file
    Given a Plexus file "pipeline.plexus" containing
      """
      USE demo;
      """
    When nervix-plexus-format checks the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0

  Scenario: Comments between statements survive formatting
    Given a Plexus file "pipeline.plexus" containing
      """
      // header

      use demo;

      // why we begin
      begin;

      // tail
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      // header

      USE demo;

      // why we begin
      BEGIN;

      // tail
      """

  Scenario: A statement holding a comment is left exactly as written
    Given a Plexus file "pipeline.plexus" containing
      """
      use    demo;

      CREATE RELAY orders // keep me
        SCHEMA order UNBRANCHED CAPACITY 1;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      USE demo;

      CREATE RELAY orders // keep me
        SCHEMA order UNBRANCHED CAPACITY 1;
      """

  Scenario: A pooled client is rewritten with its pool clause between the type and the mount
    Given a Plexus file "pipeline.plexus" containing
      """
      create postgres client postgres_main pool size min 2 max 8 mount dev_tls version 1
        config { 'addr' = 'postgresql://HOST:5432/DATABASE?sslmode=verify-full' };
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      CREATE POSTGRES CLIENT postgres_main
        POOL SIZE MIN 2 MAX 8
        MOUNT dev_tls VERSION 1
        CONFIG {
          'addr' = 'postgresql://HOST:5432/DATABASE?sslmode=verify-full'
        };
      """

  Scenario: HTTP REQUEST emitters keep their attachment, request expressions and explicit body selection when formatted
    Given a Plexus file "http_emitters.plexus" containing
      """
      create attached HTTP REQUEST EMITTER deliver_event from outgoing to api method input.request_method
        path input.request_path mode ack retry policy backoff 250ms max 30s
        encode using event_body_codec inherit event_id, payload
        invoke write_header('Content-Type', 'application/json'), write_header('X-Tenant', input.tenant)
        flush each 100ms max batch size 1MiB on message error log on general error log;
      create detached HTTP REQUEST EMITTER delete_event from outgoing where input.request_method = 'DELETE'
        to api method 'DELETE' path concat('/v1/events/', input.event_id)
        mode ack retry policy backoff 250ms max 30s without body
        invoke write_header('Idempotency-Key', input.event_id)
        flush immediate on message error log on general error log;
      """
    When nervix-plexus-format formats the Plexus file "http_emitters.plexus"
    Then the formatter exits with code 0
    And the Plexus file "http_emitters.plexus" contains
      """
      CREATE ATTACHED HTTP REQUEST EMITTER deliver_event
        FROM outgoing
        TO api METHOD input.request_method PATH input.request_path
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id, payload
        INVOKE write_header('Content-Type', 'application/json'), write_header('X-Tenant', input.tenant)
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DETACHED HTTP REQUEST EMITTER delete_event
        FROM outgoing WHERE input.request_method = 'DELETE'
        TO api METHOD 'DELETE' PATH concat('/v1/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('Idempotency-Key', input.event_id)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When nervix-plexus-format checks the Plexus file "http_emitters.plexus"
    Then the formatter exits with code 0

  Scenario: A directory is searched recursively for Plexus files
    Given a Plexus file "top.plexus" containing
      """
      use    demo  ;
      """
    And a Plexus file "nested/deep/inner.plexus" containing
      """
      begin;
      """
    And a Plexus file "nested/notes.txt" containing
      """
      use    demo  ;
      """
    When nervix-plexus-format formats the Plexus directory
    Then the formatter exits with code 0
    And the Plexus file "top.plexus" contains
      """
      USE demo;
      """
    And the Plexus file "nested/deep/inner.plexus" contains
      """
      BEGIN;
      """
    And the Plexus file "nested/notes.txt" is unchanged

  Scenario: Check mode reports unformatted files found by searching a directory
    Given a Plexus file "nested/deep/inner.plexus" containing
      """
      use    demo  ;
      """
    When nervix-plexus-format checks the Plexus directory
    Then the formatter exits with code 1
    And the last command output contains
      """
      inner.plexus
      """
    And the Plexus file "nested/deep/inner.plexus" is unchanged

  Scenario: A file that cannot be parsed is reported and left untouched
    Given a Plexus file "broken.plexus" containing
      """
      CREATE RELAY;
      """
    When nervix-plexus-format formats the Plexus file "broken.plexus"
    Then the formatter exits with code 3
    And the last command error contains
      """
      expected relay_name
      """
    And the Plexus file "broken.plexus" is unchanged

  Scenario: A duration longer than any clock can hold is reported and left untouched
    Given a Plexus file "broken.plexus" containing
      """
      create paced domain simulation with period 18446744073709551615.5s500000000ns skew 1s;
      """
    When nervix-plexus-format formats the Plexus file "broken.plexus"
    Then the formatter exits with code 3
    And the last command error contains
      """
      expected duration_literal
      """
    And the Plexus file "broken.plexus" is unchanged

  Scenario: A later statement that cannot be parsed is reported at its own line
    Given a Plexus file "broken.plexus" containing
      """
      USE demo;
      CREATE RELAY;
      """
    When nervix-plexus-format formats the Plexus file "broken.plexus"
    Then the formatter exits with code 3
    And the last command error contains
      """
      broken.plexus:2:13
      """
    And the last command error contains
      """
      expected relay_name
      """
    And the Plexus file "broken.plexus" is unchanged

  Scenario: An expression a statement embeds is reported at its own offending token
    Given a Plexus file "broken.plexus" containing
      """
      create subscription readings to metrics where input.value = = 1;
      """
    When nervix-plexus-format formats the Plexus file "broken.plexus"
    Then the formatter exits with code 3
    And the last command error contains
      """
      broken.plexus:1:61
      """
    And the last command error contains
      """
      found =
      """
    And the Plexus file "broken.plexus" is unchanged

  Scenario: A number split at its dot is not a float in a statement's expression
    Given a Plexus file "broken.plexus" containing
      """
      create subscription readings to metrics where input.value = 1 .5;
      """
    When nervix-plexus-format formats the Plexus file "broken.plexus"
    Then the formatter exits with code 3
    And the last command error contains
      """
      broken.plexus:1:63
      """
    And the last command error contains
      """
      found .
      """
    And the Plexus file "broken.plexus" is unchanged

  Scenario: A file that cannot be lexed is reported at the lex stage and left untouched
    Given a Plexus file "unlexable.plexus" containing
      """
      USE 'demo;
      """
    When nervix-plexus-format formats the Plexus file "unlexable.plexus"
    Then the formatter exits with code 3
    And the last command error contains
      """
      lex error
      """
    And the Plexus file "unlexable.plexus" is unchanged

  Scenario: Standard input is formatted to standard output
    When nervix-plexus-format formats the standard input
      """
      use    demo  ;
      """
    Then the formatter exits with code 0
    And the last command output contains
      """
      USE demo;
      """

  Scenario: An MQTT topic keeps its exact case through formatting
    Given a Plexus file "pipeline.plexus" containing
      """
      create mqtt topic ingestor sensor_readings
        from mqtt_main topic 'Sensors' session clean qos 0 mode no ack on quiesce drop
        decode using reading_codec
        to readings unbranched flush immediate on message error log
        on general error log;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      CREATE MQTT TOPIC INGESTOR sensor_readings
        FROM mqtt_main TOPIC 'Sensors' SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE DROP
      """

  Scenario: A dollar-quoted value ending in part of its delimiter keeps its value
    Given a Plexus file "pipeline.plexus" containing
      """
      create user auditor with password $p$it's "quoted"$s$p$;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      CREATE USER auditor WITH PASSWORD $s_1$it's "quoted"$s$s_1$;
      """

  Scenario: A configuration value holding a comma and a line break stays one entry
    Given a Plexus file "pipeline.plexus" containing
      """
      create kafka client kafka_main config { 'bootstrap.servers' = 'localhost:9092', 'sasl.jaas.config' = $v$first,
      second$v$ };
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      'sasl.jaas.config' = $s$first,
      second$s$
      """

  Scenario: A Protobuf message name holding a closing brace keeps its value
    Given a Plexus file "pipeline.plexus" containing
      """
      create codec notification_codec from protobuf using resource proto_bundle version 2
        config { 'file' = 'notification.proto' } message 'nervix.test.Notification}'
        to schema notification_schema with jaq transformations on ingestion '.';
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      MESSAGE 'nervix.test.Notification}'
      """

  Scenario: A domain clock period is written as one duration
    Given a Plexus file "pipeline.plexus" containing
      """
      create paced domain simulation with period 1500ms skew 250ms;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      CREATE PACED DOMAIN simulation WITH PERIOD 1500ms SKEW 250ms;
      """

  Scenario: A leap second in a start time is written as the instant it reads as
    Given a Plexus file "pipeline.plexus" containing
      """
      start at '2016-12-31T23:59:60.5Z';
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      START AT '2017-01-01T00:00:00.500+00:00' TIME RATE 1;
      """

  @kinded_emitter_formatter
  Scenario: SQS queue names retain their case and quoting
    Given a Plexus file "pipeline.plexus" containing
      """
      create SQS QUEUE EMITTER order_events from orders to sqs_main queue 'Orders.fifo' FIFO GROUP 'orders' mode single retry policy backoff 1s max 5s encode using order_codec flush immediate on message error log on general error log;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      TO sqs_main QUEUE 'Orders.fifo' FIFO GROUP 'orders'
      """

  Scenario: An array in a VALUES map is one column value
    Given a Plexus file "pipeline.plexus" containing
      """
      create CLICKHOUSE TABLE EMITTER to_ch from notifications
        to clickhouse_client table 'my_table'
        mode ack retry policy backoff 250ms max 30s values { 'tags' = [input.first_tag, input.second_tag] } batch max messages 500 max size 8MiB
        flush immediate on message error log on general error log;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      'tags' = [input.first_tag, input.second_tag]
      """

  Scenario: A carriage return inside a literal survives formatting
    Given a Plexus file "pipeline.plexus" containing the escaped text
      """
      create user auditor with password $p$first line\r\nsecond line$p$;\r\nuse demo;\r\n
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains the escaped text
      """
      CREATE USER auditor WITH PASSWORD $s$first line\r\nsecond line$s$;\nUSE demo;\n
      """

  Scenario: A trailing comment ending in a carriage return is formatted in one pass
    Given a Plexus file "pipeline.plexus" containing the escaped text
      """
      COMMIT; // done \r
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains the escaped text
      """
      COMMIT; // done\n
      """

  Scenario: Clause keywords written as calls and field scopes stay in their expressions
    Given a Plexus file "pipeline.plexus" containing
      """
      create junction peaks from sensors where (max(input.readings) > 10) filter where (output.total > 0)
        unbranched to alerts inherit all flush immediate on message error log;
      create deduplicator distinct_peaks from sensors deduplicate on max(input.readings), input.id
        max time 10m unbranched to distinct_readings inherit all flush immediate on message error log;
      create reorderer peaks_in_order from sensors by max(input.readings) max time 10s unbranched
        to ordered_readings inherit all flush immediate on message error log;
      create correlator suffix_matches left from sensors where right(left.name, 2) = right.suffix
        right from labels where output.id > 0 correlate where left.id = right.id match earliest
        max time 5s on correlation timeout drop, drop unbranched to matched set id = left.id
        flush immediate on message error log;
      alter junction peaks set filter where concat(input.name, replace(input.name, 'a', 'b')) != '',
        set detached;
      """
    When nervix-plexus-format formats the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0
    And the Plexus file "pipeline.plexus" contains
      """
      CREATE ATTACHED JUNCTION peaks
        FROM sensors WHERE max(input.readings) > 10
        FILTER WHERE output.total > 0
        UNBRANCHED
        TO alerts
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED DEDUPLICATOR distinct_peaks
        FROM sensors
        DEDUPLICATE ON max(input.readings), input.id
        MAX TIME 10m
        UNBRANCHED
        TO distinct_readings
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED REORDERER peaks_in_order
        FROM sensors
        BY max(input.readings)
        MAX TIME 10s
        UNBRANCHED
        TO ordered_readings
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED CORRELATOR suffix_matches
        LEFT FROM sensors WHERE right(left.name, 2) = right.suffix
        RIGHT FROM labels WHERE output.id > 0
        CORRELATE WHERE left.id = right.id
        MATCH EARLIEST
        MAX TIME 5s
        ON CORRELATION TIMEOUT DROP, DROP
        UNBRANCHED
        TO matched
          SET id = left.id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      ALTER JUNCTION peaks SET FILTER WHERE concat(input.name, replace(input.name, 'a', 'b')) != '', SET DETACHED;
      """
    When nervix-plexus-format checks the Plexus file "pipeline.plexus"
    Then the formatter exits with code 0

  Scenario: Fields named like clause keywords stay in their expressions
    Given a Plexus file "trips.plexus" containing
      """
      create junction route_trips from trips where input.to > 0 and on > by
        filter where output.to != max unbranched
        to routed set to = input.to, on = to + 1, by = input.by where on > 0
        flush immediate on message error log;
      create deduplicator distinct_trips from trips deduplicate on input.to, by max time 10m
        unbranched to distinct_trips_out inherit to, on flush immediate on message error log;
      create reorderer ordered_trips from trips by input.by, max max time 10s unbranched
        to ordered_trips_out inherit all except on flush immediate on message error log;
      create correlator matched_trips left from trips where left.to > 0 right from stops
        correlate where left.to = right.match match earliest max time 5s
        on correlation timeout drop, drop unbranched to matched set to = left.to
        flush immediate on message error log;
      create attached HTTP REQUEST EMITTER trip_requests from routed to api method mode path path
        mode ack retry policy backoff 250ms max 30s without body
        invoke write_header('X-Trip-Mode', mode)
        flush immediate on message error log on general error log;
      alter junction route_trips set filter where input.to > on,
        add route to archive set to = input.to flush immediate on message error log,
        set detached;
      """
    When nervix-plexus-format formats the Plexus file "trips.plexus"
    Then the formatter exits with code 0
    And the Plexus file "trips.plexus" contains
      """
      CREATE ATTACHED JUNCTION route_trips
        FROM trips WHERE input.to > 0 AND on > by
        FILTER WHERE output.to != max
        UNBRANCHED
        TO routed
          SET to = input.to,
              on = to + 1,
              by = input.by
          WHERE on > 0
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED DEDUPLICATOR distinct_trips
        FROM trips
        DEDUPLICATE ON input.to, by
        MAX TIME 10m
        UNBRANCHED
        TO distinct_trips_out
          INHERIT to, on
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED REORDERER ordered_trips
        FROM trips
        BY input.by, max
        MAX TIME 10s
        UNBRANCHED
        TO ordered_trips_out
          INHERIT ALL EXCEPT on
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED CORRELATOR matched_trips
        LEFT FROM trips WHERE left.to > 0
        RIGHT FROM stops
        CORRELATE WHERE left.to = right.match
        MATCH EARLIEST
        MAX TIME 5s
        ON CORRELATION TIMEOUT DROP, DROP
        UNBRANCHED
        TO matched
          SET to = left.to
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE ATTACHED HTTP REQUEST EMITTER trip_requests
        FROM routed
        TO api METHOD mode PATH path
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('X-Trip-Mode', mode)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      ALTER JUNCTION route_trips SET FILTER WHERE input.to > on, ADD ROUTE TO archive SET to = input.to FLUSH IMMEDIATE ON MESSAGE ERROR LOG, SET DETACHED;
      """
    When nervix-plexus-format checks the Plexus file "trips.plexus"
    Then the formatter exits with code 0

  Scenario: Names spelled like reserved words are written between backticks
    Given a Plexus file "spans.plexus" containing
      """
      create junction closed_spans from spans where input.end > input.from and `end` > 0
        unbranched using materialized state limits default { `end` = 1 }
        to closed set `end` = input.end, `from` = input.from, `status` = udf::case(input.in)
        where `in` > 0 flush immediate on message error log;
      create subscription watch to closed where `end` > 0 and input.from = 1;
      """
    When nervix-plexus-format formats the Plexus file "spans.plexus"
    Then the formatter exits with code 0
    And the Plexus file "spans.plexus" contains
      """
      CREATE ATTACHED JUNCTION closed_spans
        FROM spans WHERE input.end > input.from AND `end` > 0
        UNBRANCHED
        USING MATERIALIZED STATE limits DEFAULT { `end` = 1 }
        TO closed
          SET `end` = input.end,
              `from` = input.from,
              status = udf::case(input.in)
          WHERE `in` > 0
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION watch TO closed WHERE `end` > 0 AND input.from = 1;
      """
    When nervix-plexus-format checks the Plexus file "spans.plexus"
    Then the formatter exits with code 0
