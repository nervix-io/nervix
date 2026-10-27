Feature: OTEL declarations
  OTEL declarations are checked while the domain is stopped, without a Collector.

  Scenario Outline: Each OTEL signal requires its VALUES mappings on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (event_ts DATETIME, body STRING, span_id STRING <fields>);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT destination CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'protocol' = 'grpc', 'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "OTEL <kind> VALUES requires key '<key>'"
      """
      CREATE OTEL <signal> EMITTER outgoing FROM events TO destination <metric>
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { <values> }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | signal  | metric                                        | kind             | key      | fields                             | values                                                                                                     |
      | 1     | LOGS    |                                               | LOGS             | body     |                                    | 'time' = input.event_ts                                                                                    |
      | 3     | LOGS    |                                               | LOGS             | body     |                                    | 'time' = input.event_ts                                                                                    |
      | 1     | TRACES  |                                               | TRACES           | trace_id |                                    | 'span_id' = input.span_id, 'name' = input.body, 'start_time' = input.event_ts, 'end_time' = input.event_ts |
      | 3     | TRACES  |                                               | TRACES           | trace_id |                                    | 'span_id' = input.span_id, 'name' = input.body, 'start_time' = input.event_ts, 'end_time' = input.event_ts |
      | 1     | METRICS | METRIC 'events' UNIT '1' GAUGE                | METRIC GAUGE     | value    |                                    | 'time' = input.event_ts                                                                                    |
      | 3     | METRICS | METRIC 'events' UNIT '1' GAUGE                | METRIC GAUGE     | value    |                                    | 'time' = input.event_ts                                                                                    |
      | 1     | METRICS | METRIC 'events' UNIT '1' HISTOGRAM CUMULATIVE | METRIC HISTOGRAM | count    | , counts VEC<U64>, bounds VEC<F64> | 'time' = input.event_ts, 'bucket_counts' = input.counts, 'explicit_bounds' = input.bounds                  |
      | 3     | METRICS | METRIC 'events' UNIT '1' HISTOGRAM CUMULATIVE | METRIC HISTOGRAM | count    | , counts VEC<U64>, bounds VEC<F64> | 'time' = input.event_ts, 'bucket_counts' = input.counts, 'explicit_bounds' = input.bounds                  |

  Scenario Outline: OTEL refuses header writes and computed resources on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (event_ts DATETIME, body STRING);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT destination CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'protocol' = 'grpc', 'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "<diagnostic>"
      """
      CREATE OTEL LOGS EMITTER outgoing FROM events TO destination
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'time' = input.event_ts, 'body' = input.body }
        <mapping>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | mapping                                                    | diagnostic                      |
      | 1     | ATTRIBUTES { 'header' = write_header('x-event', 'event') } | write_header                    |
      | 3     | ATTRIBUTES { 'header' = write_header('x-event', 'event') } | write_header                    |
      | 1     | RESOURCE { 'service.name' = input.body }                   | RESOURCE values must be literal |
      | 3     | RESOURCE { 'service.name' = input.body }                   | RESOURCE values must be literal |

  Scenario Outline: An OTEL histogram field requires its declared exact type on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (event_ts DATETIME, count U64, buckets <counts>, bounds <bounds>, signed I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT destination CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'protocol' = 'grpc', 'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "VALUES key '<key>' requires <type>, found Int64"
      """
      CREATE OTEL METRICS EMITTER outgoing FROM events TO destination
        METRIC 'events' UNIT '1' HISTOGRAM CUMULATIVE
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'time' = input.event_ts, 'count' = <count>,
          'bucket_counts' = input.buckets, 'explicit_bounds' = input.bounds <extra> }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | key   | type       | count        | extra                  | counts   | bounds   |
      | 1     | count | U64        | input.signed |                        | VEC<U64> | VEC<F64> |
      | 3     | count | U64        | input.signed |                        | VEC<U64> | VEC<F64> |
      | 1     | sum   | F32 or F64 | input.count  | , 'sum' = input.signed | VEC<U64> | VEC<F64> |
      | 3     | sum   | F32 or F64 | input.count  | , 'sum' = input.signed | VEC<U64> | VEC<F64> |
      | 1     | min   | F32 or F64 | input.count  | , 'min' = input.signed | VEC<U64> | VEC<F64> |
      | 3     | min   | F32 or F64 | input.count  | , 'min' = input.signed | VEC<U64> | VEC<F64> |
      | 1     | max   | F32 or F64 | input.count  | , 'max' = input.signed | VEC<U64> | VEC<F64> |
      | 3     | max   | F32 or F64 | input.count  | , 'max' = input.signed | VEC<U64> | VEC<F64> |

  Scenario Outline: An OTEL client requires its export deadline on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "missing required client config key 'timeout_ms'"
      """
      CREATE OTEL CLIENT destination CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'protocol' = '<protocol>'
      };
      """

    Examples:
      | nodes | protocol      |
      | 1     | grpc          |
      | 3     | grpc          |
      | 1     | http/protobuf |
      | 3     | http/protobuf |

  Scenario Outline: OTEL refuses an unsupported publication mode on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "ACK"
      """
      CREATE OTEL <signal> EMITTER outgoing FROM events TO destination <metric>
        MODE NO ACK VALUES { 'time' = now(), 'body' = 'event' }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | signal  | metric                         |
      | 1     | LOGS    |                                |
      | 3     | LOGS    |                                |
      | 1     | TRACES  |                                |
      | 3     | TRACES  |                                |
      | 1     | METRICS | METRIC 'events' UNIT '1' GAUGE |
      | 3     | METRICS | METRIC 'events' UNIT '1' GAUGE |
