Feature: Emitter batching configuration
  An emitter may declare a BATCH MAX MESSAGES and MAX SIZE clause between its sink clause and its
  flush policy. The clause is optional for every sink except ClickHouse, Postgres, MySQL and
  MongoDB, whose writes always carry several rows. It renders in SHOW CREATE and DESCRIBE, is
  validated where it is written and against the sink and codec it applies to, and is added,
  replaced or removed by ALTER EMITTER as an entity-pause change.

  @emitter_batching_round_trip
  Scenario Outline: The batching clause round-trips through SHOW CREATE and DESCRIBE for every sink
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC sentry_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{message: "event", extra: {seq: .seq}}'
          ON EMITTING BATCH '{message: "batched events", extra: {records: .}}';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;

      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT pulsar_main TYPE PULSAR CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE CLIENT rabbit_main TYPE RABBITMQ CONFIG {
        'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
      };
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 1 MAX 4 CONFIG {
        'addr' = 'redis://127.0.0.1:6379/'
      };
      CREATE CLIENT mqtt_main TYPE MQTT CONFIG {
        'addr' = 'mqtt://127.0.0.1:1883',
        'client_id' = 'emitter-batching-{{test_id}}'
      };
      CREATE CLIENT nats_main TYPE NATS CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE CLIENT zeromq_main TYPE ZEROMQ CONFIG {
        'addr' = 'tcp://127.0.0.1:63002',
        'bind' = 'false'
      };
      CREATE CLIENT sqs_main TYPE SQS CONFIG {
        'endpoint' = 'http://127.0.0.1:9324',
        'region' = 'us-east-1'
      };
      CREATE CLIENT sentry_main TYPE SENTRY CONFIG {
        'dsn' = 'http://public@127.0.0.1:8000/1',
        'timeout_ms' = 5000
      };
      CREATE CLIENT syslog_main TYPE SYSLOG CONFIG {
        'protocol' = 'udp',
        'addr' = '127.0.0.1:5514'
      };
      CREATE CLIENT otel_main TYPE OTEL CONFIG {
        'endpoint' = 'http://127.0.0.1:4317',
        'protocol' = 'grpc',
        'timeout_ms' = 5000
      };
      CREATE CLIENT clickhouse_main TYPE CLICKHOUSE CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE CLIENT postgres_main TYPE POSTGRES POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'postgresql://postgres:nervix@127.0.0.1:5432/postgres?sslmode=disable'
      };
      CREATE CLIENT mysql_main TYPE MYSQL POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
      };
      CREATE CLIENT mongodb_main TYPE MONGODB POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017',
        'database' = 'nervix'
      };
      CREATE CLIENT object_store TYPE S3 CONFIG {
        'endpoint' = 'http://127.0.0.1:9000',
        'region' = 'us-east-1',
        'access_key_id' = 'test',
        'secret_access_key' = 'test',
        'path_style_access' = true
      };
      CREATE CLIENT iceberg_catalog TYPE ICEBERG_REST CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };

      CREATE EMITTER kafka_single FROM outgoing
        TO KAFKA kafka_main TOPIC kafka_single
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER kafka_batched FROM outgoing
        TO KAFKA kafka_main TOPIC kafka_batched
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL WHERE input.seq > 0
        BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER pulsar_batched FROM outgoing
        TO PULSAR pulsar_main TOPIC pulsar_batched
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 1 MAX SIZE 1B
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER rabbit_batched FROM outgoing
        TO RABBITMQ rabbit_main QUEUE rabbit_batched
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 65536 MAX SIZE 16MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER redis_batched FROM outgoing
        TO REDIS PUBSUB redis_main CHANNEL redis_batched
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 1048576B
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mqtt_batched FROM outgoing
        TO MQTT mqtt_main TOPIC mqtt_batched
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 256KB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER nats_batched FROM outgoing
        TO NATS nats_main SUBJECT nats_batched
          MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER zeromq_batched FROM outgoing
        TO ZEROMQ zeromq_main
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sqs_batched FROM outgoing
        TO SQS sqs_main QUEUE sqs_batched
          MODE BATCH RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 256KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sentry_batched FROM outgoing
        TO SENTRY sentry_main
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING sentry_codec
        INHERIT ALL
        BATCH MAX MESSAGES 50 MAX SIZE 900KB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER syslog_batched FROM outgoing
        TO SYSLOG syslog_main
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 20 MAX SIZE 60KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER otel_batched FROM outgoing
        TO OTEL otel_main LOGS
          VALUES { 'time' = NOW(), 'body' = 'batched' }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        BATCH MAX MESSAGES 1000 MAX SIZE 3MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER clickhouse_rows FROM outgoing
        TO CLICKHOUSE clickhouse_main INSERT TO TABLE clickhouse_events
          VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        BATCH MAX MESSAGES 500 MAX SIZE 8MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER postgres_rows FROM outgoing
        TO POSTGRES postgres_main INSERT TO TABLE postgres_events
          VALUES { 'seq' = input.seq }
          ON CONFLICT ('seq') DO NOTHING
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        WHERE input.seq > 0
        BATCH MAX MESSAGES 250 MAX SIZE 4MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mysql_rows FROM outgoing
        TO MYSQL mysql_main INSERT TO TABLE mysql_events
          VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mongodb_rows FROM outgoing
        TO MONGODB mongodb_main INSERT TO COLLECTION mongodb_events
          VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        BATCH MAX MESSAGES 100 MAX SIZE 16MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER iceberg_files FROM outgoing
        TO ICEBERG ON S3 object_store TABLE iceberg_events
          VALUES { 'seq' = input.seq }
          LOCATION 's3://nervix-iceberg/tables/emitter-batching-{{test_id}}'
          CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        BATCH MAX MESSAGES 65536 MAX SIZE 128MiB
        FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter         | clause                                   |
      | kafka_batched   | BATCH MAX MESSAGES 500 MAX SIZE 1MiB     |
      | pulsar_batched  | BATCH MAX MESSAGES 1 MAX SIZE 1B         |
      | rabbit_batched  | BATCH MAX MESSAGES 65536 MAX SIZE 16MiB  |
      | redis_batched   | BATCH MAX MESSAGES 100 MAX SIZE 1048576B |
      | mqtt_batched    | BATCH MAX MESSAGES 100 MAX SIZE 256KB    |
      | nats_batched    | BATCH MAX MESSAGES 100 MAX SIZE 1MiB     |
      | zeromq_batched  | BATCH MAX MESSAGES 100 MAX SIZE 1MiB     |
      | sqs_batched     | BATCH MAX MESSAGES 100 MAX SIZE 256KiB   |
      | sentry_batched  | BATCH MAX MESSAGES 50 MAX SIZE 900KB     |
      | syslog_batched  | BATCH MAX MESSAGES 20 MAX SIZE 60KiB     |
      | otel_batched    | BATCH MAX MESSAGES 1000 MAX SIZE 3MiB    |
      | clickhouse_rows | BATCH MAX MESSAGES 500 MAX SIZE 8MiB     |
      | postgres_rows   | BATCH MAX MESSAGES 250 MAX SIZE 4MiB     |
      | mysql_rows      | BATCH MAX MESSAGES 100 MAX SIZE 1MiB     |
      | mongodb_rows    | BATCH MAX MESSAGES 100 MAX SIZE 16MiB    |
      | iceberg_files   | BATCH MAX MESSAGES 65536 MAX SIZE 128MiB |
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_batched;
      """
    Then the last command output contains
      """
      CREATE ATTACHED EMITTER kafka_batched
        FROM outgoing
        TO KAFKA kafka_main TOPIC kafka_batched
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        WHERE input.seq > 0
        BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CODEC sentry_codec;
      """
    Then the last command output contains
      """
      ON EMITTING BATCH '{message: "batched events", extra: {records: .}}'
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER postgres_rows;
      """
    Then the last command output contains
      """
      sink: POSTGRES client=postgres_main table=postgres_events conflict=ON CONFLICT (seq) DO NOTHING
      batch: MAX MESSAGES 250 MAX SIZE 4MiB
      flush: FLUSH IMMEDIATE
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER kafka_single;
      """
    Then the last command output contains
      """
      batch: none
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batching_alter
  Scenario Outline: ALTER EMITTER sets, replaces and drops the batching clause as an entity pause
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER event_sink FROM outgoing
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER event_sink SET BATCH MAX MESSAGES 500 MAX SIZE 1MiB;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER event_sink;
      """
    Then the last command output contains
      """
      CREATE ATTACHED EMITTER event_sink
        FROM outgoing
        TO ZEROMQ sink
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER event_sink SET BATCH MAX MESSAGES 20 MAX SIZE 64KiB;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER event_sink;
      """
    Then the last command output contains
      """
      batch: MAX MESSAGES 20 MAX SIZE 64KiB
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER event_sink DROP BATCH;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER event_sink;
      """
    Then the last command output contains
      """
      batch: none
      """
    When these NSPL commands fail with "emitter batching is not configured"
      """
      ALTER EMITTER event_sink DROP BATCH;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batching_database_sinks
  Scenario Outline: A database emitter keeps its batching clause through every ALTER
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT otel_main TYPE OTEL CONFIG {
        'endpoint' = 'http://127.0.0.1:4317',
        'protocol' = 'grpc'
      };
      CREATE CLIENT mongodb_main TYPE MONGODB POOL SIZE MIN 1 MAX 2 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017',
        'database' = 'nervix'
      };
      CREATE EMITTER row_sink FROM outgoing
        TO MONGODB mongodb_main INSERT TO COLLECTION batching_rows
          VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms
        BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER record_sink FROM outgoing
        TO OTEL otel_main LOGS
          VALUES { 'time' = NOW(), 'body' = 'record' }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "MONGODB emitters require BATCH MAX MESSAGES"
      """
      ALTER EMITTER row_sink DROP BATCH;
      """
    When these NSPL commands fail with "MONGODB emitters require BATCH MAX MESSAGES"
      """
      ALTER EMITTER record_sink
        SET TO MONGODB mongodb_main INSERT TO COLLECTION records VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER row_sink SET BATCH MAX MESSAGES 10 MAX SIZE 2MiB;
      ALTER EMITTER record_sink
        SET TO MONGODB mongodb_main INSERT TO COLLECTION records VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms,
        SET BATCH MAX MESSAGES 25 MAX SIZE 4MiB;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter     | clause                              |
      | row_sink    | BATCH MAX MESSAGES 10 MAX SIZE 2MiB |
      | record_sink | BATCH MAX MESSAGES 25 MAX SIZE 4MiB |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batching_validation
  Scenario Outline: Batching limits and the sink and codec contracts are validated
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC sentry_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '{message: "event"}';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT sqs_main TYPE SQS CONFIG {
        'endpoint' = 'http://127.0.0.1:9324',
        'region' = 'us-east-1'
      };
      CREATE CLIENT sentry_main TYPE SENTRY CONFIG { 'dsn' = 'http://public@127.0.0.1:8000/1' };
      CREATE CLIENT mongodb_main TYPE MONGODB POOL SIZE MIN 1 MAX 2 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017',
        'database' = 'nervix'
      };
      """
    When these NSPL commands fail with "<diagnostic>"
      """
      CREATE EMITTER rejected FROM outgoing
        TO <sink>
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | sink                                                                                                                    | batch                                      | diagnostic                                                                 |
      | 1            | KAFKA kafka_main TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL         | BATCH MAX MESSAGES 0 MAX SIZE 1MiB         | BATCH MAX MESSAGES must be between 1 and 65536, found 0                    |
      | 1            | KAFKA kafka_main TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL         | BATCH MAX MESSAGES 65537 MAX SIZE 1MiB     | BATCH MAX MESSAGES must be between 1 and 65536, found 65537                |
      | 1            | KAFKA kafka_main TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL         | BATCH MAX MESSAGES 10 MAX SIZE 0MiB        | BATCH MAX SIZE must be greater than zero                                   |
      | 1            | KAFKA kafka_main TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL         | BATCH MAX MESSAGES 10 MAX SIZE 1.5MiB      | must be a whole number followed by B, KB, KiB, MB, MiB, GB, GiB, TB or TiB |
      | 1            | KAFKA kafka_main TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL         | BATCH MAX MESSAGES 10 MAX SIZE 16777216TiB | exceeds the largest size a 64-bit byte count can hold                      |
      | 1            | SQS sqs_main QUEUE events MODE BATCH RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL              | BATCH MAX MESSAGES 10 MAX SIZE 512KiB      | SQS emitters accept BATCH MAX SIZE up to 256KiB                            |
      | 1            | SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING sentry_codec INHERIT ALL                      | BATCH MAX MESSAGES 10 MAX SIZE 900KB       | requires codec 'sentry_codec' to declare an ON EMITTING BATCH              |
      | 1            | MONGODB mongodb_main INSERT TO COLLECTION events VALUES { 'seq' = input.seq } MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s |                                            | BATCH MAX MESSAGES                                                         |
      | 3            | KAFKA kafka_main TOPIC events MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL         | BATCH MAX MESSAGES 0 MAX SIZE 1MiB         | BATCH MAX MESSAGES must be between 1 and 65536, found 0                    |
      | 3            | SQS sqs_main QUEUE events MODE BATCH RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL              | BATCH MAX MESSAGES 10 MAX SIZE 512KiB      | SQS emitters accept BATCH MAX SIZE up to 256KiB                            |
      | 3            | SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING sentry_codec INHERIT ALL                      | BATCH MAX MESSAGES 10 MAX SIZE 900KB       | requires codec 'sentry_codec' to declare an ON EMITTING BATCH              |
      | 3            | MONGODB mongodb_main INSERT TO COLLECTION events VALUES { 'seq' = input.seq } MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s |                                            | BATCH MAX MESSAGES                                                         |

  @emitter_batching_codec_grammar
  Scenario: ON EMITTING BATCH is only written after ON EMITTING
    Given a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      """
    When these NSPL commands fail with "expected string_literal, found BATCH"
      """
      CREATE CODEC batch_only FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING BATCH '{records: .}';
      """

  @emitter_batching_payload_limit
  Scenario Outline: A batching emitter never publishes a payload larger than MAX SIZE
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "bounded_events_{{test_id}}" exists with 1 partitions
    And Kafka topic "bounded_events_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        error_message STRING,
        operation STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE EMITTER bounded_events FROM events
        TO KAFKA kafka_main TOPIC bounded_events_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 32B
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              error_message = error.message,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"seq":1,"note":"exactly-fit"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"seq":2,"note":"one-byte-ovr"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"seq":3,"note":"quote\"escap"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"seq":4,"note":"éééééx"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"seq":5,"note":"éééééé"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"seq":6,"note":"ok"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"note":"exactly-fit"}]
      [{"seq":4,"note":"éééééx"}]
      [{"seq":6,"note":"ok"}]
      """
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":2 | "error_code":"validation" | "operation":"encode" | exceeds MAX SIZE 32B
      "seq":3 | "error_code":"validation" | "operation":"encode" | exceeds MAX SIZE 32B
      "seq":5 | "error_code":"validation" | "operation":"encode" | exceeds MAX SIZE 32B
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
