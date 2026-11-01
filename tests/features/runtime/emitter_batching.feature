Feature: Emitter batching configuration
  An emitter may declare a BATCH MAX MESSAGES and MAX SIZE clause between its emitter clause and its
  flush policy. The clause is optional for every emitter except ClickHouse, Postgres, MySQL and
  MongoDB, whose writes always carry several rows. It renders in SHOW CREATE and DESCRIBE, is
  validated where it is written and against the emitter and codec it applies to, and is added,
  replaced or removed by ALTER EMITTER as an entity-pause change.

  @emitter_batching_round_trip
  Scenario Outline: The batching clause round-trips through SHOW CREATE and DESCRIBE for every emitter
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC sentry_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{message: "event", extra: {seq: .seq}}'
          ON EMITTING BATCH '{message: "batched events", extra: {records: .}}';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;

      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
      };
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 4 CONFIG {
        'addr' = 'redis://127.0.0.1:6379/'
      };
      CREATE MQTT CLIENT mqtt_main CONFIG {
        'addr' = 'mqtt://127.0.0.1:1883',
        'client_id' = 'emitter-batching-{{test_id}}'
      };
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE ZEROMQ CLIENT zeromq_main CONFIG {
        'addr' = 'tcp://127.0.0.1:63002',
        'bind' = 'false'
      };
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9324',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SENTRY CLIENT sentry_main CONFIG {
        'dsn' = 'http://public@127.0.0.1:8000/1',
        'timeout_ms' = 5000
      };
      CREATE SYSLOG CLIENT syslog_main CONFIG {
        'protocol' = 'udp',
        'addr' = '127.0.0.1:5514'
      };
      CREATE OTEL CLIENT otel_main CONFIG {
        'endpoint' = 'http://127.0.0.1:4317',
        'protocol' = 'grpc',
        'timeout_ms' = 5000
      };
      CREATE CLICKHOUSE CLIENT clickhouse_main CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'user' = 'default',
        'password' = 'nervix',
        'timeout_ms' = '5000'
      };
      CREATE POSTGRES CLIENT postgres_main POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'postgresql://postgres:nervix@127.0.0.1:5432/postgres?sslmode=disable'
      };
      CREATE MYSQL CLIENT mysql_main POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
      };
      CREATE MONGODB CLIENT mongodb_main POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017',
        'database' = 'nervix'
      };
      CREATE S3 CLIENT object_store CONFIG {
        'endpoint' = 'http://127.0.0.1:9000',
        'region' = 'us-east-1',
        'access_key_id' = 'test',
        'secret_access_key' = 'test',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };

      CREATE KAFKA TOPIC EMITTER kafka_single FROM outgoing
        TO kafka_main TOPIC 'kafka_single'
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER kafka_batched FROM outgoing
        TO kafka_main TOPIC 'kafka_batched'
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL WHERE input.seq > 0
        BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER pulsar_batched FROM outgoing
        TO pulsar_main TOPIC 'pulsar_batched'
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 1 MAX SIZE 1B
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE RABBITMQ QUEUE EMITTER rabbit_batched FROM outgoing
        TO rabbit_main QUEUE 'rabbit_batched'
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 65536 MAX SIZE 16MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REDIS PUBSUB EMITTER redis_batched FROM outgoing
        TO redis_main CHANNEL 'redis_batched'
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 1048576B
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC EMITTER mqtt_batched FROM outgoing
        TO mqtt_main TOPIC 'mqtt_batched'
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 256KB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER nats_batched FROM outgoing
        TO nats_main SUBJECT 'nats_batched'
          MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER zeromq_batched FROM outgoing
        TO zeromq_main
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SQS QUEUE EMITTER sqs_batched FROM outgoing
        TO sqs_main QUEUE 'sqs_batched'
          SEND BATCH MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 100 MAX SIZE 256KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SENTRY EVENT EMITTER sentry_batched FROM outgoing
        TO sentry_main
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING sentry_codec
        INHERIT ALL
        BATCH MAX MESSAGES 50 MAX SIZE 900KB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SYSLOG COLLECTOR EMITTER syslog_batched FROM outgoing
        TO syslog_main
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 20 MAX SIZE 60KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE OTEL LOGS EMITTER otel_batched FROM outgoing
        TO otel_main
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'time' = NOW(), 'body' = 'batched' } BATCH MAX MESSAGES 1000 MAX SIZE 3MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLICKHOUSE TABLE EMITTER clickhouse_rows FROM outgoing
        TO clickhouse_main TABLE 'clickhouse_events'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 500 MAX SIZE 8MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES TABLE EMITTER postgres_rows FROM outgoing
        TO postgres_main TABLE 'postgres_events'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }
          ON CONFLICT ('seq') DO NOTHING WHERE input.seq > 0
        BATCH MAX MESSAGES 250 MAX SIZE 4MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER mysql_rows FROM outgoing
        TO mysql_main TABLE 'mysql_events'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MONGODB COLLECTION EMITTER mongodb_rows FROM outgoing
        TO mongodb_main COLLECTION 'mongodb_events'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 100 MAX SIZE 16MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ICEBERG TABLE EMITTER iceberg_files FROM outgoing
        TO object_store TABLE 'iceberg_events' NAMESPACE '{{domain}}'
          LOCATION 's3://nervix-iceberg/tables/emitter-batching-{{test_id}}'
          CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 65536 MAX SIZE 128MiB
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
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_batched;
      """
    Then the last command output contains
      """
      CREATE ATTACHED KAFKA TOPIC EMITTER kafka_batched
        FROM outgoing
        TO kafka_main TOPIC 'kafka_batched'
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        WHERE input.seq > 0
        BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE CODEC sentry_codec;
      """
    Then the last command output contains
      """
      ON EMITTING BATCH '{message: "batched events", extra: {records: .}}'
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER postgres_rows;
      """
    Then the last command output contains
      """
      to: POSTGRES TABLE postgres_main TABLE 'postgres_events'
      options: ON CONFLICT ('seq') DO NOTHING
      mode: ACK RETRY POLICY BACKOFF 10ms MAX 1s
      quiesce: none
      envelope: none
      batch: MAX MESSAGES 250 MAX SIZE 4MiB
      flush: FLUSH IMMEDIATE
      errors: ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
      """
    When these Plexus commands are executed on the leader node
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
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER event_emitter FROM outgoing
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH MAX MESSAGES 500 MAX SIZE 1MiB;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER event_emitter;
      """
    Then the last command output contains
      """
      CREATE ATTACHED ZEROMQ PUSH EMITTER event_emitter
        FROM outgoing
        TO emitter
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH MAX MESSAGES 20 MAX SIZE 64KiB;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER event_emitter;
      """
    Then the last command output contains
      """
      batch: MAX MESSAGES 20 MAX SIZE 64KiB
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter DROP BATCH;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER event_emitter;
      """
    Then the last command output contains
      """
      batch: none
      """
    When these Plexus commands fail with "emitter batching is not configured"
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter DROP BATCH;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batching_database_sinks
  Scenario Outline: A database emitter keeps its batching clause through every ALTER
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT otel_main CONFIG {
        'endpoint' = 'http://127.0.0.1:4317',
        'protocol' = 'grpc',
        'timeout_ms' = 5000
      };
      CREATE MONGODB CLIENT mongodb_main POOL SIZE MIN 1 MAX 2 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017',
        'database' = 'nervix'
      };
      CREATE MONGODB COLLECTION EMITTER row_sink FROM outgoing
        TO mongodb_main COLLECTION 'batching_rows'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE OTEL LOGS EMITTER record_sink FROM outgoing
        TO otel_main
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms VALUES { 'time' = NOW(), 'body' = 'record' } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "MONGODB COLLECTION emitters require BATCH MAX MESSAGES"
      """
      ALTER MONGODB COLLECTION EMITTER row_sink DROP BATCH;
      """
    When these Plexus commands fail with "BATCH"
      """
      CREATE MONGODB COLLECTION EMITTER incomplete_replacement FROM outgoing
        TO mongodb_main COLLECTION 'records'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms VALUES { 'seq' = input.seq }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER MONGODB COLLECTION EMITTER row_sink SET BATCH MAX MESSAGES 10 MAX SIZE 2MiB;
      BEGIN;
      DROP EMITTER record_sink;
      CREATE MONGODB COLLECTION EMITTER record_sink FROM outgoing
        TO mongodb_main COLLECTION 'records'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 100ms VALUES { 'seq' = input.seq }
        BATCH MAX MESSAGES 25 MAX SIZE 4MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      COMMIT;
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
  Scenario Outline: Batching limits and the emitter and codec contracts are validated
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC sentry_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '{message: "event"}';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9324',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SENTRY CLIENT sentry_main CONFIG { 'dsn' = 'http://public@127.0.0.1:8000/1', 'timeout_ms' = 5000 };
      CREATE MONGODB CLIENT mongodb_main POOL SIZE MIN 1 MAX 2 CONFIG {
        'addr' = 'mongodb://127.0.0.1:27017',
        'database' = 'nervix'
      };
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE <emitter_kind> EMITTER rejected FROM outgoing
        TO <connector_body>
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | connector_body                                                                                                    | batch                                      | diagnostic                                                                 | emitter_kind       |
      | 1            | kafka_main TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL       | BATCH MAX MESSAGES 0 MAX SIZE 1MiB         | BATCH MAX MESSAGES must be between 1 and 4294967295, found 0               | KAFKA TOPIC        |
      | 1            | kafka_main TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL       | BATCH MAX MESSAGES 65537 MAX SIZE 1MiB     | max-batch-messages                                                         | KAFKA TOPIC        |
      | 1            | kafka_main TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL       | BATCH MAX MESSAGES 10 MAX SIZE 0MiB        | BATCH MAX SIZE must be greater than zero                                   | KAFKA TOPIC        |
      | 1            | kafka_main TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL       | BATCH MAX MESSAGES 10 MAX SIZE 1.5MiB      | must be a whole number followed by B, KB, KiB, MB, MiB, GB, GiB, TB or TiB | KAFKA TOPIC        |
      | 1            | kafka_main TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL       | BATCH MAX MESSAGES 10 MAX SIZE 16777216TiB | exceeds the largest size a 64-bit byte count can hold                      | KAFKA TOPIC        |
      | 1            | sqs_main QUEUE 'events' SEND BATCH MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL | BATCH MAX MESSAGES 10 MAX SIZE 2MiB        | SQS QUEUE emitters accept BATCH MAX SIZE up to 1MiB                        | SQS QUEUE          |
      | 1            | sentry_main MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING sentry_codec INHERIT ALL                       | BATCH MAX MESSAGES 10 MAX SIZE 900KB       | requires codec 'sentry_codec' to declare an ON EMITTING BATCH              | SENTRY EVENT       |
      | 1            | mongodb_main COLLECTION 'events' MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }           |                                            | BATCH MAX MESSAGES                                                         | MONGODB COLLECTION |
      | 3            | kafka_main TOPIC 'events' MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL       | BATCH MAX MESSAGES 0 MAX SIZE 1MiB         | BATCH MAX MESSAGES must be between 1 and 4294967295, found 0               | KAFKA TOPIC        |
      | 3            | sqs_main QUEUE 'events' SEND BATCH MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec INHERIT ALL | BATCH MAX MESSAGES 10 MAX SIZE 2MiB        | SQS QUEUE emitters accept BATCH MAX SIZE up to 1MiB                        | SQS QUEUE          |
      | 3            | sentry_main MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING sentry_codec INHERIT ALL                       | BATCH MAX MESSAGES 10 MAX SIZE 900KB       | requires codec 'sentry_codec' to declare an ON EMITTING BATCH              | SENTRY EVENT       |
      | 3            | mongodb_main COLLECTION 'events' MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }           |                                            | BATCH MAX MESSAGES                                                         | MONGODB COLLECTION |

  @emitter_batching_codec_grammar
  Scenario: ON EMITTING BATCH is only written after ON EMITTING
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      """
    When these Plexus commands fail with "expected string_literal, found BATCH"
      """
      CREATE CODEC batch_only FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING BATCH '{records: .}';
      """

  @emitter_batching_payload_limit
  Scenario Outline: A batching emitter never publishes a payload larger than MAX SIZE
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "bounded_events_{{test_id}}" exists with 1 partitions
    And Kafka topic "bounded_events_{{test_id}}" is observed
    When these Plexus commands are executed
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
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC EMITTER bounded_events FROM events
        TO kafka_main TOPIC 'bounded_events_{{test_id}}'
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
