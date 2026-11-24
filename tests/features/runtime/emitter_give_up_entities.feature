Feature: Every emitter entity gives up its rows once GIVE UP AFTER passes
  Every emitter entity's retry policy takes GIVE UP AFTER, and the host applies it to every entity
  alike. Each emitter below publishes through its own transport to a destination that refuses every
  connection, so each of its attempts fails; once GIVE UP AFTER has passed since the first failed
  attempt, the row fails with an `unavailable` error of the `publish` operation, which the emitter's
  ON MESSAGE ERROR policy receives. A client consumer emitter's destination is its application,
  whose request to receive a batch again is its failed attempt.

  @emitter_give_up
  Scenario Outline: Every network emitter entity whose destination refuses every connection gives up its row once GIVE UP AFTER passes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (id STRING);
      CREATE SCHEMA failed_event (id STRING, node STRING, error_code STRING, operation STRING);
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SYSLOG CODEC syslog_codec TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY failed_events SCHEMA failed_event UNBRANCHED;
      CREATE VHOST edge give-up-entities-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT kafka_down CONFIG {
        'bootstrap.servers' = '127.0.0.1:1', 'socket.timeout.ms' = '1000'
      };
      CREATE PULSAR CLIENT pulsar_down CONFIG { 'addr' = 'pulsar://127.0.0.1:1' };
      CREATE RABBITMQ CLIENT rabbitmq_down CONFIG {
        'addr' = 'amqp://127.0.0.1:1', 'connect_timeout_ms' = '1000'
      };
      CREATE MQTT CLIENT mqtt_down CONFIG {
        'addr' = 'mqtt://127.0.0.1:1', 'client_id' = 'give-up-{{test_id}}'
      };
      CREATE NATS CLIENT nats_down CONFIG { 'addr' = 'nats://127.0.0.1:1', 'timeout_ms' = '1000' };
      CREATE REDIS CLIENT redis_down POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1', 'connect_timeout_ms' = '1000'
      };
      CREATE ZEROMQ CLIENT zeromq_down CONFIG {
        'addr' = 'tcp://127.0.0.1:1', 'bind' = 'false', 'timeout_ms' = '1000'
      };
      CREATE SQS CLIENT sqs_down CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1',
        'access_key_id' = 'give-up-key', 'secret_access_key' = 'give-up-secret',
        'timeout_ms' = '1000'
      };
      CREATE SENTRY CLIENT sentry_down CONFIG {
        'dsn' = 'http://public@127.0.0.1:1/1', 'timeout_ms' = 1000
      };
      CREATE HTTP CLIENT http_down CONFIG { 'endpoint' = 'http://127.0.0.1:1', 'timeout_ms' = 1000 };
      CREATE OTEL CLIENT otel_down CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'protocol' = 'grpc', 'timeout_ms' = 1000
      };
      CREATE POSTGRES CLIENT postgres_down POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'postgresql://postgres:nervix@127.0.0.1:1/postgres?sslmode=disable',
        'pool_acquire_timeout_ms' = '1000'
      };
      CREATE MYSQL CLIENT mysql_down POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mysql://nervix:nervix@127.0.0.1:1/nervix', 'pool_acquire_timeout_ms' = '1000'
      };
      CREATE CLICKHOUSE CLIENT clickhouse_down CONFIG {
        'addr' = 'http://127.0.0.1:1', 'timeout_ms' = '1000'
      };
      CREATE MONGODB CLIENT mongodb_down POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mongodb://root:nervix@127.0.0.1:1/nervix?authSource=admin', 'database' = 'nervix'
      };
      CREATE SYSLOG CLIENT syslog_down CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:1', 'connect_timeout_ms' = '1000'
      };
      CREATE S3 CLIENT s3_down CONFIG {
        'endpoint' = 'http://127.0.0.1:1', 'region' = 'us-east-1',
        'access_key_id' = 'give-up', 'secret_access_key' = 'give-up'
      };
      CREATE ICEBERG_REST CLIENT catalog_down CONFIG {
        'uri' = 'http://127.0.0.1:1', 'warehouse' = 's3://warehouse'
      };
      CREATE KAFKA TOPIC EMITTER kafka_stranded FROM events
        TO kafka_down TOPIC 'stranded_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER pulsar_stranded FROM events
        TO pulsar_down TOPIC 'stranded_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ QUEUE EMITTER rabbitmq_queue_stranded FROM events
        TO rabbitmq_down QUEUE 'stranded_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ EXCHANGE EMITTER rabbitmq_exchange_stranded FROM events
        TO rabbitmq_down EXCHANGE 'stranded_{{test_id}}' ROUTING KEY input.id
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC EMITTER mqtt_stranded FROM events
        TO mqtt_down TOPIC 'stranded_{{test_id}}' QOS 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER nats_subject_stranded FROM events
        TO nats_down SUBJECT 'stranded_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE NATS STREAM EMITTER nats_stream_stranded FROM events
        TO nats_down STREAM 'stranded_{{test_id}}' SUBJECT 'stranded_stream_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE REDIS PUBSUB EMITTER redis_stranded FROM events
        TO redis_down CHANNEL 'stranded_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER zeromq_stranded FROM events
        TO zeromq_down
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SQS QUEUE EMITTER sqs_stranded FROM events
        TO sqs_down QUEUE 'stranded_{{test_id}}' SEND SINGLE
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SENTRY EVENT EMITTER sentry_stranded FROM events
        TO sentry_down
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER http_stranded FROM events
        TO http_down METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE OTEL LOGS EMITTER otel_logs_stranded FROM events
        TO otel_down
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'time' = now(), 'body' = input.id }
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE OTEL TRACES EMITTER otel_traces_stranded FROM events
        TO otel_down
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES {
            'trace_id' = '00112233445566778899aabbccddeeff',
            'span_id' = '0011223344556677',
            'name' = input.id,
            'kind' = 'INTERNAL',
            'start_time' = now(),
            'end_time' = now(),
            'status_code' = 'OK'
          }
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE OTEL METRICS EMITTER otel_metrics_stranded FROM events
        TO otel_down
          METRIC 'nervix.give_up' UNIT '1' DESCRIPTION 'Rows an emitter gave up'
          SUM MONOTONIC DELTA
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'time' = now(), 'start_time' = now(), 'value' = 1 }
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE POSTGRES TABLE EMITTER postgres_stranded FROM events
        TO postgres_down TABLE 'stranded_{{test_id}}'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER mysql_stranded FROM events
        TO mysql_down TABLE 'nervix.stranded_{{test_id}}'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE CLICKHOUSE TABLE EMITTER clickhouse_stranded FROM events
        TO clickhouse_down TABLE 'stranded_{{test_id}}'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE MONGODB COLLECTION EMITTER mongodb_stranded FROM events
        TO mongodb_down COLLECTION 'stranded_{{test_id}}'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SYSLOG COLLECTOR EMITTER syslog_stranded FROM events
        TO syslog_down
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING syslog_codec
        SET facility = 1 AS U8, severity = 3 AS U8, message = input.id
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ICEBERG TABLE EMITTER iceberg_stranded FROM events
        TO s3_down TABLE 'Stranded' NAMESPACE '{{domain}}' LOCATION 's3://warehouse/stranded'
          CATALOG catalog_down COMMIT EACH 100ms MAX SIZE 1MiB
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          VALUES { 'id' = input.id }
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_events
          SET id = input.id, node = error.node, error_code = error.class, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_events_subscription TO failed_events;
      START;
      """
    And http payload is posted to host "give-up-entities-{{test_id}}.example.com" path "/events"
      """
      {"id":"stranded-{{test_id}}"}
      """
    Then within "180s" the relay subscription receives payloads containing all fragments
      """
      "id":"stranded-{{test_id}}" | "node":"kafka_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"pulsar_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"rabbitmq_queue_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"rabbitmq_exchange_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"mqtt_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"nats_subject_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"nats_stream_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"redis_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"zeromq_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"sqs_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"sentry_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"http_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"otel_logs_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"otel_traces_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"otel_metrics_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"postgres_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"mysql_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"clickhouse_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"mongodb_stranded" | "error_code":"unavailable" | "operation":"publish"
      "id":"stranded-{{test_id}}" | "node":"iceberg_stranded" | "error_code":"unavailable" | "operation":"publish"
      """
    # The Syslog collector's route builds a record of another schema, so its given-up row is
    # observed through its LOG policy's error counter rather than an error relay.
    And within "30s" DESCRIBE EMITTER "syslog_stranded" on the leader node contains
      """
      mode: NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
      """
    And the last command output owner is saved as placeholder "syslog_owner"
    And node "{{syslog_owner}}" observability metric "nervix_message_errors_total" with labels eventually reaches at least 1
      """
      node="syslog_stranded"
      class="unavailable"
      operation="publish"
      """
    And node "{{syslog_owner}}" observability metric "nervix_emitter_give_ups_total" with labels eventually reaches at least 1
      """
      emitter="syslog_stranded"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_give_up @client_emitter
  Scenario Outline: A client consumer emitter whose application asks for a batch again gives the batch up once GIVE UP AFTER passes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR orders_in
        FROM SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER app_output FROM orders
        TO SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s GIVE UP AFTER 0s
        INHERIT id
        SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "only" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "first input" with rows
      | id  | amount |
      | o-2 | 7      |
    And consumer "only" reads output batch "first attempt"
    And output batch "first attempt" is retried
    # The emitter's route builds a record of its own output schema, so the given-up row is observed
    # through its LOG policy: the producer learns the row failed, and the error counter names it.
    Then batch "first input" fails processing because "rejected"
    And within "30s" DESCRIBE EMITTER "app_output" on the leader node contains
      """
      transient error: gave up after 0s of retries: the consumer asked to receive the delivery again
      """
    And the last command output owner is saved as placeholder "consumer_owner"
    And node "{{consumer_owner}}" observability metric "nervix_message_errors_total" with labels eventually reaches at least 1
      """
      node="app_output"
      class="unavailable"
      operation="publish"
      """
    When consumer "only" closes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
