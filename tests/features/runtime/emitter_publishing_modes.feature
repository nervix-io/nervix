Feature: Emitter publishing modes
  @emitter_publishing_mode_canonical_roundtrip
  Scenario Outline: Every emitter publishing mode round-trips through SHOW CREATE
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
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY tenant_outgoing SCHEMA event BRANCHED BY by_tenant;

      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092'
      };
      CREATE CLIENT pulsar_main TYPE PULSAR CONFIG {
        'addr' = 'pulsar://127.0.0.1:6650'
      };
      CREATE CLIENT rabbit_main TYPE RABBITMQ CONFIG {
        'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
      };
      CREATE CLIENT mqtt_main TYPE MQTT CONFIG {
        'addr' = 'mqtt://127.0.0.1:1883',
        'client_id' = 'publishing-modes-{{test_id}}'
      };
      CREATE CLIENT nats_main TYPE NATS CONFIG {
        'addr' = 'nats://127.0.0.1:4222'
      };
      CREATE CLIENT redis_main TYPE REDIS CONFIG {
        'addr' = 'redis://127.0.0.1:6379/'
      };
      CREATE CLIENT zeromq_main TYPE ZEROMQ CONFIG {
        'addr' = 'tcp://127.0.0.1:63001',
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
      CREATE CLIENT clickhouse_main TYPE CLICKHOUSE CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE CLIENT postgres_main TYPE POSTGRES CONFIG {
        'addr' = 'host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres'
      };
      CREATE CLIENT mysql_main TYPE MYSQL CONFIG {
        'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
      };
      CREATE CLIENT mongodb_main TYPE MONGODB CONFIG {
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

      CREATE EMITTER kafka_no_ack FROM outgoing
        TO KAFKA kafka_main TOPIC kafka_no_ack
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER kafka_ack_sequential FROM outgoing
        TO KAFKA kafka_main TOPIC kafka_ack_sequential
          MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 20ms MAX 2s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER kafka_ack_parallel FROM outgoing
        TO KAFKA kafka_main TOPIC kafka_ack_parallel
          MODE ACK PARALLEL MAX 3 ACK TIMEOUT 3s RETRY POLICY BACKOFF 30ms MAX 3s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER pulsar_no_ack FROM outgoing
        TO PULSAR pulsar_main TOPIC pulsar_no_ack
          MODE NO_ACK RETRY POLICY BACKOFF 40ms MAX 4s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER pulsar_ack_parallel FROM outgoing
        TO PULSAR pulsar_main TOPIC pulsar_ack_parallel
          MODE ACK PARALLEL MAX 5 ACK TIMEOUT 5s RETRY POLICY BACKOFF 50ms MAX 5s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER rabbit_no_ack FROM outgoing
        TO RABBITMQ rabbit_main QUEUE rabbit_no_ack
          MODE NO_ACK RETRY POLICY BACKOFF 60ms MAX 6s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER rabbit_ack_sequential FROM outgoing
        TO RABBITMQ rabbit_main QUEUE rabbit_ack_sequential
          MODE ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 70ms MAX 7s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER mqtt_qos_zero FROM outgoing
        TO MQTT mqtt_main TOPIC mqtt_qos_zero
          MODE QOS 0 RETRY POLICY BACKOFF 80ms MAX 8s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mqtt_qos_one FROM outgoing
        TO MQTT mqtt_main TOPIC mqtt_qos_one
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 9s RETRY POLICY BACKOFF 90ms MAX 9s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mqtt_qos_two FROM outgoing
        TO MQTT mqtt_main TOPIC mqtt_qos_two
          MODE QOS 2 ACK PARALLEL MAX 10 ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 10s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER nats_no_ack FROM outgoing
        TO NATS nats_main SUBJECT nats_no_ack
          MODE NO_ACK RETRY POLICY BACKOFF 110ms MAX 11s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER nats_jetstream FROM outgoing
        TO NATS nats_main SUBJECT nats_jetstream
          MODE JETSTREAM ACK PARALLEL MAX 12 ACK TIMEOUT 12s RETRY POLICY BACKOFF 120ms MAX 12s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER redis_no_ack FROM outgoing
        TO REDIS PUBSUB redis_main CHANNEL redis_no_ack
          MODE NO_ACK RETRY POLICY BACKOFF 130ms MAX 13s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER zeromq_no_ack FROM outgoing
        TO ZEROMQ zeromq_main
          MODE NO_ACK RETRY POLICY BACKOFF 140ms MAX 14s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER sqs_single FROM outgoing
        TO SQS sqs_main QUEUE sqs_single
          MODE SINGLE RETRY POLICY BACKOFF 150ms MAX 15s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sqs_batch FROM outgoing
        TO SQS sqs_main QUEUE sqs_batch
          MODE BATCH RETRY POLICY BACKOFF 160ms MAX 16s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sqs_fifo FROM tenant_outgoing
        TO SQS sqs_main QUEUE sqs_fifo.fifo FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 165ms MAX 16s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sentry_ack FROM outgoing
        TO SENTRY sentry_main
          MODE ACK RETRY POLICY BACKOFF 170ms MAX 17s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER clickhouse_ack FROM outgoing
        TO CLICKHOUSE clickhouse_main INSERT TO TABLE clickhouse_events
          VALUES { 'seq' = input.seq } WITH MAX BATCH 2
          MODE ACK RETRY POLICY BACKOFF 180ms MAX 18s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER postgres_ack FROM outgoing
        TO POSTGRES postgres_main INSERT TO TABLE postgres_events
          VALUES { 'seq' = input.seq } WITH MAX BATCH 3
          MODE ACK RETRY POLICY BACKOFF 190ms MAX 19s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mysql_ack FROM outgoing
        TO MYSQL mysql_main INSERT TO TABLE mysql_events
          VALUES { 'seq' = input.seq } WITH MAX BATCH 4
          MODE ACK RETRY POLICY BACKOFF 200ms MAX 20s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mongodb_ack FROM outgoing
        TO MONGODB mongodb_main INSERT TO COLLECTION mongodb_events
          VALUES { 'seq' = input.seq } WITH MAX BATCH 5
          MODE ACK RETRY POLICY BACKOFF 210ms MAX 21s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER iceberg_ack FROM outgoing
        TO ICEBERG ON S3 object_store TABLE iceberg_events
          VALUES { 'seq' = input.seq }
          LOCATION 's3://nervix-iceberg/tables/publishing-modes-{{test_id}}'
          CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 64MiB
          MODE ACK RETRY POLICY BACKOFF 220ms MAX 22s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_no_ack;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_ack_sequential;
      """
    Then the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 20ms MAX 2s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_ack_parallel;
      """
    Then the last command output contains
      """
      MODE ACK PARALLEL MAX 3 ACK TIMEOUT 3s RETRY POLICY BACKOFF 30ms MAX 3s
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER kafka_ack_parallel;
      """
    Then the last command output contains
      """
      publishing mode: ACK PARALLEL MAX 3 ACK TIMEOUT 3s RETRY POLICY BACKOFF 30ms MAX 3s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER pulsar_no_ack;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 40ms MAX 4s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER pulsar_ack_parallel;
      """
    Then the last command output contains
      """
      MODE ACK PARALLEL MAX 5 ACK TIMEOUT 5s RETRY POLICY BACKOFF 50ms MAX 5s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER rabbit_no_ack;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 60ms MAX 6s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER rabbit_ack_sequential;
      """
    Then the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 70ms MAX 7s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER mqtt_qos_zero;
      """
    Then the last command output contains
      """
      MODE QOS 0 RETRY POLICY BACKOFF 80ms MAX 8s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER mqtt_qos_one;
      """
    Then the last command output contains
      """
      MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 9s RETRY POLICY BACKOFF 90ms MAX 9s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER mqtt_qos_two;
      """
    Then the last command output contains
      """
      MODE QOS 2 ACK PARALLEL MAX 10 ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 10s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER nats_no_ack;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 110ms MAX 11s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER nats_jetstream;
      """
    Then the last command output contains
      """
      MODE JETSTREAM ACK PARALLEL MAX 12 ACK TIMEOUT 12s RETRY POLICY BACKOFF 120ms MAX 12s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER redis_no_ack;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 130ms MAX 13s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER zeromq_no_ack;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 140ms MAX 14s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER sqs_single;
      """
    Then the last command output contains
      """
      MODE SINGLE RETRY POLICY BACKOFF 150ms MAX 15s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER sqs_batch;
      """
    Then the last command output contains
      """
      MODE BATCH RETRY POLICY BACKOFF 160ms MAX 16s
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER sqs_fifo;
      """
    Then the last command output contains
      """
      sink: SQS client=sqs_main queue=sqs_fifo.fifo fifo_group=FROM BRANCH
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER sentry_ack;
      """
    Then the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 170ms MAX 17s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER clickhouse_ack;
      """
    Then the last command output contains
      """
      WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 180ms MAX 18s
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER clickhouse_ack;
      """
    Then the last command output contains
      """
      sink: CLICKHOUSE client=clickhouse_main table=clickhouse_events max_batch=2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER postgres_ack;
      """
    Then the last command output contains
      """
      WITH MAX BATCH 3 MODE ACK RETRY POLICY BACKOFF 190ms MAX 19s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER mysql_ack;
      """
    Then the last command output contains
      """
      WITH MAX BATCH 4 MODE ACK RETRY POLICY BACKOFF 200ms MAX 20s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER mongodb_ack;
      """
    Then the last command output contains
      """
      WITH MAX BATCH 5 MODE ACK RETRY POLICY BACKOFF 210ms MAX 21s
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER iceberg_ack;
      """
    Then the last command output contains
      """
      COMMIT EACH 1m MAX SIZE 64MiB MODE ACK RETRY POLICY BACKOFF 220ms MAX 22s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_publishing_mode_validation
  Scenario Outline: Invalid or incomplete emitter publishing contracts are rejected at creation
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
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY tenant_outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092'
      };
      CREATE CLIENT sqs_main TYPE SQS CONFIG {
        'endpoint' = 'http://127.0.0.1:9324',
        'region' = 'us-east-1'
      };
      CREATE CLIENT clickhouse_main TYPE CLICKHOUSE CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE EMITTER alter_fifo_group FROM tenant_outgoing
        TO SQS sqs_main QUEUE alter_orders.fifo FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    When these NSPL commands fail with "MODE"
      """
      CREATE EMITTER missing_mode FROM outgoing
        TO KAFKA kafka_main TOPIC missing_mode ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "RETRY POLICY"
      """
      CREATE EMITTER missing_retry_policy FROM outgoing
        TO KAFKA kafka_main TOPIC missing_retry_policy MODE NO_ACK ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "SEQUENTIAL"
      """
      CREATE EMITTER missing_ack_window FROM outgoing
        TO KAFKA kafka_main TOPIC missing_ack_window
          MODE ACK ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "ACK TIMEOUT"
      """
      CREATE EMITTER missing_ack_timeout FROM outgoing
        TO KAFKA kafka_main TOPIC missing_ack_timeout
          MODE ACK SEQUENTIAL RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "MAX"
      """
      CREATE EMITTER incomplete_retry_policy FROM outgoing
        TO KAFKA kafka_main TOPIC incomplete_retry_policy
          MODE NO_ACK RETRY POLICY BACKOFF 10ms ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "QOS"
      """
      CREATE EMITTER foreign_mode FROM outgoing
        TO KAFKA kafka_main TOPIC foreign_mode
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "greater than zero"
      """
      CREATE EMITTER empty_ack_window FROM outgoing
        TO KAFKA kafka_main TOPIC empty_ack_window
          MODE ACK PARALLEL MAX 0 ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "requires FIFO GROUP"
      """
      CREATE EMITTER fifo_without_group FROM tenant_outgoing
        TO SQS sqs_main QUEUE orders.fifo
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "ending in .fifo"
      """
      CREATE EMITTER group_on_standard_queue FROM tenant_outgoing
        TO SQS sqs_main QUEUE orders FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "requires branched input"
      """
      CREATE EMITTER unbranched_fifo_group FROM outgoing
        TO SQS sqs_main QUEUE orders.fifo FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "requires branched input"
      """
      CREATE EMITTER mixed_fifo_group FROM tenant_outgoing, outgoing
        TO SQS sqs_main QUEUE mixed_orders.fifo FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "requires branched input"
      """
      ALTER EMITTER alter_fifo_group ADD FROM outgoing;
      """
    When these NSPL commands fail with "exact non-sensitive STRING"
      """
      CREATE EMITTER invalid_fifo_expression FROM tenant_outgoing
        TO SQS sqs_main QUEUE orders.fifo FIFO GROUP input.seq
          MODE BATCH RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "MODE"
      """
      CREATE EMITTER fifo_on_kafka FROM outgoing
        TO KAFKA kafka_main TOPIC fifo_on_kafka FIFO GROUP FROM BRANCH
          MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "WITH MAX BATCH"
      """
      CREATE EMITTER clickhouse_without_max_batch FROM outgoing
        TO CLICKHOUSE clickhouse_main INSERT TO TABLE clickhouse_events
          VALUES { 'seq' = input.seq }
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_set_publishing_mode
  Scenario Outline: SET MODE replaces an emitter with ENTITY_PAUSE and rejects a foreign mode
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
      ALTER EMITTER event_sink
        SET MODE NO_ACK RETRY POLICY BACKOFF 20ms MAX 200ms;
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
      MODE NO_ACK RETRY POLICY BACKOFF 20ms MAX 200ms
      """
    When these NSPL commands fail with "ZEROMQ"
      """
      ALTER EMITTER event_sink
        SET MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 20ms MAX 200ms;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_set_mode_drain_timeout_rollback
  Scenario Outline: A timed-out SET MODE drain retains the old emitter and its buffered record
    Given entity gate deadline is configured as "250ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
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
      CREATE RELAY outgoing SCHEMA event UNBRANCHED CAPACITY 1;
      CREATE VHOST edge http-{{test_id}}-set-mode-timeout.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
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
    And emitter "event_sink" enters stall mode
    When http payload is posted to node "node-1" with host "http-{{test_id}}-set-mode-timeout.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "5s" DESCRIBE EMITTER "event_sink" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When these NSPL commands fail with "timed out draining domain"
      """
      ALTER EMITTER event_sink
        SET MODE NO_ACK RETRY POLICY BACKOFF 20ms MAX 200ms;
      """
    Then the last command error contains
      """
      retrying_infrastructure
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER event_sink;
      """
    Then the last command output contains
      """
      MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 100ms
      """
    When emitter "event_sink" leaves stall mode
    Then within "5s" the observed broker receives payloads
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER event_sink
        SET MODE NO_ACK RETRY POLICY BACKOFF 20ms MAX 200ms;
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
      MODE NO_ACK RETRY POLICY BACKOFF 20ms MAX 200ms
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
