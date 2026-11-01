Feature: Emitter publishing modes
  @emitter_publishing_mode_canonical_roundtrip @long_running
  Scenario Outline: Every emitter publishing mode round-trips through SHOW CREATE
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
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY tenant_outgoing SCHEMA event BRANCHED BY by_tenant;

      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092'
      };
      CREATE PULSAR CLIENT pulsar_main CONFIG {
        'addr' = 'pulsar://127.0.0.1:6650'
      };
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
      };
      CREATE MQTT CLIENT mqtt_main CONFIG {
        'addr' = 'mqtt://127.0.0.1:1883',
        'client_id' = 'publishing-modes-{{test_id}}'
      };
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = 'nats://127.0.0.1:4222'
      };
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 4 CONFIG {
        'addr' = 'redis://127.0.0.1:6379/'
      };
      CREATE ZEROMQ CLIENT zeromq_main CONFIG {
        'addr' = 'tcp://127.0.0.1:63001',
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

      CREATE KAFKA TOPIC EMITTER kafka_no_ack FROM outgoing
        TO kafka_main TOPIC 'kafka_no_ack'
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER kafka_ack_sequential FROM outgoing
        TO kafka_main TOPIC 'kafka_ack_sequential'
          MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 20ms MAX 2s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER kafka_ack_parallel FROM outgoing
        TO kafka_main TOPIC 'kafka_ack_parallel'
          MODE ACK PARALLEL MAX 3 ACK TIMEOUT 3s RETRY POLICY BACKOFF 30ms MAX 3s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE PULSAR TOPIC EMITTER pulsar_no_ack FROM outgoing
        TO pulsar_main TOPIC 'pulsar_no_ack'
          MODE NO ACK RETRY POLICY BACKOFF 40ms MAX 4s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER pulsar_ack_parallel FROM outgoing
        TO pulsar_main TOPIC 'pulsar_ack_parallel'
          MODE ACK PARALLEL MAX 5 ACK TIMEOUT 5s RETRY POLICY BACKOFF 50ms MAX 5s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE RABBITMQ QUEUE EMITTER rabbit_no_ack FROM outgoing
        TO rabbit_main QUEUE 'rabbit_no_ack'
          MODE NO ACK RETRY POLICY BACKOFF 60ms MAX 6s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE RABBITMQ QUEUE EMITTER rabbit_ack_sequential FROM outgoing
        TO rabbit_main QUEUE 'rabbit_ack_sequential'
          MODE ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 70ms MAX 7s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE MQTT TOPIC EMITTER mqtt_qos_zero FROM outgoing
        TO mqtt_main TOPIC 'mqtt_qos_zero'
          MODE QOS 0 RETRY POLICY BACKOFF 80ms MAX 8s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC EMITTER mqtt_qos_one FROM outgoing
        TO mqtt_main TOPIC 'mqtt_qos_one'
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 9s RETRY POLICY BACKOFF 90ms MAX 9s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC EMITTER mqtt_qos_two FROM outgoing
        TO mqtt_main TOPIC 'mqtt_qos_two'
          MODE QOS 2 ACK PARALLEL MAX 10 ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 10s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE NATS SUBJECT EMITTER nats_no_ack FROM outgoing
        TO nats_main SUBJECT 'nats_no_ack'
          MODE NO ACK RETRY POLICY BACKOFF 110ms MAX 11s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER nats_jetstream FROM outgoing
        TO nats_main SUBJECT 'nats_jetstream'
          MODE JETSTREAM ACK PARALLEL MAX 12 ACK TIMEOUT 12s RETRY POLICY BACKOFF 120ms MAX 12s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE REDIS PUBSUB EMITTER redis_no_ack FROM outgoing
        TO redis_main CHANNEL 'redis_no_ack'
          MODE NO ACK RETRY POLICY BACKOFF 130ms MAX 13s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER zeromq_no_ack FROM outgoing
        TO zeromq_main
          MODE NO ACK RETRY POLICY BACKOFF 140ms MAX 14s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE SQS QUEUE EMITTER sqs_single FROM outgoing
        TO sqs_main QUEUE 'sqs_single'
          MODE SINGLE RETRY POLICY BACKOFF 150ms MAX 15s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SQS QUEUE EMITTER sqs_batch FROM outgoing
        TO sqs_main QUEUE 'sqs_batch'
          MODE BATCH RETRY POLICY BACKOFF 160ms MAX 16s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SQS QUEUE EMITTER sqs_fifo FROM tenant_outgoing
        TO sqs_main QUEUE 'sqs_fifo.fifo' FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 165ms MAX 16s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SENTRY EVENT EMITTER sentry_ack FROM outgoing
        TO sentry_main
          MODE ACK RETRY POLICY BACKOFF 170ms MAX 17s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CLICKHOUSE TABLE EMITTER clickhouse_ack FROM outgoing
        TO clickhouse_main TABLE 'clickhouse_events'
          MODE ACK RETRY POLICY BACKOFF 180ms MAX 18s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES TABLE EMITTER postgres_ack FROM outgoing
        TO postgres_main TABLE 'postgres_events'
          MODE ACK RETRY POLICY BACKOFF 190ms MAX 19s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 3 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER mysql_ack FROM outgoing
        TO mysql_main TABLE 'mysql_events'
          MODE ACK RETRY POLICY BACKOFF 200ms MAX 20s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 4 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MONGODB COLLECTION EMITTER mongodb_ack FROM outgoing
        TO mongodb_main COLLECTION 'mongodb_events'
          MODE ACK RETRY POLICY BACKOFF 210ms MAX 21s VALUES { 'seq' = input.seq } BATCH MAX MESSAGES 5 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ICEBERG TABLE EMITTER iceberg_ack FROM outgoing
        TO object_store TABLE 'iceberg_events' NAMESPACE '{{domain}}'
          LOCATION 's3://nervix-iceberg/tables/publishing-modes-{{test_id}}'
          CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 64MiB MODE ACK RETRY POLICY BACKOFF 220ms MAX 22s VALUES { 'seq' = input.seq } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_no_ack;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_ack_sequential;
      """
    Then the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 20ms MAX 2s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER kafka_ack_parallel;
      """
    Then the last command output contains
      """
      MODE ACK PARALLEL MAX 3 ACK TIMEOUT 3s RETRY POLICY BACKOFF 30ms MAX 3s
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER kafka_ack_parallel;
      """
    Then the last command output contains
      """
      mode: ACK PARALLEL MAX 3 ACK TIMEOUT 3s RETRY POLICY BACKOFF 30ms MAX 3s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER pulsar_no_ack;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 40ms MAX 4s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER pulsar_ack_parallel;
      """
    Then the last command output contains
      """
      MODE ACK PARALLEL MAX 5 ACK TIMEOUT 5s RETRY POLICY BACKOFF 50ms MAX 5s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER rabbit_no_ack;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 60ms MAX 6s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER rabbit_ack_sequential;
      """
    Then the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 70ms MAX 7s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER mqtt_qos_zero;
      """
    Then the last command output contains
      """
      MODE QOS 0 RETRY POLICY BACKOFF 80ms MAX 8s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER mqtt_qos_one;
      """
    Then the last command output contains
      """
      MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 9s RETRY POLICY BACKOFF 90ms MAX 9s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER mqtt_qos_two;
      """
    Then the last command output contains
      """
      MODE QOS 2 ACK PARALLEL MAX 10 ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 10s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER nats_no_ack;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 110ms MAX 11s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER nats_jetstream;
      """
    Then the last command output contains
      """
      MODE JETSTREAM ACK PARALLEL MAX 12 ACK TIMEOUT 12s RETRY POLICY BACKOFF 120ms MAX 12s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER redis_no_ack;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 130ms MAX 13s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER zeromq_no_ack;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 140ms MAX 14s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER sqs_single;
      """
    Then the last command output contains
      """
      MODE SINGLE RETRY POLICY BACKOFF 150ms MAX 15s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER sqs_batch;
      """
    Then the last command output contains
      """
      MODE BATCH RETRY POLICY BACKOFF 160ms MAX 16s
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER sqs_fifo;
      """
    Then the last command output contains
      """
      to: SQS QUEUE sqs_main QUEUE 'sqs_fifo.fifo'
      options: FIFO GROUP FROM BRANCH
      mode: SINGLE RETRY POLICY BACKOFF 165ms MAX 16s
      quiesce: none
      envelope: none
      batch: none
      flush: FLUSH IMMEDIATE
      errors: ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER sentry_ack;
      """
    Then the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 170ms MAX 17s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER clickhouse_ack;
      """
    Then the last command output contains
      """
      BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      """
    And the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 180ms MAX 18s
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER clickhouse_ack;
      """
    Then the last command output contains
      """
      to: CLICKHOUSE TABLE clickhouse_main TABLE 'clickhouse_events'
      options: none
      mode: ACK RETRY POLICY BACKOFF 180ms MAX 18s
      quiesce: none
      envelope: none
      batch: MAX MESSAGES 2 MAX SIZE 1MiB
      flush: FLUSH IMMEDIATE
      errors: ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
      """
    And the last command output contains
      """
      flush: FLUSH IMMEDIATE
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER postgres_ack;
      """
    Then the last command output contains
      """
      BATCH MAX MESSAGES 3 MAX SIZE 1MiB
      """
    And the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 190ms MAX 19s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER mysql_ack;
      """
    Then the last command output contains
      """
      BATCH MAX MESSAGES 4 MAX SIZE 1MiB
      """
    And the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 200ms MAX 20s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER mongodb_ack;
      """
    Then the last command output contains
      """
      BATCH MAX MESSAGES 5 MAX SIZE 1MiB
      """
    And the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 210ms MAX 21s
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER iceberg_ack;
      """
    Then the last command output contains
      """
      COMMIT EACH 1m MAX SIZE 64MiB
      """
    And the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 220ms MAX 22s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_publishing_mode_validation
  Scenario Outline: Invalid or incomplete emitter publishing contracts are rejected at creation
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
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY tenant_outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092'
      };
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9324',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE CLICKHOUSE CLIENT clickhouse_main CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'user' = 'default',
        'password' = 'nervix',
        'timeout_ms' = '5000'
      };
      CREATE SQS QUEUE EMITTER alter_fifo_group FROM tenant_outgoing
        TO sqs_main QUEUE 'alter_orders.fifo' FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    When these Plexus commands fail with "MODE"
      """
      CREATE KAFKA TOPIC EMITTER missing_mode FROM outgoing
        TO kafka_main TOPIC 'missing_mode' ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "RETRY POLICY"
      """
      CREATE KAFKA TOPIC EMITTER missing_retry_policy FROM outgoing
        TO kafka_main TOPIC 'missing_retry_policy' MODE NO ACK ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "SEQUENTIAL"
      """
      CREATE KAFKA TOPIC EMITTER missing_ack_window FROM outgoing
        TO kafka_main TOPIC 'missing_ack_window'
          MODE ACK ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "ACK TIMEOUT"
      """
      CREATE KAFKA TOPIC EMITTER missing_ack_timeout FROM outgoing
        TO kafka_main TOPIC 'missing_ack_timeout'
          MODE ACK SEQUENTIAL RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "MAX"
      """
      CREATE KAFKA TOPIC EMITTER incomplete_retry_policy FROM outgoing
        TO kafka_main TOPIC 'incomplete_retry_policy'
          MODE NO ACK RETRY POLICY BACKOFF 10ms ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "QOS"
      """
      CREATE KAFKA TOPIC EMITTER foreign_mode FROM outgoing
        TO kafka_main TOPIC 'foreign_mode'
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "greater than zero"
      """
      CREATE KAFKA TOPIC EMITTER empty_ack_window FROM outgoing
        TO kafka_main TOPIC 'empty_ack_window'
          MODE ACK PARALLEL MAX 0 ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "requires FIFO GROUP"
      """
      CREATE SQS QUEUE EMITTER fifo_without_group FROM tenant_outgoing
        TO sqs_main QUEUE 'orders.fifo'
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "ending in .fifo"
      """
      CREATE SQS QUEUE EMITTER group_on_standard_queue FROM tenant_outgoing
        TO sqs_main QUEUE 'orders' FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "requires branched input"
      """
      CREATE SQS QUEUE EMITTER unbranched_fifo_group FROM outgoing
        TO sqs_main QUEUE 'orders.fifo' FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "requires branched input"
      """
      CREATE SQS QUEUE EMITTER mixed_fifo_group FROM tenant_outgoing, outgoing
        TO sqs_main QUEUE 'mixed_orders.fifo' FIFO GROUP FROM BRANCH
          MODE SINGLE RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "requires branched input"
      """
      ALTER SQS QUEUE EMITTER alter_fifo_group ADD FROM outgoing;
      """
    When these Plexus commands fail with "exact non-sensitive STRING"
      """
      CREATE SQS QUEUE EMITTER invalid_fifo_expression FROM tenant_outgoing
        TO sqs_main QUEUE 'orders.fifo' FIFO GROUP input.seq
          MODE BATCH RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "MODE"
      """
      CREATE KAFKA TOPIC EMITTER fifo_on_kafka FROM outgoing
        TO kafka_main TOPIC 'fifo_on_kafka' FIFO GROUP FROM BRANCH
          MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "BATCH MAX MESSAGES"
      """
      CREATE CLICKHOUSE TABLE EMITTER clickhouse_without_batch FROM outgoing
        TO clickhouse_main TABLE 'clickhouse_events'
          MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_set_publishing_mode
  Scenario Outline: SET MODE replaces the publishing policy with ENTITY_PAUSE
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
      ALTER ZEROMQ PUSH EMITTER event_emitter
        SET MODE NO ACK RETRY POLICY BACKOFF 20ms MAX 200ms;
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
      MODE NO ACK RETRY POLICY BACKOFF 20ms MAX 200ms
      """
    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_set_mode_drain_timeout_rollback
  Scenario Outline: A timed-out SET MODE drain retains the old emitter and its buffered record
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
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
      CREATE RELAY outgoing SCHEMA event UNBRANCHED CAPACITY 1;
      CREATE VHOST edge http-{{test_id}}-set-mode-timeout.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
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
    And emitter "event_emitter" enters stall mode
    When http payload is posted to node "node-1" with host "http-{{test_id}}-set-mode-timeout.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "5s" DESCRIBE EMITTER "event_emitter" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these Plexus commands fail with "timed out draining domain"
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter
        SET MODE NO ACK RETRY POLICY BACKOFF 20ms MAX 200ms;
      """
    Then the last command error contains
      """
      retrying_infrastructure
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER event_emitter;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
      """
    When emitter "event_emitter" leaves stall mode
    Then within "5s" the observed broker receives payloads
      """
      "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter
        SET MODE NO ACK RETRY POLICY BACKOFF 20ms MAX 200ms;
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
      MODE NO ACK RETRY POLICY BACKOFF 20ms MAX 200ms
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
