Feature: Every connector entity uses the shared inspection rows

  @connector_entity_inspection
  Scenario Outline: A stopped ingestor exposes its entity and effective options through the session
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-kinded-ingestors.example.com;
      CREATE ENDPOINT http_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT ws_ingress ON edge PATH '/stream' TYPE WEBSOCKETS;
      CREATE HTTP CLIENT http_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9',
        'timeout_ms' = 5000
      };
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE RABBITMQ CLIENT rabbit_main CONFIG { 'addr' = 'amqp://127.0.0.1:5672' };
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = 'redis://127.0.0.1:6379', 'subscription_buffer_messages' = '1024', 'idle_timeout_ms' = '30000' };
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE PROMETHEUS CLIENT prom_main CONFIG { 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = 5000 };
      CREATE ZEROMQ CLIENT zmq_main CONFIG { 'addr' = 'tcp://127.0.0.1:5555', 'bind' = 'false' };
      CREATE SQS CLIENT sqs_main CONFIG { 'endpoint' = 'http://127.0.0.1:9324', 'region' = 'us-east-1' };
      CREATE WEBSOCKETS CLIENT ws_main CONFIG { 'endpoint' = 'ws://127.0.0.1:9/stream' };
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      CREATE HTTP REQUEST INGESTOR http_request_source
        FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR http_endpoint_source
        FROM http_ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WEBSOCKETS ENDPOINT INGESTOR ws_endpoint_source
        FROM ws_ingress MODE NO ACK ON QUIESCE REJECT RETRY AFTER 5s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_connection_source
        FROM ws_main HEADERS { 'x-trace' = 'stream' } MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR kafka_source
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC INGESTOR pulsar_source
        FROM pulsar_main TOPIC 'persistent://public/default/Orders' SUBSCRIPTION 'Readers' OWNED BY INGESTOR
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ QUEUE INGESTOR rabbitmq_source
        FROM rabbit_main QUEUE 'Orders'
        MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS PUBSUB INGESTOR redis_source
        FROM redis_main CHANNEL 'Orders' MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC INGESTOR mqtt_source
        FROM mqtt_main TOPIC 'devices/+/Orders' SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT INGESTOR nats_source
        FROM nats_main SUBJECT 'orders.*' QUEUE GROUP 'Readers' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS STREAM INGESTOR nats_stream_source
        FROM nats_main STREAM 'Orders' CONSUMER 'Readers'
        MODE ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS QUEUE INGESTOR sqs_source
        FROM sqs_main QUEUE 'Orders.fifo'
        MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ PULL INGESTOR zeromq_source
        FROM zmq_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG LISTENER INGESTOR syslog_source
        FROM syslog_main MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PROMETHEUS QUERY INGESTOR prometheus_source
        FROM prom_main QUERY 'up' EVERY 15s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT PRODUCER INGESTOR client_source
        FROM SCHEMA event MODE ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And these Plexus statements are executed on the leader node
      """
      DESCRIBE INGESTOR <name>;
      """
    Then the last command output contains
      """
      from: <phrase> <reference>
      options: <options>
      mode: <mode>
      quiesce: <quiesce>
      envelope: none
      batch: none
      flush: events=FLUSH IMMEDIATE
      errors: ON GENERAL ERROR LOG; events=ON MESSAGE ERROR LOG
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW INGESTORS;
      """
    Then the last command output contains
      """
      <name> from=<phrase>
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW CREATE INGESTOR <name>;
      """
    Then the last command output contains
      """
      CREATE <phrase> INGESTOR <name>
      """

    Examples:
      | cluster_size | name                 | phrase                | reference                                              | options                                              | mode                                                                  | quiesce               |
      | 1            | http_request_source  | HTTP REQUEST          | http_main                                              | METHOD 'GET' MAX BODY SIZE 16MiB EVERY 1s            | NO ACK                                                                | SUSPEND               |
      | 3            | http_request_source  | HTTP REQUEST          | http_main                                              | METHOD 'GET' MAX BODY SIZE 16MiB EVERY 1s            | NO ACK                                                                | SUSPEND               |
      | 1            | http_endpoint_source | HTTP ENDPOINT         | http_ingress                                           | none                                                 | NO ACK                                                                | BUFFER MAX SIZE 1MiB  |
      | 3            | http_endpoint_source | HTTP ENDPOINT         | http_ingress                                           | none                                                 | NO ACK                                                                | BUFFER MAX SIZE 1MiB  |
      | 1            | ws_endpoint_source   | WEBSOCKETS ENDPOINT   | ws_ingress                                             | none                                                 | NO ACK                                                                | REJECT RETRY AFTER 5s |
      | 3            | ws_endpoint_source   | WEBSOCKETS ENDPOINT   | ws_ingress                                             | none                                                 | NO ACK                                                                | REJECT RETRY AFTER 5s |
      | 1            | ws_connection_source | WEBSOCKETS CONNECTION | ws_main                                                | HEADERS { 'x-trace' = 'stream' }                     | NO ACK                                                                | DROP                  |
      | 3            | ws_connection_source | WEBSOCKETS CONNECTION | ws_main                                                | HEADERS { 'x-trace' = 'stream' }                     | NO ACK                                                                | DROP                  |
      | 1            | kafka_source         | KAFKA TOPIC           | kafka_main TOPIC 'Orders'                              | OFFSET BY CONSUMER GROUP 'Readers' INSTANCES 1       | NO ACK                                                                | SUSPEND               |
      | 3            | kafka_source         | KAFKA TOPIC           | kafka_main TOPIC 'Orders'                              | OFFSET BY CONSUMER GROUP 'Readers' INSTANCES 1       | NO ACK                                                                | SUSPEND               |
      | 1            | pulsar_source        | PULSAR TOPIC          | pulsar_main TOPIC 'persistent://public/default/Orders' | SUBSCRIPTION 'Readers' OWNED BY INGESTOR INSTANCES 1 | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s      | SUSPEND               |
      | 3            | pulsar_source        | PULSAR TOPIC          | pulsar_main TOPIC 'persistent://public/default/Orders' | SUBSCRIPTION 'Readers' OWNED BY INGESTOR INSTANCES 1 | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s      | SUSPEND               |
      | 1            | rabbitmq_source      | RABBITMQ QUEUE        | rabbit_main QUEUE 'Orders'                             | INSTANCES 1                                          | ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s        | SUSPEND               |
      | 3            | rabbitmq_source      | RABBITMQ QUEUE        | rabbit_main QUEUE 'Orders'                             | INSTANCES 1                                          | ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s        | SUSPEND               |
      | 1            | redis_source         | REDIS PUBSUB          | redis_main CHANNEL 'Orders'                            | none                                                 | NO ACK                                                                | DROP                  |
      | 3            | redis_source         | REDIS PUBSUB          | redis_main CHANNEL 'Orders'                            | none                                                 | NO ACK                                                                | DROP                  |
      | 1            | mqtt_source          | MQTT TOPIC            | mqtt_main TOPIC 'devices/+/Orders'                     | INSTANCES 1 SESSION PERSISTENT QOS 1                 | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s       | SUSPEND               |
      | 3            | mqtt_source          | MQTT TOPIC            | mqtt_main TOPIC 'devices/+/Orders'                     | INSTANCES 1 SESSION PERSISTENT QOS 1                 | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s       | SUSPEND               |
      | 1            | nats_source          | NATS SUBJECT          | nats_main SUBJECT 'orders.*'                           | QUEUE GROUP 'Readers' INSTANCES 1                    | NO ACK                                                                | DROP                  |
      | 3            | nats_source          | NATS SUBJECT          | nats_main SUBJECT 'orders.*'                           | QUEUE GROUP 'Readers' INSTANCES 1                    | NO ACK                                                                | DROP                  |
      | 1            | nats_stream_source   | NATS STREAM           | nats_main STREAM 'Orders'                              | CONSUMER 'Readers'                                   | ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s      | SUSPEND               |
      | 3            | nats_stream_source   | NATS STREAM           | nats_main STREAM 'Orders'                              | CONSUMER 'Readers'                                   | ACK SEQUENTIAL ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 1s      | SUSPEND               |
      | 1            | sqs_source           | SQS QUEUE             | sqs_main QUEUE 'Orders.fifo'                           | INSTANCES 1                                          | ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m         | SUSPEND               |
      | 3            | sqs_source           | SQS QUEUE             | sqs_main QUEUE 'Orders.fifo'                           | INSTANCES 1                                          | ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m         | SUSPEND               |
      | 1            | zeromq_source        | ZEROMQ PULL           | zmq_main                                               | none                                                 | NO ACK                                                                | SUSPEND               |
      | 3            | zeromq_source        | ZEROMQ PULL           | zmq_main                                               | none                                                 | NO ACK                                                                | SUSPEND               |
      | 1            | syslog_source        | SYSLOG LISTENER       | syslog_main                                            | none                                                 | NO ACK                                                                | DROP                  |
      | 3            | syslog_source        | SYSLOG LISTENER       | syslog_main                                            | none                                                 | NO ACK                                                                | DROP                  |
      | 1            | prometheus_source    | PROMETHEUS QUERY      | prom_main                                              | QUERY 'up' MAX BODY SIZE 16MiB EVERY 15s             | NO ACK                                                                | SUSPEND               |
      | 3            | prometheus_source    | PROMETHEUS QUERY      | prom_main                                              | QUERY 'up' MAX BODY SIZE 16MiB EVERY 15s             | NO ACK                                                                | SUSPEND               |
      | 1            | client_source        | CLIENT PRODUCER       | SCHEMA event                                           | none                                                 | ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s | SUSPEND               |
      | 3            | client_source        | CLIENT PRODUCER       | SCHEMA event                                           | none                                                 | ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s | SUSPEND               |

  @connector_entity_inspection
  Scenario Outline: A stopped emitter exposes its declared entity through the session
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-kinded-ingestors.example.com;
      CREATE ENDPOINT http_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT ws_ingress ON edge PATH '/stream' TYPE WEBSOCKETS;
      CREATE HTTP CLIENT http_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9',
        'timeout_ms' = 5000
      };
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE RABBITMQ CLIENT rabbit_main CONFIG { 'addr' = 'amqp://127.0.0.1:5672' };
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = 'redis://127.0.0.1:6379', 'subscription_buffer_messages' = '1024', 'idle_timeout_ms' = '30000' };
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE PROMETHEUS CLIENT prom_main CONFIG { 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = 5000 };
      CREATE ZEROMQ CLIENT zmq_main CONFIG { 'addr' = 'tcp://127.0.0.1:5555', 'bind' = 'false' };
      CREATE SQS CLIENT sqs_main CONFIG { 'endpoint' = 'http://127.0.0.1:9324', 'region' = 'us-east-1' };
      CREATE WEBSOCKETS CLIENT ws_main CONFIG { 'endpoint' = 'ws://127.0.0.1:9/stream' };
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      CREATE HTTP REQUEST INGESTOR http_request_source
        FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR http_endpoint_source
        FROM http_ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WEBSOCKETS ENDPOINT INGESTOR ws_endpoint_source
        FROM ws_ingress MODE NO ACK ON QUIESCE REJECT RETRY AFTER 5s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_connection_source
        FROM ws_main HEADERS { 'x-trace' = 'stream' } MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR kafka_source
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC INGESTOR pulsar_source
        FROM pulsar_main TOPIC 'persistent://public/default/Orders' SUBSCRIPTION 'Readers' OWNED BY INGESTOR
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ QUEUE INGESTOR rabbitmq_source
        FROM rabbit_main QUEUE 'Orders'
        MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS PUBSUB INGESTOR redis_source
        FROM redis_main CHANNEL 'Orders' MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC INGESTOR mqtt_source
        FROM mqtt_main TOPIC 'devices/+/Orders' SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT INGESTOR nats_source
        FROM nats_main SUBJECT 'orders.*' QUEUE GROUP 'Readers' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS QUEUE INGESTOR sqs_source
        FROM sqs_main QUEUE 'Orders.fifo'
        MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ PULL INGESTOR zeromq_source
        FROM zmq_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG LISTENER INGESTOR syslog_source
        FROM syslog_main MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PROMETHEUS QUERY INGESTOR prometheus_source
        FROM prom_main QUERY 'up' EVERY 15s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT PRODUCER INGESTOR client_source
        FROM SCHEMA event MODE ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SCHEMA telemetry (event_ts DATETIME, body STRING, trace_id STRING, span_id STRING, value F64);
      CREATE RELAY telemetry_events SCHEMA telemetry UNBRANCHED;
      CREATE SENTRY CLIENT sentry_main CONFIG { 'dsn' = 'http://public@127.0.0.1:9000/1', 'timeout_ms' = '1000' };
      CREATE OTEL CLIENT collector CONFIG { 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc', 'timeout_ms' = '1000' };
      CREATE CLICKHOUSE CLIENT clickhouse_main CONFIG { 'addr' = 'http://127.0.0.1:8123', 'user' = 'default', 'database' = 'test', 'timeout_ms' = '1000' };
      CREATE POSTGRES CLIENT postgres_main POOL SIZE MIN 0 MAX 2 CONFIG { 'addr' = 'postgres://test:test@127.0.0.1:5432/test?sslmode=disable' };
      CREATE MYSQL CLIENT mysql_main POOL SIZE MIN 0 MAX 2 CONFIG { 'addr' = 'mysql://test:test@127.0.0.1:3306/test' };
      CREATE MONGODB CLIENT mongodb_main POOL SIZE MIN 0 MAX 2 CONFIG { 'addr' = 'mongodb://127.0.0.1:27017', 'database' = 'test' };
      CREATE S3 CLIENT storage CONFIG { 'region' = 'us-east-1', 'access_key_id' = 'test', 'secret_access_key' = 'test' };
      CREATE ICEBERG_REST CLIENT catalog CONFIG { 'uri' = 'http://127.0.0.1:8181', 'warehouse' = 'warehouse' };
      CREATE <phrase> EMITTER outgoing FROM <relay> TO <destination> MODE <mode>
        <payload> <batching> FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE EMITTER outgoing;
      """
    Then the last command output contains
      """
      to: <phrase> <reference>
      options: <options>
      mode: <mode>
      quiesce: none
      envelope: none
      batch: <batch_description>
      flush: FLUSH IMMEDIATE
      errors: ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW EMITTERS;
      """
    Then the last command output contains
      """
      outgoing to=<phrase>
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      CREATE ATTACHED <phrase> EMITTER outgoing
      """

    Examples:
      | cluster_size | phrase             | reference                                              | options                                                                                                     | mode                                                            | relay            | destination                                                                                                                        | payload                                                                                                                                            | batching                             | batch_description              |
      | 1            | CLIENT CONSUMER    | SCHEMA event                                           | none                                                                                                        | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s | events           | SCHEMA event                                                                                                                       | INHERIT ALL                                                                                                                                        | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | CLIENT CONSUMER    | SCHEMA event                                           | none                                                                                                        | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s | events           | SCHEMA event                                                                                                                       | INHERIT ALL                                                                                                                                        | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | HTTP REQUEST       | http_main                                              | METHOD 'POST' PATH '/events'                                                                                | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | http_main METHOD 'POST' PATH '/events'                                                                                             | WITHOUT BODY                                                                                                                                       |                                      | none                           |
      | 3            | HTTP REQUEST       | http_main                                              | METHOD 'POST' PATH '/events'                                                                                | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | http_main METHOD 'POST' PATH '/events'                                                                                             | WITHOUT BODY                                                                                                                                       |                                      | none                           |
      | 1            | KAFKA TOPIC        | kafka_main TOPIC 'Orders'                              | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | kafka_main TOPIC 'Orders'                                                                                                          | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | KAFKA TOPIC        | kafka_main TOPIC 'Orders'                              | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | kafka_main TOPIC 'Orders'                                                                                                          | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | PULSAR TOPIC       | pulsar_main TOPIC 'persistent://public/default/Orders' | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | pulsar_main TOPIC 'persistent://public/default/Orders'                                                                             | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | PULSAR TOPIC       | pulsar_main TOPIC 'persistent://public/default/Orders' | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | pulsar_main TOPIC 'persistent://public/default/Orders'                                                                             | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | RABBITMQ QUEUE     | rabbit_main QUEUE 'Orders'                             | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | rabbit_main QUEUE 'Orders'                                                                                                         | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | RABBITMQ QUEUE     | rabbit_main QUEUE 'Orders'                             | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | rabbit_main QUEUE 'Orders'                                                                                                         | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | REDIS PUBSUB       | redis_main CHANNEL 'Orders'                            | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | redis_main CHANNEL 'Orders'                                                                                                        | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | REDIS PUBSUB       | redis_main CHANNEL 'Orders'                            | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | redis_main CHANNEL 'Orders'                                                                                                        | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | MQTT TOPIC         | mqtt_main TOPIC 'devices/Orders'                       | none                                                                                                        | QOS 0 RETRY POLICY BACKOFF 100ms MAX 1s                         | events           | mqtt_main TOPIC 'devices/Orders'                                                                                                   | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | MQTT TOPIC         | mqtt_main TOPIC 'devices/Orders'                       | none                                                                                                        | QOS 0 RETRY POLICY BACKOFF 100ms MAX 1s                         | events           | mqtt_main TOPIC 'devices/Orders'                                                                                                   | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | NATS SUBJECT       | nats_main SUBJECT 'orders.created'                     | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | nats_main SUBJECT 'orders.created'                                                                                                 | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | NATS SUBJECT       | nats_main SUBJECT 'orders.created'                     | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | nats_main SUBJECT 'orders.created'                                                                                                 | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | ZEROMQ PUSH        | zmq_main                                               | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | zmq_main                                                                                                                           | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | ZEROMQ PUSH        | zmq_main                                               | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | zmq_main                                                                                                                           | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | SQS QUEUE          | sqs_main QUEUE 'Orders'                                | none                                                                                                        | SINGLE RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | sqs_main QUEUE 'Orders'                                                                                                            | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | SQS QUEUE          | sqs_main QUEUE 'Orders'                                | none                                                                                                        | SINGLE RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | sqs_main QUEUE 'Orders'                                                                                                            | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | SENTRY EVENT       | sentry_main                                            | PLATFORM other                                                                                              | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | sentry_main                                                                                                                        | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | SENTRY EVENT       | sentry_main                                            | PLATFORM other                                                                                              | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | sentry_main                                                                                                                        | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | SYSLOG COLLECTOR   | syslog_main                                            | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | syslog_main                                                                                                                        | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 3            | SYSLOG COLLECTOR   | syslog_main                                            | none                                                                                                        | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s                        | events           | syslog_main                                                                                                                        | ENCODE USING event_codec INHERIT ALL                                                                                                               |                                      | none                           |
      | 1            | OTEL LOGS          | collector                                              | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | telemetry_events | collector                                                                                                                          | VALUES { 'time' = input.event_ts, 'body' = input.body }                                                                                            |                                      | none                           |
      | 3            | OTEL LOGS          | collector                                              | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | telemetry_events | collector                                                                                                                          | VALUES { 'time' = input.event_ts, 'body' = input.body }                                                                                            |                                      | none                           |
      | 1            | OTEL TRACES        | collector                                              | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | telemetry_events | collector                                                                                                                          | VALUES { 'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = input.body, 'start_time' = input.event_ts, 'end_time' = input.event_ts } |                                      | none                           |
      | 3            | OTEL TRACES        | collector                                              | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | telemetry_events | collector                                                                                                                          | VALUES { 'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = input.body, 'start_time' = input.event_ts, 'end_time' = input.event_ts } |                                      | none                           |
      | 1            | OTEL METRICS       | collector                                              | METRIC 'Orders' UNIT '1' GAUGE                                                                              | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | telemetry_events | collector METRIC 'Orders' UNIT '1' GAUGE                                                                                           | VALUES { 'time' = input.event_ts, 'value' = input.value }                                                                                          |                                      | none                           |
      | 3            | OTEL METRICS       | collector                                              | METRIC 'Orders' UNIT '1' GAUGE                                                                              | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | telemetry_events | collector METRIC 'Orders' UNIT '1' GAUGE                                                                                           | VALUES { 'time' = input.event_ts, 'value' = input.value }                                                                                          |                                      | none                           |
      | 1            | CLICKHOUSE TABLE   | clickhouse_main TABLE 'Orders'                         | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | clickhouse_main TABLE 'Orders'                                                                                                     | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | CLICKHOUSE TABLE   | clickhouse_main TABLE 'Orders'                         | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | clickhouse_main TABLE 'Orders'                                                                                                     | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | POSTGRES TABLE     | postgres_main TABLE 'Orders'                           | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | postgres_main TABLE 'Orders'                                                                                                       | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | POSTGRES TABLE     | postgres_main TABLE 'Orders'                           | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | postgres_main TABLE 'Orders'                                                                                                       | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | MYSQL TABLE        | mysql_main TABLE 'Orders'                              | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | mysql_main TABLE 'Orders'                                                                                                          | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | MYSQL TABLE        | mysql_main TABLE 'Orders'                              | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | mysql_main TABLE 'Orders'                                                                                                          | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | MONGODB COLLECTION | mongodb_main COLLECTION 'Orders'                       | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | mongodb_main COLLECTION 'Orders'                                                                                                   | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | MONGODB COLLECTION | mongodb_main COLLECTION 'Orders'                       | none                                                                                                        | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | mongodb_main COLLECTION 'Orders'                                                                                                   | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | ICEBERG TABLE      | storage TABLE 'Orders'                                 | NAMESPACE 'Analytics.Events' LOCATION 's3://warehouse/Orders' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | storage TABLE 'Orders' NAMESPACE 'Analytics.Events' LOCATION 's3://warehouse/Orders' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | ICEBERG TABLE      | storage TABLE 'Orders'                                 | NAMESPACE 'Analytics.Events' LOCATION 's3://warehouse/Orders' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB | ACK RETRY POLICY BACKOFF 100ms MAX 1s                           | events           | storage TABLE 'Orders' NAMESPACE 'Analytics.Events' LOCATION 's3://warehouse/Orders' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB | VALUES { 'seq' = input.seq }                                                                                                                       | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | MAX MESSAGES 10 MAX SIZE 64KiB |
