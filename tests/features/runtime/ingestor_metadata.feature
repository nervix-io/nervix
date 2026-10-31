Feature: Typed ingestor metadata
  Every ingestor entity declares the typed metadata.<field> set its messages expose. The registry
  refuses a route or a TIMESTAMP AT clause that reads a field the entity does not declare, and an
  ingestor may take its event time from a metadata field its entity declares as DATETIME. The
  refusals inspect stopped graph definitions without connecting to any external system.

  Scenario Outline: A route that reads a metadata field its entity does not declare is refused at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA tagged ( seq I64, tag STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA tagged UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-ingestor-metadata.example.com;
      CREATE ENDPOINT http_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT ws_ingress ON edge PATH '/stream' TYPE WEBSOCKETS;
      CREATE HTTP CLIENT http_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9/events',
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
      """
    When these Plexus statements fail with "ingestor 'http_request_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but HTTP REQUEST ingestors declare only metadata.status and metadata.polled_at"
      """
      CREATE HTTP REQUEST INGESTOR http_request_source
        FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'http_endpoint_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but HTTP ENDPOINT ingestors declare no metadata field"
      """
      CREATE HTTP ENDPOINT INGESTOR http_endpoint_source
        FROM http_ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'ws_endpoint_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but WEBSOCKETS ENDPOINT ingestors declare no metadata field"
      """
      CREATE WEBSOCKETS ENDPOINT INGESTOR ws_endpoint_source
        FROM ws_ingress MODE NO ACK ON QUIESCE REJECT RETRY AFTER 5s DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'ws_connection_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but WEBSOCKETS CONNECTION ingestors declare only metadata.frame_type"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_connection_source
        FROM ws_main MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'kafka_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but KAFKA TOPIC ingestors declare only metadata.topic, metadata.partition and metadata.offset"
      """
      CREATE KAFKA TOPIC INGESTOR kafka_source
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'pulsar_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but PULSAR TOPIC ingestors declare no metadata field"
      """
      CREATE PULSAR TOPIC INGESTOR pulsar_source
        FROM pulsar_main TOPIC 'persistent://public/default/Orders' SUBSCRIPTION 'Readers' OWNED BY INGESTOR
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'rabbitmq_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but RABBITMQ QUEUE ingestors declare no metadata field"
      """
      CREATE RABBITMQ QUEUE INGESTOR rabbitmq_source
        FROM rabbit_main QUEUE 'Orders'
        MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'redis_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but REDIS PUBSUB ingestors declare no metadata field"
      """
      CREATE REDIS PUBSUB INGESTOR redis_source
        FROM redis_main CHANNEL 'Orders' MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'mqtt_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but MQTT TOPIC ingestors declare no metadata field"
      """
      CREATE MQTT TOPIC INGESTOR mqtt_source
        FROM mqtt_main TOPIC 'devices/+/Orders' SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'nats_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but NATS SUBJECT ingestors declare no metadata field"
      """
      CREATE NATS SUBJECT INGESTOR nats_source
        FROM nats_main SUBJECT 'orders.*' QUEUE GROUP 'Readers' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'nats_stream_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but NATS STREAM ingestors declare no metadata field"
      """
      CREATE NATS STREAM INGESTOR nats_stream_source
        FROM nats_main STREAM 'Orders' CONSUMER 'Readers'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'sqs_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but SQS QUEUE ingestors declare no metadata field"
      """
      CREATE SQS QUEUE INGESTOR sqs_source
        FROM sqs_main QUEUE 'Orders.fifo'
        MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'zeromq_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but ZEROMQ PULL ingestors declare no metadata field"
      """
      CREATE ZEROMQ PULL INGESTOR zeromq_source
        FROM zmq_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'syslog_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but SYSLOG LISTENER ingestors declare only metadata.peer_addr"
      """
      CREATE SYSLOG LISTENER INGESTOR syslog_source
        FROM syslog_main MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'prometheus_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but PROMETHEUS QUERY ingestors declare no metadata field"
      """
      CREATE PROMETHEUS QUERY INGESTOR prometheus_source
        FROM prom_main QUERY 'up' EVERY 15s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'client_source' in domain '{{domain}}' reads metadata.missing in its route to 'events', but CLIENT PRODUCER ingestors declare no metadata field"
      """
      CREATE CLIENT PRODUCER INGESTOR client_source
        FROM SCHEMA event MODE ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO events INHERIT ALL SET tag = metadata.missing UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE KAFKA TOPIC INGESTOR kafka_declared
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET tag = metadata.topic UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR kafka_declared;
      """
    Then the last command output contains
      """
      SET tag = metadata.topic
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: TIMESTAMP AT reads only a metadata field its entity declares as DATETIME
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE HTTP CLIENT http_main CONFIG {
        'endpoint' = 'http://127.0.0.1:9/events',
        'timeout_ms' = 5000
      };
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };
      """
    When these Plexus statements fail with "ingestor 'kafka_offset_time' in domain '{{domain}}' takes its TIMESTAMP AT metadata.offset, but KAFKA TOPIC ingestors declare metadata.offset as I64, not DATETIME"
      """
      CREATE KAFKA TOPIC INGESTOR kafka_offset_time
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP AT metadata.offset
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'kafka_missing_time' in domain '{{domain}}' takes its TIMESTAMP AT metadata.missing, but KAFKA TOPIC ingestors declare only metadata.topic, metadata.partition and metadata.offset"
      """
      CREATE KAFKA TOPIC INGESTOR kafka_missing_time
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP AT metadata.missing
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'http_status_time' in domain '{{domain}}' takes its TIMESTAMP AT metadata.status, but HTTP REQUEST ingestors declare metadata.status as U16, not DATETIME"
      """
      CREATE HTTP REQUEST INGESTOR http_status_time
        FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP AT metadata.status
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "ingestor 'mqtt_time' in domain '{{domain}}' takes its TIMESTAMP AT metadata.polled_at, but MQTT TOPIC ingestors declare no metadata field"
      """
      CREATE MQTT TOPIC INGESTOR mqtt_time
        FROM mqtt_main TOPIC 'devices/+/Orders' SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP AT metadata.polled_at
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE HTTP REQUEST INGESTOR http_poll_time
        FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP AT metadata.polled_at
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR http_poll_time;
      """
    Then the last command output contains
      """
      TIMESTAMP AT metadata.polled_at
      """
    When these Plexus statements fail with "ingestor 'http_poll_time' in domain '{{domain}}' takes its TIMESTAMP AT metadata.status, but HTTP REQUEST ingestors declare metadata.status as U16, not DATETIME"
      """
      ALTER HTTP REQUEST INGESTOR http_poll_time SET TIMESTAMP AT metadata.status;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE HTTP REQUEST INGESTOR http_now_time
        FROM http_main METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      ALTER HTTP REQUEST INGESTOR http_now_time SET TIMESTAMP AT metadata.polled_at;
      SHOW CREATE INGESTOR http_now_time;
      """
    Then the last command output contains
      """
      TIMESTAMP AT metadata.polled_at
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A paced HTTP polling ingestor takes each record's event time from metadata.polled_at
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 1s;
      CREATE SCHEMA event ( user_id I64 );
      CREATE SCHEMA polled_event ( user_id I64, polled_at DATETIME OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA polled_event UNBRANCHED;
      CREATE HTTP CLIENT source CONFIG {
        'endpoint' = '{{mock_http_addr}}/http/{{test_id}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR polled FROM source
        METHOD 'GET' EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TIMESTAMP AT metadata.polled_at
        TO events INHERIT ALL SET polled_at = metadata.polled_at UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_events TO events;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    Then within "60s" the relay subscription receives a payload
      """
      "polled_at":"2000-01-01T00:
      """
    And the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
