Feature: Ingestors name their connector and entity in the statement head

  @kinded_ingestor_show_create
  Scenario Outline: SHOW CREATE INGESTOR renders the kinded head of every connector and entity
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
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-kinded-ingestors.example.com;
      CREATE ENDPOINT http_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT ws_ingress ON edge PATH '/stream' TYPE WEBSOCKETS;
      CREATE CLIENT http_main TYPE HTTP CONFIG {
        'endpoint' = 'http://127.0.0.1:9/events',
        'method' = 'GET',
        'timeout_ms' = 5000
      };
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT pulsar_main TYPE PULSAR CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE CLIENT rabbit_main TYPE RABBITMQ CONFIG { 'addr' = 'amqp://127.0.0.1:5672' };
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = 'redis://127.0.0.1:6379' };
      CREATE CLIENT mqtt_main TYPE MQTT CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };
      CREATE CLIENT nats_main TYPE NATS CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG { 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = 5000 };
      CREATE CLIENT zmq_main TYPE ZEROMQ CONFIG { 'addr' = 'tcp://127.0.0.1:5555', 'bind' = 'false' };
      CREATE CLIENT sqs_main TYPE SQS CONFIG { 'endpoint' = 'http://127.0.0.1:9324', 'region' = 'us-east-1' };
      CREATE CLIENT ws_main TYPE WEBSOCKETS CONFIG { 'endpoint' = 'ws://127.0.0.1:9/stream' };
      CREATE CLIENT syslog_main TYPE SYSLOG CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      CREATE HTTP REQUEST INGESTOR http_request_source
        FROM http_main EVERY 1s MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
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
        FROM ws_main MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
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
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR http_request_source;
      """
    Then the last command output contains
      """
      CREATE HTTP REQUEST INGESTOR http_request_source
        FROM http_main EVERY 1s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR http_endpoint_source;
      """
    Then the last command output contains
      """
      CREATE HTTP ENDPOINT INGESTOR http_endpoint_source
        FROM http_ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR ws_endpoint_source;
      """
    Then the last command output contains
      """
      CREATE WEBSOCKETS ENDPOINT INGESTOR ws_endpoint_source
        FROM ws_ingress MODE NO ACK ON QUIESCE REJECT RETRY AFTER 5s
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR ws_connection_source;
      """
    Then the last command output contains
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_connection_source
        FROM ws_main MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR kafka_source;
      """
    Then the last command output contains
      """
      CREATE KAFKA TOPIC INGESTOR kafka_source
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers' MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR pulsar_source;
      """
    Then the last command output contains
      """
      CREATE PULSAR TOPIC INGESTOR pulsar_source
        FROM pulsar_main TOPIC 'persistent://public/default/Orders' SUBSCRIPTION 'Readers' OWNED BY INGESTOR MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR rabbitmq_source;
      """
    Then the last command output contains
      """
      CREATE RABBITMQ QUEUE INGESTOR rabbitmq_source
        FROM rabbit_main QUEUE 'Orders' MODE ACK SEQUENTIAL ACK TIMEOUT 20s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR redis_source;
      """
    Then the last command output contains
      """
      CREATE REDIS PUBSUB INGESTOR redis_source
        FROM redis_main CHANNEL 'Orders' MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR mqtt_source;
      """
    Then the last command output contains
      """
      CREATE MQTT TOPIC INGESTOR mqtt_source
        FROM mqtt_main TOPIC 'devices/+/Orders' SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 2s ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR nats_source;
      """
    Then the last command output contains
      """
      CREATE NATS SUBJECT INGESTOR nats_source
        FROM nats_main SUBJECT 'orders.*' QUEUE GROUP 'Readers' INSTANCES 1 MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR sqs_source;
      """
    Then the last command output contains
      """
      CREATE SQS QUEUE INGESTOR sqs_source
        FROM sqs_main QUEUE 'Orders.fifo' MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 2s MAX 1m ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR zeromq_source;
      """
    Then the last command output contains
      """
      CREATE ZEROMQ PULL INGESTOR zeromq_source
        FROM zmq_main MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR syslog_source;
      """
    Then the last command output contains
      """
      CREATE SYSLOG LISTENER INGESTOR syslog_source
        FROM syslog_main MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR prometheus_source;
      """
    Then the last command output contains
      """
      CREATE PROMETHEUS QUERY INGESTOR prometheus_source
        FROM prom_main QUERY 'up' EVERY 15s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR client_source;
      """
    Then the last command output contains
      """
      CREATE CLIENT PRODUCER INGESTOR client_source
        FROM SCHEMA event MODE ACK PARALLEL MAX 64 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s ON QUIESCE SUSPEND
        TIMESTAMP NOW
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_ingestor_describe
  Scenario Outline: DESCRIBE INGESTOR and SHOW INGESTORS name the connector and entity an ingestor reads
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
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC INGESTOR kafka_source
        FROM kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT PRODUCER INGESTOR client_source
        FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s
        ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      DESCRIBE INGESTOR kafka_source;
      """
    Then the last command output contains
      """
      from: KAFKA TOPIC kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers' MODE NO ACK ON QUIESCE SUSPEND
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE INGESTOR client_source;
      """
    Then the last command output contains
      """
      from: CLIENT PRODUCER SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 2s ON QUIESCE SUSPEND
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW INGESTORS;
      """
    Then the last command output contains
      """
      kafka_source from=KAFKA TOPIC codec=event_codec
      """
    And the last command output contains
      """
      client_source from=CLIENT PRODUCER schema=event
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_ingestor_alter_kind
  Scenario Outline: ALTER INGESTOR applies only under the head of the stored connector and entity
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
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-kinded-alter.example.com;
      CREATE ENDPOINT ingress_a ON edge PATH '/a' TYPE HTTP;
      CREATE ENDPOINT ingress_b ON edge PATH '/b' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR web_source
        FROM ingress_a MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "the stored ingestor reads HTTP ENDPOINT, so ALTER KAFKA TOPIC INGESTOR cannot alter it"
      """
      ALTER KAFKA TOPIC INGESTOR web_source SET GENERAL ERROR IGNORE;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER HTTP ENDPOINT INGESTOR web_source
        SET FROM ingress_b MODE NO ACK ON QUIESCE REJECT RETRY AFTER 3s,
        SET GENERAL ERROR IGNORE;
      SHOW CREATE INGESTOR web_source;
      """
    Then the last command output contains
      """
      CREATE HTTP ENDPOINT INGESTOR web_source
        FROM ingress_b MODE NO ACK ON QUIESCE REJECT RETRY AFTER 3s
      """
    And the last command output contains
      """
        ON GENERAL ERROR IGNORE;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_ingestor_reference_kind
  Scenario Outline: An ingestor refuses an endpoint or a client the connector and entity of its head cannot read
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
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-kinded-references.example.com;
      CREATE ENDPOINT http_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT ws_ingress ON edge PATH '/stream' TYPE WEBSOCKETS;
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = 'redis://127.0.0.1:6379' };
      """
    When these Plexus commands fail with "HTTP ENDPOINT ingestor 'web_source' in domain '{{domain}}' requires a TYPE HTTP endpoint, but endpoint 'ws_ingress' is TYPE WEBSOCKETS"
      """
      CREATE HTTP ENDPOINT INGESTOR web_source
        FROM ws_ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "WEBSOCKETS ENDPOINT ingestor 'stream_source' in domain '{{domain}}' requires a TYPE WEBSOCKETS endpoint, but endpoint 'http_ingress' is TYPE HTTP"
      """
      CREATE WEBSOCKETS ENDPOINT INGESTOR stream_source
        FROM http_ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "KAFKA TOPIC ingestor 'kafka_source' in domain '{{domain}}' requires a KAFKA client, but client 'redis_main' is a REDIS client"
      """
      CREATE KAFKA TOPIC INGESTOR kafka_source
        FROM redis_main TOPIC 'orders' OFFSET BY DOMAIN
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_ingestor_completion
  Scenario Outline: Completion offers the kinded heads and then only references of that kind
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE VHOST edge http-{{test_id}}-kinded-completion.example.com;
      CREATE ENDPOINT http_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT ws_ingress ON edge PATH '/stream' TYPE WEBSOCKETS;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT redis_main TYPE REDIS POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = 'redis://127.0.0.1:6379' };
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "CREATE |" on node "{{leader}}"
    Then the CLI output contains "KAFKA TOPIC INGESTOR"
    And the CLI output contains "HTTP ENDPOINT INGESTOR"
    And the CLI output contains "CLIENT PRODUCER INGESTOR"
    When the CLI suggests for "CREATE KAFKA TOPIC INGESTOR orders_in FROM |" on node "{{leader}}"
    Then the CLI output contains "kafka_main"
    And the CLI output does not contain "redis_main"
    When the CLI suggests for "CREATE HTTP ENDPOINT INGESTOR web_in FROM |" on node "{{leader}}"
    Then the CLI output contains "http_ingress"
    And the CLI output does not contain "ws_ingress"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_ingestor_case_preserving_topic
  Scenario Outline: A Kafka topic ingestor consumes the topic exactly as its name is written
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "Orders_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR orders_source
        FROM kafka_main TOPIC 'Orders_{{test_id}}' OFFSET BY CONSUMER GROUP 'Readers_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    When Kafka message is published to topic "Orders_{{test_id}}"
      """
      {"seq":7}
      """
    Then the relay subscription receives a payload
      """
      "seq":7
      """
    And Kafka consumer group "Readers_{{test_id}}" eventually has 1 consumers

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
