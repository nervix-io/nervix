Feature: Emitters write envelope clauses only where their entity declares them

  An emitter's envelope clauses follow its payload and set what its transport carries beside each
  payload. A clause or PROPERTIES key the emitter's entity does not declare is refused where it is
  written. KAFKA TOPIC declares TIMESTAMP, the RabbitMQ entities declare MANDATORY, and no entity
  declares another clause or a property, so a node refuses each clause form on every entity that
  does not declare it, and a declared clause refuses a value that is not exactly the type it carries.

  @emitter_envelope
  Scenario Outline: Every emitter entity refuses the envelope clauses it does not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "found PERSISTENT"
      """
      CREATE CLIENT CONSUMER EMITTER generated FROM events TO SCHEMA outgoing
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        PERSISTENT
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found MANDATORY"
      """
      CREATE HTTP REQUEST EMITTER generated FROM events TO destination_client
        METHOD 'POST' PATH '/events' MODE ACK RETRY POLICY BACKOFF 1s MAX 30s
        ENCODE USING events_codec
        MANDATORY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found RETAIN"
      """
      CREATE KAFKA TOPIC EMITTER generated FROM events TO destination_client TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        RETAIN
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found TIMESTAMP"
      """
      CREATE PULSAR TOPIC EMITTER generated FROM events TO destination_client TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        TIMESTAMP occurred_at
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found DELIVER"
      """
      CREATE RABBITMQ QUEUE EMITTER generated FROM events TO destination_client QUEUE 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        DELIVER AFTER 5s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found DELAY"
      """
      CREATE REDIS PUBSUB EMITTER generated FROM events TO destination_client CHANNEL 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        DELAY 5s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found REPLY"
      """
      CREATE MQTT TOPIC EMITTER generated FROM events TO destination_client TOPIC 'devices/Orders'
        MODE QOS 0 RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        REPLY TO reply_queue
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found MESSAGE"
      """
      CREATE NATS SUBJECT EMITTER generated FROM events TO destination_client SUBJECT 'orders.Created'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        MESSAGE ID id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found DEDUPLICATION"
      """
      CREATE ZEROMQ PUSH EMITTER generated FROM events TO destination_client
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        DEDUPLICATION ID id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found PROPERTIES"
      """
      CREATE SQS QUEUE EMITTER generated FROM events TO destination_client QUEUE 'orders'
        MODE SINGLE RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        PROPERTIES {'content_type' = 'application/json'}
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found PERSISTENT"
      """
      CREATE SENTRY EVENT EMITTER generated FROM events TO destination_client
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        PERSISTENT
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found MANDATORY"
      """
      CREATE SYSLOG COLLECTOR EMITTER generated FROM events TO destination_client
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
        MANDATORY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found RETAIN"
      """
      CREATE OTEL LOGS EMITTER generated FROM events TO destination_client
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        RETAIN
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found TIMESTAMP"
      """
      CREATE OTEL TRACES EMITTER generated FROM events TO destination_client
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        TIMESTAMP occurred_at
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found DELIVER"
      """
      CREATE OTEL METRICS EMITTER generated FROM events TO destination_client
        METRIC 'temperature' UNIT 'Cel' GAUGE
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        DELIVER AFTER 5s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found DELAY"
      """
      CREATE CLICKHOUSE TABLE EMITTER generated FROM events TO destination_client TABLE 'sales.Orders'
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        DELAY 5s
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found REPLY"
      """
      CREATE POSTGRES TABLE EMITTER generated FROM events TO destination_client TABLE 'Orders'
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        REPLY TO reply_queue
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found MESSAGE"
      """
      CREATE MYSQL TABLE EMITTER generated FROM events TO destination_client TABLE 'Orders'
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        MESSAGE ID id
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found DEDUPLICATION"
      """
      CREATE MONGODB COLLECTION EMITTER generated FROM events TO destination_client COLLECTION 'Orders'
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        DEDUPLICATION ID id
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "found PROPERTIES"
      """
      CREATE ICEBERG TABLE EMITTER generated FROM events TO destination_client TABLE 'Orders'
        NAMESPACE 'Analytics.Events' LOCATION 's3://warehouse/orders' CATALOG catalog
        COMMIT EACH 10s MAX SIZE 64MiB
        MODE ACK RETRY POLICY BACKOFF 1s MAX 30s VALUES {'id' = id, 'name' = name}
        PROPERTIES {'content_type' = 'application/json'}
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      found PROPERTIES
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_envelope
  Scenario Outline: A declared TIMESTAMP refuses a value that is not an exact, non-null and non-sensitive DATETIME
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (
        seq I64,
        event_at DATETIME,
        maybe_at DATETIME OPTIONAL,
        secret_at DATETIME SENSITIVE,
        label STRING
      );
      CREATE SCHEMA published ( seq I64 );
      CREATE WIRE JSON SCHEMA published_wire MODE STRICT ( seq integer );
      CREATE CODEC published_codec FROM WIRE JSON SCHEMA published_wire TO SCHEMA published;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    When these Plexus commands fail with "envelope TIMESTAMP requires an exact, non-null, non-sensitive DATETIME value"
      """
      CREATE KAFKA TOPIC EMITTER labelled FROM events TO broker TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING published_codec
        TIMESTAMP input.label
        SET seq = input.seq
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "envelope TIMESTAMP requires an exact, non-null, non-sensitive DATETIME value"
      """
      CREATE KAFKA TOPIC EMITTER optional_time FROM events TO broker TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING published_codec
        TIMESTAMP input.maybe_at
        SET seq = input.seq
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "envelope TIMESTAMP requires an exact, non-null, non-sensitive DATETIME value"
      """
      CREATE KAFKA TOPIC EMITTER secret_time FROM events TO broker TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING published_codec
        TIMESTAMP input.secret_at
        SET seq = input.seq
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE KAFKA TOPIC EMITTER leaked_time FROM events TO broker TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING published_codec
        TIMESTAMP leak_sensitive(input.secret_at)
        SET seq = input.seq
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER event_time FROM events TO broker TOPIC 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING published_codec
        TIMESTAMP input.event_at
        SET seq = input.seq
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER leaked_time;
      """
    Then the last command output contains
      """
      TIMESTAMP leak_sensitive(input.secret_at)
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
