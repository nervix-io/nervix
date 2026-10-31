Feature: Emitters write envelope clauses only where their entity declares them

  An emitter's envelope clauses follow its payload and set what its transport carries beside each
  payload. A clause or PROPERTIES key the emitter's entity does not declare is refused where it is
  written. Only SQS QUEUE declares any yet, DELAY and DEDUPLICATION ID, so a node refuses each
  other clause form on SQS QUEUE and every clause form on every other emitter entity.

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
        SEND SINGLE MODE ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING events_codec
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
