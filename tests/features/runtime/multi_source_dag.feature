Feature: Multi-source DAG routing
  Scenario Outline: Multiple ingestors flow through dedicated and shared branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "dag_rabbit_{{test_id}}" exists
    And Redis channel "dag_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        source string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY kafka_ingress SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY rabbit_ingress SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE RELAY kafka_projected SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE RELAY rabbit_projected SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE RELAY shared_dispatch SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
        };
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC dag_kafka_{{test_id}} OFFSET BY CONSUMER GROUP dag_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING notification_codec
        TO kafka_ingress
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR rabbit_notifications
        FROM RABBITMQ rabbit_main QUEUE dag_rabbit_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING notification_codec
        TO rabbit_ingress
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR kafka_branch FROM kafka_ingress
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_kafka_notifications
        TO kafka_projected
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE REINGESTOR rabbit_branch FROM rabbit_ingress
        TO rabbit_projected
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE DEDUPLICATOR kafka_shared FROM kafka_projected
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_kafka_notifications
        TO shared_dispatch
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE DEDUPLICATOR rabbit_shared FROM rabbit_projected
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_kafka_notifications
        TO shared_dispatch
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE EMITTER kafka_out FROM kafka_projected ENCODE USING notification_codec TO REDIS PUBSUB redis_main CHANNEL dag_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER rabbit_out FROM rabbit_projected ENCODE USING notification_codec TO REDIS PUBSUB redis_main CHANNEL dag_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER shared_out FROM shared_dispatch ENCODE USING notification_codec TO REDIS PUBSUB redis_main CHANNEL dag_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION kafka_projected_subscription TO kafka_projected;
        CREATE SUBSCRIPTION rabbit_projected_subscription TO rabbit_projected;
        CREATE SUBSCRIPTION shared_dispatch_subscription TO shared_dispatch;
        START;
      """
    When Kafka message is published to topic "dag_kafka_{{test_id}}"
      """
      {"user_id":11,"source":"kafka"}
      """
    And RabbitMQ message is published to queue "dag_rabbit_{{test_id}}"
      """
      {"user_id":22,"source":"rabbit"}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "source":"kafka"
      "source":"kafka"
      "source":"rabbit"
      "source":"rabbit"
      """
    And within "5s" the observed broker receives payloads
      """
      "source":"kafka"
      "source":"kafka"
      "source":"rabbit"
      "source":"rabbit"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
