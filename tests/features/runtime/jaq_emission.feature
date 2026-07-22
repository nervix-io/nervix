Feature: JAQ emission
  Scenario Outline: Kafka emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE EMITTER kafka_notifications FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: RabbitMQ emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
        };
        CREATE EMITTER rabbit_notifications FROM notifications ENCODE USING notification_codec TO RABBITMQ rabbit_main QUEUE notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Redis emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Redis channel "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE EMITTER redis_notifications FROM notifications ENCODE USING notification_codec TO REDIS PUBSUB redis_main CHANNEL notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-emitter-{{test_id}}'
        };
        CREATE EMITTER mqtt_notifications_out FROM notifications ENCODE USING notification_codec TO MQTT mqtt_main TOPIC notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: NATS emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = 'nats://127.0.0.1:4222'
        };
        CREATE EMITTER nats_notifications FROM notifications ENCODE USING notification_codec TO NATS nats_main SUBJECT notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: ZeroMQ emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE EMITTER zeromq_notifications FROM notifications ENCODE USING notification_codec TO ZEROMQ zeromq_main
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: SQS emitter applies JAQ transformation before emitting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.' ON EMITTING '{payload: .}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT sqs_main
        TYPE SQS
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9324',
          'region' = 'us-east-1'
        };
        CREATE EMITTER sqs_notifications FROM notifications ENCODE USING notification_codec TO SQS sqs_main QUEUE notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"payload":{"user_id":42}}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
