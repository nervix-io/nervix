Feature: JAQ transformation
  Scenario Outline: HTTP endpoint ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }

        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      {"payload":"aligned","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }

        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then Kafka consumer group "nervix_cucumber_{{test_id}}" eventually has 1 consumers
    And the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: RabbitMQ ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = 'amqp://guest:guest@127.0.0.1:5672/%2f'
        };
        CREATE INGESTOR rabbit_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_rabbit_notifications VALUES { user_id = notifications.user_id }

        FROM RABBITMQ rabbit_main
        QUEUE notifications_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then RabbitMQ queue "notifications_{{test_id}}" eventually has 1 consumers
    When RabbitMQ message is published to queue "notifications_{{test_id}}"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Redis ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_redis_notifications;
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE INGESTOR redis_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_redis_notifications VALUES { user_id = notifications.user_id }

        FROM REDIS PUBSUB redis_main
        CHANNEL notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Redis message is published to channel "notifications_{{test_id}}"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-jaq-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }

        FROM MQTT mqtt_main
        TOPIC notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: NATS ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_nats_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_nats_notifications;
        CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = 'nats://127.0.0.1:4222'
        };
        CREATE INGESTOR nats_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_nats_notifications VALUES { user_id = notifications.user_id }

        FROM NATS nats_main
        SUBJECT notifications_{{test_id}}
        QUEUE GROUP nats_notifications_group_{{test_id}}
        INSTANCES 1
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And NATS message is published to subject "notifications_{{test_id}}"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: ZeroMQ ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_zeromq_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_zeromq_notifications;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };
        CREATE INGESTOR zeromq_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_zeromq_notifications VALUES { user_id = notifications.user_id }

        FROM ZEROMQ zeromq_main
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And ZeroMQ message is published
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: SQS ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_sqs_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_sqs_notifications;
        CREATE CLIENT sqs_main
        TYPE SQS
        CONFIG {
          'endpoint' = 'http://127.0.0.1:9324',
          'region' = 'us-east-1'
        };
        CREATE INGESTOR sqs_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_sqs_notifications VALUES { user_id = notifications.user_id }

        FROM SQS sqs_main
        QUEUE notifications_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And SQS message is published to queue "notifications_{{test_id}}"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      "payload":"aligned","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Websocket endpoint ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.payload';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE VHOST edge ws-{{test_id}}.example.com;
        CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
        CREATE INGESTOR ws_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }

        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And websocket message is published to host "ws-{{test_id}}.example.com" path "/ws"
      """
      {"payload":{"user_id":42,"payload":"aligned"}}
      """
    Then the relay subscription receives a payload
      """
      {"payload":"aligned","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: HTTP client ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id, payload: "aligned"}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE CLIENT http_main
        TYPE HTTP
        CONFIG {
          'endpoint' = 'http://127.0.0.1:18080/http/{{test_id}}',
          'method' = 'GET',
          'timeout_ms' = 5000
        };
        CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }

        FROM HTTP http_main EVERY 1s ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"payload":"aligned","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Websocket client ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id, payload: "aligned"}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = 'ws://127.0.0.1:18080/ws/{{test_id}}'
        };
        CREATE INGESTOR ws_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }

        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"payload":"aligned","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Prometheus ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE CODEC sample_codec
        FROM JSON
        TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '{source, value: (.value * 2), timestamp}';
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE CLIENT prom_main
        TYPE PROMETHEUS
        CONFIG {
          'addr' = 'http://127.0.0.1:9090',
          'timeout_ms' = 5000
        };
        CREATE INGESTOR prom_samples
        TO samples FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING sample_codec
        BRANCHED BY by_prom_samples VALUES { source = samples.source }

        FROM PROMETHEUS prom_main
        QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
        EVERY 1s ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION samples_subscription TO samples;
        START;
      """
    Then the relay subscription receives a payload
      """
      "value":85.0
      """
    And the last relay subscription payload contains key fragment '{"source":"local"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
