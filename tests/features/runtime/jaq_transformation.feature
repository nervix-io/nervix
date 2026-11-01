Feature: JAQ transformation
  Scenario Outline: HTTP endpoint ingestor applies JAQ transformation before decoding
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM http_notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given Kafka is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given RabbitMQ is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_{{test_id}}" exists
    When these Plexus commands are executed
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
        CREATE RABBITMQ CLIENT rabbit_main
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'notifications_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given Redis is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{redis_addr}}',
          'subscription_buffer_messages' = '1024',
          'idle_timeout_ms' = '30000'
        };
        CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main CHANNEL 'notifications_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_redis_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE MQTT CLIENT mqtt_main
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-jaq-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE NATS SUBJECT INGESTOR nats_notifications
        FROM nats_main SUBJECT 'notifications_{{test_id}}' QUEUE GROUP 'nats_notifications_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_nats_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE ZEROMQ CLIENT zeromq_main
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };
        CREATE ZEROMQ PULL INGESTOR zeromq_notifications
        FROM zeromq_main MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_zeromq_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_{{test_id}}" exists
    When these Plexus commands are executed
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
        CREATE SQS CLIENT sqs_main
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1',
          'access_key_id' = 'scenario-key',
          'secret_access_key' = 'scenario-secret'
        };
        CREATE SQS QUEUE INGESTOR sqs_notifications
        FROM sqs_main QUEUE 'notifications_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_sqs_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE WEBSOCKETS ENDPOINT INGESTOR ws_notifications
        FROM ws_notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_ws_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE HTTP CLIENT http_main
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/http/{{test_id}}',
          'timeout_ms' = 5000
        };
        CREATE HTTP REQUEST INGESTOR http_notifications
        FROM http_main METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE WEBSOCKETS CLIENT ws_main
        CONFIG {
          'endpoint' = '{{mock_ws_addr}}/ws/{{test_id}}'
        };
        CREATE WEBSOCKETS CONNECTION INGESTOR ws_notifications
        FROM ws_main MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_ws_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given Prometheus is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE CODEC sample_codec
        FROM JSON
        TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '{source: .metric.source, value: (.value * 2), timestamp}';
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE PROMETHEUS CLIENT prom_main
        CONFIG {
          'addr' = '{{prometheus_addr}}',
          'timeout_ms' = 5000
        };
        CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'label_replace(vector(42.5), "source", "local", "", "")' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples
        INHERIT ALL
        BRANCHED BY by_prom_samples
        SET source = message.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
