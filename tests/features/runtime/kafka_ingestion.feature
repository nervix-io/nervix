Feature: Kafka ingestion
  Scenario Outline: Kafka ingestor delivers JSON payloads to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        INSTANCES <instances>
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then Kafka consumer group "nervix_cucumber_{{test_id}}" eventually has <instances> consumers
    And the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 1         | 0             |
      | 3            | 2         | 0             |
      | 3            | 1         | 1             |
      | 3            | 2         | 1             |

  Scenario Outline: Kafka ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "kafka_notifications" enters fault mode
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_reconnect_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_reconnect_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "kafka_notifications" leaves fault mode
    And Kafka message is published to topic "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL blocks downstream publish after immediate NoAck
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":41}
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK PARALLEL blocks downstream publish after ack timeout
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    And emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":51}
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":52}
      """
    Then the observed broker does not receive a payload within "700ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK ingestor waits while an attached emitter is stalled
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}}
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    And emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":77}
      """
    Then the observed broker does not receive a payload within "1200ms"
    When emitter "kafka_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      "user_id":77
      """
    And the observed broker does not receive a payload within "1200ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL waits for batch timeout before dispatching a partial batch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 500ms ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":61}
      """
    Then the relay subscription does not receive a payload within "300ms"
    And the relay subscription receives a payload
      """
      "user_id":61
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL ignores detached emitter failures
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DETACHED EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL replays on attached emitter failure
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "2s" the relay subscription receives payloads
      """
      "user_id":43
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL ignores detached deduplicator branch failures
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DETACHED DEDUPLICATOR passthrough
        FROM notifications
        TO forwarded_notifications BRANCHED BY by_kafka_notifications
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM forwarded_notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":51}
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":52}
      """
    Then the relay subscription receives a payload
      """
      "user_id":52
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL replays on attached deduplicator branch failure
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR passthrough
        FROM notifications
        TO forwarded_notifications BRANCHED BY by_kafka_notifications
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM forwarded_notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION forwarded_notifications_subscription TO forwarded_notifications;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":51}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":51}
      """
    When emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":53}
      """
    Then the relay subscription receives a payload
      """
      "user_id":53
      """
    And within "2s" the relay subscription receives payloads
      """
      "user_id":53
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
