Feature: Kafka ingestor domain pacing
  Scenario Outline: Unpaced Kafka ingestors consume messages without an explicit timestamp field
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_pacing_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Paced Kafka ingestors reject messages until the domain clock starts
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 200ms SKEW 1s;
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_pacing_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then within "5s" repeatedly publishing Kafka message to topic "notifications_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Unpaced Kafka ingestors accept explicit timestamp fields without a domain clock
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
        occurred_at DATETIME
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        occurred_at string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification
        ENCODE occurred_at AS RFC3339;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP AT occurred_at
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_pacing_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    Then the relay subscription receives a payload
      """
      "occurred_at":"2026-04-07T00:00:00+00:00"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Paced Kafka ingestors require a running domain clock even with explicit timestamps
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 200ms SKEW 100000h;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        occurred_at DATETIME
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        occurred_at string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification
        ENCODE occurred_at AS RFC3339;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP AT occurred_at
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_pacing_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then within "5s" repeatedly publishing Kafka message to topic "notifications_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Domain-owned Kafka offsets resume from persisted state after cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO notifications;
      """
    Then within "10s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      kafka observed partitions: 0
      """
    Then the relay subscription does not receive a payload within "1s"
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Domain-owned Kafka offsets reset on START AT NOW
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    And these NSPL commands are executed on the leader node
      """
      START AT NOW;
      """
    Then within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      kafka observed partitions: 0
      """
    Then the relay subscription does not receive a payload within "1s"
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Domain-owned Kafka offsets rebalance multiple instances when partitions change
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed on the leader node
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY DOMAIN
        INSTANCES 2
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      kafka observed partitions: 0
      """
    When Kafka message is published to topic "notifications_{{test_id}}" partition 0
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When Kafka topic "notifications_{{test_id}}" partition count is changed to 2
    Then within "5s" repeatedly publishing Kafka message to topic "notifications_{{test_id}}" partition 1 yields a relay subscription payload
      """
      {"user_id":43}
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE INGESTOR kafka_notifications;
      """
    Then the last command output contains
      """
      kafka observed partitions: 0,1
      """
    And the last command output contains
      """
      kafka instance 0 partitions: 0
      """
    And the last command output contains
      """
      kafka instance 1 partitions: 1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Domain-owned Kafka offsets reconcile unexpected partition shrink after topic reset
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_{{test_id}}" exists with 2 partitions
    When these NSPL commands are executed on the leader node
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY DOMAIN
        INSTANCES 2
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      kafka observed partitions: 0,1
      """
    When Kafka message is published to topic "notifications_{{test_id}}" partition 1
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    When Kafka topic "notifications_{{test_id}}" is reset to 1 partitions
    Then within "10s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      kafka observed partitions: 0
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE INGESTOR kafka_notifications;
      """
    Then the last command output contains
      """
      kafka instance 0 partitions: 0
      """
    And the last command output contains
      """
      kafka instance 1 partitions: -
      """
    When Kafka message is published to topic "notifications_{{test_id}}" partition 0
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Stopped unpaced domains do not ingest until explicitly started
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_kafka_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "1s"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
