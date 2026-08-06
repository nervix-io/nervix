Feature: NATS emission
  Scenario Outline: NATS emitter publishes JSON payloads from a relay
    Given MQTT is running
    And NATS is running
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
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
          'addr' = '{{nats_addr}}'
        };
        CREATE EMITTER nats_notifications FROM notifications TO NATS nats_main SUBJECT notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "nats_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "10s" DESCRIBE EMITTER "nats_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "nats_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @nats_jetstream_missing_stream
  Scenario Outline: NATS JetStream emitter retries a missing stream until it is provisioned
    Given Kafka is running
    And NATS is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "jetstream_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE SCHEMA user_id_branch ( user_id I64 );
      CREATE BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC jetstream_in_{{test_id}}
          OFFSET BY CONSUMER GROUP jetstream_boundary_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          BRANCHED BY by_kafka_notifications
          SET user_id = message.user_id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT nats_main TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE EMITTER nats_jetstream FROM notifications
        TO NATS nats_main SUBJECT jetstream_out_{{test_id}}
          MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 1s
            RETRY POLICY BACKOFF 100ms MAX 500ms
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And Kafka message is published to topic "jetstream_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "2s" Kafka consumer group "jetstream_boundary_group_{{test_id}}" next offset for topic "jetstream_in_{{test_id}}" partition 0 is "below 1"
    And within "10s" DESCRIBE EMITTER "nats_jetstream" on the leader node contains
      """
      no stream found for given subject
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    When NATS JetStream stream "notifications_{{test_id}}" is provisioned for subject "jetstream_out_{{test_id}}"
    Then NATS JetStream stream "notifications_{{test_id}}" eventually contains a payload on subject "jetstream_out_{{test_id}}"
      """
      {"user_id":42}
      """
    And within "5s" Kafka consumer group "jetstream_boundary_group_{{test_id}}" next offset for topic "jetstream_in_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
