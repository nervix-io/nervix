Feature: Pulsar emission
  Scenario Outline: Pulsar emitter publishes JSON payloads from a relay
    Given Pulsar is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "notifications_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_pulsar_ingress SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_ingress;
        CREATE CLIENT pulsar_main
        TYPE PULSAR
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
        CREATE INGESTOR pulsar_ingress
        FROM PULSAR pulsar_main TOPIC notifications_in_{{test_id}} SUBSCRIPTION nervix_pulsar_emission_{{test_id}} INSTANCES 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_ingress
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER pulsar_notifications FROM notifications TO PULSAR pulsar_main TOPIC notifications_out_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "pulsar_notifications" enters stall mode
    And Pulsar message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "pulsar_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "pulsar_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @pulsar_emitter_ack_boundary
  Scenario Outline: Pulsar mode controls when the input offset is committed
    Given Kafka is running
    And Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "pulsar_boundary_in_{{test_id}}" exists with 1 partitions
    And Pulsar topic "pulsar_boundary_out_{{test_id}}" is observed
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
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC pulsar_boundary_in_{{test_id}}
          OFFSET BY CONSUMER GROUP pulsar_boundary_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          BRANCHED BY by_kafka_notifications
          SET user_id = message.user_id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT pulsar_sink TYPE PULSAR CONFIG {
        'addr' = '{{pulsar_stallable_addr}}'
      };
      CREATE ATTACHED EMITTER pulsar_boundary FROM notifications
        TO PULSAR pulsar_sink TOPIC pulsar_boundary_out_{{test_id}}
          MODE <publishing_mode>
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "pulsar_boundary_group_{{test_id}}" eventually has 1 consumers
    When the stallable endpoint "pulsar" is paused
    And Kafka message is published to topic "pulsar_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "2s" Kafka consumer group "pulsar_boundary_group_{{test_id}}" next offset for topic "pulsar_boundary_in_{{test_id}}" partition 0 is "<offset_condition>"
    When the stallable endpoint "pulsar" is resumed
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And within "5s" Kafka consumer group "pulsar_boundary_group_{{test_id}}" next offset for topic "pulsar_boundary_in_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size | publishing_mode                                                       | offset_condition |
      | 1            | NO_ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |
      | 3            | NO_ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |
