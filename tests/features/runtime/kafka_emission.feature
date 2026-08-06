Feature: Kafka emission
  Scenario Outline: Kafka emitter filter-map publishes message fields and headers
    Given Kafka is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_headers_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        amount I64,
        raw STRING,
        active BOOL
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        amount integer,
        raw string,
        active boolean
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA emitted_notification (
        tenant STRING,
        amount I64,
        normalized STRING
      );

      CREATE WIRE JSON SCHEMA emitted_notification_wire MODE STRICT (
        tenant string,
        amount integer,
        normalized string
      );

      CREATE CODEC emitted_notification_codec
        FROM WIRE JSON SCHEMA emitted_notification_wire
        TO SCHEMA emitted_notification;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;

      CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
      CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress
        TOPIC notifications_headers_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          BRANCHED BY by_mqtt_notifications
          SET tenant = message.tenant
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE EMITTER kafka_notifications
        FROM notifications
        TO KAFKA kafka_main TOPIC notifications_headers_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING emitted_notification_codec
        INHERIT ALL EXCEPT raw, active
        SET amount = amount + 1,
            normalized = lower(input.raw)
        WHERE input.active
        INVOKE write_header(lower("TENANT"), output.tenant),
               write_header(lower("ROUTE"), "primary"),
               write_header(lower("ROUTE"), output.normalized)
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "notifications_headers_in_{{test_id}}"
      """
      {"tenant":"acme","amount":42,"raw":"FAST-LANE","active":true}
      """
    Then the observed broker receives a payload
      """
      {"tenant":"acme","amount":43,"normalized":"fast-lane"}
      """
    And the last observed broker message has headers
      """
      tenant=acme
      route=primary
      route=fast-lane
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Kafka emitter publishes JSON payloads from a relay
    Given Kafka is running
    And MQTT is running
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
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE EMITTER kafka_notifications FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 2s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "kafka_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker does not receive a payload within "500ms"
    And within "5s" DESCRIBE EMITTER "kafka_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "kafka_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @kafka_emitter_ack_boundary
  Scenario Outline: Kafka emitter mode controls when the input offset is committed
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "mode_boundary_in_{{test_id}}" exists with 1 partitions
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
      CREATE CLIENT unavailable_kafka_sink TYPE KAFKA CONFIG {
        'bootstrap.servers' = '127.0.0.1:1',
        'message.timeout.ms' = '100',
        'socket.timeout.ms' = '100'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC mode_boundary_in_{{test_id}}
          OFFSET BY CONSUMER GROUP mode_boundary_group_{{test_id}}
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
      CREATE ATTACHED EMITTER kafka_mode_boundary FROM notifications
        TO KAFKA unavailable_kafka_sink TOPIC unavailable_mode_boundary
          MODE <publishing_mode>
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "mode_boundary_group_{{test_id}}" eventually has 1 consumers
    And Kafka message is published to topic "mode_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "3s" Kafka consumer group "mode_boundary_group_{{test_id}}" next offset for topic "mode_boundary_in_{{test_id}}" partition 0 is "<offset_condition>"

    Examples:
      | cluster_size | publishing_mode                                                       | offset_condition |
      | 1            | NO_ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |
      | 3            | NO_ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |

  @detached_confirming_mode
  Scenario Outline: A DETACHED confirming emitter commits upstream while retaining publish retries
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "detached_confirming_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "detached_confirming_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "detached_confirming_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
      FROM KAFKA kafka_main TOPIC detached_confirming_in_{{test_id}}
        OFFSET BY CONSUMER GROUP detached_confirming_group_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
      DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE DETACHED EMITTER kafka_detached_confirming
      FROM notifications
      TO KAFKA kafka_main TOPIC detached_confirming_out_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "detached_confirming_group_{{test_id}}" eventually has 1 consumers
    And emitter "kafka_detached_confirming" enters stall mode
    And Kafka message is published to topic "detached_confirming_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "10s" Kafka consumer group "detached_confirming_group_{{test_id}}" next offset for topic "detached_confirming_in_{{test_id}}" partition 0 is "at least 1"
    And the observed broker does not receive a payload within "500ms"
    And within "5s" DESCRIBE EMITTER "kafka_detached_confirming" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And emitter "kafka_detached_confirming" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
