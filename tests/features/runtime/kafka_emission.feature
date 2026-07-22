Feature: Kafka emission
  Scenario Outline: Kafka emitter filter-map publishes message fields and headers
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

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
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

      CREATE STRICT WIRE JSON SCHEMA emitted_notification_wire (
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
          'addr' = 'mqtt://127.0.0.1:1883',
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
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE EMITTER kafka_notifications
        FROM notifications
        ENCODE USING emitted_notification_codec
        TO KAFKA kafka_main TOPIC notifications_headers_out_{{test_id}}
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
