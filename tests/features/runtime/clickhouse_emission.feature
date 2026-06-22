Feature: ClickHouse emission
  Scenario Outline: ClickHouse emitter inserts mapped rows from a relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ClickHouse table "notifications_out_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        action string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CLIENT clickhouse_client
        TYPE CLICKHOUSE
        CONFIG {
          'addr' = 'http://127.0.0.1:8123',
          'user' = 'default',
          'password' = 'nervix'
        };

      CREATE EMITTER to_ch
        FROM notifications
        TO CLICKHOUSE clickhouse_client INSERT TO TABLE notifications_out_{{test_id}}
        VALUES {
          "clickhouse_user_id" = notifications.user_id,
          "clickhouse_now" = NOW() AS STRING,
          "clickhouse_action" = LOWER(notifications.action)
        }
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      START;
      """
    And emitter "to_ch" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN"}
      """
    Then within "5s" DESCRIBE EMITTER "to_ch" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "to_ch" leaves fault mode
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":42,"clickhouse_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
