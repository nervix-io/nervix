Feature: ClickHouse TLS resource mounts
  Scenario Outline: ClickHouse emitter inserts over HTTPS with a mounted resource directory
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And ClickHouse TLS table "tls_notifications_out_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
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
          'client_id' = 'nervix-cucumber-clickhouse-tls-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }

        FROM MQTT mqtt_ingress
        TOPIC tls_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE CLIENT clickhouse_client
        TYPE CLICKHOUSE
        MOUNT dev_tls
        CONFIG {
          'addr' = 'https://127.0.0.1:8124',
          'user' = 'default',
          'password' = 'nervix',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };
        CREATE EMITTER to_ch
        FROM notifications
        TO CLICKHOUSE clickhouse_client INSERT TO TABLE tls_notifications_out_{{test_id}}
        VALUES {
          "clickhouse_user_id" = notifications.user_id,
          "clickhouse_now" = NOW() AS STRING,
          "clickhouse_action" = LOWER(notifications.action)
        }
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "tls_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":42,"clickhouse_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
