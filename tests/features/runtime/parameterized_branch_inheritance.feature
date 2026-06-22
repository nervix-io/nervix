Feature: Parameterized branch inheritance
  Scenario Outline: Downstream deduplicators preserve the ingestor parameter key
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_user_id_branch;
      CREATE RELAY projected_notifications SCHEMA notification PARAMETERIZED BY tenant_user_id_branch;

      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-parameterized-processor-{{test_id}}'
        };

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR passthrough
        FROM notifications
        TO projected_notifications PARAMETERIZED BY tenant_user_id_branch
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO projected_notifications;
      START;
      """
    And MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"tenant":"acme","user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "tenant":"acme","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme","user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
