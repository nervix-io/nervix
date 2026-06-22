Feature: Forwarder
  Scenario Outline: Forwarder preserves parameterization and applies filter-map rewrites
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification_in (
        tenant STRING,
        user_id I64,
        active BOOL,
        amount I64,
        raw STRING
      );

      CREATE SCHEMA notification_out (
        tenant STRING,
        user_id I64,
        amount I64,
        normalized STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer,
        active boolean,
        amount integer,
        raw string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE RELAY notifications SCHEMA notification_in PARAMETERIZED BY tenant_user_id_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE RELAY projected_notifications SCHEMA notification_out PARAMETERIZED BY tenant_user_id_branch;

      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-forwarder-{{test_id}}'
        };

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE FORWARDER project_notifications
        FROM notifications
        TO projected_notifications PARAMETERIZED BY tenant_user_id_branch
        FLUSH IMMEDIATE
        SET notifications.normalized = lower(trim(notifications.raw)), notifications.amount = notifications.amount + 1
        UNSET notifications.raw, notifications.active
        WHERE notifications.active ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO projected_notifications;
      START;
      """
    When MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"tenant":"acme","user_id":42,"active":true,"amount":9,"raw":"  HELLO  "}
      """
    Then the relay subscription receives a payload
      """
      "normalized":"hello"
      """
    And the last relay subscription payload contains
      """
      "tenant":"acme"
      "user_id":42
      "amount":10
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme","user_id":42}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload does not contain "active\""
    When MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"tenant":"acme","user_id":42,"active":false,"amount":99,"raw":"should drop"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
