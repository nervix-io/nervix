Feature: Branched session subscriptions
  Scenario Outline: Session subscriptions collect records from all branched branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64,
        tenant STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        tenant string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_tenant_branch ( user_id I64, tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_tenant_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-branched-subscription-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id, tenant = notifications.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    When MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42,"tenant":"acme"}
      """
    And MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":7,"tenant":"beta"}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "tenant":"acme","user_id":42
      "tenant":"beta","user_id":7
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Session subscriptions filter-map collected branched records
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64,
        tenant STRING,
        active BOOL,
        raw STRING,
        normalized STRING OPTIONAL
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        tenant string,
        active boolean,
        raw string,
        normalized string OPTIONAL
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_tenant_branch ( user_id I64, tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_tenant_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-branched-subscription-filter-map-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id, tenant = notifications.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications
        SET notifications.normalized = lower(trim(notifications.raw))
        UNSET notifications.raw, notifications.active, notifications.user_id
        WHERE notifications.tenant = 'acme' AND notifications.active;
        START;
      """
    When MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42,"tenant":"acme","active":true,"raw":"  HELLO  "}
      """
    And MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":7,"tenant":"beta","active":true,"raw":"ignored"}
      """
    And MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":8,"tenant":"acme","active":false,"raw":"ignored"}
      """
    Then the relay subscription receives a payload
      """
      "normalized":"hello","tenant":"acme"
      """
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload does not contain "active\""
    And the last relay subscription payload does not contain "user_id\""

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
