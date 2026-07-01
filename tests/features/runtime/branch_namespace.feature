Feature: Branch namespace
  Scenario Outline: Processor filter-map reads the current branch key
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
        amount I64
      );

      CREATE SCHEMA notification_out (
        tenant STRING,
        user_id I64,
        amount I64,
        branch_tenant STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer,
        active boolean,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications BY tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification_in BRANCHED BY by_mqtt_notifications;
      CREATE RELAY projected_notifications SCHEMA notification_out BRANCHED BY by_mqtt_notifications;

      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-branch-namespace-{{test_id}}'
        }; CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { tenant = notifications.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC branch_namespace_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR project_notifications
        FROM notifications
        TO projected_notifications
          SET projected_notifications.branch_tenant = branch.tenant,
              projected_notifications.amount = notifications.amount + 1
          UNSET notifications.active
          WHERE branch.tenant = notifications.tenant
        BRANCHED BY by_mqtt_notifications
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO projected_notifications WHERE projected_notifications.tenant = 'acme';
      START;
      """
    When MQTT message is published to topic "branch_namespace_{{test_id}}"
      """
      {"tenant":"acme","user_id":42,"active":true,"amount":7}
      """
    Then the relay subscription receives a payload
      """
      "branch_tenant":"acme"
      """
    And the last relay subscription payload contains
      """
      "tenant":"acme"
      "user_id":42
      "amount":8
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Reingestor branch mapping reads the current branch key
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

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_http_notifications BY tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;

      CREATE IF NOT EXISTS BRANCH by_copy_notifications BY tenant_branch TTL 5m;
      CREATE RELAY copied_notifications SCHEMA notification BRANCHED BY by_copy_notifications;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP; CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = notifications.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG; CREATE REINGESTOR copy_notifications
        FROM notifications
        TO copied_notifications
        BRANCHED BY by_copy_notifications VALUES { tenant = branch.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO copied_notifications;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "tenant":"acme","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
