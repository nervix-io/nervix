Feature: Ingestor parameterization consistency
  Scenario Outline: Ingestors targeting the same relay must share PARAMETERIZED BY schema
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "conflicting parameterizations"
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE SCHEMA tenant_user_branch (
        tenant STRING,
        user_id I64
      );

      CREATE SCHEMA user_branch (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-parameterized-mqtt-{{test_id}}'
        };

      CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications PARAMETERIZED BY tenant_user_branch VALUES {
          tenant = notifications.tenant,
          user_id = notifications.user_id
        } TTL 5m; CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS BRANCH by_redis_notifications PARAMETERIZED BY user_branch VALUES {
          user_id = notifications.user_id
        } TTL 5m; CREATE INGESTOR redis_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_redis_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM REDIS PUBSUB redis_main
        CHANNEL notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Multiple ingestors can share one parameterization schema for one relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE SCHEMA tenant_user_branch (
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

      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_user_branch;

      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-parameterized-mqtt-a-{{test_id}}'
        };

      CREATE CLIENT mqtt_secondary
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-parameterized-mqtt-b-{{test_id}}'
        };

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications PARAMETERIZED BY tenant_user_branch VALUES {
          tenant = notifications.tenant,
          user_id = notifications.user_id
        } TTL 5m; CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_main
        TOPIC notifications_a_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications_secondary PARAMETERIZED BY tenant_user_branch VALUES {
          tenant = notifications.tenant,
          user_id = notifications.user_id
        } TTL 5m; CREATE INGESTOR mqtt_notifications_secondary
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications_secondary
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_secondary
        TOPIC notifications_b_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      START;
      """
    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
