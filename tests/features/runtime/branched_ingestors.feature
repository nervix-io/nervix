Feature: Ingestor branch consistency
  Scenario Outline: Ingestors targeting the same relay must use its exact named branch
    Given Redis is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "must use its exact declared branch"
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA tenant_user_branch TTL 5m;

      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;

      CREATE MQTT CLIENT mqtt_main
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-branched-mqtt-{{test_id}}'
        };

      CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{redis_addr}}',
          'subscription_buffer_messages' = '1024',
          'idle_timeout_ms' = '30000'
        }; CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_branch TTL 5m; CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main CHANNEL 'notifications_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_redis_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Differently named branches with the same schema are incompatible
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "must use its exact declared branch"
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE SCHEMA tenant_user_branch (
        tenant STRING,
        user_id I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications_secondary SCHEMA tenant_user_branch TTL 5m;

      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications_secondary;

      CREATE MQTT CLIENT mqtt_main
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-branched-mqtt-a-{{test_id}}'
        };

      CREATE MQTT CLIENT mqtt_secondary
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-branched-mqtt-b-{{test_id}}'
        };

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA tenant_user_branch TTL 5m; CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_a_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG; CREATE MQTT TOPIC INGESTOR mqtt_notifications_secondary
        FROM mqtt_secondary TOPIC 'notifications_b_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications_secondary
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
