Feature: ClickHouse emission
  Scenario Outline: ClickHouse emitter inserts mapped rows from a relay
    Given MQTT is running
    And ClickHouse is running
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
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
        CREATE CLIENT clickhouse_client
        TYPE CLICKHOUSE
        CONFIG {
          'addr' = '{{clickhouse_addr}}',
          'user' = 'default',
          'password' = 'nervix'
        };
        CREATE EMITTER to_ch FROM notifications TO CLICKHOUSE clickhouse_client INSERT TO TABLE notifications_out_{{test_id}} VALUES { "clickhouse_user_id" = input.user_id, "clickhouse_now" = NOW() AS STRING, "clickhouse_action" = LOWER(input.action) } WITH MAX BATCH 500 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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

  @database_emitter_modes @max_batch
  Scenario Outline: ClickHouse WITH MAX BATCH splits one oversized flush into multiple inserts
    Given MQTT is running
    And ClickHouse is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ClickHouse MergeTree table "batch_ch_{{test_id}}" with merges stopped exists
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
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT mqtt_ingress
      TYPE MQTT
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-clickhouse-batch-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC clickhouse_batch_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT clickhouse_client
      TYPE CLICKHOUSE
      CONFIG {
        'addr' = '{{clickhouse_addr}}',
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE EMITTER to_ch
      FROM notifications
      TO CLICKHOUSE clickhouse_client INSERT TO TABLE batch_ch_{{test_id}}
      VALUES { "clickhouse_user_id" = input.user_id }
      WITH MAX BATCH 2
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When 5 JSON messages with user id 42 are rapidly published to "MQTT" input "clickhouse_batch_in_{{test_id}}"
    Then the ClickHouse table eventually contains 5 rows in at least 3 parts

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
