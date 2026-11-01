Feature: ClickHouse emission
  Scenario Outline: ClickHouse writes after the server closes idle connections
    Given MQTT is running
    And ClickHouse is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ClickHouse table "idle_ch_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (user_id integer, action string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT ingress CONFIG {
        'addr' = '{{mqtt_addr}}', 'client_id' = 'clickhouse-idle-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR incoming FROM ingress TOPIC 'clickhouse_idle_{{test_id}}'
        SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLICKHOUSE CLIENT outgoing CONFIG {
        'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix',
        'timeout_ms' = '5000', 'tcp_keepalive_ms' = '30000', 'pool_idle_timeout_ms' = '100'
      };
      CREATE CLICKHOUSE TABLE EMITTER to_ch FROM events TO outgoing TABLE 'idle_ch_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'clickhouse_user_id' = input.user_id, 'clickhouse_action' = input.action,
          'clickhouse_now' = 'connection-policy' }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "clickhouse_idle_{{test_id}}"
      """
      {"user_id":41,"action":"first"}
      """
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":41,"clickhouse_action":"first"}
      """
    When the ClickHouse server's idle connection timeout has elapsed
    And MQTT message is published to topic "clickhouse_idle_{{test_id}}"
      """
      {"user_id":42,"action":"after-idle"}
      """
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":42,"clickhouse_action":"after-idle"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_execution_time
  Scenario Outline: ClickHouse emitter inserts mapped rows from a relay
    Given MQTT is running
    And ClickHouse is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And ClickHouse table "notifications_out_{{test_id}}" exists
    When these Plexus commands are executed
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
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLICKHOUSE CLIENT clickhouse_client
        CONFIG {
          'addr' = '{{clickhouse_addr}}',
          'user' = 'default',
          'password' = 'nervix',
          'timeout_ms' = '5000'
        };
        CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications TO clickhouse_client TABLE 'notifications_out_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "clickhouse_user_id" = input.user_id, "clickhouse_now" = NOW() AS STRING, "clickhouse_action" = CASE WHEN NOW() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN LOWER(input.action) ELSE 'physical-time' END } BATCH MAX MESSAGES 500 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
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
  Scenario Outline: ClickHouse BATCH MAX MESSAGES splits one oversized flush into multiple inserts
    Given MQTT is running
    And ClickHouse is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ClickHouse MergeTree table "batch_ch_{{test_id}}" with merges stopped exists
    When these Plexus commands are executed
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
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-clickhouse-batch-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'clickhouse_batch_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLICKHOUSE CLIENT clickhouse_client
      CONFIG {
        'addr' = '{{clickhouse_addr}}',
        'user' = 'default',
        'password' = 'nervix',
        'timeout_ms' = '5000'
      };
      CREATE CLICKHOUSE TABLE EMITTER to_ch
      FROM notifications
      TO clickhouse_client TABLE 'batch_ch_{{test_id}}'
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { "clickhouse_user_id" = input.user_id } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
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
