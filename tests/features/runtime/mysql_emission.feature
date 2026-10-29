Feature: MySQL emission
  @domain_execution_time @retained_task_handles
  Scenario Outline: MySQL emitter inserts mapped rows from a relay
    Given MQTT is running
    And MySQL is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And MySQL table "Notifications_mysql_out_{{test_id}}" exists
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
          'client_id' = 'nervix-cucumber-mysql-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'mysql_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE MYSQL CLIENT mysql_client
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = '{{mysql_addr}}'
        };
        CREATE MYSQL TABLE EMITTER to_mysql FROM notifications TO mysql_client TABLE 'nervix.Notifications_mysql_out_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mysql_user_id" = input.user_id, "mysql_now" = NOW() AS STRING, "mysql_action" = CASE WHEN NOW() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN LOWER(input.action) ELSE 'physical-time' END } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And emitter "to_mysql" enters stall mode
    Then within "10s" repeatedly publishing MQTT message to topic "mysql_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "5s" DESCRIBE EMITTER "to_mysql" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "to_mysql" leaves fault mode
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":42,"mysql_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: MySQL emitter handles insert conflicts with <conflict_action>
    Given MQTT is running
    And MySQL is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "notifications_mysql_conflict_{{test_id}}" with primary key exists
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
          'client_id' = 'nervix-cucumber-mysql-conflict-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'mysql_conflict_notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE MYSQL CLIENT mysql_client
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = '{{mysql_addr}}'
        };
        CREATE MYSQL TABLE EMITTER to_mysql FROM notifications TO mysql_client TABLE 'notifications_mysql_conflict_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mysql_user_id" = input.user_id, "mysql_now" = NOW() AS STRING, "mysql_action" = LOWER(input.action) } ON CONFLICT <conflict_action> BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And these Plexus commands are executed on the active session
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "emitter" "to_mysql" is saved as placeholder "mysql_emitter_owner"
    When MQTT message is published to topic "mysql_conflict_notifications_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "action":"OPEN"
      """
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":42,"mysql_action":"open"}
      """
    When MQTT message is published to topic "mysql_conflict_notifications_in_{{test_id}}"
      """
      {"user_id":42,"action":"CLOSE"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "action":"CLOSE"
      """
    And within "5s" node "{{mysql_emitter_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="EMITTER"
      target="to_mysql"
      direction="sent"
      relay="notifications"
      """
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":42,"mysql_action":"<expected_action>"}
      """

    Examples:
      | cluster_size | replica_count | conflict_action | expected_action |
      | 1            | 0             | DO UPDATE       | close           |
      | 3            | 0             | DO UPDATE       | close           |
      | 3            | 1             | DO UPDATE       | close           |
      | 1            | 0             | DO NOTHING      | open            |
      | 3            | 0             | DO NOTHING      | open            |
      | 3            | 1             | DO NOTHING      | open            |

  @database_emitter_modes @max_batch @mysql_max_batch
  Scenario Outline: MySQL BATCH MAX MESSAGES splits one oversized flush into multiple inserts
    Given MQTT is running
    And MySQL is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "batch_mysql_{{test_id}}" recording insert commands exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-mysql-batch-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'mysql_batch_in_{{test_id}}' MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE MYSQL CLIENT mysql_client POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = '{{mysql_addr}}'
      };
      CREATE MYSQL TABLE EMITTER to_mysql
      FROM notifications
      TO mysql_client TABLE 'batch_mysql_{{test_id}}'
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { "mysql_user_id" = input.user_id } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When 5 JSON messages with user id 42 are rapidly published to "MQTT" input "mysql_batch_in_{{test_id}}"
    Then the MySQL table eventually contains 5 rows from at least 3 insert commands

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Every user of a named client on one node borrows from the same pool, so this scenario is
  # one-node by construction: the pool of one connection is what the three emitters share.
  # The first writer holds the only connection behind a table lock while the poison write queues
  # for it, then a second blocked writer queues behind the poison write. Once the first lock is
  # released, the poison write must finish on the connection it receives, isolating the rejected
  # row there, before the second writer can take that connection and block on its own lock.
  @database_emitter_modes @poison_isolation_shared_pool
  Scenario: A MySQL write isolates its rejected row on the connection it borrowed while others queue for the pool
    Given MySQL is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "first_held_{{test_id}}" exists
    And MySQL table "second_held_{{test_id}}" exists
    And MySQL table "poison_mysql_{{test_id}}" rejecting poison actions exists
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        error_message STRING,
        source_user_id I64,
        source_action STRING
      );
      CREATE CODEC notification_codec FROM JSON TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY first_notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY second_notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT poison_ingress ON edge PATH '/poison' TYPE HTTP;
      CREATE ENDPOINT first_ingress ON edge PATH '/first' TYPE HTTP;
      CREATE ENDPOINT second_ingress ON edge PATH '/second' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_poison
        FROM poison_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR http_first
        FROM first_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO first_notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR http_second
        FROM second_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO second_notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL CLIENT mysql_shared
        POOL SIZE MIN 1 MAX 1
        CONFIG {
          'addr' = '{{mysql_addr}}'
        };
      CREATE MYSQL TABLE EMITTER to_mysql FROM notifications
        TO mysql_shared TABLE 'poison_mysql_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { "mysql_user_id" = input.user_id, "mysql_now" = NOW() AS STRING, "mysql_action" = LOWER(input.action) } BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO emitter_errors
        SET error_code = error.code,
            error_message = error.message,
            source_user_id = input.user_id,
            source_action = input.action
        ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER first_writer FROM first_notifications
        TO mysql_shared TABLE 'first_held_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mysql_user_id" = input.user_id, "mysql_now" = NOW() AS STRING, "mysql_action" = LOWER(input.action) } BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER second_writer FROM second_notifications
        TO mysql_shared TABLE 'second_held_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mysql_user_id" = input.user_id, "mysql_now" = NOW() AS STRING, "mysql_action" = LOWER(input.action) } BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/first"
      """
      [{"user_id":9,"action":"WARMUP"}]
      """
    And the MySQL table "first_held_{{test_id}}" eventually holds exactly these rows
      """
      {"mysql_user_id":9,"mysql_action":"warmup"}
      """
    Given MySQL table "first_held_{{test_id}}" is locked against inserts
    And MySQL table "second_held_{{test_id}}" is locked against inserts
    When http payload is posted to host "http-{{test_id}}.example.com" path "/first"
      """
      [{"user_id":10,"action":"FIRST"}]
      """
    Then MySQL eventually reports an insert into table "first_held_{{test_id}}" waiting for its lock
    When http payload is posted to host "http-{{test_id}}.example.com" path "/poison"
      """
      [{"user_id":1,"action":"HEALTHY_A"},{"user_id":2,"action":"POISON"},{"user_id":3,"action":"HEALTHY_B"}]
      """
    Then within "30s" DESCRIBE EMITTER "to_mysql" on the leader node contains
      """
      waiting for a connection from client 'mysql_shared'
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/second"
      """
      [{"user_id":20,"action":"SECOND"}]
      """
    Then within "30s" DESCRIBE EMITTER "second_writer" on the leader node contains
      """
      waiting for a connection from client 'mysql_shared'
      """
    When the lock on MySQL table "first_held_{{test_id}}" is released
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "error_code":"external" | "error_message":"MySQL rejected record with SQLSTATE HY000 and code 3819" | "source_user_id":2 | "source_action":"POISON"
      """
    And the MySQL table "poison_mysql_{{test_id}}" eventually holds exactly these rows
      """
      {"mysql_user_id":1,"mysql_action":"healthy_a"}
      {"mysql_user_id":3,"mysql_action":"healthy_b"}
      """
    And MySQL recorded 4 inserts into table "poison_mysql_{{test_id}}" on one connection
    And the MySQL table "first_held_{{test_id}}" eventually holds exactly these rows
      """
      {"mysql_user_id":9,"mysql_action":"warmup"}
      {"mysql_user_id":10,"mysql_action":"first"}
      """
    When the lock on MySQL table "second_held_{{test_id}}" is released
    Then the MySQL table "second_held_{{test_id}}" eventually holds exactly these rows
      """
      {"mysql_user_id":20,"mysql_action":"second"}
      """
    And the relay subscription does not receive a payload within "1s"
