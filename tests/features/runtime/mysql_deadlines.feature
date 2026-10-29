Feature: MySQL operation deadlines

  Scenario Outline: A MySQL insert deadline is reported and its rows are retried after a table unlock
    Given MySQL is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "deadline_{{test_id}}" with primary key exists
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge deadline-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR incoming FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = '{{mysql_addr}}', 'timeout_ms' = '3000'
      };
      CREATE MYSQL TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'deadline_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES {
          'mysql_user_id' = input.user_id,
          'mysql_now' = NOW() AS STRING,
          'mysql_action' = LOWER(input.action)
        } ON CONFLICT DO UPDATE
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "deadline-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1,"action":"WARMUP"}
      """
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":1,"mysql_action":"warmup"}
      """
    Given MySQL table "deadline_{{test_id}}" is locked against inserts
    When http payload is posted to host "deadline-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"action":"RETRY"}
      """
    Then within "30s" DESCRIBE EMITTER "outgoing" on the leader node contains
      """
      within timeout_ms 3s
      """
    When the lock on MySQL table "deadline_{{test_id}}" is released
    Then the MySQL table "deadline_{{test_id}}" eventually holds exactly these rows
      """
      {"mysql_user_id":1,"mysql_action":"warmup"}
      {"mysql_user_id":2,"mysql_action":"retry"}
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: A MySQL emitter whose server never answers is reported within timeout_ms
    Given a <nodes> node nervix cluster is started
    And a silent endpoint "mysql_peer" is listening
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mysql://{{mysql_peer}}/nervix',
        'timeout_ms' = '1000'
      };
      CREATE MYSQL TABLE EMITTER outgoing FROM notifications TO db TABLE 'notifications'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'mysql_user_id' = input.user_id }
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "mysql_peer" eventually accepts a connection
    And within "30s" DESCRIBE EMITTER "outgoing" on the leader node contains
      """
      within timeout_ms 1s
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  # Pools are node-local, so contention for one connection has a one-node topology.
  Scenario: MySQL emitters report a pool checkout deadline while their shared connection is busy
    Given MySQL is running
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "deadline_{{test_id}}" with primary key exists
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge deadline-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR incoming FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = '{{mysql_addr}}', 'timeout_ms' = '3000'
      };
      CREATE MYSQL TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'deadline_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES {
          'mysql_user_id' = input.user_id,
          'mysql_now' = NOW() AS STRING,
          'mysql_action' = LOWER(input.action)
        } ON CONFLICT DO UPDATE
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER second FROM notifications
        TO db TABLE 'deadline_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES { 'mysql_user_id' = input.user_id, 'mysql_now' = NOW() AS STRING, 'mysql_action' = LOWER(input.action) }
        ON CONFLICT DO UPDATE BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL TABLE EMITTER third FROM notifications
        TO db TABLE 'deadline_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES { 'mysql_user_id' = input.user_id, 'mysql_now' = NOW() AS STRING, 'mysql_action' = LOWER(input.action) }
        ON CONFLICT DO UPDATE BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "deadline-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1,"action":"WARMUP"}
      """
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":1,"mysql_action":"warmup"}
      """
    Given MySQL table "deadline_{{test_id}}" is locked against inserts
    When http payload is posted to host "deadline-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"action":"RETRY"}
      """
    Then within "60s" one of MySQL emitters "outgoing,second,third" reports an exhausted pool deadline
    When the lock on MySQL table "deadline_{{test_id}}" is released
    Then the MySQL table "deadline_{{test_id}}" eventually holds exactly these rows
      """
      {"mysql_user_id":1,"mysql_action":"warmup"}
      {"mysql_user_id":2,"mysql_action":"retry"}
      """

