Feature: MySQL client pool keys

  Scenario Outline: MySQL clients expose their pool policy on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'mysql://127.0.0.1:3306/nervix',
        'pool_acquire_timeout_ms' = '250',
        'pool_idle_timeout_ms' = '1000',
        'pool_max_lifetime_ms' = '1000',
        'pool_maintenance_interval_ms' = '1000'
      };
      DESCRIBE CLIENT db;
      """
    Then the last command output contains
      """
      pool_acquire_timeout_ms: 250
      """
    And the last command output contains
      """
      pool_max_lifetime_ms: 1000
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: A MySQL pool retires its one-second maximum-age connection on <nodes> nodes
    Given MySQL is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MySQL table "Pool_age_{{test_id}}" exists
    And a stallable endpoint "mysql_counted" stands in front of "mysql_addr"
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MYSQL CLIENT db POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{mysql_counted}}',
        'pool_max_lifetime_ms' = '1000',
        'pool_maintenance_interval_ms' = '100'
      };
      CREATE MYSQL TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'nervix.Pool_age_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES { 'mysql_user_id' = input.user_id, 'mysql_action' = input.action }
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the stallable endpoint "mysql_counted" eventually accepts 2 connections

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: MySQL refuses an unknown pool key on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "pool_acquire_typo_ms"
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mysql://127.0.0.1/nervix', 'pool_acquire_typo_ms' = '1000'
      };
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: MySQL emitters report the configured pool acquire deadline on <nodes> nodes
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
      CREATE HTTP ENDPOINT ingress ON edge PATH '/notifications';
      CREATE HTTP ENDPOINT INGESTOR incoming FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = '{{mysql_addr}}', 'pool_acquire_timeout_ms' = '1000'
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
      CREATE PLACEMENT first_local FROM notifications TO outgoing REQUIRE COLOCATION;
      CREATE PLACEMENT second_local FROM notifications TO second REQUIRE COLOCATION;
      CREATE PLACEMENT third_local FROM notifications TO third REQUIRE COLOCATION;
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
    Then within "60s" one of MySQL emitters "outgoing,second,third" reports a "pool_acquire_timeout_ms" exhausted pool deadline
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

  Scenario Outline: MySQL clients show pool defaults on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1
        CONFIG { 'addr' = 'mysql://127.0.0.1/nervix' };
      DESCRIBE CLIENT db;
      """
    Then the last command output contains
      """
      pool_acquire_timeout_ms: 30000 (default)
      """
    And the last command output contains
      """
      pool_idle_timeout_ms: 0 (default)
      """
    And the last command output contains
      """
      pool_maintenance_interval_ms: 1000 (default)
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: MySQL refuses an unusable pool lifetime at CREATE on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<key>"
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'mysql://127.0.0.1/nervix', '<key>' = '<value>'
      };
      """

    Examples:
      | nodes | key                          | value                |
      | 1     | pool_acquire_timeout_ms      | 0                    |
      | 3     | pool_acquire_timeout_ms      | 0                    |
      | 1     | pool_maintenance_interval_ms | 0                    |
      | 3     | pool_maintenance_interval_ms | 0                    |
      | 1     | pool_max_lifetime_ms         | 1001                 |
      | 3     | pool_max_lifetime_ms         | 1001                 |
      | 1     | pool_idle_timeout_ms         | 18446744073709551615 |
      | 3     | pool_idle_timeout_ms         | 18446744073709551615 |
