Feature: Postgres statement deadlines

  Scenario Outline: A Postgres insert blocked by a table lock times out and retries on <nodes> nodes
    Given Postgres is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "statement_timeout_{{test_id}}" with primary key exists
    And the Postgres table is locked against inserts
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pg-timeout-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = '{{postgres_addr}}', 'timeout_ms' = '1000'
      };
      CREATE POSTGRES TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'statement_timeout_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES {
          'postgres_user_id' = input.user_id,
          'postgres_now' = NOW() AS STRING,
          'postgres_action' = LOWER(input.action)
        }
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "pg-timeout-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42,"action":"RETRY"}
      """
    And within "30s" DESCRIBE EMITTER "outgoing" on the leader node contains
      """
      timeout_ms 1s
      """
    When the Postgres table lock is released
    Then the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":42,"postgres_action":"retry"}
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  # The lock wait proves checkout finished. The retry delay keeps this failed attempt visible
  # while a cross-node DESCRIBE runs; the query itself still has a one-second response deadline.
  Scenario Outline: A Postgres emitter whose service stops answering is reported within timeout_ms on <nodes> nodes
    Given Postgres is running
    And a <nodes> node nervix cluster is started
    And a stallable endpoint "postgres_peer" stands in front of "postgres_addr"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "silent_statement_{{test_id}}" with primary key exists
    And the Postgres table is locked against inserts
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pg-silent-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = '{{postgres_peer}}', 'timeout_ms' = '1000',
        'pool_test_before_acquire' = 'false'
      };
      CREATE POSTGRES TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'silent_statement_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 10s MAX 10s
        VALUES {
          'postgres_user_id' = input.user_id,
          'postgres_now' = NOW() AS STRING,
          'postgres_action' = LOWER(input.action)
        }
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "pg-silent-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"action":"AFTER"}
      """
    And Postgres eventually reports an insert into table "silent_statement_{{test_id}}" waiting for its lock
    When the stallable endpoint "postgres_peer" is paused
    Then within "20s" DESCRIBE EMITTER "outgoing" on the leader node contains
      """
      did not finish within timeout_ms 1s
      """
    When the Postgres table lock is released
    When the stallable endpoint "postgres_peer" is resumed
    Then the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":2,"postgres_action":"after"}
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: A Postgres client preserves its statement deadline through inspection on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable', 'timeout_ms' = '1500'
      };
      SHOW CREATE CLIENT db;
      """
    Then the last command output contains
      """
      'timeout_ms' = '1500'
      """
    When these Plexus statements are executed on the leader node
      """
      DESCRIBE CLIENT db;
      """
    Then the last command output contains
      """
      timeout_ms: 1500
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: A Postgres client refuses a statement deadline it cannot enforce on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "invalid client config key 'timeout_ms'"
      """
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable', 'timeout_ms' = '<timeout>'
      };
      """

    Examples:
      | nodes | timeout    |
      | 1     | 0          |
      | 3     | 0          |
      | 1     | 1.5        |
      | 3     | 1.5        |
      | 1     | 2147483648 |
      | 3     | 2147483648 |

  Scenario Outline: Postgres refuses an untyped null mapping at CREATE and ALTER on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable' };
      CREATE POSTGRES TABLE EMITTER configured FROM events TO db TABLE 'events'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "NULL requires a declared optional assignment target"
      """
      CREATE POSTGRES TABLE EMITTER refused FROM events TO db TABLE 'events'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = NULL }
        BATCH MAX MESSAGES 1 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus statements fail with "NULL requires a declared optional assignment target"
      """
      ALTER POSTGRES TABLE EMITTER configured SET TO db TABLE 'events'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = NULL };
      """
    And these Plexus statements fail with "unknown client config key 'unrecognized_option'"
      """
      CREATE POSTGRES CLIENT refused POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable',
        'unrecognized_option' = 'value'
      };
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |
