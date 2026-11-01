Feature: Postgres connection pool keys
  A Postgres client declares how its pool treats a connection: how long one may sit idle, how old
  one may become, how long a borrow may take, and whether a borrow tests the connection first. The
  server's connection list, which application_name scopes to one client, shows what the pool holds.

  Scenario Outline: A Postgres pool retires its connection at a declared lifetime on <nodes> nodes
    Given Postgres is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "pool_lifetime_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pool-lifetime-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{postgres_addr}}&application_name=nervix_lifetime_{{test_id}}',
        '<key>' = '1000'
      };
      CREATE POSTGRES TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'pool_lifetime_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES {
          "postgres_user_id" = input.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(input.action)
        }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Postgres eventually replaces the connection of application "nervix_lifetime_{{test_id}}" at least "1s" after it started
    And node "node-1" eventually accepts http traffic for host "pool-lifetime-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42,"action":"OPEN"}
      """
    And the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":42,"postgres_action":"open"}
      """

    Examples:
      | nodes | key                  |
      | 1     | pool_max_lifetime_ms |
      | 3     | pool_max_lifetime_ms |
      | 1     | pool_idle_timeout_ms |
      | 3     | pool_idle_timeout_ms |

  # A write that failed would wait out the five-minute backoff, far beyond the row's wait, so the
  # row arriving shows that the first write found a usable connection.
  Scenario Outline: A Postgres borrow replaces a connection the server closed before its write on <nodes> nodes
    Given Postgres is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "pool_tested_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pool-tested-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{postgres_addr}}&application_name=nervix_tested_{{test_id}}'<tested>
      };
      CREATE POSTGRES TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'pool_tested_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 5m MAX 5m
        VALUES {
          "postgres_user_id" = input.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(input.action)
        }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "pool-tested-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1,"action":"WARMUP"}
      """
    And the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":1,"postgres_action":"warmup"}
      """
    When Postgres terminates the connections of application "nervix_tested_{{test_id}}"
    Then Postgres eventually reports 0 connections for application "nervix_tested_{{test_id}}"
    When http payload is posted to host "pool-tested-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"action":"AFTER"}
      """
    Then the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":1,"postgres_action":"warmup"}
      {"postgres_user_id":2,"postgres_action":"after"}
      """

    Examples:
      | nodes | tested                                |
      | 1     |                                       |
      | 3     |                                       |
      | 1     | , 'pool_test_before_acquire' = 'true' |
      | 3     | , 'pool_test_before_acquire' = 'true' |

  Scenario Outline: A Postgres client that skips the borrow test finds a closed connection at its write on <nodes> nodes
    Given Postgres is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "pool_untested_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pool-untested-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{postgres_addr}}&application_name=nervix_untested_{{test_id}}',
        'pool_test_before_acquire' = 'false'
      };
      CREATE POSTGRES TABLE EMITTER outgoing FROM notifications
        TO db TABLE 'pool_untested_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 30s MAX 30s
        VALUES {
          "postgres_user_id" = input.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(input.action)
        }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "pool-untested-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1,"action":"WARMUP"}
      """
    And the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":1,"postgres_action":"warmup"}
      """
    When Postgres terminates the connections of application "nervix_untested_{{test_id}}"
    Then Postgres eventually reports 0 connections for application "nervix_untested_{{test_id}}"
    When http payload is posted to host "pool-untested-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"action":"AFTER"}
      """
    Then within "30s" DESCRIBE EMITTER "outgoing" on the leader node contains
      """
      transient error:
      """
    And the Postgres table eventually holds exactly these rows
      """
      {"postgres_user_id":1,"postgres_action":"warmup"}
      {"postgres_user_id":2,"postgres_action":"after"}
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  # Pools are node-local, so contention for one connection has a one-node topology.
  Scenario: A Postgres borrow from a full pool ends at the declared acquire deadline
    Given Postgres is running
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Postgres table "pool_deadline_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge pool-deadline-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{postgres_addr}}',
        'pool_acquire_timeout_ms' = '1000'
      };
      CREATE POSTGRES TABLE EMITTER writer_one FROM notifications
        TO db TABLE 'pool_deadline_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES {
          "postgres_user_id" = input.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(input.action)
        }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES TABLE EMITTER writer_two FROM notifications
        TO db TABLE 'pool_deadline_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
        VALUES {
          "postgres_user_id" = input.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = LOWER(input.action)
        }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "pool-deadline-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1,"action":"WARMUP"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":1,"postgres_action":"warmup"}
      """
    Given the Postgres table is locked against inserts
    When http payload is posted to host "pool-deadline-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"action":"HELD"}
      """
    Then within "30s" DESCRIBE EMITTER "writer_one" or "writer_two" on the leader node contains
      """
      within pool_acquire_timeout_ms 1s
      """
    When the Postgres table lock is released
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":2,"postgres_action":"held"}
      """
