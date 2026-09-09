Feature: Shared database client pools
  A pool belongs to one named client in one domain on one physical node. Every local emitter of
  that client borrows from the same pool, so the declared maximum bounds the node's connections
  rather than each emitter's, and a borrower that cannot be served reports the wait instead of
  looking idle next to a connection it does not hold.

  Background:
    Given Postgres is running
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """

  Scenario: Emitters sharing a client stay within its declared maximum
    Given Postgres table "shared_pool_out_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT postgres_shared
        TYPE POSTGRES
        POOL SIZE MIN 1 MAX 2
        CONFIG {
          'addr' = '{{postgres_addr}}&application_name=nervix_shared_{{test_id}}'
        };
      CREATE EMITTER writer_one FROM notifications
        TO POSTGRES postgres_shared INSERT TO TABLE shared_pool_out_{{test_id}}
        VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) }
        WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER writer_two FROM notifications
        TO POSTGRES postgres_shared INSERT TO TABLE shared_pool_out_{{test_id}}
        VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) }
        WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER writer_three FROM notifications
        TO POSTGRES postgres_shared INSERT TO TABLE shared_pool_out_{{test_id}}
        VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) }
        WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":1,"action":"OPEN"}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":2,"action":"OPEN"}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":2,"postgres_action":"open"}
      """
    And Postgres never reports more than 2 connections for application "nervix_shared_{{test_id}}"

  Scenario: A borrower waiting on a full pool reports the wait and recovers
    Given Postgres table "contended_pool_out_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64, action STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer, action string );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT postgres_contended
        TYPE POSTGRES
        POOL SIZE MIN 1 MAX 1
        CONFIG {
          'addr' = '{{postgres_addr}}&application_name=nervix_contended_{{test_id}}'
        };
      CREATE EMITTER writer_one FROM notifications
        TO POSTGRES postgres_contended INSERT TO TABLE contended_pool_out_{{test_id}}
        VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) }
        WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER writer_two FROM notifications
        TO POSTGRES postgres_contended INSERT TO TABLE contended_pool_out_{{test_id}}
        VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) }
        WITH MAX BATCH 2 MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":1,"action":"OPEN"}
      """
    Given the Postgres table is locked against inserts
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":2,"action":"OPEN"}
      """
    Then within "20s" DESCRIBE EMITTER "writer_two" on the leader node contains
      """
      waiting for a connection from client 'postgres_contended'
      """
    And Postgres never reports more than 1 connections for application "nervix_contended_{{test_id}}"
    When the Postgres table lock is released
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":2,"postgres_action":"open"}
      """
