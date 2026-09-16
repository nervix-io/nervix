@shutdown_qualification @exclusive
Feature: Real process crash recovery

  Scenario: SIGKILL during interleaved traffic preserves only checkpoints durable before the crash
    Given Postgres is running
    And a nervix-server process is started with state snapshot interval "50ms"
    And Postgres table "process_crash_{{test_id}}" exists
    And the server process is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA crash_event (
        user_id I64,
        tenant STRING
      );
      CREATE WIRE JSON SCHEMA crash_event_wire MODE STRICT (
        user_id integer,
        tenant string
      );
      CREATE CODEC crash_event_codec
        FROM WIRE JSON SCHEMA crash_event_wire
        TO SCHEMA crash_event;
      CREATE SCHEMA crash_tenant (
        tenant STRING
      );
      CREATE BRANCH by_crash_tenant SCHEMA crash_tenant TTL 5m;
      CREATE RELAY crash_input SCHEMA crash_event BRANCHED BY by_crash_tenant;
      CREATE RELAY crash_unique SCHEMA crash_event BRANCHED BY by_crash_tenant;
      CREATE VHOST crash_edge crash-{{test_id}}.example.com;
      CREATE ENDPOINT crash_ingress ON crash_edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR crash_source
        FROM ENDPOINT crash_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING crash_event_codec
        TO crash_input
          INHERIT ALL
          BRANCHED BY by_crash_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR crash_deduplicator FROM crash_input
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_crash_tenant
        TO crash_unique
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE CLIENT crash_postgres TYPE POSTGRES
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{postgres_addr}}'
        };
      CREATE EMITTER crash_output FROM crash_unique
        TO POSTGRES crash_postgres INSERT TO TABLE process_crash_{{test_id}}
        VALUES {
          "postgres_user_id" = input.user_id,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = input.tenant
        }
        WITH MAX BATCH 64 MODE ACK RETRY POLICY BACKOFF 100ms MAX 5s
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":101,"tenant":"alpha"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":201,"tenant":"beta"}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":101,"postgres_action":"alpha"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":201,"postgres_action":"beta"}
      """
    When the server process receives SIGTERM
    Then the server process exits with status 0
    And the server process log contains "shutdown terminal-teardown phase finished"

    When the server process is restarted from its existing database
    And the server process eventually accepts http payload with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":101,"tenant":"alpha"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":201,"tenant":"beta"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":102,"tenant":"alpha"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":202,"tenant":"beta"}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":102,"postgres_action":"alpha"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":202,"postgres_action":"beta"}
      """

    When HTTP load against the server process begins at id 10000 with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":{{load_id}},"tenant":"alpha"}
      {"user_id":{{load_id}},"tenant":"beta"}
      """
    Then the server process load admits at least 50 payloads
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":10000,"postgres_action":"alpha"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":10001,"postgres_action":"beta"}
      """
    When the server process receives SIGKILL
    Then the server process is terminated by SIGKILL within "10s" of the last signal
    And the server process log does not contain "shutdown terminal-teardown phase finished"

    When the server process is restarted from its existing database
    And the server process eventually accepts http payload with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":101,"tenant":"alpha"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":201,"tenant":"beta"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":102,"tenant":"alpha"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":202,"tenant":"beta"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":103,"tenant":"alpha"}
      """
    And http payload is posted to the server process with host "crash-{{test_id}}.example.com" path "/events"
      """
      {"user_id":203,"tenant":"beta"}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":103,"postgres_action":"alpha"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":203,"postgres_action":"beta"}
      """
    And the Postgres table contains exactly one row for each of these user ids
      """
      101
      201
      102
      202
      """
    When the server process receives SIGTERM
    Then the server process exits with status 0
    And the server process log contains "shutdown terminal-teardown phase finished"
