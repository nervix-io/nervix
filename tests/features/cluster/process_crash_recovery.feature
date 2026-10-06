@shutdown_qualification
Feature: Real process crash recovery

  Scenario: SIGKILL restores interleaved incomplete window branches from durable snapshots
    Given Postgres is running
    And a nervix-server process is started with state snapshot interval "50ms"
    And Postgres table "process_window_{{test_id}}" exists
    And the server process is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA window_input (tenant STRING, value I64);
      CREATE SCHEMA window_output (tenant STRING, samples I64);
      CREATE WIRE JSON SCHEMA window_wire MODE STRICT (tenant string, value integer);
      CREATE CODEC window_codec FROM WIRE JSON SCHEMA window_wire TO SCHEMA window_input;
      CREATE SCHEMA window_tenant (tenant STRING);
      CREATE BRANCH by_window_tenant SCHEMA window_tenant TTL 5m;
      CREATE RELAY window_inputs SCHEMA window_input BRANCHED BY by_window_tenant;
      CREATE RELAY window_outputs SCHEMA window_output BRANCHED BY by_window_tenant;
      CREATE VHOST window_edge window-crash-{{test_id}}.example.com;
      CREATE ENDPOINT window_ingress ON window_edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR window_source
        FROM ENDPOINT window_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING window_codec
        TO window_inputs INHERIT ALL BRANCHED BY by_window_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR crash_window FROM window_inputs
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_window_tenant
        TO window_outputs
          SET tenant = FIRST(input.tenant), samples = COUNT(input.value)
          ON MESSAGE ERROR LOG;
      CREATE CLIENT crash_postgres TYPE POSTGRES
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{postgres_addr}}'
        };
      CREATE EMITTER window_output FROM window_outputs
        TO POSTGRES crash_postgres INSERT TO TABLE process_window_{{test_id}}
        VALUES {
          "postgres_user_id" = input.samples,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = input.tenant
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 5s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to the server process with host "window-crash-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":10}
      """
    And http payload is posted to the server process with host "window-crash-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":100}
      """
    And physical time passes for "3s"
    When the server process receives SIGKILL
    Then the server process is terminated by SIGKILL within "10s" of the last signal
    When the server process is restarted from its existing database
    And the server process eventually accepts http payload with host "window-crash-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":20}
      """
    And http payload is posted to the server process with host "window-crash-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":200}
      """
    And http payload is posted to the server process with host "window-crash-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":30}
      """
    And http payload is posted to the server process with host "window-crash-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":300}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":3,"postgres_action":"acme"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":3,"postgres_action":"beta"}
      """
    And the Postgres table eventually contains exactly 2 rows
    When the server process receives SIGTERM
    Then the server process exits with status 0

  Scenario: SIGKILL restores every open window of a window processor that restarts beside other processors
    Given Postgres is running
    And a nervix-server process is started with state snapshot interval "50ms"
    And Postgres table "process_open_windows_{{test_id}}" exists
    And the server process is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading (tenant STRING, value I64);
      CREATE SCHEMA reading_window (tenant STRING, samples I64, total I64);
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (tenant string, value integer);
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA reading_tenant (tenant STRING);
      CREATE BRANCH by_reading_tenant SCHEMA reading_tenant TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_reading_tenant;
      CREATE RELAY reading_windows SCHEMA reading_window BRANCHED BY by_reading_tenant;
      CREATE RELAY unique_readings SCHEMA reading BRANCHED BY by_reading_tenant;
      CREATE RELAY copied_readings SCHEMA reading BRANCHED BY by_reading_tenant;
      CREATE VHOST reading_edge readings-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON reading_edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_reading_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR aggregate_readings FROM readings
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_reading_tenant
        TO reading_windows
          SET tenant = FIRST(input.tenant), samples = COUNT(input.value), total = SUM(input.value)
          ON MESSAGE ERROR LOG;
      CREATE JUNCTION copy_reading FROM readings BRANCHED BY by_reading_tenant
        TO copied_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR unique_reading FROM readings
        DEDUPLICATE ON input.value
        MAX TIME 10m
        BRANCHED BY by_reading_tenant
        TO unique_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CLIENT reading_postgres TYPE POSTGRES
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{postgres_addr}}'
        };
      CREATE EMITTER copied_output FROM copied_readings
        TO POSTGRES reading_postgres INSERT TO TABLE process_open_windows_{{test_id}}
        VALUES {
          "postgres_user_id" = input.value,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = concat('copied:', input.tenant)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 5s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER unique_output FROM unique_readings
        TO POSTGRES reading_postgres INSERT TO TABLE process_open_windows_{{test_id}}
        VALUES {
          "postgres_user_id" = input.value,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = concat('unique:', input.tenant)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 5s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER window_output FROM reading_windows
        TO POSTGRES reading_postgres INSERT TO TABLE process_open_windows_{{test_id}}
        VALUES {
          "postgres_user_id" = input.total,
          "postgres_now" = NOW() AS STRING,
          "postgres_action" = concat('window:', input.tenant)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 5s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to the server process with host "readings-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","value":10}
      """
    And http payload is posted to the server process with host "readings-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":100}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":10,"postgres_action":"unique:alpha"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":100,"postgres_action":"unique:beta"}
      """
    When physical time passes for "3s"
    And the server process receives SIGKILL
    Then the server process is terminated by SIGKILL within "10s" of the last signal
    When the server process is restarted from its existing database
    And the server process eventually accepts http payload with host "readings-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","value":20}
      """
    And http payload is posted to the server process with host "readings-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":200}
      """
    And http payload is posted to the server process with host "readings-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","value":30}
      """
    And http payload is posted to the server process with host "readings-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":300}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":60,"postgres_action":"window:alpha"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":600,"postgres_action":"window:beta"}
      """
    When the server process receives SIGTERM
    Then the server process exits with status 0

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
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 5s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
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
