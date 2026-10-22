Feature: Sentry emission
  @domain_execution_time
  Scenario Outline: Sentry emitter publishes codec JSON as authenticated event envelopes
    Given Sentry is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Sentry endpoint is published under fixture DNS
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA error_event (
        message STRING,
        level STRING,
        environment STRING
      );
      CREATE WIRE JSON SCHEMA error_event_wire MODE STRICT (
        message string,
        level string,
        environment string
      );
      CREATE CODEC error_event_codec
      FROM WIRE JSON SCHEMA error_event_wire
      TO SCHEMA error_event;
      CREATE RELAY errors SCHEMA error_event UNBRANCHED;
      CREATE VHOST edge sentry-{{test_id}}.example.com;
      CREATE ENDPOINT error_ingress
      ON edge
      PATH '/errors'
      TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR error_source
      FROM error_ingress MODE NO ACK
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING error_event_codec
      TIMESTAMP NOW
      TO errors
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SENTRY CLIENT sentry_main
      CONFIG {
        'dsn' = '{{sentry_dns_dsn}}',
        'timeout_ms' = 5000
      };
      CREATE EMITTER sentry_errors
      FROM errors
      TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING error_event_codec
      INHERIT ALL
      SET environment = CASE
        WHEN now() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN input.environment
        ELSE 'physical-time'
      END
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      {"message":"database unavailable","level":"error","environment":"{{test_id}}"}
      """
    Then Sentry eventually receives an event
      """
      {"message":"database unavailable","level":"error","environment":"{{test_id}}"}
      """
    And the Sentry event timestamp is before "2001-01-01T00:00:00Z"
    And the DNS fixture eventually receives a question for "sentry.nervix.test"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @sentry_retry_envelope
  Scenario Outline: Sentry retries send the same prepared envelope and distinct payloads get distinct ids
    Given HTTP receiver "sentry" is running
    And HTTP receiver "sentry" answers with
      """
      respond 503
      respond 200
      """
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA retry_event (message STRING, environment STRING);
      CREATE CODEC retry_ingest FROM JSON TO SCHEMA retry_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC retry_codec FROM JSON TO SCHEMA retry_event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.' ON EMITTING BATCH '.[0]';
      CREATE RELAY errors SCHEMA retry_event UNBRANCHED;
      CREATE VHOST edge sentry-{{test_id}}.example.com;
      CREATE ENDPOINT error_ingress ON edge PATH '/errors' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR error_source
        FROM error_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING retry_ingest
        TO errors INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SENTRY CLIENT sentry_main CONFIG {
        'dsn' = 'http://public@127.0.0.1:{{http_receiver_port.sentry}}/1',
        'timeout_ms' = 5000
      };
      CREATE EMITTER sentry_errors FROM errors
        TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING retry_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      [{"message":"retry me","environment":"{{test_id}}"}]
      """
    Then HTTP receiver "sentry" eventually receives at least 2 requests
    And HTTP receiver "sentry" request 2 repeats the body of request 1
    And Sentry receiver "sentry" recorded 1 distinct event across 2 attempts
      """
      {"message":"retry me","environment":"{{test_id}}","platform":"other"}
      """
    When http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      [{"message":"retry me","environment":"{{test_id}}"}]
      """
    Then HTTP receiver "sentry" eventually receives at least 3 requests
    And Sentry receiver "sentry" recorded 2 distinct events across 3 attempts
      """
      {"message":"retry me","environment":"{{test_id}}","platform":"other"}
      """

    Examples:
      | cluster_size | batch                                |
      | 1            |                                      |
      | 3            |                                      |
      | 1            | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |

  @sentry_retry_envelope
  Scenario Outline: Sentry retries preserve the supplied event id spelling
    Given HTTP receiver "sentry" is running
    And HTTP receiver "sentry" answers with
      """
      respond 503
      respond 200
      """
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA retry_event (message STRING, environment STRING, event_id STRING, timestamp STRING, platform STRING);
      CREATE CODEC retry_ingest FROM JSON TO SCHEMA retry_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC retry_codec FROM JSON TO SCHEMA retry_event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.' ON EMITTING BATCH '.[0]';
      CREATE RELAY errors SCHEMA retry_event UNBRANCHED;
      CREATE VHOST edge sentry-{{test_id}}.example.com;
      CREATE ENDPOINT error_ingress ON edge PATH '/errors' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR error_source
        FROM error_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING retry_ingest
        TO errors INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SENTRY CLIENT sentry_main CONFIG {
        'dsn' = 'http://public@127.0.0.1:{{http_receiver_port.sentry}}/1',
        'timeout_ms' = 5000
      };
      CREATE EMITTER sentry_errors FROM errors
        TO SENTRY sentry_main MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING retry_codec INHERIT ALL

        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      [{"message":"supplied id","environment":"{{test_id}}","event_id":"<event_id>","timestamp":"2000-01-01T00:00:00Z","platform":"rust"}]
      """
    Then HTTP receiver "sentry" eventually receives at least 2 requests
    And HTTP receiver "sentry" request 2 repeats the body of request 1
    And Sentry receiver "sentry" recorded 1 distinct event across 2 attempts
      """
      {"message":"supplied id","environment":"{{test_id}}","event_id":"<event_id>","timestamp":"2000-01-01T00:00:00Z","platform":"rust"}
      """

    Examples:
      | cluster_size | event_id                             |
      | 1            | 12345678123456781234567812345678     |
      | 3            | 12345678123456781234567812345678     |
      | 1            | 12345678-1234-5678-1234-567812345678 |
      | 3            | 12345678-1234-5678-1234-567812345678 |
