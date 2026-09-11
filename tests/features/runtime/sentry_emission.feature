Feature: Sentry emission
  @domain_execution_time
  Scenario Outline: Sentry emitter publishes codec JSON as authenticated event envelopes
    Given Sentry is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed
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
      CREATE INGESTOR error_source
      FROM ENDPOINT error_ingress MODE NO_ACK SEQUENTIAL
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING error_event_codec
      TIMESTAMP NOW
      TO errors
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT sentry_main
      TYPE SENTRY
      CONFIG {
        'dsn' = '{{sentry_dsn}}',
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

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
