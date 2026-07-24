Feature: Sentry emission
  Scenario Outline: Sentry emitter publishes codec JSON as authenticated event envelopes
    Given Sentry is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA error_event (
        message STRING,
        level STRING,
        environment STRING
      );
      CREATE STRICT WIRE JSON SCHEMA error_event_wire (
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
      DECODE USING error_event_codec
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
      ENCODE USING error_event_codec
      TO SENTRY sentry_main
      INHERIT ALL
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "sentry-{{test_id}}.example.com" path "/errors"
      """
      {"message":"database unavailable","level":"error","environment":"{{test_id}}"}
      """
    Then Sentry eventually receives an event
      """
      {"message":"database unavailable","level":"error","environment":"{{test_id}}"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
