Feature: Generator node
  Scenario Outline: Generator emits records from materialized relay state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        amount I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications
        SCHEMA notification
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        UNPARAMETERIZED
        EACH 100ms
        FLUSH IMMEDIATE
        SET generated_notifications.user_id = notifications.user_id,
            generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO generated_notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":7}
      """
    Then the relay subscription receives a payload
      """
      {"amount":7,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Generator flushes buffered records on flush cadence
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        amount I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications
        SCHEMA notification
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        UNPARAMETERIZED
        EACH 100ms
        FLUSH EACH 1s MAX BATCH SIZE 1MiB
        SET generated_notifications.user_id = notifications.user_id,
            generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO generated_notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":7}
      """
    Then the relay subscription does not receive a payload within "500ms"
    And within "3s" the relay subscription receives a payload
      """
      {"amount":7,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Paced generators follow domain logical time
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        amount I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications
        SCHEMA notification
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        UNPARAMETERIZED
        EACH 2s
        FLUSH IMMEDIATE
        SET generated_notifications.user_id = notifications.user_id,
            generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO generated_notifications;
      START AT NOW TIME RATE 20.0;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":7}
      """
    Then within "500ms" the relay subscription receives a payload
      """
      {"amount":7,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
