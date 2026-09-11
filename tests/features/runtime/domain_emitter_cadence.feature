Feature: Domain-paced emitter cadence
  @domain_emitter_cadence
  Scenario Outline: Emitter cadence follows logical time while retry and ACK deadlines remain physical
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And Kafka topic "emitter_cadence_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge emitter-cadence-{{test_id}}.example.com;
      CREATE ENDPOINT emitter_cadence_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE INGESTOR emitter_cadence_source
        FROM ENDPOINT emitter_cadence_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE EMITTER kafka_cadence FROM notifications TO KAFKA kafka_main TOPIC emitter_cadence_out_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 1s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 20s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 20.0;
      """
    And http payload is posted to host "emitter-cadence-{{test_id}}.example.com" path "/events"
      """
      {"user_id":1,"action":"paced"}
      """
    Then within "4s" the observed broker receives payloads
      """
      {"user_id":1,"action":"paced"}
      """
    When emitter "kafka_cadence" enters stall mode
    And http payload is posted to host "emitter-cadence-{{test_id}}.example.com" path "/events"
      """
      {"user_id":2,"action":"retried"}
      """
    Then within "5s" DESCRIBE EMITTER "kafka_cadence" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the observed broker does not receive a payload within "500ms"
    When emitter "kafka_cadence" leaves stall mode
    Then within "4s" the observed broker receives payloads
      """
      {"user_id":2,"action":"retried"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_emitter_cadence
  Scenario Outline: Slow domain time holds an emitter flush until a force flush drains it
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And Kafka topic "emitter_hold_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge emitter-hold-{{test_id}}.example.com;
      CREATE ENDPOINT emitter_hold_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE INGESTOR emitter_hold_source
        FROM ENDPOINT emitter_hold_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE EMITTER kafka_hold FROM notifications TO KAFKA kafka_main TOPIC emitter_hold_out_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.01;
      """
    And http payload is posted to host "emitter-hold-{{test_id}}.example.com" path "/events"
      """
      {"user_id":7,"action":"held"}
      """
    Then the observed broker does not receive a payload within "2s"
    When these NSPL commands are executed
      """
      ALTER INGESTOR emitter_hold_source SET QUIESCE BUFFER MAX SIZE 2MiB;
      """
    Then within "10s" the observed broker receives payloads
      """
      {"user_id":7,"action":"held"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_emitter_cadence
  Scenario Outline: Emitter FLUSH IMMEDIATE keeps its physical minimum in a slow domain
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And Kafka topic "emitter_immediate_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge emitter-immediate-{{test_id}}.example.com;
      CREATE ENDPOINT emitter_immediate_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE INGESTOR emitter_immediate_source
        FROM ENDPOINT emitter_immediate_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE EMITTER kafka_immediate FROM notifications TO KAFKA kafka_main TOPIC emitter_immediate_out_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.01;
      """
    And http payload is posted to host "emitter-immediate-{{test_id}}.example.com" path "/events"
      """
      {"user_id":9,"action":"immediate"}
      """
    Then within "2s" the observed broker receives payloads
      """
      {"user_id":9,"action":"immediate"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_emitter_cadence
  Scenario Outline: Iceberg flush and commit boundaries follow domain logical time
    Given Iceberg dependencies are running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "cadence_notifications_{{test_id}}" exists at "s3://nervix-iceberg/tables/cadence_notifications_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-cadence-{{test_id}}.example.com;
      CREATE ENDPOINT iceberg_cadence_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE INGESTOR iceberg_cadence_source
        FROM ENDPOINT iceberg_cadence_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT s3_main
        TYPE S3
        CONFIG {
          'endpoint' = '{{rustfs_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true
        };
      CREATE CLIENT iceberg_catalog
        TYPE ICEBERG_REST
        CONFIG {
          'uri' = '{{iceberg_rest_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse'
        };
      CREATE EMITTER iceberg_cadence FROM notifications TO ICEBERG ON S3 s3_main TABLE cadence_notifications_{{test_id}} VALUES { 'user_id' = input.user_id, 'action' = input.action } LOCATION 's3://nervix-iceberg/tables/cadence_notifications_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 2s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH EACH 2s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 20.0;
      """
    And http payload is posted to host "iceberg-cadence-{{test_id}}.example.com" path "/events"
      """
      {"user_id":11,"action":"paced"}
      """
    Then within "2s" the Iceberg table "cadence_notifications_{{test_id}}" contains a row
      """
      {"user_id":11,"action":"paced"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_emitter_cadence
  Scenario Outline: A stalled emitter keeps its upstream acknowledgement alive on the physical clock
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And Kafka topic "ack_keepalive_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC ack_keepalive_in_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC ack_keepalive_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.01;
      """
    And emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "ack_keepalive_in_{{test_id}}"
      """
      {"user_id":77}
      """
    Then the observed broker does not receive a payload within "1200ms"
    When emitter "kafka_forward" leaves stall mode
    Then within "4s" the observed broker receives payloads
      """
      {"user_id":77}
      """
    And the observed broker does not receive a payload within "1200ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
