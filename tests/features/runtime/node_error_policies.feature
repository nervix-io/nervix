Feature: Runtime node error policies
  Scenario Outline: Runtime nodes require their mandatory error policy blocks
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail
      """
      <statement>;
      """

    Examples:
      | node             | statement                                                                                                                                                                                                                                                                                                                                                                                            |
      | ingestor         | CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR http_notifications TO notifications DECODE USING notification_codec PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL |
      | reingestor       | CREATE REINGESTOR repartition FROM notifications TO tenant_notifications PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                         |
      | router           | CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" DEFAULT TO info_ss UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                                                 |
      | default router   | CREATE ROUTER pass_through FROM notifications DEFAULT TO forwarded_notifications UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                                                                                |
      | unifier          | CREATE UNIFIER join_streams FROM notifications_a, notifications_b TO notifications_all UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                                                                          |
      | deduplicator     | CREATE DEDUPLICATOR dedup_txns FROM inbound TO deduped UNPARAMETERIZED DEDUPLICATE ON inbound.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                                                       |
      | window processor | CREATE WINDOW PROCESSOR latency_window FROM metrics TO metric_summaries UNPARAMETERIZED WIDTH 10s DURATION STEP 5s DURATION AGGREGATE metric_summaries.total_latency = SUM(metrics.latency)                                                                                                                                                                                                          |
      | generator        | CREATE GENERATOR synth TO alerts EACH 100ms FLUSH EACH 100ms MAX BATCH SIZE 1MiB SET alerts.user_id = notifications.user_id                                                                                                                                                                                                                                                                          |
      | emitter          | CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                                                        |
      | inferencer       | CREATE INFERENCER score_model FROM features TO scored UNPARAMETERIZED USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" = features.vector } OUTPUTS { "score" = scored.score } FLUSH IMMEDIATE                                                                                                                                                                       |

  Scenario Outline: Pure runtime processors reject general error policies
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail
      """
      <statement>;
      """

    Examples:
      | node             | statement                                                                                                                                                                                                                                                                                                  |
      | reingestor       | CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE REINGESTOR repartition FROM notifications TO tenant_notifications PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | router           | CREATE ROUTER log_router FROM incoming_logs TO errors_ss WHERE incoming_logs.level = "error" DEFAULT TO info_ss UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                             |
      | default router   | CREATE ROUTER pass_through FROM notifications DEFAULT TO forwarded_notifications UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                                            |
      | unifier          | CREATE UNIFIER join_streams FROM notifications_a, notifications_b TO notifications_all UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                                      |
      | deduplicator     | CREATE DEDUPLICATOR dedup_txns FROM inbound TO deduped UNPARAMETERIZED DEDUPLICATE ON inbound.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                   |
      | window processor | CREATE WINDOW PROCESSOR latency_window FROM metrics TO metric_summaries UNPARAMETERIZED WIDTH 10s DURATION STEP 5s DURATION AGGREGATE metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                      |
      | generator        | CREATE GENERATOR synth TO alerts EACH 100ms FLUSH EACH 100ms MAX BATCH SIZE 1MiB SET alerts.user_id = notifications.user_id ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                                                      |
      | inferencer       | CREATE INFERENCER score_model FROM features TO scored UNPARAMETERIZED USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" = features.vector } OUTPUTS { "score" = scored.score } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                   |
      | reorderer        | CREATE REORDERER order_notifications FROM incoming_notifications TO ordered_notifications UNPARAMETERIZED BY incoming_notifications.sequence MAX TIME 10s FLUSH EACH 2s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                      |

  Scenario: General error policy rejects DLQ because it is not tied to a concrete message
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail
      """
      CREATE EMITTER kafka_emit
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR DLQ error_stream SET error_message = general_error.message FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      """

  Scenario Outline: Emitter message errors can be routed to a DLQ relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE SCHEMA error_record (
        error_message STRING,
        failed_node STRING,
        failed_record STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;
      CREATE RELAY error_stream SCHEMA error_record;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE EMITTER zeromq_notifications
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_main
        ON MESSAGE ERROR DLQ error_stream SET error_message = message_error.message, failed_node = message_error.node, failed_record = message_error.record
        ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO error_stream;
      START;
      """
    And emitter "zeromq_notifications" enters fault mode
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "error_message":"fault injector failed emitter 'zeromq_notifications'"
      """
    And the last relay subscription payload contains
      """
      "failed_node":"zeromq_notifications"
      """
    And the last relay subscription payload contains
      """
      "failed_record":"{\"user_id\":42}"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Attached emitter message errors can be ignored without replaying the source message
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}}
        ON MESSAGE ERROR IGNORE
        ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
