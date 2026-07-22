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
      | node             | statement                                                                                                                                                                                                                                                                                  |
      | ingestor         | CREATE INGESTOR http_notifications FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL DECODE USING notification_codec TO notifications INHERIT ALL BRANCHED BY by_http_notifications SET user_id = message.user_id FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON GENERAL ERROR LOG |
      | reingestor       | CREATE REINGESTOR repartition FROM notifications TO tenant_notifications INHERIT ALL BRANCHED BY by_repartition SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                           |
      | junction         | CREATE JUNCTION join_streams FROM notifications_a, notifications_b UNBRANCHED TO notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                        |
      | deduplicator     | CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX TIME 10m UNBRANCHED TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                        |
      | window processor | CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 10s DURATION STEP 5s DURATION UNBRANCHED TO metric_summaries SET tenant = FIRST(input.tenant), total_latency = SUM(input.latency)                                                                                                |
      | generator        | CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms UNBRANCHED TO alerts SET user_id = relay_state.notifications.user_id FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                         |
      | emitter          | CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON GENERAL ERROR LOG                                                                                             |
      | inferencer       | CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE                  |

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
      | node             | statement                                                                                                                                                                                                                                                                                                           |
      | reingestor       | CREATE REINGESTOR repartition FROM notifications TO tenant_notifications INHERIT ALL BRANCHED BY by_repartition SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                          |
      | junction         | CREATE JUNCTION join_streams FROM notifications_a, notifications_b UNBRANCHED TO notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                                       |
      | deduplicator     | CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX TIME 10m UNBRANCHED TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                       |
      | window processor | CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 10s DURATION STEP 5s DURATION UNBRANCHED TO metric_summaries SET tenant = FIRST(input.tenant), total_latency = SUM(input.latency) ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                               |
      | generator        | CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms UNBRANCHED TO alerts SET user_id = relay_state.notifications.user_id FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                        |
      | inferencer       | CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | reorderer        | CREATE REORDERER order_notifications FROM incoming_notifications BY input.sequence MAX TIME 10s UNBRANCHED TO ordered_notifications INHERIT ALL FLUSH EACH 2s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                         |

  Scenario: General error policy rejects SEND TO because it is not tied to a concrete message
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
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR SEND TO error_stream SET error_message = error.message;
      """

  Scenario Outline: Emitter message errors can be sent to an error relay
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
        error_reference STRING,
        error_code STRING,
        error_message STRING,
        operation STRING,
        source_user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE RELAY error_stream SCHEMA error_record BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR SEND TO error_stream
        SET error_reference = error.reference,
            error_code = error.code,
            error_message = error.message,
            operation = error.operation,
            source_user_id = input.user_id
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION error_stream_subscription TO error_stream;
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
      "operation":"publish","source_user_id":42
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR IGNORE
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
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
