Feature: Cluster leader failover

  Scenario: Cluster elects a new leader when the current leader stops
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually reports leader "node-1"
    And node "node-2" eventually reports leader "node-1"
    And node "node-3" eventually reports leader "node-1"
    And node "node-2" eventually reports raft voters "node-1,node-2,node-3"
    When node "node-1" is stopped
    Then node "node-2" eventually reports a leader other than "node-1"
    And node "node-3" eventually reports a leader other than "node-1"
    And node "node-2" eventually reports raft voters "node-1,node-2,node-3"

  Scenario Outline: Dead scheduled node primary failover promotes a live replica
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE fraud_model;
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_model}}';
      CREATE SCHEMA notification (
        user_id I64,
        tenant STRING,
        level STRING
      );
      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I64
      );
      CREATE SCHEMA metric (
        tenant STRING,
        latency U64
      );
      CREATE SCHEMA metric_summary (
        tenant STRING,
        total_latency U64
      );
      CREATE SCHEMA features (
        tenant STRING,
        vector <vector_type>
      );
      CREATE SCHEMA scored (
        score <score_type>
      );
      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        tenant string,
        level string
      );
      CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );
      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        tenant string,
        latency integer
      );
      CREATE STRICT WIRE JSON SCHEMA features_wire (
        tenant string,
        vector array
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;
      CREATE CODEC features_codec
        FROM WIRE JSON SCHEMA features_wire
        TO SCHEMA features;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_tenant_partition SCHEMA tenant_branch TTL 5m;
      CREATE IF NOT EXISTS BRANCH by_notification_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_notification_source WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY source_only SCHEMA notification UNBRANCHED;
      CREATE RELAY generated_notifications SCHEMA notification BRANCHED BY by_notification_source;
      CREATE RELAY tenant_notifications SCHEMA notification BRANCHED BY by_tenant_partition;
      CREATE RELAY errors_ss SCHEMA notification BRANCHED BY by_notification_source;
      CREATE RELAY info_ss SCHEMA notification BRANCHED BY by_notification_source;
      CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_notification_source;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS BRANCH by_notifications_a_source SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications_a SCHEMA notification BRANCHED BY by_notifications_a_source;
      CREATE RELAY notifications_b SCHEMA notification BRANCHED BY by_notifications_a_source;
      CREATE RELAY notifications_all SCHEMA notification BRANCHED BY by_notifications_a_source;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE IF NOT EXISTS BRANCH by_transaction_source SCHEMA transaction_id_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_transaction_source;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_transaction_source;
      CREATE IF NOT EXISTS BRANCH by_metric_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_metric_source;
      CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_metric_source;
      CREATE IF NOT EXISTS BRANCH by_feature_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_feature_source;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_feature_source;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
      CREATE INGESTOR notification_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_notification_source
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR notifications_a_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications_a
        INHERIT ALL
        BRANCHED BY by_notifications_a_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR notifications_b_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications_b
        INHERIT ALL
        BRANCHED BY by_notifications_a_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR transaction_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_transaction_source
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR metric_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING metric_codec
        TO metrics
        INHERIT ALL
        BRANCHED BY by_metric_source
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR feature_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING features_codec
        TO features
        INHERIT ALL
        BRANCHED BY by_feature_source
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <create_statement>;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "<node_kind>" "<node_name>" is saved as placeholder "failed_primary_node"
    And the first replica for scheduled "<node_kind>" "<node_name>" in the last cluster status is saved as placeholder "expected_promoted_replica"
    When node "{{failed_primary_node}}" is stopped
    Then node "{{expected_promoted_replica}}" eventually observes a stable leader
    And within "20s" node "{{expected_promoted_replica}}" eventually reports scheduled "<node_kind>" "<node_name>" owner equals placeholder "expected_promoted_replica"

    Examples:
      | node_kind        | node_name           | vector_type   | score_type    | create_statement                                                                                                                                                                                                                                                                                                                                                      |
      | ingestor         | source_ingestor     | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INGESTOR source_ingestor FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s DECODE USING notification_codec TO source_only INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | reingestor       | tenant_partition    | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE REINGESTOR tenant_partition FROM notifications TO tenant_notifications INHERIT ALL BRANCHED BY by_tenant_partition SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                       |
      | junction         | join_streams        | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE JUNCTION join_streams FROM notifications_a, notifications_b BRANCHED BY by_notifications_a_source TO notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                   |
      | deduplicator     | dedup_txns          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX TIME 10m BRANCHED BY by_transaction_source TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                       |
      | window_processor | latency_window      | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 10s DURATION STEP 5s DURATION BRANCHED BY by_metric_source TO metric_summaries SET tenant = FIRST(input.tenant), total_latency = SUM(input.latency) ON MESSAGE ERROR LOG                                                                                                                                    |
      | generator        | synth_notifications | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE GENERATOR synth_notifications USING MATERIALIZED STATE notifications EACH 100ms BRANCHED BY by_notification_source TO generated_notifications SET user_id = relay_state.notifications.user_id, tenant = relay_state.notifications.tenant, level = relay_state.notifications.level FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                         |
      | inferencer       | score_model         | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 1 FILE 'models/simple_score.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } BRANCHED BY by_feature_source TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                              |
      | emitter          | kafka_emit          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                                                                   |

  Scenario Outline: Dead scheduled node primary failover falls back to another live node without a replica
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE fraud_model;
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_model}}';
      CREATE SCHEMA notification (
        user_id I64,
        tenant STRING,
        level STRING
      );
      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I64
      );
      CREATE SCHEMA metric (
        tenant STRING,
        latency U64
      );
      CREATE SCHEMA metric_summary (
        tenant STRING,
        total_latency U64
      );
      CREATE SCHEMA features (
        tenant STRING,
        vector <vector_type>
      );
      CREATE SCHEMA scored (
        score <score_type>
      );
      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        tenant string,
        level string
      );
      CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );
      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        tenant string,
        latency integer
      );
      CREATE STRICT WIRE JSON SCHEMA features_wire (
        tenant string,
        vector array
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;
      CREATE CODEC features_codec
        FROM WIRE JSON SCHEMA features_wire
        TO SCHEMA features;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_tenant_partition SCHEMA tenant_branch TTL 5m;
      CREATE IF NOT EXISTS BRANCH by_notification_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_notification_source WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY source_only SCHEMA notification UNBRANCHED;
      CREATE RELAY generated_notifications SCHEMA notification BRANCHED BY by_notification_source;
      CREATE RELAY tenant_notifications SCHEMA notification BRANCHED BY by_tenant_partition;
      CREATE RELAY errors_ss SCHEMA notification BRANCHED BY by_notification_source;
      CREATE RELAY info_ss SCHEMA notification BRANCHED BY by_notification_source;
      CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_notification_source;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE IF NOT EXISTS BRANCH by_notifications_a_source SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications_a SCHEMA notification BRANCHED BY by_notifications_a_source;
      CREATE RELAY notifications_b SCHEMA notification BRANCHED BY by_notifications_a_source;
      CREATE RELAY notifications_all SCHEMA notification BRANCHED BY by_notifications_a_source;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE IF NOT EXISTS BRANCH by_transaction_source SCHEMA transaction_id_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_transaction_source;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_transaction_source;
      CREATE IF NOT EXISTS BRANCH by_metric_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_metric_source;
      CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_metric_source;
      CREATE IF NOT EXISTS BRANCH by_feature_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_feature_source;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_feature_source;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;
      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
      CREATE INGESTOR notification_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_notification_source
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR notifications_a_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications_a
        INHERIT ALL
        BRANCHED BY by_notifications_a_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR notifications_b_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications_b
        INHERIT ALL
        BRANCHED BY by_notifications_a_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR transaction_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_transaction_source
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR metric_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING metric_codec
        TO metrics
        INHERIT ALL
        BRANCHED BY by_metric_source
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR feature_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING features_codec
        TO features
        INHERIT ALL
        BRANCHED BY by_feature_source
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <create_statement>;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "<node_kind>" "<node_name>" is saved as placeholder "failed_primary_node"
    And a node other than placeholder "failed_primary_node" is saved as placeholder "query_node"
    When node "{{failed_primary_node}}" is stopped
    Then node "{{query_node}}" eventually observes a stable leader
    And within "20s" node "{{query_node}}" eventually reports scheduled "<node_kind>" "<node_name>" owner different from placeholder "failed_primary_node"

    Examples:
      | node_kind        | node_name           | vector_type   | score_type    | create_statement                                                                                                                                                                                                                                                                                                                                                      |
      | ingestor         | source_ingestor     | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INGESTOR source_ingestor FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s DECODE USING notification_codec TO source_only INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | reingestor       | tenant_partition    | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE REINGESTOR tenant_partition FROM notifications TO tenant_notifications INHERIT ALL BRANCHED BY by_tenant_partition SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                       |
      | junction         | join_streams        | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE JUNCTION join_streams FROM notifications_a, notifications_b BRANCHED BY by_notifications_a_source TO notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                   |
      | deduplicator     | dedup_txns          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX TIME 10m BRANCHED BY by_transaction_source TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                       |
      | window_processor | latency_window      | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 10s DURATION STEP 5s DURATION BRANCHED BY by_metric_source TO metric_summaries SET tenant = FIRST(input.tenant), total_latency = SUM(input.latency) ON MESSAGE ERROR LOG                                                                                                                                    |
      | generator        | synth_notifications | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE GENERATOR synth_notifications USING MATERIALIZED STATE notifications EACH 100ms BRANCHED BY by_notification_source TO generated_notifications SET user_id = relay_state.notifications.user_id, tenant = relay_state.notifications.tenant, level = relay_state.notifications.level FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                         |
      | inferencer       | score_model         | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 1 FILE 'models/simple_score.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } BRANCHED BY by_feature_source TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                              |
      | emitter          | kafka_emit          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG                                                                                                                                                   |
