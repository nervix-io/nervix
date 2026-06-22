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
        tenant STRING,
        score <score_type>
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        tenant string,
        level string
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE JSON WIRE SCHEMA features_wire (
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
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_branch WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY source_only SCHEMA notification;
      CREATE RELAY generated_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY tenant_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY errors_ss SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY info_ss SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY forwarded_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY notifications_a SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY notifications_b SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY notifications_all SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY inbound SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE RELAY deduped SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;
      CREATE RELAY features SCHEMA features PARAMETERIZED BY tenant_branch;
      CREATE RELAY scored SCHEMA scored PARAMETERIZED BY tenant_branch;

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

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR notification_source
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR notifications_a_source
        TO notifications_a
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications_a.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR notifications_b_source
        TO notifications_b
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications_b.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING ); CREATE INGESTOR transaction_source
        TO inbound
        DECODE USING transaction_codec
        PARAMETERIZED BY transaction_id_branch VALUES { transaction_id = inbound.transaction_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_source
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR feature_source
        TO features
        DECODE USING features_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = features.tenant } TTL 5m
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      <create_statement>;

      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "<node_kind>" "<node_name>" is saved as placeholder "failed_primary_node"
    And the first replica for scheduled "<node_kind>" "<node_name>" in the last cluster status is saved as placeholder "expected_promoted_replica"
    When node "{{failed_primary_node}}" is stopped
    Then node "{{expected_promoted_replica}}" eventually observes a stable leader
    And within "20s" node "{{expected_promoted_replica}}" eventually reports scheduled "<node_kind>" "<node_name>" owner equals placeholder "expected_promoted_replica"

    Examples:
      | node_kind        | node_name           | vector_type   | score_type    | create_statement                                                                                                                                                                                                                                                                                                                                                                                                            |
      | ingestor         | source_ingestor     | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR source_ingestor TO source_only DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | reingestor       | tenant_partition    | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE REINGESTOR tenant_partition FROM notifications TO tenant_notifications PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                                               |
      | router           | log_router          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE ROUTER log_router FROM notifications TO errors_ss WHERE notifications.level = "error" DEFAULT TO info_ss PARAMETERIZED BY tenant_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                                                                    |
      | forwarder        | pass_through        | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE FORWARDER pass_through FROM notifications TO forwarded_notifications PARAMETERIZED BY tenant_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                                                                                                        |
      | unifier          | join_streams        | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE UNIFIER join_streams FROM notifications_a, notifications_b TO notifications_all PARAMETERIZED BY user_id_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                                                                                            |
      | deduplicator     | dedup_txns          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE DEDUPLICATOR dedup_txns FROM inbound TO deduped PARAMETERIZED BY transaction_id_branch DEDUPLICATE ON inbound.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                                                                  |
      | window_processor | latency_window      | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE WINDOW PROCESSOR latency_window FROM metrics TO metric_summaries PARAMETERIZED BY tenant_branch WIDTH 10s DURATION STEP 5s DURATION AGGREGATE metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG                                                                                                                                                                                             |
      | generator        | synth_notifications | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE GENERATOR synth_notifications TO generated_notifications PARAMETERIZED BY tenant_branch EACH 100ms FLUSH IMMEDIATE SET generated_notifications.user_id = notifications.user_id, generated_notifications.tenant = notifications.tenant, generated_notifications.level = notifications.level ON MESSAGE ERROR LOG                                                                                                      |
      | inferencer       | score_model         | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INFERENCER score_model FROM features TO scored PARAMETERIZED BY tenant_branch USING RESOURCE fraud_model VERSION 1 FILE 'models/simple_score.onnx' INPUTS { "features" = features.vector } OUTPUTS { "score" = scored.score } FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                                                                                   |
      | emitter          | kafka_emit          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                                                                                     |

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
        tenant STRING,
        score <score_type>
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer,
        tenant string,
        level string
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE JSON WIRE SCHEMA features_wire (
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
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_branch WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY source_only SCHEMA notification;
      CREATE RELAY generated_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY tenant_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY errors_ss SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY info_ss SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY forwarded_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE RELAY notifications_a SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY notifications_b SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY notifications_all SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY inbound SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE RELAY deduped SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;
      CREATE RELAY features SCHEMA features PARAMETERIZED BY tenant_branch;
      CREATE RELAY scored SCHEMA scored PARAMETERIZED BY tenant_branch;

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

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR notification_source
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR notifications_a_source
        TO notifications_a
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications_a.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR notifications_b_source
        TO notifications_b
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications_b.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING ); CREATE INGESTOR transaction_source
        TO inbound
        DECODE USING transaction_codec
        PARAMETERIZED BY transaction_id_branch VALUES { transaction_id = inbound.transaction_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_source
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR feature_source
        TO features
        DECODE USING features_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = features.tenant } TTL 5m
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      <create_statement>;

      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "<node_kind>" "<node_name>" is saved as placeholder "failed_primary_node"
    And a node other than placeholder "failed_primary_node" is saved as placeholder "query_node"
    When node "{{failed_primary_node}}" is stopped
    Then node "{{query_node}}" eventually observes a stable leader
    And within "20s" node "{{query_node}}" eventually reports scheduled "<node_kind>" "<node_name>" owner different from placeholder "failed_primary_node"

    Examples:
      | node_kind        | node_name           | vector_type   | score_type    | create_statement                                                                                                                                                                                                                                                                                                                                               |
      | ingestor         | source_ingestor     | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INGESTOR source_ingestor TO source_only DECODE USING notification_codec UNPARAMETERIZED FLUSH EACH 100ms MAX BATCH SIZE 1MiB FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | reingestor       | tenant_partition    | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE REINGESTOR tenant_partition FROM notifications TO tenant_notifications PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                     |
      | router           | log_router          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE ROUTER log_router FROM notifications TO errors_ss WHERE notifications.level = "error" DEFAULT TO info_ss PARAMETERIZED BY tenant_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                       |
      | forwarder        | pass_through        | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE FORWARDER pass_through FROM notifications TO forwarded_notifications PARAMETERIZED BY tenant_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                                           |
      | unifier          | join_streams        | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE UNIFIER join_streams FROM notifications_a, notifications_b TO notifications_all PARAMETERIZED BY user_id_branch FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                                               |
      | deduplicator     | dedup_txns          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE DEDUPLICATOR dedup_txns FROM inbound TO deduped PARAMETERIZED BY transaction_id_branch DEDUPLICATE ON inbound.transaction_id MAX TIME 10m FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG                                                                                                                                                     |
      | window_processor | latency_window      | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE WINDOW PROCESSOR latency_window FROM metrics TO metric_summaries PARAMETERIZED BY tenant_branch WIDTH 10s DURATION STEP 5s DURATION AGGREGATE metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG                                                                                                                                |
      | generator        | synth_notifications | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE GENERATOR synth_notifications TO generated_notifications PARAMETERIZED BY tenant_branch EACH 100ms FLUSH IMMEDIATE SET generated_notifications.user_id = notifications.user_id, generated_notifications.tenant = notifications.tenant, generated_notifications.level = notifications.level ON MESSAGE ERROR LOG                                         |
      | inferencer       | score_model         | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE INFERENCER score_model FROM features TO scored PARAMETERIZED BY tenant_branch USING RESOURCE fraud_model VERSION 1 FILE 'models/simple_score.onnx' INPUTS { "features" = features.vector } OUTPUTS { "score" = scored.score } FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                      |
      | emitter          | kafka_emit          | ARRAY<F32, 2> | ARRAY<F32, 1> | CREATE EMITTER kafka_emit FROM notifications ENCODE USING notification_codec TO KAFKA kafka_main TOPIC notifications_out ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB                                                                                                                                                        |
