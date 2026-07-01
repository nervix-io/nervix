Feature: Window processor metrics

  Scenario Outline: DESCRIBE WINDOW PROCESSOR and Prometheus report window traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );
        CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64
      );
        CREATE STRICT WIRE JSON SCHEMA metric_wire (
        tenant string,
        latency integer
      );
        CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_window_metrics_source BY tenant_branch TTL 5m;
        CREATE RELAY metrics_input SCHEMA metric BRANCHED BY by_window_metrics_source;
        CREATE RELAY metrics_summary SCHEMA metric_summary BRANCHED BY by_window_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT window_metrics_ingress ON edge PATH '/window-metrics' TYPE HTTP;
        CREATE INGESTOR window_metrics_source
        TO metrics_input
        DECODE USING metric_codec
        BRANCHED BY by_window_metrics_source VALUES { tenant = metrics_input.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT window_metrics_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE WINDOW PROCESSOR window_metrics_node
        FROM metrics_input
        TO metrics_summary BRANCHED BY by_window_metrics_source
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        AGGREGATE
          metrics_summary.tenant = FIRST(metrics_input.tenant),
          metrics_summary.sample_count = COUNT(metrics_input.latency) ON MESSAGE ERROR LOG;
        SUBSCRIBE SESSION TO metrics_summary;
        START;
      """
    When http payloads are posted concurrently to host "http-{{test_id}}.example.com" path "/window-metrics"
      """
      {"tenant":"acme","latency":10}
      {"tenant":"acme","latency":20}
      {"tenant":"acme","latency":30}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "sample_count":3
      """
    When http payloads are posted concurrently to host "http-{{test_id}}.example.com" path "/window-metrics"
      """
      {"tenant":"acme","latency":40}
      {"tenant":"acme","latency":50}
      {"tenant":"acme","latency":60}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "sample_count":3
      """
    And the last relay subscription payload contains
      """
      "tenant":"acme"
      """
    When these NSPL commands are executed
      """
      DESCRIBE WINDOW PROCESSOR window_metrics_node;
      """
    Then the last command output contains
      """
      window processor: window_metrics_node
      """
    And the last command output contains
      """
      messages_total received relay=metrics_input physical_node=node-1 total=6
      """
    And the last command output contains
      """
      messages_total sent relay=metrics_summary physical_node=node-1 total=2
      """
    And the last command output contains
      """
      batches_total received relay=metrics_input physical_node=node-1 total=2
      """
    And the last command output contains
      """
      batches_total sent relay=metrics_summary physical_node=node-1 total=2
      """
    And the last command output contains
      """
      delivery_latency_seconds received relay=metrics_input physical_node=node-1
      """
    And the last command output metric "messages_total" "received" relay "metrics_input" physical node "node-1" has values
      """
      total=6
      """
    And the last command output metric "messages_total" "sent" relay "metrics_summary" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "received" relay "metrics_input" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "sent" relay "metrics_summary" physical node "node-1" has values
      """
      total=2
      """
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target_kind="WINDOW_PROCESSOR"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target="window_metrics_node"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains "nervix_batches_total"
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 6
      """
      target_kind="WINDOW_PROCESSOR"
      target="window_metrics_node"
      direction="received"
      relay="metrics_input"
      """
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="WINDOW_PROCESSOR"
      target="window_metrics_node"
      direction="sent"
      relay="metrics_summary"
      """
    And node "node-1" observability metric "nervix_delivery_latency_seconds_count" with labels eventually equals 6
      """
      target_kind="WINDOW_PROCESSOR"
      target="window_metrics_node"
      direction="received"
      relay="metrics_input"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
