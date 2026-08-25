Feature: Junction metrics

  Scenario Outline: DESCRIBE JUNCTION and Prometheus report junction traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_junction_metrics SCHEMA tenant_branch TTL 5m;
      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_junction_metrics;
      CREATE RELAY routed SCHEMA notification BRANCHED BY by_junction_metrics;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT junction_metrics_ingress
      ON edge
      PATH '/junction-metrics'
      TYPE HTTP;
      CREATE INGESTOR junction_metrics_source
      FROM ENDPOINT junction_metrics_ingress MODE NO_ACK SEQUENTIAL
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
      TO incoming
        INHERIT ALL
        BRANCHED BY by_junction_metrics
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE JUNCTION junction_metrics_node
      FROM incoming
      BRANCHED BY by_junction_metrics
      TO routed
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION routed_subscription TO routed;
      START;
      """
    When http payloads are posted concurrently to host "http-{{test_id}}.example.com" path "/junction-metrics"
      """
      {"tenant":"acme","sequence":1}
      {"tenant":"acme","sequence":2}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "sequence":1
      "sequence":2
      """
    When these NSPL commands are executed
      """
      DESCRIBE JUNCTION junction_metrics_node;
      """
    Then the last command output contains
      """
      junction: junction_metrics_node
      """
    And the last command output owner is saved as placeholder "junction_metrics_owner"
    And the last command output contains
      """
      from: incoming
      """
    And the last command output contains
      """
      branch: by_junction_metrics
      """
    And the last command output contains
      """
      output 0: into=routed construction=present branch=NODE-WIDE flush=100ms max-batch-size=1MiB
      """
    And the last command output contains
      """
      messages_total received relay=incoming physical_node={{junction_metrics_owner}} total=2
      """
    And the last command output contains
      """
      messages_total sent relay=routed physical_node={{junction_metrics_owner}} total=2
      """
    And node "{{junction_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'target_kind="JUNCTION"'
    And node "{{junction_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'target="junction_metrics_node"'
    And node "{{junction_metrics_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="JUNCTION"
      target="junction_metrics_node"
      direction="received"
      relay="incoming"
      """
    And node "{{junction_metrics_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="JUNCTION"
      target="junction_metrics_node"
      direction="sent"
      relay="routed"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
