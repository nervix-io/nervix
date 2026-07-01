Feature: Deduplicator metrics

  Scenario Outline: DESCRIBE DEDUPLICATOR and Prometheus report processor traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA transaction (
        tenant STRING,
        transaction_id STRING
      );
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        tenant string,
        transaction_id string
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_dedup_metrics_source BY tenant_branch TTL 5m;
        CREATE RELAY raw_txns SCHEMA transaction BRANCHED BY by_dedup_metrics_source;
        CREATE RELAY deduped_txns SCHEMA transaction BRANCHED BY by_dedup_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT dedup_metrics_ingress ON edge PATH '/dedup-metrics' TYPE HTTP;
        CREATE INGESTOR dedup_metrics_source
        TO raw_txns
        DECODE USING transaction_codec
        BRANCHED BY by_dedup_metrics_source VALUES { tenant = raw_txns.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT dedup_metrics_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_metrics_node
        FROM raw_txns TO deduped_txns BRANCHED BY by_dedup_metrics_source
        DEDUPLICATE ON raw_txns.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        SUBSCRIBE SESSION TO deduped_txns;
        START;
      """
    And http payloads are posted concurrently to host "http-{{test_id}}.example.com" path "/dedup-metrics"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"acme","transaction_id":"txn-2"}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"acme","transaction_id":"txn-2"}
      """
    When these NSPL commands are executed
      """
      DESCRIBE DEDUPLICATOR dedup_metrics_node;
      """
    Then the last command output contains
      """
      deduplicator: dedup_metrics_node
      """
    And the last command output contains
      """
      messages_total received relay=raw_txns physical_node=node-1 total=3
      """
    And the last command output contains
      """
      messages_total sent relay=deduped_txns physical_node=node-1 total=2
      """
    And the last command output contains
      """
      batches_total received relay=raw_txns physical_node=node-1 total=1
      """
    And the last command output contains
      """
      batches_total sent relay=deduped_txns physical_node=node-1 total=1
      """
    And the last command output contains
      """
      delivery_latency_seconds received relay=raw_txns physical_node=node-1
      """
    And the last command output contains
      """
      p90_1m=
      """
    And the last command output metric "messages_total" "received" relay "raw_txns" physical node "node-1" has values
      """
      total=3
      """
    And the last command output metric "messages_total" "sent" relay "deduped_txns" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "received" relay "raw_txns" physical node "node-1" has values
      """
      total=1
      """
    And the last command output metric "batches_total" "sent" relay "deduped_txns" physical node "node-1" has values
      """
      total=1
      """
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target_kind="DEDUPLICATOR"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target="dedup_metrics_node"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains "nervix_delivery_latency_seconds_bucket"
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 3
      """
      target_kind="DEDUPLICATOR"
      target="dedup_metrics_node"
      direction="received"
      relay="raw_txns"
      """
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="DEDUPLICATOR"
      target="dedup_metrics_node"
      direction="sent"
      relay="deduped_txns"
      """
    And node "node-1" observability metric "nervix_delivery_latency_seconds_count" with labels eventually equals 3
      """
      target_kind="DEDUPLICATOR"
      target="dedup_metrics_node"
      direction="received"
      relay="raw_txns"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
