Feature: Domain metrics

  Scenario Outline: DESCRIBE DOMAIN reports input/output and processed traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
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
      CREATE RELAY domain_metrics_raw SCHEMA transaction PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY domain_metrics_deduped SCHEMA transaction PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT domain_metrics_ingress ON edge PATH '/domain-metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR domain_metrics_source
        TO domain_metrics_raw
        DECODE USING transaction_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = domain_metrics_raw.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT domain_metrics_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR domain_metrics_dedup
        FROM domain_metrics_raw TO domain_metrics_deduped PARAMETERIZED BY tenant_branch
        DEDUPLICATE ON domain_metrics_raw.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE EMITTER domain_metrics_sink
        FROM domain_metrics_deduped
        ENCODE USING transaction_codec
        TO ZEROMQ zeromq_main ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/domain-metrics"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    Then the observed broker receives a payload
      """
      "transaction_id":"txn-1"
      """
    When these NSPL commands are executed
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      domain: {{domain}}
      """
    And the last command output contains
      """
      status: running
      """
    And the last command output contains
      """
      input_output:
      """
    And the last command output contains
      """
      processed:
      """
    And the last command output metric "messages_total" "sent" relay "domain_metrics_raw" physical node "node-1" has values
      """
      total=1
      """
    And the last command output metric "messages_total" "received" relay "domain_metrics_deduped" physical node "node-1" has values
      """
      total=1
      """
    And the last command output metric "messages_total" "received" relay "domain_metrics_raw" physical node "node-1" has values
      """
      total=1
      """
    And the last command output contains
      """
      messages_total sent relay=domain_metrics_deduped physical_node=node-1 total=2
      """
    And the last command output contains
      """
      messages_per_batch sent relay=domain_metrics_raw physical_node=node-1
      """
    And the last command output contains
      """
      messages_per_batch received relay=domain_metrics_raw physical_node=node-1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
