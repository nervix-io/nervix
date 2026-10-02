Feature: Replica catch-up of branch-keyed state
  A replica keeps the branch lifecycle and the state of every branch of a branch-keyed entity
  current through catch-up rounds: one when the owner announces a checkpoint, and one every
  replication poll interval, which learns only what changed in the owner's catalog of the entity's
  branch checkpoints since the previous round. These scenarios lose every checkpoint announcement
  on every node, so each checkpoint a replica holds arrived through a round it ran on its own.

  Scenario: A WASM checkpoint reaches its replica through catch-up rounds when every announcement is lost
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And runtime state checkpoint announcements are lost
    And a 3 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_catch_up_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_catch_up_filter;
      UPLOAD RESOURCE wasm_catch_up_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_catch_up_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_catch_up_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_catch_up_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      kind=wasm_processor name=filter_even_rows owner=
      """
    And Kafka consumer group "wasm_catch_up_group_{{test_id}}" eventually has 1 consumers
    # Each checkpoint releases its source acknowledgement only once the replica holds it, and the
    # replica learns of it only from its own catch-up rounds.
    When Kafka message is published to topic "wasm_catch_up_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_catch_up_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    And Kafka message is published to topic "wasm_catch_up_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_catch_up_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    Then within "90s" Kafka consumer group "wasm_catch_up_group_{{test_id}}" next offset for topic "wasm_catch_up_in_{{test_id}}" partition 0 is "at least 4"
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      key={"tenant":"beta"} | "tenant":"beta" | "value":12
      """

  Scenario: A promoted replica suppresses the duplicates of every branch it caught up when every announcement was lost
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And runtime state checkpoint announcements are lost
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING, amount I64 );
      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string,
        amount integer
      );
      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge dedup-catch-up-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/dedup' TYPE HTTP;
      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      START;
      DESCRIBE DEDUPLICATOR dedup_txns;
      """
    Then the last command output owner is saved as placeholder "failed_owner"
    And the first replica in the last command output is saved as placeholder "promoted_replica"
    When these NSPL commands are executed on node "{{promoted_replica}}"
      """
      CREATE SUBSCRIPTION before_failover TO deduped;
      """
    And http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"acme","transaction_id":"txn-2","amount":30}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":40}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "transaction_id":"txn-1"
      key={"tenant":"beta"} | "transaction_id":"txn-1"
      key={"tenant":"acme"} | "transaction_id":"txn-2"
      key={"tenant":"beta"} | "transaction_id":"txn-2"
      """
    # The owner publishes the keys within 100 milliseconds, and each replica round, at least one a
    # second, catches up the branches that changed.
    When physical time passes for "3s"
    And node "{{failed_owner}}" is stopped
    Then node "{{promoted_replica}}" eventually observes a stable leader
    And within "60s" node "{{promoted_replica}}" eventually reports scheduled "deduplicator" "dedup_txns" owner equals placeholder "promoted_replica"
    When these NSPL commands are executed on node "{{promoted_replica}}"
      """
      CREATE SUBSCRIPTION after_failover TO deduped;
      """
    # Each branch's duplicate is posted before its new record, so a duplicate the promoted replica let
    # through would reach the subscription before that branch's new record does.
    Then node "{{promoted_replica}}" eventually accepts http traffic for host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"acme","transaction_id":"txn-2","amount":30}
      """
    When http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"acme","transaction_id":"txn-3","amount":50}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "dedup-catch-up-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"beta","transaction_id":"txn-3","amount":60}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "transaction_id":"txn-3"
      key={"tenant":"beta"} | "transaction_id":"txn-3"
      """
