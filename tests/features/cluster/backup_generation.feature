Feature: Bounded complete restore generations
  Restoring a complete state set publishes one durable generation before START.

  @restore_installation
  Scenario Outline: A complete restore generation with <branches> saves of <save_mib> MiB on <cluster_size> nodes preserves branches and source offsets
    Given Kafka is running
    And runtime replication is configured with replica count <replicas> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has a state-counting WASM fixture with <save_mib> MiB saves in resource directory "wasm_processor"
    And Kafka topic "backup_wasm_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE SCHEMA result ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY filtered_metrics SCHEMA result BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC backup_wasm_in_{{test_id}}
          OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 30s
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
        USING RESOURCE wasm_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET tenant = branch.tenant, note = coalesce(note, "even")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    When these NSPL commands are executed through the client on the leader node
      """
      START;
      """
    When round 1 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: <branches>
      """
    When round 2 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" the restore subscription receives one isolated even row for each of <branches> tenants
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "stateful.nvxb" is larger than 32 MiB
    Given the active domain is saved as placeholder "source_domain"
    And restoring domain "{{domain}}_durable_failure" fails after durable state publication
    When the CLI restores "domain {{domain}} --as {{domain}}_durable_failure" from "stateful.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "runtime state"
    Given the active domain is "{{source_domain}}_durable_failure"
    When these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    When the cluster is restarted
    And these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the active domain is "{{source_domain}}"
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And the cluster's reclaimed restore bytes are saved
    When the CLI restores "domain {{domain}} --as {{domain}}_copy --resume" from "stateful.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 1 resource versions and 11 models
    And the CLI restore reports domain "{{domain}}_copy" as "RUNNING" at start version 1
    Given the active domain is "{{domain}}_copy"
    When the cluster is restarted
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    Then within "30s" node "restored-1" eventually reports materialized state for relay "raw_metrics" containing
      """
      key={"tenant":"restore-tenant-0"} payload={"tenant":"restore-tenant-0","value":2}
      """
    When these NSPL commands are executed through the client on the leader node
      """
      STOP;
      START;
      """
    Then within "30s" node "restored-1" eventually reports materialized state for relay "raw_metrics" containing
      """
      relay 'raw_metrics' materialized state is empty
      """
    Then within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      kafka observed partitions: 0
      """
    And within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      kafka instance 0 partitions: 0
      """
    And within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      transient error: -
      """
    And within "10s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      ready: true
      """
    And the relay subscription does not receive a payload within "2s"
    When round 3 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals <branches>
    When round 4 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: <branches>
      """
    Then within "120s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals <restored_inputs>
    And within "30s" DESCRIBE DOMAIN section "processed" metric "messages_total" "sent" relay "filtered_metrics" across physical nodes totals <branches>
    And within "30s" the restore subscription receives one isolated even row for each of <branches> tenants
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "restored-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "stateful.nvxb" and "restored-state.nvxb" keep all <branches> WASM processor branch incarnations
    And at least <reclaimed_mib> MiB of replaced restore chunks are reclaimed
    Given the cluster's reclaimed restore bytes are saved
    When the CLI restores "domain {{source_domain}} --as {{source_domain}}_purged" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{source_domain}}_purged" with 1 resource versions and 11 models
    Given the active domain is "{{source_domain}}_purged"
    And node "restored-1" has a state-counting WASM fixture with <save_mib> MiB saves in resource directory "wasm_processor"
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    When these NSPL commands are executed through the client on the leader node
      """
      REBIND RESOURCE wasm_filter TO VERSION 2 FOR WASM PROCESSOR filter_even_rows;
      START;
      """
    Then at least <reclaimed_mib> MiB of replaced restore chunks are reclaimed

    @order_generation_two_saves_single
    Examples:
      | cluster_size | save_mib | branches | restored_inputs | replicas | reclaimed_mib |
      | 1            | 20       | 2        | 4               | 0        | 40            |

    @order_generation_two_saves_cluster
    Examples:
      | cluster_size | save_mib | branches | restored_inputs | replicas | reclaimed_mib |
      | 3            | 20       | 2        | 4               | 0        | 40            |

    @order_generation_many_saves_single
    Examples:
      | cluster_size | save_mib | branches | restored_inputs | replicas | reclaimed_mib |
      | 1            | 1        | 40       | 80              | 0        | 40            |

    @order_generation_many_saves_cluster
    Examples:
      | cluster_size | save_mib | branches | restored_inputs | replicas | reclaimed_mib |
      | 3            | 1        | 40       | 80              | 1        | 80            |
