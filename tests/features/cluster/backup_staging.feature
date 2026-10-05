Feature: Restore staging reclamation and storage accounting
  Failed unpublished installations are reclaimed while durable activation gates stay closed.

  @restore_installation
  Scenario Outline: Restore quota failures reclaim storage across restart and preserve an active retry on <cluster_size> nodes
    Given restore checkpoint staging is limited to 524288 bytes
    And Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has a state-counting WASM fixture with 1 MiB saves in resource directory "wasm_processor"
    And Kafka topic "backup_wasm_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE SCHEMA result ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
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
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    When round 1 of Kafka messages for 2 restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: 2
      """
    When round 2 of Kafka messages for 2 restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" the restore subscription receives one isolated even row for each of 2 tenants
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the active domain is saved as placeholder "source_domain"
    When the CLI restores "domain {{domain}} --as {{domain}}_quota" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "restore checkpoint staging quota"
    And abandoned restore storage is reclaimed across every node
    Given the active domain is "{{source_domain}}_quota"
    When these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    When the cluster is restarted
    Then restore checkpoint staging usage is zero on every node
    When these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the active domain is "{{source_domain}}"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_quota_retry" from "stateful.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "restore checkpoint staging quota"
    And abandoned restore storage is reclaimed across every node
    Given restore checkpoint staging is limited to 8388608 bytes
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And restoring domain "{{domain}}_copy" by coordinator "{{leader}}" pauses before state publication
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "stateful.nvxb" is streamed to node "{{leader}}" under execution reference "{{test_id}}-active-restore" in the background
    Then restoring domain "{{domain}}_copy" by coordinator "{{leader}}" has reached state publication
    And an active restore retains its staging through completed maintenance sweeps on every node
    When state publication of domain "{{domain}}_copy" by coordinator "{{leader}}" is released
    Then the background restore stream's outcome is "completed" as "executed"
    And restore checkpoint staging usage is zero on every node
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "restored-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "stateful.nvxb" and "restored-state.nvxb" keep all 2 WASM processor branch incarnations

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
