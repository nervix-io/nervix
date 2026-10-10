Feature: Guest saves above the bulk budget
  A public backup stages complete guest saves within every owner's bulk budget.

  @restore_installation @deloxide_stress_restore
  Scenario Outline: Guest saves above the bulk budget stream, retry and resume on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has a state-counting WASM fixture with 40 MiB saves in resource directory "wasm_processor"
    And Kafka topic "backup_guest_in_{{test_id}}" exists with 1 partitions
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
        FROM KAFKA kafka_ingress TOPIC backup_guest_in_{{test_id}}
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
    When round 1 of Kafka messages for 2 restore tenants is published to topic "backup_guest_in_{{test_id}}"
    Then within "120s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: 2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least <owners> distinct owners
    When round 2 of Kafka messages for 2 restore tenants is published to topic "backup_guest_in_{{test_id}}"
    Then within "120s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals 4
    And within "120s" the restore subscription receives one isolated even row for each of 2 tenants
    When round 3 of Kafka messages for 2 restore tenants is published to topic "backup_guest_in_{{test_id}}"
    Then within "120s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals 6
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "guest-first.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And the measured backup kept every node within its bulk budget without a bulk refusal
    Given the next guest save capture of "filter_even_rows" in domain "{{domain}}" is interrupted after 2 chunks
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "guest-interrupted.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "was interrupted"
    And backup archive "guest-interrupted.nvxb" does not exist
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "guest-retry.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And the measured backup kept every node within its bulk budget without a bulk refusal
    And backup archives "guest-first.nvxb" and "guest-retry.nvxb" have identical guest saves above 32 MiB
    Given the active domain is saved as placeholder "source_domain"
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{source_domain}} --as {{domain}}_copy --resume" from "guest-retry.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 1 resource versions and 11 models
    And the CLI restore reports domain "{{domain}}_copy" as "RUNNING" at start version 1
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "guest-restored.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And the measured backup kept every node within its bulk budget without a bulk refusal
    And backup archives "guest-retry.nvxb" and "guest-restored.nvxb" have identical guest saves above 32 MiB
    When round 4 of Kafka messages for 2 restore tenants is published to topic "backup_guest_in_{{test_id}}"
    Then within "120s" the restore subscription receives one isolated even row for each of 2 tenants

    Examples:
      | cluster_size | owners |
      | 1            | 1      |
      | 3            | 2      |
