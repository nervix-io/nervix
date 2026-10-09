Feature: Large native restore metadata
  Restoring a complete state set publishes one durable generation before START.

  @restore_installation
  Scenario Outline: Native lifecycle and Kafka metadata above the bulk budget on <cluster_size> nodes restore exactly
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "10m"
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
    When round 1 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: <branches>
      """
    When round 2 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" the restore subscription receives one isolated even row for each of <branches> tenants
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "stateful.nvxb" is copied to "native.nvxb" with native metadata above the bulk budget
    Given the active domain is saved as placeholder "source_domain"
    And restoring domain "{{domain}}_durable_failure" fails after durable state publication
    When the CLI restores "domain {{domain}} --as {{domain}}_durable_failure" from "native.nvxb" on node "{{leader}}" reporting JSON with memory measurements
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
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "native.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 1 resource versions and 11 models
    And the CLI restore execution reference is saved as placeholder "native_restore_reference"
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least <restore_owners> distinct owners
    And the last cluster status placements of restored work are saved as placeholder "native_restore_placements"
    When restore "DOMAIN {{source_domain}} AS {{domain}}" of backup archive "native.nvxb" is streamed to node "{{leader}}" under execution reference "{{native_restore_reference}}"
    Then the restore stream's outcome is "completed" as "recovered"
    And the restore stream's report shows every step applied
    And the restore stream's complete report is saved as placeholder "native_restore_report"
    When the cluster is restarted
    Then the current leader node is saved as placeholder "leader"
    When restore "DOMAIN {{source_domain}} AS {{domain}}" of backup archive "native.nvxb" is streamed to node "{{leader}}" under execution reference "{{native_restore_reference}}"
    Then the restore stream's outcome is "completed" as "recovered"
    And the restore stream's complete report matches placeholder "native_restore_report"
    And stopped restored nodes preserve every native metadata value from backup archive "native.nvxb"
    When the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      START;
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
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status preserves restored work placements from placeholder "native_restore_placements"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    Then the relay subscription does not receive a payload within "2s"
    When round 3 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals <branches>
    When round 4 of Kafka messages for <branches> restore tenants is published to topic "backup_wasm_in_{{test_id}}"
    Then within "30s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: <branches>
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals <restored_inputs>
    And within "30s" DESCRIBE DOMAIN section "processed" metric "messages_total" "sent" relay "filtered_metrics" across physical nodes totals <branches>
    And within "30s" the restore subscription receives one isolated even row for each of <branches> tenants
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status preserves restored work placements from placeholder "native_restore_placements"

    Examples:
      | cluster_size | save_mib | branches | restored_inputs | restore_owners |
      | 1            | 1        | 2        | 4               | 1              |
      | 3            | 1        | 2        | 4               | 2              |

  Scenario Outline: Native lifecycle and Kafka metadata above the bulk budget back up within it and resume exactly on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has a state-counting WASM fixture with 1 MiB saves in resource directory "wasm_processor"
    And Kafka topic "backup_native_in_{{test_id}}" exists with 1 partitions
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
        FROM KAFKA kafka_ingress TOPIC backup_native_in_{{test_id}}
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
      START;
      """
    When round 1 of Kafka messages for 2 restore tenants is published to topic "backup_native_in_{{test_id}}"
    And round 2 of Kafka messages for 2 restore tenants is published to topic "backup_native_in_{{test_id}}"
    Then within "30s" DESCRIBE WASM PROCESSOR "filter_even_rows" on the leader node contains
      """
      state structures: 2
      """
    And within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals 4
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "stateful.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "stateful.nvxb" is copied to "native.nvxb" with native metadata above the bulk budget
    And the active domain is saved as placeholder "source_domain"
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{source_domain}} --as {{source_domain}}_stopped" from "native.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{source_domain}}_stopped" with 1 resource versions and 11 models
    And the CLI restore reports domain "{{source_domain}}_stopped" as "STOPPED" at start version 1
    Given the next native metadata capture of "metric_source" in domain "{{source_domain}}_stopped" is interrupted after 512 entries
    When the CLI backs up "domain {{source_domain}}_stopped --timeout 60s" from node "{{leader}}" into "native-stopped.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "was interrupted"
    And backup archive "native-stopped.nvxb" does not exist
    When the CLI backs up "domain {{source_domain}}_stopped --timeout 60s" from node "{{leader}}" into "native-stopped.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{source_domain}}_stopped"
    And the measured backup kept every node within its bulk budget without a bulk refusal
    And backup archives "native.nvxb" and "native-stopped.nvxb" preserve complete domain "{{source_domain}}" restored as stopped domain "{{source_domain}}_stopped"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "resumed"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{source_domain}}_stopped --as {{source_domain}}_resumed --resume" from "native-stopped.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore succeeded, restoring domain "{{source_domain}}_resumed" with 1 resource versions and 11 models
    And the CLI restore reports domain "{{source_domain}}_resumed" as "RUNNING" at start version 1
    Given the active domain is "{{source_domain}}_resumed"
    Then within "30s" DESCRIBE INGESTOR "metric_source" on the leader node contains
      """
      ready: true
      """
    When the CLI backs up "domain {{source_domain}}_resumed --timeout 120s" from node "{{leader}}" into "native-resumed.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{source_domain}}_resumed"
    And the measured backup kept every node within its bulk budget without a bulk refusal
    And backup archive "native-resumed.nvxb" holds every branch lifecycle and Kafka offset of backup archive "native.nvxb"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
