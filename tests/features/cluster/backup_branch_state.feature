Feature: Deduplicator and window state in backup archives

  Background:
    Given these NSPL commands are saved as placeholder "branch_state_models"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA payment ( tenant STRING, transaction_id STRING, amount I64 );
      CREATE WIRE JSON SCHEMA payment_wire MODE STRICT ( tenant string, transaction_id string, amount integer );
      CREATE CODEC payment_codec FROM WIRE JSON SCHEMA payment_wire TO SCHEMA payment;
      CREATE SCHEMA metric ( tenant STRING, latency I64 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( tenant string, latency integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA metric_summary (
        tenant STRING,
        samples I64,
        total I64 OPTIONAL,
        first_latency I64 OPTIONAL,
        last_latency I64 OPTIONAL,
        smallest I64 OPTIONAL,
        largest I64 OPTIONAL,
        latency_p0 F64 OPTIONAL
      );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY payments SCHEMA payment BRANCHED BY by_tenant;
      CREATE RELAY unique_payments SCHEMA payment BRANCHED BY by_tenant;
      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant;
      CREATE VHOST edge backup-branch-state-{{test_id}}.example.com;
      CREATE ENDPOINT payment_ingress ON edge PATH '/payments' TYPE HTTP;
      CREATE ENDPOINT metric_ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR payment_source FROM ENDPOINT payment_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING payment_codec
        TO payments INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR metric_source FROM ENDPOINT metric_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR unique_payment_filter FROM payments
        DEDUPLICATE ON input.transaction_id, input.amount
        MAX TIME 3m
        BRANCHED BY by_tenant
        TO unique_payments INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR latency_window FROM metrics
        WIDTH 2 MESSAGES
        STEP 1 MESSAGES
        BRANCHED BY by_tenant
        TO metric_summaries
          SET tenant = FIRST(input.tenant), samples = COUNT(input.latency), total = SUM(input.latency),
              first_latency = FIRST(input.latency), last_latency = LAST(input.latency),
              smallest = MIN(input.latency), largest = MAX(input.latency),
              latency_p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '10m')
          ON MESSAGE ERROR LOG;
      START;
      """

  @restore_installation @deloxide_stress
  Scenario Outline: Deduplicator keys and half-filled windows captured at a quiesced cut continue per branch after RESUME
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_state_models}}
      CREATE SUBSCRIPTION source_payments TO unique_payments;
      """
    And the current time is saved as timestamp placeholder "keys_seen"
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-3","amount":30}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      key={"tenant":"beta"} | "transaction_id":"txn-2"
      key={"tenant":"alpha"} | "transaction_id":"txn-3"
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION source_summaries TO metric_summaries;
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":70}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":80}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "samples":2 | "total":80 | "first_latency":10 | "last_latency":70 | "latency_p0":15.0
      key={"tenant":"beta"} | "samples":2 | "total":100 | "first_latency":20 | "last_latency":80 | "latency_p0":25.0
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "branch-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "branch-state.nvxb" as json
    Then the described backup has exactly 2 "deduplicator" state sections
    And the described backup has exactly 2 "window_processor" state sections
    Given at least "40s" have passed since timestamp placeholder "keys_seen"
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "branch-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 20 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And the CLI restore reports no warnings
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "branch-state-resumed.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "branch-state.nvxb" and "branch-state-resumed.nvxb" have identical deduplicator state
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_summaries TO metric_summaries;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":30}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":40}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "samples":2 | "total":100 | "first_latency":70 | "last_latency":30 | "smallest":30 | "largest":70 | "latency_p0":15.0
      key={"tenant":"beta"} | "samples":2 | "total":120 | "first_latency":80 | "last_latency":40 | "smallest":40 | "largest":80 | "latency_p0":25.0
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_payments TO unique_payments;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-4","amount":40}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-5","amount":50}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "transaction_id":"txn-1"
      key={"tenant":"alpha"} | "transaction_id":"txn-2"
      key={"tenant":"alpha"} | "transaction_id":"txn-4"
      key={"tenant":"beta"} | "transaction_id":"txn-5"
      """
    And within "5m" repeatedly posting http payload to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments" yields a relay subscription payload saved as timestamp placeholder "key_expired"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And timestamp placeholder "key_expired" is at least "3m" after timestamp placeholder "keys_seen"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @restore_installation
  Scenario Outline: A stopped restore re-exports identical deduplicator and window sections, an interrupted installation keeps START gated, and WITHOUT STATE installs neither
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_state_models}}
      CREATE SUBSCRIPTION source_summaries TO metric_summaries;
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":70}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":80}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "total":80 | "latency_p0":15.0
      key={"tenant":"beta"} | "total":100 | "latency_p0":25.0
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "branch-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And restoring domain "{{domain}}" fails before installing its first deduplicator or window checkpoint
    When the CLI restores "domain {{domain}} --resume" from "branch-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "runtime state"
    When these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}}" from "branch-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 20 models
    And the CLI restore reports domain "{{domain}}" as "STOPPED" at start version 1
    And the CLI restore reports no warnings
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "branch-state-stopped.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "branch-state-stopped.nvxb" as json
    Then the described backup has exactly 2 "deduplicator" state sections
    And the described backup has exactly 2 "window_processor" state sections
    And backup archives "branch-state.nvxb" and "branch-state-stopped.nvxb" preserve complete domain "{{domain}}" restored as stopped domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume --without-state" from "branch-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 20 models
    And the CLI restore reports no warnings
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_payments TO unique_payments;
      CREATE SUBSCRIPTION restored_summaries TO metric_summaries;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":30}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":50}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      key={"tenant":"alpha"} | "samples":2 | "total":80 | "first_latency":30 | "last_latency":50 | "latency_p0":35.0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @restore_installation @deloxide_stress
  Scenario: A delayed coordinator cannot replace resumed deduplicator keys and windows
    Given runtime replication is configured with replica count 1 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_state_models}}
      CREATE SUBSCRIPTION source_summaries TO metric_summaries;
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":70}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":80}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "total":80 | "latency_p0":15.0
      key={"tenant":"beta"} | "total":100 | "latency_p0":25.0
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "branch-state-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh 3 node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Then a node other than placeholder "leader" is saved as placeholder "survivor"
    Given restoring domain "{{domain}}" by coordinator "{{leader}}" pauses before state publication
    When restore "DOMAIN {{domain}} RESUME" of backup archive "branch-state-stale.nvxb" is streamed to node "{{leader}}" under execution reference "branch-state-stale-restore" in the background
    Then restoring domain "{{domain}}" by coordinator "{{leader}}" has reached state publication
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    And restore "DOMAIN {{domain}} RESUME" of backup archive "branch-state-stale.nvxb" completes on node "{{survivor}}" under execution reference "branch-state-stale-restore"
    And within "60s" node "{{survivor}}" accepts http payload for host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION resumed_payments TO unique_payments;
      CREATE SUBSCRIPTION resumed_summaries TO metric_summaries;
      """
    And http payload is posted to node "{{survivor}}" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "{{survivor}}" with host "backup-branch-state-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-3","amount":30}
      """
    And http payload is posted to node "{{survivor}}" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":30}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-3"
      key={"tenant":"alpha"} | "samples":2 | "total":100 | "latency_p0":15.0
      """
    When the CLI backs up "domain {{domain}}" from node "{{survivor}}" into "branch-state-before-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When state publication of domain "{{domain}}" by coordinator "{{leader}}" is released
    Then state publication of domain "{{domain}}" by coordinator "{{leader}}" is refused
    When the CLI backs up "domain {{domain}}" from node "{{survivor}}" into "branch-state-after-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "branch-state-before-stale.nvxb" and "branch-state-after-stale.nvxb" have identical deduplicator and window state

  @restore_installation
  Scenario Outline: A changed window model or branch incarnation starts an empty window with a restore warning
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_state_models}}
      CREATE SUBSCRIPTION source_summaries TO metric_summaries;
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":10}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":20}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":70}
      """
    And http payload is posted to host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":80}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "total":80 | "latency_p0":15.0
      key={"tenant":"beta"} | "total":100 | "latency_p0":25.0
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "branch-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "branch-state.nvxb" is copied to "changed-window.nvxb" with "'10m'" replaced by "'11m'" in the models of domain "{{domain}}"
    And backup archive "branch-state.nvxb" is copied to "advanced-incarnation.nvxb" with the archived branch incarnations of window processor "latency_window" advanced
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "changed-window.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 20 models
    And the CLI restore warns "skipped window_processor state 'latency_window'"
    And the CLI restore warns "the archived window model differs from the restored window processor"
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION changed_summaries TO metric_summaries;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":30}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":50}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "samples":2 | "total":80 | "first_latency":30 | "last_latency":50
      """
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "advanced-incarnation.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 20 models
    And the CLI restore warns "skipped window_processor state 'latency_window'"
    And the CLI restore warns "the archived window branch incarnation differs from the restored branch lifecycle"
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION advanced_summaries TO metric_summaries;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":40}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":60}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "samples":2 | "total":100 | "first_latency":40 | "last_latency":60
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
