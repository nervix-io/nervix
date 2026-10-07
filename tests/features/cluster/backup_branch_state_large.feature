Feature: Deduplicator keyspaces and windows larger than the bulk budget in backup archives
  A restore converts each archived keyspace and window into its native checkpoint one bounded
  group at a time, and a restored window larger than the bulk budget persists again.

  Scenario Outline: A keyspace and a window above the bulk budget resume exactly beside many small branches on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When a large branch state restore workload is created
    And round 1 of 40 large deduplicator keys is posted for tenant "large"
    And 1 large window rows with latencies from 5 are posted for tenant "large"
    And 40 large window rows with latencies from 11 are posted for tenant "large"
    And one deduplicator key and one window row are posted for each of 24 small tenants
    Then within "120s" the large branch state subscription reports round 1 of 40 keys for tenant "large", a key for each of 24 small tenants, and each fragment set
      """
      key={"tenant":"large"} | "samples":41 | "total":1225 | "first_latency":5 | "last_latency":50 | "smallest":5 | "largest":50 | "latency_p0":5.0 | "note_bytes":42991616
      """
    When the large branch state subscription is closed
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "large-branch-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup "large-branch-state.nvxb" holds a deduplicator keyspace and a window of tenant "large" above 32 MiB beside 24 small branches of each
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And the active domain is saved as placeholder "source_domain"
    And restoring domain "{{domain}}_large_failure" fails after durable state publication
    When the CLI restores "domain {{domain}} --as {{domain}}_large_failure --resume" from "large-branch-state.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "runtime state"
    Given the active domain is "{{source_domain}}_large_failure"
    When the cluster is restarted
    And these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the active domain is "{{source_domain}}"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_stopped" from "large-branch-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_stopped" with 0 resource versions and 21 models
    And the CLI restore reports domain "{{domain}}_stopped" as "STOPPED" at start version 1
    And the CLI restore reports no warnings
    When the CLI backs up "domain {{domain}}_stopped --timeout 120s" from node "{{leader}}" into "large-branch-stopped.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}_stopped"
    And backup archives "large-branch-state.nvxb" and "large-branch-stopped.nvxb" preserve complete domain "{{domain}}" restored as stopped domain "{{domain}}_stopped"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "large-branch-state.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 21 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And the CLI restore reports no warnings
    And the CLI restore execution reference is saved as placeholder "large_resume_reference"
    When restore "DOMAIN {{domain}} RESUME" of backup archive "large-branch-state.nvxb" is streamed to node "{{leader}}" under execution reference "{{large_resume_reference}}"
    Then the restore stream's outcome is "completed" as "recovered"
    And the restore stream's report shows every step applied
    And within "60s" node "restored-1" accepts http payload for host "backup-large-branch-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1,"note":"probe"}
      """
    When the large branch state subscription is opened on the leader node
    And round 1 of 40 large deduplicator keys is posted for tenant "large"
    And one deduplicator key and one window row are posted for each of 24 small tenants
    And round 2 of 2 large deduplicator keys is posted for tenant "large"
    And 1 large window rows with latencies from 60 are posted for tenant "large"
    Then within "120s" the large branch state subscription reports round 2 of 2 keys for tenant "large" and each fragment set
      """
      key={"tenant":"large"} | "samples":41 | "total":1280 | "first_latency":11 | "last_latency":60 | "smallest":11 | "largest":60 | "latency_p0":5.0 | "note_bytes":42991616
      """
    When the large branch state subscription is closed
    And the cluster is restarted
    Then within "60s" node "restored-1" accepts http payload for host "backup-large-branch-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1,"note":"probe"}
      """
    When the large branch state subscription is opened on the leader node
    And round 2 of 2 large deduplicator keys is posted for tenant "large"
    And round 3 of 1 large deduplicator keys is posted for tenant "large"
    And 1 large window rows with latencies from 70 are posted for tenant "large"
    Then within "120s" the large branch state subscription reports round 3 of 1 keys for tenant "large" and each fragment set
      """
      key={"tenant":"large"} | "samples":41 | "total":1339 | "first_latency":12 | "last_latency":70 | "smallest":12 | "largest":70 | "latency_p0":5.0 | "note_bytes":42991616
      """

    @restore_installation
    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |

    Examples:
      | cluster_size | replica_count |
      | 3            | 1             |
