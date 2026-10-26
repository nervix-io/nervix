Feature: Admitted capture of stored deduplicator and window checkpoints
  A stopped domain and a stopped restore capture one stored checkpoint at a time, charge its
  retained view, and write bounded archive groups that preserve every key, row and aggregate.

  @long_running
  Scenario Outline: Stored branch checkpoints above the bulk budget re-export exactly under admission on <cluster_size> nodes
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
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "running.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup "running.nvxb" holds a deduplicator keyspace and a window of tenant "large" above 32 MiB beside 24 small branches of each
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    And the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "stopped.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And the measured backup admits a stored branch checkpoint above 32 MiB
    And backup archives "running.nvxb" and "stopped.nvxb" have identical deduplicator and window state
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "stopped.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 21 models
    And the CLI restore reports domain "{{domain}}_copy" as "STOPPED" at start version 1
    When the CLI backs up "domain {{domain}}_copy --timeout 120s" from node "{{leader}}" into "re-export.nvxb" reporting JSON with memory measurements
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}_copy"
    And the measured backup admits a stored branch checkpoint above 32 MiB
    And backup archives "stopped.nvxb" and "re-export.nvxb" preserve complete domain "{{domain}}" restored as stopped domain "{{domain}}_copy"

    @restore_installation @deloxide_stress_restore
    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |

    Examples:
      | cluster_size | replica_count |
      | 3            | 1             |
