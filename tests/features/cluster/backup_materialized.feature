Feature: Materialized backup and archived lifecycle resumption

  Scenario Outline: RESUME reproduces an unpaced archived lifecycle in a fresh cluster
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "resume.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume" from "resume.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 0 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And every node reports the resumed lifecycle archived in "resume.nvxb"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}} pace=UNPACED status=RUNNING
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @restore_installation @deloxide_stress_restore
  Scenario Outline: A resumed materialized cut preserves interleaved branches and generators through restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, value I64, source STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, source string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY state SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generated SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge backup-materialized-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO state INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR produce USING MATERIALIZED STATE state EACH 100ms BRANCHED BY by_tenant
        TO generated SET tenant = branch.tenant, value = relay_state.state.value, source = relay_state.state.source
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    When http payload is posted to host "backup-materialized-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"alpha","value":1,"source":"alpha-first"}
      """
    And http payload is posted to host "backup-materialized-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"beta","value":2,"source":"beta-state"}
      """
    And http payload is posted to host "backup-materialized-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"alpha","value":3,"source":"alpha-state"}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      key={"tenant":"alpha"} payload={"source":"alpha-state","tenant":"alpha","value":3}
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      key={"tenant":"beta"} payload={"source":"beta-state","tenant":"beta","value":2}
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "materialized.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "materialized.nvxb" as text
    Then the CLI output contains "materialized_relay=state"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And the active domain is saved as placeholder "source_domain"
    And restoring domain "{{domain}}_staging_failure" fails before installing its first materialized checkpoint
    When the CLI restores "domain {{domain}} --as {{domain}}_staging_failure --resume" from "materialized.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "runtime state"
    Given the active domain is "{{source_domain}}_staging_failure"
    When these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    Given the active domain is "{{source_domain}}"
    And restoring domain "{{domain}}_durable_failure" fails after durable state publication
    When the CLI restores "domain {{domain}} --as {{domain}}_durable_failure --resume" from "materialized.nvxb" on node "{{leader}}" reporting JSON
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
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    Given the active domain is "{{source_domain}}"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume --dry-run" from "materialized.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI dry run planned domain "{{domain}}" with 0 resource versions and 11 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    When the CLI restores "domain {{domain}}" from "materialized.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 11 models
    And the CLI restore reports domain "{{domain}}" as "STOPPED" at start version 1
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "materialized-stopped.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "materialized.nvxb" and "materialized-stopped.nvxb" preserve complete domain "{{domain}}" restored as stopped domain "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      relay 'state' materialized state is empty
      """
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "materialized.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 11 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And every node reports the resumed lifecycle archived in "materialized.nvxb"
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"tenant":"alpha"} payload={"source":"alpha-state","tenant":"alpha","value":3}
      """
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"tenant":"beta"} payload={"source":"beta-state","tenant":"beta","value":2}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION generated_subscription TO generated;
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "source":"alpha-state" | "value":3
      key={"tenant":"beta"} | "source":"beta-state" | "value":2
      """
    When the cluster is restarted
    Then within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"tenant":"alpha"} payload={"source":"alpha-state","tenant":"alpha","value":3}
      """
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"tenant":"beta"} payload={"source":"beta-state","tenant":"beta","value":2}
      """
    When these NSPL commands are executed on the leader node
      """
      STOP;
      START;
      """
    Then within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      relay 'state' materialized state is empty
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @restore_installation @deloxide_stress_restore
  Scenario Outline: Materialized generations larger than the bulk budget resume on every assigned owner and replica
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When a materialized restore workload with <relays> relays is created
    And round 1 of <row_kib> KiB materialized rows is submitted for <tenants> tenants
    Then within "180s" the materialized generators report round 1, <row_kib> KiB, <relays> relays and <tenants> tenants
    When round 2 of <row_kib> KiB materialized rows is submitted for <tenants> tenants
    Then within "180s" the materialized generators report round 2, <row_kib> KiB, <relays> relays and <tenants> tenants
    When the materialized summary subscription "summary_subscription" is closed
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "large-materialized.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup "large-materialized.nvxb" contains <relays> materialized relays with <tenants> records each and more than 32 MiB of columns
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And the active domain is saved as placeholder "source_domain"
    And restoring domain "{{domain}}_large_failure" fails after durable state publication
    When the CLI restores "domain {{domain}} --as {{domain}}_large_failure --resume" from "large-materialized.nvxb" on node "{{leader}}" reporting JSON with memory measurements
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "runtime state"
    Given the active domain is "{{source_domain}}_large_failure"
    When the cluster is restarted
    And these NSPL commands fail with "restore state installation is incomplete"
      """
      START;
      """
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    Given the active domain is "{{source_domain}}"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_stopped" from "large-materialized.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore reports domain "{{domain}}_stopped" as "STOPPED" at start version 1
    When the CLI backs up "domain {{domain}}_stopped --timeout 60s" from node "{{leader}}" into "large-stopped.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}_stopped"
    And backup archives "large-materialized.nvxb" and "large-stopped.nvxb" preserve complete domain "{{domain}}" restored as stopped domain "{{domain}}_stopped"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When restore "DOMAIN {{domain}} RESUME" of backup archive "large-materialized.nvxb" is streamed to node "{{leader}}" under execution reference "large-resume"
    Then the restore stream's outcome is "completed" as "executed"
    And the restore stream's report shows every step applied
    And every node reports the resumed lifecycle archived in "large-materialized.nvxb"
    When restore "DOMAIN {{domain}} RESUME" of backup archive "large-materialized.nvxb" is streamed to node "{{leader}}" under execution reference "large-resume"
    Then the restore stream's outcome is "completed" as "recovered"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_summaries TO summaries;
      """
    Then within "180s" the materialized generators report round 2, <row_kib> KiB, <relays> relays and <tenants> tenants
    When the cluster is restarted
    Then every node reports the resumed lifecycle archived in "large-materialized.nvxb"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION reopened_summaries TO summaries;
      """
    Then within "180s" the materialized generators report round 2, <row_kib> KiB, <relays> relays and <tenants> tenants

    @order_resume_fanout_single
    Examples:
      | cluster_size | replica_count | relays | row_kib | tenants |
      | 1            | 0             | 40     | 512     | 2       |

    @order_resume_fanout_cluster
    Examples:
      | cluster_size | replica_count | relays | row_kib | tenants |
      | 3            | 1             | 40     | 512     | 2       |

    @order_resume_tenants_single
    Examples:
      | cluster_size | replica_count | relays | row_kib | tenants |
      | 1            | 0             | 1      | 1024    | 40      |

    @order_resume_tenants_cluster
    Examples:
      | cluster_size | replica_count | relays | row_kib | tenants |
      | 3            | 1             | 1      | 1024    | 40      |

  @restore_installation @deloxide_stress_restore
  Scenario Outline: A delayed coordinator cannot replace a resumed materialized generation larger than the bulk budget
    Given runtime replication is configured with replica count 1 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When a materialized restore workload with <relays> relays is created
    And round 1 of <row_kib> KiB materialized rows is submitted for <tenants> tenants
    Then within "180s" the materialized generators report round 1, <row_kib> KiB, <relays> relays and <tenants> tenants
    When round 2 of <row_kib> KiB materialized rows is submitted for <tenants> tenants
    Then within "180s" the materialized generators report round 2, <row_kib> KiB, <relays> relays and <tenants> tenants
    When the materialized summary subscription "summary_subscription" is closed
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "large-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup "large-stale.nvxb" contains <relays> materialized relays with <tenants> records each and more than 32 MiB of columns
    Given the cluster is replaced by a fresh 3 node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Then a node other than placeholder "leader" is saved as placeholder "survivor"
    Given restoring domain "{{domain}}_copy" by coordinator "{{leader}}" pauses before state publication
    When restore "DOMAIN {{domain}} AS {{domain}}_copy RESUME" of backup archive "large-stale.nvxb" is streamed to node "{{leader}}" under execution reference "large-stale-restore" in the background
    Then restoring domain "{{domain}}_copy" by coordinator "{{leader}}" has reached state publication
    When leadership is transferred from node "{{leader}}" to node "{{survivor}}"
    Then node "{{survivor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    And restore "DOMAIN {{domain}} AS {{domain}}_copy RESUME" of backup archive "large-stale.nvxb" completes on node "{{survivor}}" under execution reference "large-stale-restore"
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION resumed_summaries TO summaries;
      """
    Then within "180s" the materialized generators report round 2, <row_kib> KiB, <relays> relays and <tenants> tenants
    When round 3 of <row_kib> KiB materialized rows is submitted for <tenants> tenants
    Then within "180s" the materialized generators report round 3, <row_kib> KiB, <relays> relays and <tenants> tenants
    When the materialized summary subscription "resumed_summaries" is closed
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{survivor}}" into "large-before-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When state publication of domain "{{domain}}" by coordinator "{{leader}}" is released
    Then state publication of domain "{{domain}}" by coordinator "{{leader}}" is refused
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{survivor}}" into "large-after-stale.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "large-before-stale.nvxb" and "large-after-stale.nvxb" have identical materialized generations

    @order_materialized_fanout
    Examples:
      | relays | row_kib | tenants |
      | 40     | 512     | 2       |

    @order_materialized_tenants
    Examples:
      | relays | row_kib | tenants |
      | 1      | 1024    | 40      |

  @restore_installation @deloxide_stress_restore
  Scenario Outline: A paced RESUME retains its mapping and admits TIMESTAMP AT in the projected window
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10s;
      CREATE SCHEMA event ( sequence I64, occurred_at DATETIME );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( sequence integer, occurred_at string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event
        ENCODE occurred_at AS RFC3339;
      CREATE SCHEMA sequence_key ( sequence I64 );
      CREATE BRANCH by_sequence SCHEMA sequence_key TTL 30m;
      CREATE RELAY state SCHEMA event BRANCHED BY by_sequence WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge backup-paced-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec TIMESTAMP AT occurred_at
        TO state INHERIT ALL BRANCHED BY by_sequence SET sequence = message.sequence
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.5;
      """
    Given client "driver" is connected to node "node-1"
    When client "driver" executes these NSPL commands
      """
      ATTACH DOMAIN CLOCK;
      """
    Then within "10s" client "driver" receives a tick for its attached domain clock
    When client "driver" posts event 1 to host "backup-paced-{{test_id}}.example.com" path "/events" at the newest tick center its attached clock admits
    Then within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      key={"sequence":1}
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "paced.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "paced.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 9 models
    And every node reports the resumed lifecycle archived in "paced.nvxb"
    Given client "driver" is connected to node "restored-1"
    When client "driver" executes these NSPL commands
      """
      ATTACH DOMAIN CLOCK;
      CREATE SUBSCRIPTION restored_events TO state;
      """
    Then within "10s" client "driver" receives a tick for its attached domain clock
    And client "driver" observes resumed clock progress beyond backup "paced.nvxb"'s frontier
    When client "driver" posts event 2 to host "backup-paced-{{test_id}}.example.com" path "/events" at the newest tick center its attached clock admits
    Then within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"sequence":1}
      """
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"sequence":2}
      """
    When the cluster is restarted
    Then every node reports the resumed lifecycle archived in "paced.nvxb"
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      key={"sequence":2}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A restore refuses an archive whose materialized Arrow section declares a body it does not carry
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I32 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY state SCHEMA event UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge backup-ipc-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO state INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "backup-ipc-{{test_id}}.example.com" path "/state"
      """
      {"value":42}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      payload={"value":42}
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "source.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "source.nvxb" is copied to "malformed.nvxb" with a materialized Arrow body declaring <declared> bytes
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_restored --resume <mode>" from "malformed.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "misframed"
    And every node still answers cluster status

    Examples:
      | cluster_size | mode      | declared            |
      | 1            | --dry-run | 1152921504606846976 |
      | 3            | --dry-run | 1152921504606846976 |
      | 1            |           | 1152921504606846976 |
      | 3            |           | 1152921504606846976 |
      | 1            | --dry-run | -1                  |
      | 3            |           | -1                  |
