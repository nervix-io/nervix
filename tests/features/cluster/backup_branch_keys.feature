Feature: Archived branch keys checked against the restored branch declarations
  A restore installs an archived branch key only when it is a key of the branching its restored
  entity declares: absent exactly when the entity runs unbranched, and otherwise holding exactly
  the fields of the branch's key schema, each with a value of its declared type. Any other key
  refuses the restore before it changes anything, naming the archive section, the place in it and
  the field.

  Background:
    Given these NSPL commands are saved as placeholder "branch_key_models"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( tenant STRING, shard I32, order_id STRING, amount I64 );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT ( tenant string, shard integer, order_id string, amount integer );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_event;
      CREATE SCHEMA tenant_shard ( tenant STRING, shard I32 );
      CREATE BRANCH by_tenant_shard SCHEMA tenant_shard TTL 30m;
      CREATE RELAY orders SCHEMA order_event BRANCHED BY by_tenant_shard WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY unique_orders SCHEMA order_event BRANCHED BY by_tenant_shard;
      CREATE VHOST edge backup-branch-keys-{{test_id}}.example.com;
      CREATE ENDPOINT order_ingress ON edge PATH '/orders' TYPE HTTP;
      CREATE INGESTOR order_source FROM ENDPOINT order_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_codec
        TO orders INHERIT ALL BRANCHED BY by_tenant_shard SET tenant = message.tenant, shard = message.shard
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR unique_order_filter FROM orders
        DEDUPLICATE ON input.order_id
        MAX TIME 30m
        BRANCHED BY by_tenant_shard
        TO unique_orders INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """

  Scenario Outline: A restore refuses archived branch keys of another shape before it changes anything on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_key_models}}
      CREATE SUBSCRIPTION source_orders TO unique_orders;
      """
    And http payload is posted to host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"alpha","shard":1,"order_id":"o-1","amount":10}
      """
    And http payload is posted to host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"beta","shard":2,"order_id":"o-2","amount":20}
      """
    And http payload is posted to host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"alpha","shard":1,"order_id":"o-3","amount":30}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha","shard":1} | "order_id":"o-1" | "amount":10
      key={"tenant":"beta","shard":2} | "order_id":"o-2" | "amount":20
      key={"tenant":"alpha","shard":1} | "order_id":"o-3" | "amount":30
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "orders.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "orders.nvxb" as json
    Then the described backup has exactly 2 "deduplicator" state sections
    Given backup archive "orders.nvxb" is copied to "renamed.nvxb" with field "shard" renamed to "zone" in every branch key under "branch_lifecycle/deduplicator/unique_order_filter"
    And backup archive "orders.nvxb" is copied to "extra.nvxb" with field "region" added to every branch key under "branch_lifecycle/deduplicator/unique_order_filter"
    And backup archive "orders.nvxb" is copied to "retyped.nvxb" with field "shard" holding a string in every branch key under "branch_lifecycle/deduplicator/unique_order_filter"
    And backup archive "orders.nvxb" is copied to "unbranched.nvxb" with every branch key under "branch_lifecycle/deduplicator/unique_order_filter" unbranched
    And backup archive "orders.nvxb" is copied to "ingestor.nvxb" with field "shard" removed from every branch key under "branch_lifecycle/ingestor/order_source"
    And backup archive "orders.nvxb" is copied to "descriptor.nvxb" with field "shard" holding a string in every branch key under "deduplicator/unique_order_filter"
    And backup archive "orders.nvxb" is copied to "identities.nvxb" with field "shard" holding a string in every branch key under "materialized_relay/orders"
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --dry-run" from "renamed.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "the branch key of lifecycle entry 1 in archive section 'domains/{{domain}}/state/branch_lifecycle/deduplicator/unique_order_filter/branches.rkyv' does not belong to the branching of deduplicator 'unique_order_filter' in domain '{{domain}}': it is not a key of branch 'by_tenant_shard': the key has no field 'shard'"
    When the CLI restores "domain {{domain}}" from "extra.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "it is not a key of branch 'by_tenant_shard': the key has field 'region', which its branch schema does not declare"
    When the CLI restores "domain {{domain}} --resume" from "retyped.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "it is not a key of branch 'by_tenant_shard': field 'shard' does not hold a value of its declared type: expected I32, found STRING"
    When the CLI restores "domain {{domain}} --resume" from "retyped.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "field 'shard' does not hold a value of its declared type: expected I32, found STRING"
    When the CLI restores "domain {{domain}} --resume" from "unbranched.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "does not belong to the branching of deduplicator 'unique_order_filter' in domain '{{domain}}': it is unbranched, where the entity runs in branch 'by_tenant_shard'"
    When the CLI restores "domain {{domain}} --resume" from "ingestor.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "the branch key of lifecycle entry 1 in archive section 'domains/{{domain}}/state/branch_lifecycle/ingestor/order_source/branches.rkyv' does not belong to the branching of ingestor 'order_source' in domain '{{domain}}': it is not a key of branch 'by_tenant_shard': the key has no field 'shard'"
    When the CLI restores "domain {{domain}} --resume --dry-run" from "descriptor.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "/descriptor.rkyv' does not belong to the branching of deduplicator 'unique_order_filter' in domain '{{domain}}': it is not a key of branch 'by_tenant_shard': field 'shard' does not hold a value of its declared type: expected I32, found STRING"
    When the CLI restores "domain {{domain}} --resume" from "identities.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "materialized state of relay 'orders' in domain '{{domain}}' could not be prepared: the branch key of record identity 1 in archive section 'domains/{{domain}}/state/materialized_relay/orders/groups/0000000000/identities.rkyv' does not belong to the relay's branching: it is not a key of branch 'by_tenant_shard': field 'shard' does not hold a value of its declared type: expected I32, found STRING"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output does not contain
      """
      {{domain}}
      """
    When the CLI restores "domain {{domain}} --resume" from "orders.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 11 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And the CLI restore reports no warnings
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"probe","shard":9,"order_id":"o-probe","amount":0}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_orders TO unique_orders;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"alpha","shard":1,"order_id":"o-1","amount":11}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"beta","shard":2,"order_id":"o-1","amount":12}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"beta","shard":2,"order_id":"o-2","amount":22}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-keys-{{test_id}}.example.com" path "/orders"
      """
      {"tenant":"alpha","shard":1,"order_id":"o-4","amount":40}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta","shard":2} | "order_id":"o-1" | "amount":12
      key={"tenant":"alpha","shard":1} | "order_id":"o-4" | "amount":40
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
