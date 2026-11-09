Feature: Permanent cluster identity
  A cluster creates its own signing identity at bootstrap and reports the public identity through
  DESCRIBE CLUSTER. Replication preserves that identity and a backup only describes its origin.

  Scenario Outline: Bootstrap reports a permanent cluster identity on <cluster_size> nodes
    Given the bootstrap node will propose competing cluster identities
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      cluster uid:
      """
    And the last command output contains
      """
      cluster key:
      """
    And the last command output contains
      """
      cluster created:
      """
    And the public cluster identity is saved as "origin"
    And both competing bootstrap identity proposals completed
    And every node eventually reports the public cluster identity saved as "origin"
    When the cluster is restarted
    Then every node eventually reports the public cluster identity saved as "origin"
    When node "node-<cluster_size>" is stopped
    And node "node-<cluster_size>" is started
    Then every node eventually reports the public cluster identity saved as "origin"
    Given node "<joining_node>" has joined the cluster
    Then every node eventually reports the public cluster identity saved as "origin"

    Examples:
      | cluster_size | joining_node |
      | 1            | node-2       |
      | 3            | node-4       |

  Scenario: A new leader reports the same cluster identity
    Given a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the public cluster identity is saved as "origin"
    When leadership is transferred to node "node-2"
    Then every node eventually reports the public cluster identity saved as "origin"

  Scenario Outline: A cluster backup describes its origin and restores as a clone on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA identity_row (id U32);
      CREATE RELAY identity_rows SCHEMA identity_row UNBRANCHED;
      DESCRIBE CLUSTER;
      """
    Then the public cluster identity is saved as "origin"
    And the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster --without-state" from node "{{leader}}" into "identity.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "identity.nvxb" as text
    Then the CLI output contains "cluster uid: {{origin_uid}}"
    When the CLI describes backup archive "identity.nvxb" as json
    Then the inspected backup has the source identity saved as "origin"
    When all nodes are stopped
    And the stopped cluster is replaced with a fresh <cluster_size> node cluster
    And these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the public cluster identity differs from the identity saved as "origin"
    And the current leader node is saved as placeholder "clone_leader"
    When the CLI restores "cluster --on-existing-user skip" from "identity.nvxb" on node "{{clone_leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 2 models
    And every node eventually reports the public cluster identity saved as "clone"
    When these Plexus commands are executed on the leader node
      """
      USE {{domain}};
      SHOW CREATE RELAY identity_rows;
      """
    Then the last command output contains
      """
      CREATE RELAY identity_rows
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
