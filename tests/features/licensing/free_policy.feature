Feature: Embedded free licensing policy
  A cluster without an activation derives its permissions from the embedded free policy.
  The shipped policy admits existing Nervix usage and is observable on every node.

  Scenario: A follower describes its committed license without a selected domain
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "licensing_leader"
    And a node other than placeholder "licensing_leader" is saved as placeholder "licensing_follower"
    And node "{{licensing_follower}}" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      quota cluster.nodes: 3 of unlimited
      """
    And the last license inspection has typed cluster scope

  Scenario Outline: The free policy is published without a selected domain on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      processing: running
      quota cluster.nodes: <cluster_size> of unlimited
      quota cluster.domains: 0 of unlimited
      conditions not understood: 0
      """
    And the last license inspection has typed cluster scope
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.domains: 2 of unlimited
      """
    When the cluster is restarted
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      quota cluster.domains: 2 of unlimited
      """
    And the licensing service has received no requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @quota_denial
  Scenario Outline: Domain admission retains the last slot and refuses a new domain on <cluster_size> nodes
    Given the free licensing quota "cluster.domains" is limited to 1
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE IF NOT EXISTS UNPACED DOMAIN {{domain}};
      """
    And these Plexus commands fail with "licensing quota 'cluster.domains'"
      """
      CREATE UNPACED DOMAIN {{domain}}_refused;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      processing: running
      quota cluster.domains: 1 of 1
      """
    And the current leader node is saved as placeholder "licensing_leader"
    And node "{{licensing_leader}}" observability metric "nervix_licensing_admissions_refused_total" with labels eventually equals 1
      """
      condition="cluster.domains"
      state="over_limit"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @quota_denial
  Scenario Outline: Node admission retains existing membership and stops a refused newcomer on <cluster_size> nodes
    Given the free licensing quota "cluster.nodes" is limited to <cluster_size>
    And a <cluster_size> node nervix cluster is started
    When the cluster is restarted
    And node "<joining_node>" attempts to join
    Then node "<joining_node>" refused to join because "licensing quota 'cluster.nodes'"
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      processing: running
      quota cluster.nodes: <cluster_size> of <cluster_size>
      """

    Examples:
      | cluster_size | joining_node |
      | 1            | node-2       |
      | 3            | node-4       |

  @quota_denial
  Scenario Outline: Restoring another domain cannot bypass the domain quota on <cluster_size> nodes
    Given the free licensing quota "cluster.domains" is limited to 1
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "licensing_leader"
    When the CLI backs up "domain {{domain}}" from node "{{licensing_leader}}" into "licensing.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "licensing.nvxb" on node "{{licensing_leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "licensing quota 'cluster.domains'"
    And node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      processing: running
      quota cluster.domains: 1 of 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
