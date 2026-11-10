Feature: Durable signed licensing state
  Every node verifies the current signed bytes at recovery and snapshot installation.

  Scenario Outline: Activation survives restart on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "durable_license"
    When the cluster is restarted
    Then node "node-1" has the license decision saved as placeholder "durable_license"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A lagging follower verifies an activated snapshot
    Given an HTTPS licensing test service is available
    And raft snapshots after 8 entries retaining 2 covered entries
    And a 3 node nervix cluster is started
    And node "node-3" is stopped
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "snapshot_license"
    When 40 domains named "license_snapshot" are created on the leader node
    Then within "60s" the leader node has purged its covered raft log and reports fewer retained bytes
    When node "node-3" is started
    Then within "60s" node "node-3" recovers by installing a raft snapshot
    And node "node-3" has the license decision saved as placeholder "snapshot_license"

  Scenario Outline: A damaged stored decision loads degraded and refreshes on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "before_damage"
    And node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.nodes: <cluster_size> of 16
      """
    When the licensing service becomes unreachable
    And the cluster restarts with a damaged stored license decision
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      policy: degraded
      warning: decision_unverifiable
      quota cluster.nodes: <cluster_size> of unlimited
      """
    When the licensing service becomes reachable
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the committed license decision is newer than placeholder "before_damage"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Editing signed ephemeral expiry preserves the independent binding on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "ephemeral_binding"
    When the licensing service becomes unreachable
    And the cluster restarts with an edited signed ephemeral expiry
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      policy: degraded
      verification: unverifiable
      warning: decision_unverifiable
      """
    And the independent activation binding matches placeholder "ephemeral_binding"
    When the cluster licensing clock passes the "ephemeral" deadline
    Then the independent activation expiry from placeholder "ephemeral_binding" has been reached

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
