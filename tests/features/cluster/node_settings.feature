Feature: Node settings

  A node reads every setting it is given, and the relationships between them, before it opens a
  store or binds a listener. A setting that breaks a relationship with another refuses the node's
  start with a typed message naming both, and leaves the rest of the cluster as it was.

  Scenario Outline: A node given a drain timeout its shutdown timeout cannot hold refuses to start on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with drain-timeout = 5m
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "drain-timeout (300s) must not exceed shutdown-timeout (240s)"
    Given node-<cluster_size> starts with drain-timeout = 1m
    When node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually observes a stable leader

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A node given an election timeout that does not exceed its heartbeat refuses to start on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with raft-election-timeout-min = 200ms
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "raft-election-timeout-min (200ms) must exceed raft-heartbeat-interval (200ms)"
    Given node-<cluster_size> starts with raft-election-timeout-min = 1s
    When node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually observes a stable leader

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
