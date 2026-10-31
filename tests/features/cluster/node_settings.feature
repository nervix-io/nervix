Feature: Node settings

  A node reads every setting it is given, and the relationships between them, before it opens a
  store or binds a listener. A setting that breaks a relationship with another refuses the node's
  start with a typed message naming both, and leaves the rest of the cluster as it was.

  Scenario Outline: A node restarts with a large fractional snapshot interval after another setting is applied on a cluster of <cluster_size>
    Given runtime replication is configured with replica count 0 and snapshot interval "<snapshot_interval>"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      ALTER CLUSTER SET COMMAND_EXECUTION_CAPACITY = 64;
      """
    When node "node-1" is stopped
    And node "node-1" is started
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports status containing "node=node-1 state-snapshot-interval running=<reported_interval> revision="

    Examples:
      | cluster_size | snapshot_interval       | reported_interval                             |
      | 1            | 4294967294s 601538000ns | 136years 1month 5days 19h 54m 38s 601ms 538us |
      | 3            | 4294967294s 601538000ns | 136years 1month 5days 19h 54m 38s 601ms 538us |
      | 1            | 4294967294s 601000000ns | 136years 1month 5days 19h 54m 38s 601ms       |
      | 3            | 4294967294s 601000000ns | 136years 1month 5days 19h 54m 38s 601ms       |

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

  Scenario Outline: A node given a heartbeat below the millisecond consensus runs timings at refuses to start on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with raft-heartbeat-interval = 500us
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "raft-heartbeat-interval (500µs) must be at least 1ms, the resolution consensus runs timings at"
    Given node-<cluster_size> starts with raft-heartbeat-interval = 200ms
    When node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually observes a stable leader

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
