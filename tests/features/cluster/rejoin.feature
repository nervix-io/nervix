Feature: Cluster node rejoin

  Scenario: Stopped leader rejoins the cluster after failover
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually reports leader "node-1"
    And node "node-2" eventually reports leader "node-1"
    And node "node-3" eventually reports leader "node-1"
    And node "node-2" eventually reports raft voters "node-1,node-2,node-3"
    When node "node-1" is stopped
    Then node "node-2" eventually reports a leader other than "node-1"
    When node "node-1" is started
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports raft state "Follower"
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    And node "node-3" eventually reports interconnect to "node-1" as "connected"
    And node "node-2" eventually reports raft voters "node-1,node-2,node-3"
