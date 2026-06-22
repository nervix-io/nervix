Feature: Drop node

  Scenario: A stopped node can be removed from cluster membership
    Given a 3 node nervix cluster is started
    And node "node-3" is stopped
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports status containing "raft member 'node-3' is marked unavailable"
    When these NSPL commands are executed through the client on node "node-1"
      """
      DROP NODE node-3;
      """
    Then node "node-1" eventually reports raft voters "node-1,node-2"

  Scenario: A live node cannot be removed from cluster membership
    Given a 3 node nervix cluster is started
    When these NSPL commands fail through the client on node "node-1" with "cannot drop live node 'node-2'"
      """
      DROP NODE node-2;
      """
    Then node "node-1" eventually reports raft voters "node-1,node-2,node-3"
