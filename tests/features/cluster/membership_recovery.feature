Feature: Recovery of an interrupted joint membership change

  @joint_membership_recovery @deloxide_stress
  Scenario Outline: A refused uniform step is reconciled before any one of three voters stops
    Given a 2 node nervix cluster is started
    When leadership is transferred to node "node-1"
    Then node "node-1" eventually reports status containing "raft.membership_joint: false"
    Given the promotion of node "node-3" will be interrupted after its joint entry applies
    When node "node-3" starts joining without waiting for voter convergence
    Then the promotion of node "node-3" was interrupted in the applied joint configuration
    And node "node-1" eventually reports status containing "raft.membership_joint: false"
    And node "node-1" eventually reports raft voters "node-1,node-2,node-3"
    When node "<stopped>" is stopped
    And 2 domains named "joint_recovered" are created on the leader node
    Then within "60s" node "<first>" has applied 2 domains named "joint_recovered"
    And within "60s" node "<second>" has applied 2 domains named "joint_recovered"

    Examples:
      | stopped | first  | second |
      | node-1  | node-2 | node-3 |
      | node-2  | node-1 | node-3 |
      | node-3  | node-1 | node-2 |
