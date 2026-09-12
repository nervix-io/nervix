Feature: Pipelined raft replication and bounded log retention

  Scenario: Pipelined replication preserves committed order
    Given a 3 node nervix cluster is started
    And node "node-3" is stopped
    When 40 domains named "pipelined" are created on the leader node
    And node "node-3" is started
    Then within "60s" node "node-3" has applied 40 domains named "pipelined"
    When leadership is transferred to node "node-3"
    And 40 domains named "relayed" are created on the leader node
    Then within "60s" every node has applied 40 domains named "relayed"
    And within "60s" every node has applied 40 domains named "pipelined"

  Scenario: A lagging follower recovers after bounded log compaction
    Given raft snapshots after 8 entries retaining 2 covered entries
    And a 3 node nervix cluster is started
    And node "node-3" is stopped
    When 40 domains named "compacted" are created on the leader node
    Then within "60s" the leader node has purged its covered raft log
    When node "node-3" is started
    Then within "60s" node "node-3" recovers by installing a raft snapshot
    And within "60s" node "node-3" has applied 40 domains named "compacted"
