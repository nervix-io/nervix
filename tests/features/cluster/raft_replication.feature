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
    Then within "60s" the leader node has purged its covered raft log and reports fewer retained bytes
    And the leader node released its bulk-memory reservation after snapshot compaction
    When node "node-3" is started
    Then within "60s" node "node-3" recovers by installing a raft snapshot
    And within "60s" node "node-3" has applied 40 domains named "compacted"

  @shutdown_qualification
  Scenario: An interrupted snapshot installation finishes on the next start
    Given raft snapshots after 8 entries retaining 2 covered entries
    And a 3 node nervix cluster is started
    And node "node-3" is stopped
    When 40 domains named "interrupted" are created on the leader node
    Then within "60s" the leader node has purged its covered raft log
    Given node "node-3" interrupts its next raft snapshot installation
    When node "node-3" is started without waiting for it to catch up
    Then within "60s" node "node-3" has interrupted a raft snapshot installation
    When node "node-3" is stopped
    And node "node-3" is started
    Then within "60s" node "node-3" recovers by installing a raft snapshot
    And within "60s" node "node-3" has applied 40 domains named "interrupted"

  @exclusive
  Scenario: Durable follower catch-up stays bounded and preserves its append stream
    Given a 3 node nervix cluster is started
    And node "node-3" is stopped
    When 1024 domains named "durable_backlog" are created on the leader node
    Given consensus commits on node "node-3" take "2ms"
    When node "node-3" starts catching up while the leader keeps creating domains named "durable_live"
    Then node "node-3" applies 1024 domains named "durable_backlog" and the concurrent writes within its durable storage bound using at most 2 append streams
    And node "node-3" held its queued append batches inside its commands memory budget while catching up
