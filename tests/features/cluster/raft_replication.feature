Feature: Pipelined raft replication and bounded log retention

  Scenario: A paced follower recovers an occupied listener before its catch-up start
    Given a 3 node nervix cluster is started
    When node "node-3" is stopped
    Given inbound interconnect traffic to stopped node "node-3" is paced at 32768 bytes per second
    And the next start of node "node-3" has an occupied observability port
    When node "node-3" starts catching up through its paced interconnect
    Then node "node-3" eventually reports raft voters "node-1,node-2,node-3"

  @long_running
  Scenario: A rate-limited follower catches up without restarting a moving append stream
    Given a 3 node nervix cluster is started
    When node "node-3" is stopped
    And 1024 domains named "moving_append" are created on the leader node
    And leadership is transferred to node "node-2"
    Given inbound interconnect traffic to stopped node "node-3" is paced at 32768 bytes per second
    When node "node-3" starts catching up through its paced interconnect
    Then within "120s" the paced follower applies 1024 domains named "moving_append"
    And the paced catch-up uses at most two data streams

  Scenario: Failed follower stream setup stays paced while the surviving quorum commits
    Given a 3 node nervix cluster is started
    When node "node-3" is stopped
    Then node "node-1" eventually reports status containing "raft member 'node-3' is not currently visible in gossip"
    Given append-stream setup attempts toward node "node-3" are observed
    When 40 domains named "paced_majority" are created on the leader node
    Then within "60s" node "node-1" has applied 40 domains named "paced_majority"
    And within "60s" node "node-2" has applied 40 domains named "paced_majority"
    And append-stream setup attempts toward node "node-3" respect the caller retry delay
    When node "node-3" is started
    Then within "60s" node "node-3" has applied 40 domains named "paced_majority"

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
    And within "60s" the leader node released its snapshot-section bulk-memory reservation
    When node "node-3" is started
    Then within "60s" node "node-3" recovers by installing a raft snapshot
    And within "60s" node "node-3" has applied 40 domains named "compacted"

  @shutdown_qualification
  Scenario: An interrupted snapshot installation finishes on the next start
    Given raft snapshots after 8 entries retaining 2 covered entries
    And a 3 node nervix cluster is started
    And node "node-3" raft log head is saved before stop
    And node "node-3" is stopped
    When 40 domains named "interrupted" are created on the leader node
    Then within "60s" the leader node has purged its covered raft log and reports fewer retained bytes
    And within "60s" the leader node has purged its covered raft log beyond stopped node "node-3"
    Given node "node-3" interrupts its next raft snapshot installation
    And the next start of node "node-3" has an occupied observability port
    When node "node-3" is started without waiting for it to catch up
    Then within "60s" node "node-3" has interrupted a raft snapshot installation
    When node "node-3" is stopped
    And node "node-3" is started
    Then within "60s" node "node-3" recovers by installing a raft snapshot
    And within "60s" node "node-3" has applied 40 domains named "interrupted"

  @long_running
  Scenario: Durable follower catch-up stays bounded and preserves its append stream
    Given raft heartbeat interval is configured as "2500ms"
    And a 3 node nervix cluster is started
    And node "node-3" is stopped
    When 1024 domains named "durable_backlog" are created on the leader node
    Given consensus commits on node "node-3" take "2ms"
    And the next start of node "node-3" has an occupied observability port
    When node "node-3" starts catching up while the leader keeps creating domains named "durable_live"
    Then node "node-3" applies 1024 domains named "durable_backlog" and the concurrent writes within its durable storage bound using at most 2 append streams
    And node "node-3" held its queued append batches inside its commands memory budget while catching up
