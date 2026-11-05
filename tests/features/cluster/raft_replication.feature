Feature: Pipelined raft replication and bounded log retention

  Scenario: A late heartbeat answer inside the leader lease keeps an idle leader writing
    Given a 3 node nervix cluster is started
    When follower heartbeat answers take "300ms" while the leader idles for "3s"
    Then the delayed-heartbeat leader creates domain "late_heartbeat_0" within "60s"
    And within "60s" every node has applied 1 domains named "late_heartbeat"

  Scenario: A new leader recovers one failed append submission without rewinding its probe
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And client "owner" is connected to the leader node
    When client "owner" executes these Plexus commands
      """
      BEGIN;
      CREATE SCHEMA append_recovery (value STRING);
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When 128 domains named "append_history" are created on the leader node
    Then within "60s" every node has applied 128 domains named "append_history"
    And the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    And a node other than placeholders "old_leader" and "new_leader" is saved as placeholder "lagging_follower"
    When node "{{lagging_follower}}" is stopped
    And 40 domains named "append_backlog" are created on the leader node
    Given node "{{new_leader}}" will fail its next data append submission to node "{{lagging_follower}}"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    And node "{{lagging_follower}}" is started without waiting for it to catch up
    Then node "{{new_leader}}" failed one append submission to node "{{lagging_follower}}" above the replay batch boundary
    Given client "resumed" is connected to node "{{new_leader}}" with cluster seeds
    When client "resumed" attaches to transaction "{{transaction_id}}"
    And client "resumed" attempts to commit its transaction
    Then client "resumed" commit was refused because its expected preview is stale
    And transaction "{{transaction_id}}" eventually has state "OPEN"
    When client "resumed" executes these Plexus commands
      """
      COMMIT;
      """
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    And within "60s" every node has applied 40 domains named "append_backlog"
    When 4 domains named "append_after_failure" are created on the leader node
    Then within "60s" every node has applied 4 domains named "append_after_failure"
    And node "{{new_leader}}" confirms a linearizable read beyond its failed append submission

  Scenario: A paced follower recovers an occupied listener before its catch-up start
    Given a 3 node nervix cluster is started
    When node "node-3" is stopped
    Given inbound interconnect traffic to stopped node "node-3" is paced at 32768 bytes per second
    And the next start of node "node-3" has an occupied observability port
    When node "node-3" starts catching up through its paced interconnect
    Then node "node-3" eventually reports raft voters "node-1,node-2,node-3"

  @long_running @slots_2
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
