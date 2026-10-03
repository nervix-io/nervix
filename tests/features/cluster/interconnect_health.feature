Feature: Interconnect health coordination

  Scenario: A connected quorum completes controls while its leader cannot reach one follower
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA quorum_event ( id I64 );
      CREATE RELAY quorum_events_1 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_2 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_3 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_4 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_5 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_6 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_7 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_8 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_9 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_10 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_11 SCHEMA quorum_event UNBRANCHED;
      CREATE RELAY quorum_events_12 SCHEMA quorum_event UNBRANCHED;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least 3 distinct owners
    When runtime preparation on node "node-3" is paused
    And application health probes from node "node-1" to node "node-3" fail
    Then node "node-1" eventually reports interconnect to "node-3" as "unavailable"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    And within "30s" node "node-1" reports no scheduled work on "node-3"
    And node "node-3" reaches its runtime preparation pause
    And node "node-2" sees node "node-3" live
    And within "30s" these NSPL commands complete on node "node-2"
      """
      CREATE RELAY quorum_canary SCHEMA quorum_event UNBRANCHED;
      CREATE USER quorum_admin WITH PASSWORD 'created-password';
      """
    When runtime preparation on node "node-3" is released
    And application health probes from node "node-1" to node "node-3" are restored
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually reports status containing "name=quorum_canary"

  Scenario: A partitioned peer does not evict connected peers or move their work
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA partition_event ( id I64 );
      CREATE RELAY partition_events_1 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_2 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_3 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_4 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_5 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_6 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_7 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_8 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_9 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_10 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_11 SCHEMA partition_event UNBRANCHED;
      CREATE RELAY partition_events_12 SCHEMA partition_event UNBRANCHED;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least 3 distinct owners
    And the last cluster status work on healthy nodes "node-1,node-3" is saved
    When application health responses from node "node-2" fail
    And consensus connectivity for node "node-2" is blocked
    And gossip exchanges involving node "node-2" are blocked with a "5s" send delay
    Then for "20s" healthy nodes "node-1,node-3" keep each other live and their scheduled work while node "node-2" waits at least "10s" for failover
    And within "30s" node "node-1" reports no scheduled work on "node-2"
    When gossip exchanges involving node "node-2" are restored
    And consensus connectivity for node "node-2" is restored

  Scenario: A stopped peer does not evict connected peers or move their work
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA stop_event ( id I64 );
      CREATE RELAY stop_events_1 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_2 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_3 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_4 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_5 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_6 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_7 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_8 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_9 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_10 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_11 SCHEMA stop_event UNBRANCHED;
      CREATE RELAY stop_events_12 SCHEMA stop_event UNBRANCHED;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least 3 distinct owners
    And the last cluster status work on healthy nodes "node-1,node-3" is saved
    When node "node-2" is stopped
    Then for "20s" healthy nodes "node-1,node-3" keep each other live and their scheduled work after node "node-2" stops
    And within "30s" node "node-1" reports no scheduled work on "node-2"

  Scenario: A silent peer does not delay peer health or control-plane work
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA health_event ( id I64 );
      CREATE RELAY baseline_health_events SCHEMA health_event UNBRANCHED;
      START;
      """
    And health requests from node "node-1" to node "node-2" pause before responding
    And health requests from node "node-1" to node "node-3" pause before responding
    Then the health response pause from node "node-1" to node "node-2" is reached
    And within "30s" the health response pause from node "node-1" to node "node-3" is reached
    When the health response pause from node "node-1" to node "node-3" is released
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And within "9s" these NSPL commands complete on node "node-1"
      """
      CREATE USER health_probe_admin WITH PASSWORD 'created-password';
      CREATE RELAY independent_health_events SCHEMA health_event UNBRANCHED;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "relay" "independent_health_events" is saved as placeholder "independent_health_owner"
    When the health response pause from node "node-1" to node "node-2" is released
    Then node "node-1" eventually observes a stable leader
