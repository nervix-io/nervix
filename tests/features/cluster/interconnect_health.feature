@exclusive
Feature: Interconnect health coordination

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
    And within "750ms" the health response pause from node "node-1" to node "node-3" is reached
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
