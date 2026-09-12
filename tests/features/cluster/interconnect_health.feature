Feature: Interconnect health coordination

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
    And within "750ms" these NSPL commands complete on node "node-1"
      """
      CREATE USER health_probe_admin WITH PASSWORD 'created-password';
      """
    And within "750ms" these NSPL commands complete on node "node-1"
      """
      CREATE RELAY independent_health_events SCHEMA health_event UNBRANCHED;
      """
    And within "750ms" these NSPL commands complete on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "relay" "independent_health_events" is saved as placeholder "independent_health_owner"
    When the health response pause from node "node-1" to node "node-2" is released
    Then node "node-1" eventually observes a stable leader
