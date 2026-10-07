@restarted_voter_observation
Feature: Restarted voter observation before automatic failover

  Scenario: A voter first heard through a relayed heartbeat keeps its ownership after cluster restart
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA restart_event ( id I64 );
      CREATE RELAY restart_events SCHEMA restart_event UNBRANCHED;
      START;
      RELOCATE RELAY restart_events ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      kind=relay name=restart_events owner=node-3
      """
    When the cluster restarts with voter "node-3"'s first heartbeat relayed to node-1
    Then node-1 reaches automatic scheduling with the relayed voter marked unavailable
    When node-1 completes that scheduling pass and receives further voter heartbeats
    Then node "node-1" sees node "node-3" live
    When these NSPL commands are executed on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      kind=relay name=restart_events owner=node-3
      """
