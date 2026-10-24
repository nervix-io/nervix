Feature: Whole-cluster restart keeps scheduled work in place
  A restart of the whole cluster starts every node again at once, as an operator's restart does, so
  each voter is observed live within the startup grace a newly started leader gives voters it has
  not yet observed, and none of their scheduled work is failed over.

  Scenario: A voter restarted beside a peer slow to report ready keeps its ownership after cluster restart
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
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
    When the cluster restarts while node "node-2" reports ready only after the restarted leader's startup voter observation ends
    And these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      kind=relay name=restart_events owner=node-3
      """
