Feature: Application progress by request

  Command completion reads the application progress of every live node over the interconnect.
  A node pushes its progress to the leader when a phase advances, and a revision barrier queries
  a node whose progress it has not received. Each answer belongs to one process incarnation.

  @command_completion
  Scenario Outline: A node answers the revisions it applied, and a restarted node answers only its new incarnation, on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA progress_event ( id I64 );
      START;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE RELAY progress_events SCHEMA progress_event UNBRANCHED;
      """
    Then the last command request succeeded
    And within "20s" node "<node>" answers node "node-1" the revisions node "node-1" made ready
    When the application progress incarnation node "<node>" answers node "node-1" is saved
    And node "<node>" is stopped
    And runtime preparation on node "<node>" is paused
    And node "<node>" is started without waiting for it to catch up
    Then node "<node>" reaches its runtime preparation pause
    And within "30s" node "<node>" answers node "node-1" a new incarnation that prepared and readied no runtime revision
    When runtime preparation on node "<node>" is released
    Then within "30s" node "<node>" answers node "node-1" the revisions node "node-1" made ready

    Examples:
      | nodes | node   |
      | 1     | node-1 |
      | 3     | node-2 |

  @command_completion @slots_2
  Scenario Outline: A relay creation whose progress reports and revision replies are both held completes once its <released> are released
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA progress_event ( id I64 );
      START;
      """
    And client "progress" is connected to node "node-1"
    And application progress reports from node "node-2" pause before sending
    And application progress reports from node "node-3" pause before sending
    And application revision responses from node "node-2" pause before responding
    And application revision responses from node "node-3" pause before responding
    When client "progress" begins executing these Plexus commands in the background
      """
      CREATE RELAY held_progress_events SCHEMA progress_event UNBRANCHED;
      """
    Then within "30s" the application progress report pause on node "node-2" is reached
    And within "30s" the application progress report pause on node "node-3" is reached
    And the application revision response pause on node "node-2" is reached
    And the application revision response pause on node "node-3" is reached
    When <released> from node "node-2" are released
    And <released> from node "node-3" are released
    Then within "20s" the background Plexus execution succeeds after the release
    When <held> from node "node-2" are released
    And <held> from node "node-3" are released
    Then node "node-1" eventually observes a stable leader

    Examples:
      | released                       | held                           |
      | application progress reports   | application revision responses |
      | application revision responses | application progress reports   |
