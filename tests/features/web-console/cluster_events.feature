Feature: Web console cluster events
  The node panel beside the connection pill shows the membership and leadership notices of the
  session's node, and the REPL transcript keeps its command lines and the runtime and
  control-plane notices.

  Scenario Outline: A model change, a subscription and START add no line beside their outcomes on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA source_event ( value I64 );
      CREATE RELAY events SCHEMA source_event UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the web console is opened on node "{{leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    When selector ".prompt-row input" is filled with "START;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "started domain '{{domain}}'"
    When selector ".prompt-row input" is filled with "CREATE RELAY audit SCHEMA source_event UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "created relay 'audit'"
    When selector ".prompt-row input" is filled with "CREATE SUBSCRIPTION live_events TO events;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".subscription-tab[data-subscription-state='active']" contains "EVENTS"
    When selector ".repl-toolbar button:has-text('Plexus REPL')" is clicked
    And selector ".prompt-row input" is filled with "DELETE SUBSCRIPTION live_events;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".subscription-tab[data-subscription-name='live_events']" eventually disappears
    When selector ".prompt-row input" is filled with "LIST DOMAINS;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "{{domain}} pace=UNPACED"
    And selector ".terminal .term-line.info" shows exactly these lines
      """
      connected to leader '{{leader}}'
      """
    And selector ".node-panel-toggle[data-event-count='0']" exists

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: The node panel shows one event when a node leaves and one when it returns
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "observer"
    And a node other than placeholder "observer" is saved as placeholder "departing"
    When the web console is opened on node "{{observer}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".node-panel-toggle" is clicked
    And node "{{departing}}" is stopped
    Then selector ".node-panel" contains "node '{{departing}}' left"
    When node "{{departing}}" is started
    Then selector ".node-panel" contains "node '{{departing}}' joined (incarnation "
    And selector ".node-panel .node-event[data-occurrences='1']" contains "node '{{departing}}' left" exactly 1 times
    And selector ".terminal" contains "connected to leader '{{observer}}'"
    And selector ".terminal .term-line.info" shows exactly these lines
      """
      connected to leader '{{observer}}'
      """
