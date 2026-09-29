Feature: Web console domain clock

  Scenario Outline: The selected domain clock follows its mapping, ticks, and lifecycle
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      CREATE UNPACED DOMAIN {{domain}}_other;
      """
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".domain-clock" contains "{{domain}}"
    And selector ".domain-clock" contains "STOPPED"
    And selector ".domain-clock" contains "generation 0"
    When selector ".prompt-row input" is filled with "START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".domain-clock" contains "PACED"
    And selector ".domain-clock" contains "generation 1"
    And selector ".domain-clock" contains "rate 2"
    And selector ".domain-clock" contains "period 100ms"
    And selector ".domain-clock" contains "skew 10ms"
    And selector ".domain-clock-logical" contains "2030-01-01"
    And selector ".domain-clock-logical" advances as a timestamp within 5000 milliseconds
    And selector ".domain-clock-tick-id" advances as a number within 10000 milliseconds
    And selector ".terminal" contains "domain clock [{{domain}}] tick:"
    When selector ".prompt-row input" is filled with "STOP;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".domain-clock" contains "STOPPED"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    Then selector ".domain-clock" contains "{{domain}}_other"
    And selector ".domain-clock" contains "STOPPED"
    When selector ".prompt-row input" is filled with "START;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".domain-clock" contains "{{domain}}_other"
    And selector ".domain-clock" contains "UNPACED"
    And selector ".domain-clock-logical" contains "—"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".domain-clock" contains "{{domain}}"
    And selector ".domain-clock" contains "STOPPED"
    When selector ".prompt-row input" is filled with "ATTACH DOMAIN CLOCK;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "already follows the clock"
    When selector ".prompt-row input" is filled with "DETACH DOMAIN CLOCK;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".domain-clock" contains "DETACHED"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    Then selector ".domain-clock" contains "UNPACED"
    When selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}']" is clicked
    Then selector ".domain-clock" contains "STOPPED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A reconnected console restores one attachment to its selected domain
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 500ms SKEW 10ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".domain-clock" contains "PACED"
    And selector ".domain-clock-tick-id" advances as a number within 10000 milliseconds
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".terminal" contains "connected to leader '{{new_leader}}'"
    And selector ".domain-clock" contains "PACED" for 1500 milliseconds
    And selector ".domain-clock-tick-id" advances as a number within 10000 milliseconds
    And selector ".terminal" contains "domain clock [{{domain}}] attached" exactly 2 times
