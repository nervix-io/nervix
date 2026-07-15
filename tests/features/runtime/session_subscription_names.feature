Feature: Named session subscriptions
  Scenario Outline: Named subscriptions span domains and unsubscribe by name
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN secondary_{{test_id}};
      """
    When these NSPL commands are executed on the active session
      """
      CREATE SCHEMA event ( value STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      CREATE SUBSCRIPTION primary_events TO events;
      """
    Then the last command output contains
      """
      created subscription 'primary_events' in domain '{{domain}}'
      """
    When the active session targets domain "secondary_{{test_id}}"
    And these NSPL commands are executed on the active session
      """
      CREATE SCHEMA event ( value STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      CREATE SUBSCRIPTION secondary_events TO events;
      """
    Then the last command output contains
      """
      created subscription 'secondary_events' in domain 'secondary_{{test_id}}'
      """
    When these NSPL commands fail on the active session
      """
      CREATE SUBSCRIPTION primary_events TO events;
      """
    Then the last command error contains
      """
      session subscription 'primary_events' already exists
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION primary_events;
      """
    Then the last command output contains
      """
      deleted subscription 'primary_events' from domain '{{domain}}'
      """
    When these NSPL commands are executed on the active session
      """
      CREATE SUBSCRIPTION primary_events TO events;
      """
    Then the last command output contains
      """
      created subscription 'primary_events' in domain 'secondary_{{test_id}}'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Subscription names are local to each session
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      """
    When these NSPL commands are executed on the active session
      """
      CREATE SUBSCRIPTION events_view TO events;
      """
    Then the last command output contains
      """
      created subscription 'events_view' in domain '{{domain}}'
      """
    When a new session executes these NSPL commands
      """
      CREATE SUBSCRIPTION events_view TO events;
      """
    Then the last command output contains
      """
      created subscription 'events_view' in domain '{{domain}}'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
