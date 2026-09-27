Feature: Web console visual creation
  @visual_create_happy
  Scenario Outline: Popup forms create domains, users, and resource catalogs through the REPL dispatcher
    Given a <cluster_size> node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"

    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='domain']" is pressed with "Enter"
    Then selector ".create-dialog" contains "Create domain"
    And selector ".create-pace-options" contains "UNPACED"
    And selector ".create-pace-options" contains "PACED"
    And selector ".create-placement-options" contains "PREFER COLOCATION"
    When selector ".create-name" is filled with "{{domain}}"
    And selector ".create-placement-options [data-value='PREFER COLOCATION']" is pressed with "Enter"
    And selector ".create-if-not-exists" is clicked
    Then selector ".create-preview" contains "CREATE IF NOT EXISTS UNPACED DOMAIN {{domain}} PLACEMENT PREFER COLOCATION;"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Completed"
    And selector ".terminal" contains "CREATE IF NOT EXISTS UNPACED DOMAIN {{domain}} PLACEMENT PREFER COLOCATION;"
    And selector ".terminal" contains "created domain '{{domain}}'"
    When selector ".create-close" is pressed with "Enter"

    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='user']" is pressed with "Enter"
    Then selector ".create-dialog" contains "Create user"
    When selector ".create-name" is filled with "visual_user"
    And selector ".create-password" is filled with "visual-secret"
    Then selector ".create-preview" contains "CREATE USER visual_user WITH PASSWORD '********';"
    And selector ".create-preview" does not contain "visual-secret"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Completed"
    And selector ".terminal" contains "CREATE USER visual_user WITH PASSWORD '********';"
    And selector ".terminal" does not contain "visual-secret"
    And selector ".transaction-indicator" does not exist
    When selector ".create-close" is pressed with "Enter"

    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    Then selector ".create-dialog" contains "Create resource"
    And selector ".create-scope" contains "{{domain}}"
    When selector ".create-name" is filled with "visual_bundle"
    Then selector ".create-preview" contains "CREATE RESOURCE visual_bundle;"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Completed"
    And selector ".resource-dialog" contains "visual_bundle"
    And selector ".resource-dialog" contains "VERSIONS"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_create_rejected
  Scenario: A rejected visual create keeps its draft and inline failure
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE retained_bundle;
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    And selector ".create-name" is filled with "retained_bundle"
    And selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "already exists"
    And selector ".create-name" has value "retained_bundle"
    And selector ".create-submit" exists

  Scenario: Invalid input stays local and a newer control search fences stale choices
    Given a 1 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='domain']" is pressed with "Enter"
    And selector ".create-pace-options .create-choice-search" is filled with "wall time"
    And selector ".create-pace-options .create-choice-search" is filled with "receives progress"
    Then selector ".create-pace-options [data-value='UNPACED']" exists
    And selector ".create-pace-options [data-value='PACED']" does not exist
    When selector ".create-name" is filled with "not a domain"
    And selector ".create-submit" is pressed with "Enter"
    Then selector ".create-validation" contains "Domain name"
    And selector ".create-name" has value "not a domain"
    And selector ".terminal" does not contain "CREATE DOMAIN not a domain"

  Scenario: A retained resource draft changes its captured domain only by explicit action
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE DOMAIN {{domain}}_other;
      """
    And the web console is opened on the leader node
    Then selector ".domain-select" contains "{{domain}}"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    And selector ".create-name" is filled with "scoped_bundle"
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-close" is pressed with "Enter"
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    Then selector ".domain-select" contains "{{domain}}_other"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    Then selector ".create-name" has value "scoped_bundle"
    And selector ".create-scope" contains "{{domain}}"
    And selector ".create-scope-change" exists
    When selector ".create-scope-change" is pressed with "Enter"
    Then selector ".create-scope" contains "{{domain}}_other"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Completed"
    And selector ".terminal" contains "created resource 'scoped_bundle'"

  Scenario: A resource create reports that it is queued in the attached transaction
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    And selector ".create-name" is filled with "staged_bundle"
    And selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Queued in transaction"
    And selector ".create-status" contains "position 1"
    And selector ".resource-dialog" does not exist
    When selector ".create-close" is pressed with "Enter"
    And selector ".transaction-indicator" is pressed with "Enter"
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    And selector ".transaction-inspector" contains "staged_bundle"

  Scenario: One admitted popup create survives a leader change without duplicate effects
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given command response delivery on node "{{old_leader}}" pauses after execution
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    And selector ".create-name" is filled with "recovered_bundle"
    And selector ".create-submit" is pressed with "Enter"
    Then the command response delivery pause on node "{{old_leader}}" is reached
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    And the command response delivery pause on node "{{old_leader}}" is released
    Then selector ".terminal" contains "connected to leader '{{new_leader}}'"
    And selector ".create-status" contains "Completed"
    And selector ".terminal" contains "created resource 'recovered_bundle'" exactly 1 times
    And selector ".resource-dialog" contains "recovered_bundle"
