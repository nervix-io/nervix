Feature: The web console recovers a command answered with a reply of another kind
  @console_reply_kind
  Scenario Outline: A REPL command answered with a reply of another kind recovers its own execution on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    Given the next successful reply to console command "CREATE SCHEMA answered_once ( value I64 );" is a reply of another kind
    When selector ".prompt-row input" is filled with "CREATE SCHEMA answered_once ( value I64 );"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "protocol violation: the server answered with a reply of another kind; pending command outcomes are unknown"
    And selector ".terminal" contains "quiesce level: DYNAMIC; planned relocations: 0" exactly 1 times
    And the console command recovered its own execution reference after a reply of another kind
    And the web console never held two session connections at once

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @console_reply_kind
  Scenario Outline: A visual Create answered with a reply of another kind recovers its own execution on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    Given the next successful reply to console command "CREATE RESOURCE answered_bundle;" is a reply of another kind
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    And selector ".create-name" is filled with "answered_bundle"
    Then selector ".create-preview" contains "CREATE RESOURCE answered_bundle;"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".terminal" contains "protocol violation: the server answered with a reply of another kind; pending command outcomes are unknown"
    And selector ".create-status" contains "Completed"
    And selector ".terminal" contains "created resource 'answered_bundle'" exactly 1 times
    And selector ".resource-dialog" contains "answered_bundle"
    And the console command recovered its own execution reference after a reply of another kind
    And the web console never held two session connections at once

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
