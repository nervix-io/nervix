Feature: The web console keeps uncertain commands after unreadable replies
  @console_unreadable_reply
  Scenario Outline: An unreadable REPL reply interrupts the session and recovers its command once
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node with the reply fixture
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    Given the next reply to console command "CREATE SCHEMA answered_once ( value I64 );" is unreadable as "<failure>"
    When selector ".prompt-row input" is filled with "CREATE SCHEMA answered_once ( value I64 );"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "<diagnostic>"
    And the reply fixture observed at least 2 attempts from the unreadable reply under the original execution reference
    And selector ".terminal" contains "protocol violation"
    And selector ".terminal" contains "quiesce level: DYNAMIC; planned relocations: 0" exactly 1 times
    And the web console never held two session connections at once

    Examples:
      | cluster_size | failure | diagnostic                        |
      | 1            | decode  | failed to decode a server message |
      | 3            | decode  | failed to decode a server message |
      | 1            | split   | failed to reassemble a reply      |
      | 3            | split   | failed to reassemble a reply      |

  @console_unreadable_reply
  Scenario Outline: An unreadable visual Create reply interrupts the session and recovers its command once
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node with the reply fixture
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".prompt-row" contains "{{domain}}"
    Given the next reply to console command "CREATE RESOURCE answered_bundle;" is unreadable as "<failure>"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='resource']" is pressed with "Enter"
    And selector ".create-name" is filled with "answered_bundle"
    Then selector ".create-preview" contains "CREATE RESOURCE answered_bundle;"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".terminal" contains "<diagnostic>"
    And the reply fixture observed at least 2 attempts from the unreadable reply under the original execution reference
    And selector ".create-status" contains "Completed"
    And selector ".terminal" contains "created resource 'answered_bundle'" exactly 1 times
    And selector ".resource-dialog" contains "answered_bundle"
    And the web console never held two session connections at once


    Examples:
      | cluster_size | failure | diagnostic                        |
      | 1            | decode  | failed to decode a server message |
      | 3            | decode  | failed to decode a server message |
      | 1            | split   | failed to reassemble a reply      |
      | 3            | split   | failed to reassemble a reply      |

  @console_unreadable_restore
  Scenario Outline: A restore reply naming another execution leaves the restore outcome unknown
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item ( id I64 );
      CREATE RELAY items SCHEMA item UNBRANCHED;
      """
    When the web console is opened on the leader node with the reply fixture
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".backup-menu-button" is clicked
    And selector ".backup-destination" is filled with "source.nvxb"
    And selector ".backup-submit" is clicked and the web console's download is saved as backup archive "source.nvxb"
    Then selector ".backup-status" contains "downloaded 'source.nvxb'"
    When selector ".backup-tab[data-tab='restore']" is clicked
    And selector ".restore-file" is given backup archive "source.nvxb"
    And selector ".restore-domain" is filled with "{{domain}}"
    And selector ".restore-target" is filled with "{{domain}}_copy"
    And selector ".restore-dry-run" is clicked
    Then selector ".restore-status" contains "dry run planned"
    Given the next reply to console command "RESTORE DOMAIN {{domain}} AS {{domain}}_copy FROM 'source.nvxb';" is unreadable as "restore"
    When selector ".restore-submit" is clicked
    Then the reply fixture observed at least 1 attempts from the unreadable reply under the original execution reference
    And selector ".restore-status" contains "unknown"
    # Once the call has ended as Unknown, it has made no attempt after the substituted reply.
    And the reply fixture observed at least 1 attempts from the unreadable reply under the original execution reference
    And selector ".restore-error" contains "the outcome of restore '{{own_execution}}' is unknown"
    And selector ".restore-error" contains "{{foreign_execution}}"
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      domain: {{domain}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
