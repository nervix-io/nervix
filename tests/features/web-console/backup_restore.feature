Feature: Web console backup and restore
  @console_backup_download
  Scenario Outline: A console backup downloads a verified archive whose manifest lists the backed-up domains
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item ( id I64 );
      CREATE RELAY items SCHEMA item UNBRANCHED;
      CREATE UNPACED DOMAIN {{domain}}_other;
      """
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".backup-menu-button" is clicked
    Then selector ".backup-dialog" contains "Back up"
    When selector ".backup-scope-cluster" is clicked
    And selector ".backup-destination" is filled with "console-cluster.nvxb"
    Then selector ".backup-preview" contains "BACKUP CLUSTER TO 'console-cluster.nvxb';"
    When selector ".backup-submit" is clicked and the web console's download is saved as backup archive "console-cluster.nvxb"
    Then selector ".backup-status" contains "downloaded 'console-cluster.nvxb'"
    And backup archive "console-cluster.nvxb" has the size and digest selector ".backup-status" shows
    And selector ".backup-summary" contains "{{domain}}"
    And selector ".backup-summary" contains "{{domain}}_other"
    And selector ".terminal" contains "BACKUP CLUSTER TO 'console-cluster.nvxb';"
    And selector ".terminal" contains "backed up the cluster: 2 domains"
    And selector ".terminal" contains "archive downloaded as 'console-cluster.nvxb'"
    When the CLI describes backup archive "console-cluster.nvxb" as text
    Then the CLI output contains "scope: cluster"
    And the CLI output contains "- domain={{domain}} "
    And the CLI output contains "- domain={{domain}}_other "

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @console_backup_reload
  Scenario Outline: A console backup download interrupted by a page reload resumes after the reload
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item ( id I64 );
      CREATE RELAY items SCHEMA item UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    Given backup archive downloads on node "{{leader}}" pause before the archive streams
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".backup-menu-button" is clicked
    And selector ".backup-destination" is filled with "resumed.nvxb"
    Then selector ".backup-preview" contains "BACKUP DOMAIN {{domain}} TO 'resumed.nvxb';"
    When selector ".backup-submit" is clicked
    Then the backup archive download pause on node "{{leader}}" is reached
    And selector ".backup-status" contains "downloading 0 of"
    When the web console page is reloaded and its next download is saved as backup archive "resumed.nvxb"
    Then selector ".backup-status" contains "downloaded 'resumed.nvxb'"
    And backup archive "resumed.nvxb" has the size and digest selector ".backup-status" shows
    And selector ".terminal" contains "resuming backup"
    When the backup archive download pause on node "{{leader}}" is released
    And the CLI describes backup archive "resumed.nvxb" as text
    Then the CLI output contains "- domain={{domain}} "

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @console_restore
  Scenario Outline: A console restore uploads its archive with progress, shows the dry run, and restores the domain
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item ( id I64 );
      CREATE RELAY items SCHEMA item UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "source.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".backup-menu-button" is clicked
    And selector ".backup-tab[data-tab='restore']" is clicked
    And selector ".restore-file" is given backup archive "source.nvxb"
    And selector ".restore-domain" is filled with "{{domain}}"
    And selector ".restore-target" is filled with "{{domain}}_copy"
    Then selector ".restore-dry-run-preview" contains "RESTORE DOMAIN {{domain}} AS {{domain}}_copy FROM 'source.nvxb' DRY RUN;"
    And selector ".restore-preview" contains "RESTORE DOMAIN {{domain}} AS {{domain}}_copy FROM 'source.nvxb';"
    When selector ".restore-dry-run" is clicked
    Then selector ".restore-status" contains "dry run planned"
    And selector ".restore-progress-text" reports backup archive "source.nvxb" sent in full
    And selector ".restore-plan" contains "{{domain}} as {{domain}}_copy"
    And selector ".restore-plan" contains "create domain '{{domain}}_copy': planned"
    And selector ".restore-impact .inspector-item[data-name='items']" exists
    And selector ".terminal" contains "RESTORE DOMAIN {{domain}} AS {{domain}}_copy FROM 'source.nvxb' DRY RUN;"
    When selector ".restore-submit" is clicked
    Then selector ".restore-status" contains "restored"
    And selector ".restore-result" contains "create domain '{{domain}}_copy': applied"
    And selector ".restore-result" contains "apply models of domain '{{domain}}_copy': applied"
    And selector ".terminal" contains "RESTORE DOMAIN {{domain}} AS {{domain}}_copy FROM 'source.nvxb';"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}}_copy pace=UNPACED status=STOPPED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @console_restore_refusals
  Scenario Outline: The console shows a restore's typed refusals and the steps a failed restore applied
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA zip_code_entry ( zip STRING, city STRING );
      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT ( zip string, city string );
      CREATE CODEC zip_code_entry_codec FROM WIRE JSON SCHEMA zip_code_entry_wire TO SCHEMA zip_code_entry;
      CREATE HASH MAP zip_codes_by_zip KEY zip FROM RESOURCE zip_codes VERSION 1 PATH 'lookup.jsonl' DECODE USING zip_code_entry_codec;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "domain.nvxb" is copied to "corrupted.nvxb" with one byte of version 1 of resource "zip_codes" in domain "{{domain}}" changed
    And backup archive "domain.nvxb" is copied to "missing_path.nvxb" with "PATH 'lookup.jsonl'" replaced by "PATH 'missing.jsonl'" in the models of domain "{{domain}}"
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".backup-menu-button" is clicked
    And selector ".backup-tab[data-tab='restore']" is clicked
    And selector ".restore-file" is given backup archive "domain.nvxb"
    And selector ".restore-domain" is filled with "{{domain}}"
    And selector ".restore-dry-run" is clicked
    Then selector ".restore-error" contains "domain '{{domain}}' already exists; restore it AS another name"
    And selector ".terminal" contains "domain '{{domain}}' already exists; restore it AS another name"
    When selector ".restore-target" is filled with "{{domain}}_copy"
    And selector ".restore-file" is given backup archive "corrupted.nvxb"
    And selector ".restore-dry-run" is clicked
    Then selector ".restore-error" contains "the archive is not a valid backup archive"
    When selector ".restore-file" is given backup archive "missing_path.nvxb"
    And selector ".restore-dry-run" is clicked
    Then selector ".restore-status" contains "dry run planned"
    When selector ".restore-submit" is clicked
    Then selector ".restore-status" contains "restore failed"
    And selector ".restore-error" contains "restore failed at step 'apply models of domain '{{domain}}_copy''"
    And selector ".restore-result" contains "create domain '{{domain}}_copy': applied"
    And selector ".restore-result" contains "import resource versions of domain '{{domain}}_copy': applied"
    And selector ".restore-result" contains "apply models of domain '{{domain}}_copy': failed"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}}_copy pace=UNPACED status=STOPPED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @console_backup_shared_session
  Scenario: The REPL and the backup controls share one console session and its leader redirect
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item ( id I64 );
      CREATE RELAY items SCHEMA item UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the web console is opened on node "{{follower}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{leader}}'"
    When selector ".prompt-row input" is filled with "BACKUP DOMAIN {{domain}} TO 'typed.nvxb';"
    And selector ".prompt-row input" is pressed with "Enter" and the web console's download is saved as backup archive "typed.nvxb"
    Then selector ".backup-status" contains "downloaded 'typed.nvxb'"
    And selector ".terminal" contains "BACKUP DOMAIN {{domain}} TO 'typed.nvxb';"
    And selector ".terminal" contains "archive downloaded as 'typed.nvxb'"
    When selector ".prompt-row input" is filled with "RESTORE DOMAIN {{domain}} AS {{domain}}_typed FROM 'typed.nvxb';"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".restore-status" contains "choose the archive"
    When selector ".restore-file" is given backup archive "typed.nvxb"
    Then selector ".restore-preview" contains "RESTORE DOMAIN {{domain}} AS {{domain}}_typed FROM 'typed.nvxb';"
    When selector ".restore-dry-run" is clicked
    Then selector ".restore-status" contains "dry run planned"
    When selector ".restore-submit" is clicked
    Then selector ".restore-status" contains "restored"
    And selector ".terminal" contains "connected to leader '{{leader}}'" exactly 1 times
    And selector ".terminal" does not contain "redirected"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}}_typed pace=UNPACED status=STOPPED
      """
