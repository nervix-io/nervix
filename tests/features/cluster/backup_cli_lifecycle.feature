@backup_cli_lifecycle
Feature: Scenario ownership of backup CLI processes

  Scenario Outline: Cancelling a paused background backup ends its CLI process
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    And a fresh backup execution reference is saved as placeholder "backup_reference"
    When the CLI begins backing up "domain {{domain}} --timeout 5m --execution-reference {{backup_reference}}" from node "{{leader}}" into "cancelled.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When the scenario cancels its background CLI backup
    Then the cancelled background CLI process has ended
    When the backup cut pause for domain "{{domain}}" is released
    Then node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"
    When the CLI backs up "domain {{domain}} --timeout 5m --execution-reference {{backup_reference}}" from node "{{leader}}" into "recovered.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And the CLI output contains "{{backup_reference}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
