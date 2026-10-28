@backup_cut_diagnostics @deloxide_stress_restore
Feature: Attributing a quiesced backup cut's timeout

  Scenario Outline: A capture timeout reports its active phase and completed drain
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    When the CLI begins backing up "domain {{domain}} --timeout 20s --backup-wait-timeout 1m" from node "{{leader}}" into "capture-timeout.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    And the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI capture timeout reports the elapsed completed drain
    And backup archive "capture-timeout.nvxb" does not exist
    When the backup cut pause for domain "{{domain}}" is released
    Then node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
