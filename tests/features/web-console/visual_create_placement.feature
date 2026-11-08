@visual_create_placement @slots_3
Feature: A domain-wide placement rule is created through the console

  Scenario Outline: A placement popup previews and submits a domain-wide rule
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is pressed with "Enter"
    And selector ".create-menu [data-create-kind='placement']" is pressed with "Enter"
    Then selector ".create-dialog" contains "Create placement"
    And selector ".create-scope" contains "{{domain}}"
    And selector ".create-placement-coverage" contains "Entire domain"
    When selector ".create-name" is filled with "domain_policy"
    And selector ".create-placement-options [data-value='PREFER COLOCATION']" is pressed with "Enter"
    And selector ".create-placement-rank" is filled with "2"
    Then selector ".create-preview" contains "CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION RANK 2;"
    When selector ".create-submit" is pressed with "Enter"
    Then selector ".create-status" contains "Completed"
    And selector ".terminal" contains "CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION RANK 2;"
    When selector ".create-close" is pressed with "Enter"
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT domain_policy;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT domain_policy DOMAIN WIDE PREFER COLOCATION RANK 2;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
