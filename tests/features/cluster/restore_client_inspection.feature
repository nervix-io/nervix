@restore_client_inspection @deloxide_stress_restore
Feature: Accepted restores wait for client inspection capacity

  Scenario Outline: An accepted restore waits for client inspection capacity on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = 'http://127.0.0.1:19080',
        'timeout_ms' = 5000
      };
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "client.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given command admission on node "{{leader}}" pauses before proposal
    When restore "DOMAIN {{domain}} AS {{domain}}_copy" of backup archive "client.nvxb" is streamed to node "{{leader}}" under execution reference "{{test_id}}-inspection" in the background
    Then the restoring model batch pauses on node "{{leader}}"
    Given client inspection storage admission for domain "{{domain}}_copy" is observed
    When filesystem workers and their queue on node "{{leader}}" are held full
    And the command admission pause on node "{{leader}}" is released
    Then client inspection of domain "{{domain}}_copy" has polled its storage admission
    When the scenario releases its client inspection capacity
    Then the background restore stream completes with a known outcome
    And the restore stream's report shows every step applied
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE CLIENT api;
      """
    Then the last command output contains
      """
      CREATE HTTP CLIENT api
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
