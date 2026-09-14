Feature: Domain creation
  Scenario Outline: CREATE DOMAIN creates a stopped unpaced domain
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    Then the last command output contains
      """
      created domain '{{domain}}'
      """
    When these NSPL commands are executed on node "node-1"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      domain: {{domain}}
      status: stopped
      """
    When these NSPL commands are executed on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      {{domain}} status=Stopped pace=UNPACED
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @command_completion
  Scenario: A created stopped domain is immediately visible through every follower
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on node "node-2"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      domain: {{domain}}
      status: stopped
      """
    When these NSPL commands are executed on node "node-3"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      domain: {{domain}}
      status: stopped
      """
