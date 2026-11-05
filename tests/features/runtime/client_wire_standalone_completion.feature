@client_wire_standalone_completion
Feature: Standalone command completion after another coordinator commits
  Scenario Outline: A command recovers its recorded completion after a failed commit attempt on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA recovery_event (key I64);
      CREATE RELAY recovery_input SCHEMA recovery_event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "original_leader"
    Given standalone preparation on node "{{original_leader}}" pauses at its commit_failure boundary
    And the leader refuses the next model publication in domain "{{domain}}" and the pause release after it with "a saturated consensus log"
    When this Plexus command request with execution reference "completed-command-{{test_id}}" begins executing in the background on the leader node
      """
      CREATE RELAY recovered_command SCHEMA recovery_event UNBRANCHED;
      """
    Then standalone preparation on node "{{original_leader}}" reaches its commit_failure boundary
    And the refusal of the model publication in domain "{{domain}}" and of its pause release was delivered
    And the transaction targeted by execution reference "completed-command-{{test_id}}" is saved as placeholder "transaction_id"
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When standalone preparation on node "{{original_leader}}" leaves its commit_failure boundary
    Then the background command request succeeds
    When this Plexus command request with execution reference "completed-command-{{test_id}}" is executed on the leader node
      """
      CREATE RELAY recovered_command SCHEMA recovery_event UNBRANCHED;
      """
    Then the last command request succeeded
    When these Plexus statements are executed on the leader node
      """
      SHOW CREATE RELAY recovered_command;
      """
    Then the last command output contains
      """
      SCHEMA recovery_event
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
