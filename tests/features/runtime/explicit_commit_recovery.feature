Feature: An explicit COMMIT reports its durable transaction outcome

  @explicit_commit_recovery
  Scenario Outline: An obsolete failed COMMIT attempt reports the commit another coordinator finished
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA explicit_commit_event (key I64);
      CREATE RELAY explicit_commit_input SCHEMA explicit_commit_event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "owner" is connected to node "{{leader}}"
    When client "owner" executes these Plexus commands
      """
      BEGIN;
      ALTER SCHEMA explicit_commit_event ADD FIELD recovered I64 OPTIONAL;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the leader refuses the next model publication in domain "{{domain}}" and the pause release after it with "a saturated consensus log"
    And explicit COMMIT for transaction "{{transaction_id}}" on node "{{leader}}" pauses after an execution error
    When client "owner" begins executing these Plexus commands in the background
      """
      COMMIT;
      """
    Then the explicit COMMIT failure pause for transaction "{{transaction_id}}" on node "{{leader}}" is reached
    And the refusal of the model publication in domain "{{domain}}" and of its pause release was delivered
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When the explicit COMMIT failure pause for transaction "{{transaction_id}}" on node "{{leader}}" is released
    Then the background Plexus execution succeeds
    And client "owner" transaction state is "COMMITTED"
    When client "owner" executes these Plexus commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}';
      """
    Then the last inspection reports
      """
      transaction: {{transaction_id}}
      state: COMMITTED
      accepted operations: 1
      applied operations: 1
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE SCHEMA explicit_commit_event;
      """
    Then the last command output contains
      """
      recovered I64 OPTIONAL
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
