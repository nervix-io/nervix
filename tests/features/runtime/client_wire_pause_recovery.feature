@client_wire_pause_recovery
Feature: Transaction-owned domain pause recovery
  Scenario Outline: A completed schema alteration releases its domain before the next command on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA pause_event (key I64);
      CREATE RELAY pause_input SCHEMA pause_event UNBRANCHED;
      START;
      BEGIN;
      ALTER SCHEMA pause_event ADD FIELD note STRING OPTIONAL;
      COMMIT;
      """
    When these Plexus statements are executed on the leader node
      """
      ALTER SCHEMA pause_event ADD FIELD next_note STRING OPTIONAL;
      """
    Then the last command output contains
      """
      pause_event
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_pause_leadership @deloxide_stress
  Scenario: A refused pause release remains recoverable when its coordinator regains leadership
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      BEGIN;
      CREATE SCHEMA pause_event (key I64);
      CREATE RELAY pause_input SCHEMA pause_event UNBRANCHED;
      START;
      ALTER SCHEMA pause_event ADD FIELD note STRING OPTIONAL;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "transaction_id"
    Given domain resume on node "{{old_leader}}" pauses at its proposal boundary
    And domain resume on node "{{old_leader}}" pauses at its failure boundary
    And domain resume on node "{{new_leader}}" pauses at its proposal boundary
    When the active session begins this Plexus command request with execution reference "pause-commit-{{test_id}}" in the background
      """
      COMMIT;
      """
    Then domain resume on node "{{old_leader}}" reaches its proposal boundary
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    And domain resume on node "{{new_leader}}" reaches its proposal boundary
    When domain resume on node "{{old_leader}}" leaves its proposal boundary
    Then domain resume on node "{{old_leader}}" reaches its failure boundary
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{old_leader}}"
    When domain resume on node "{{old_leader}}" leaves its failure boundary
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    And transaction "{{transaction_id}}" report step 3 eventually records planned quiesce "DOMAIN_PAUSE", actual quiesce "DOMAIN_PAUSE", execution "APPLIED", and outcomes "REQUESTED,CONFIRMED,FAILED,RELEASED"
    And the background command request succeeds
    When domain resume on node "{{new_leader}}" leaves its proposal boundary
    And this Plexus command request with execution reference "pause-commit-{{test_id}}" and transaction position 4 is replayed on the leader node
      """
      COMMIT;
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "pause-commit-{{test_id}}" and transaction position 4 is replayed on the leader node
      """
      COMMIT;
      """
    Then the last command request succeeded
    When these Plexus statements are executed on the leader node
      """
      ALTER SCHEMA pause_event ADD FIELD next_note STRING OPTIONAL;
      """
    Then the last command output contains
      """
      pause_event
      """
