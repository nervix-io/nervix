Feature: Applying commands retain their transaction outcomes
  @client_wire_tombstone_retention @deloxide_stress_transaction_retention
  Scenario Outline: An applying command keeps its transaction through a retention sweep
    Given the transaction tombstone retention is configured as "2s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN cleanup_{{test_id}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command finalization on node "{{leader}}" pauses before its outcome is recorded
    When the active session begins this Plexus command request with execution reference "retained-{{test_id}}" in the background
      """
      <request>
      """
    Then the command finalization pause on node "{{leader}}" is reached
    And the transaction targeted by execution reference "retained-{{test_id}}" is saved as placeholder "held_transaction"
    Given client "cleanup" is connected to node "{{leader}}"
    When client "cleanup" selects domain "cleanup_{{test_id}}"
    And client "cleanup" executes these Plexus statements
      """
      BEGIN;
      """
    Then client "cleanup" transaction id is saved as placeholder "cleanup_transaction"
    When client "cleanup" executes these Plexus statements
      """
      REVERT;
      """
    Then transaction "{{cleanup_transaction}}" is eventually removed
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      id={{held_transaction}}
      """
    And the last command output contains
      """
      state=COMMITTED
      """
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE TRANSACTION '{{held_transaction}}';
      """
    Then the last command output contains
      """
      state: COMMITTED
      """
    And the last command output contains
      """
      outcome APPLIED
      """
    When the command finalization pause on node "{{leader}}" is released
    Then the background command request succeeds
    When this Plexus command request with execution reference "retained-{{test_id}}" is executed on the leader node
      """
      <request>
      """
    Then the last command request succeeded
    And transaction "{{held_transaction}}" is eventually removed

    Examples:
      | cluster_size | request                                           |
      | 1            | CREATE RESOURCE retained_resource;                |
      | 3            | CREATE RESOURCE retained_resource;                |
      | 1            | BEGIN; CREATE RESOURCE retained_resource; COMMIT; |
      | 3            | BEGIN; CREATE RESOURCE retained_resource; COMMIT; |

  @client_wire_tombstone_recovery @deloxide_stress_transaction_retention
  Scenario Outline: Leadership recovery uses a finished transaction beyond its age retention
    Given the transaction tombstone retention is configured as "2s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN cleanup_{{test_id}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given command finalization on node "{{old_leader}}" pauses before its outcome is recorded
    When the active session begins this Plexus command request with execution reference "recovered-{{test_id}}" in the background
      """
      <request>
      """
    Then the command finalization pause on node "{{old_leader}}" is reached
    And the transaction targeted by execution reference "recovered-{{test_id}}" is saved as placeholder "held_transaction"
    Given client "cleanup" is connected to node "{{old_leader}}"
    When client "cleanup" selects domain "cleanup_{{test_id}}"
    And client "cleanup" executes these Plexus statements
      """
      BEGIN;
      """
    Then client "cleanup" transaction id is saved as placeholder "cleanup_transaction"
    When client "cleanup" executes these Plexus statements
      """
      REVERT;
      """
    Then transaction "{{cleanup_transaction}}" is eventually removed
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When the command finalization pause on node "{{old_leader}}" is released
    Then the background command request reports an unknown outcome because leadership moved
    When this Plexus command request with execution reference "recovered-{{test_id}}" is executed on the leader node
      """
      <request>
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE retained_resource;
      """
    Then the last command output contains
      """
      resource: retained_resource
      """
    And transaction "{{held_transaction}}" is eventually removed

    Examples:
      | request                                           |
      | CREATE RESOURCE retained_resource;                |
      | BEGIN; CREATE RESOURCE retained_resource; COMMIT; |
