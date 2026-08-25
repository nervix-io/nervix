Feature: NSPL transactions
  Scenario: An open transaction survives leader failover and the client resumes it
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA before_failover (
        value STRING
      );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When client "owner" executes these NSPL commands
      """
      CREATE SCHEMA after_failover (
        value STRING
      );
      COMMIT;
      """
    Then the last command output contains
      """
      stored model 'after_failover'
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA before_failover;
      SHOW CREATE SCHEMA after_failover;
      """
    Then the last command output contains
      """
      CREATE SCHEMA after_failover (value STRING);
      """

  Scenario: REVERT remains available after leader failover
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    When client "owner" executes these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      BEGIN;
      CREATE SCHEMA reverted_after_failover (
        value STRING
      );
      """
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When client "owner" executes these NSPL commands
      """
      REVERT;
      """
    Then the last command output contains
      """
      transaction reverted
      """
    When these NSPL commands fail with "schema 'reverted_after_failover' does not exist"
      """
      SHOW CREATE SCHEMA reverted_after_failover;
      """

  Scenario Outline: A clean session close reverts its open transaction
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    Given client "owner" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" closes its session cleanly
    Then transaction "{{transaction_id}}" eventually has state "REVERTED"
    When client "observer" fails to attach to transaction "{{transaction_id}}"
    Then the last command error contains
      """
      finished with outcome REVERTED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A commit interrupted between steps is completed by the new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    And client "observer" is connected to node "{{new_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA resumed_commit (
        value STRING
      );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit on node "{{old_leader}}" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "{{old_leader}}" after 1 statement is reached
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When client "observer" fails to attach to transaction "{{transaction_id}}"
    Then the last command error contains
      """
      finished with outcome COMMITTED
      """
    And the last command output contains
      """
      created domain '{{domain}}'
      """
    And the last command output contains
      """
      stored model 'resumed_commit'
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA resumed_commit;
      """
    Then the last command output contains
      """
      CREATE SCHEMA resumed_commit (value STRING);
      """
    When the transaction commit pause on node "{{old_leader}}" after 1 statement is released
    Then the background NSPL execution is discarded

  Scenario: A failing resumed commit records the failing step and preserves its prefix
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    And client "observer" is connected to node "{{new_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      CREATE DOMAIN {{domain}};
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit on node "{{old_leader}}" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "{{old_leader}}" after 1 statement is reached
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    And transaction "{{transaction_id}}" eventually has state "FAILED"
    When client "observer" fails to attach to transaction "{{transaction_id}}"
    Then client "observer" transaction state is "FAILED" with failing step 2
    And the last command error contains
      """
      finished with outcome FAILED
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      domain: {{domain}}
      """
    When the transaction commit pause on node "{{old_leader}}" after 1 statement is released
    Then the background NSPL execution is discarded

  Scenario: Attaching from a second session takes over an open transaction
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Given client "owner" is connected to the leader node
    And client "taker" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "taker" attaches to transaction "{{transaction_id}}"
    And client "owner" fails to execute these NSPL commands
      """
      REVERT;
      """
    Then the last command error contains
      """
      was taken over by another session
      """
    When client "taker" executes these NSPL commands
      """
      REVERT;
      """
    Then the last command output contains
      """
      transaction reverted
      """

  Scenario Outline: Non-configuration statements are rejected while a transaction is open
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      """
    And client "owner" fails to execute these NSPL commands
      """
      <statement>
      """
    Then the last command error contains
      """
      <error>
      """

    Examples:
      | statement                                                 | error                                                       |
      | SHOW TRANSACTIONS;                                        | cannot be queued in a transaction                           |
      | DESCRIBE DOMAIN;                                          | cannot be queued in a transaction                           |
      | CREATE SUBSCRIPTION tx_view TO missing_relay;             | session-scoped and client-local statements cannot be queued |
      | UPLOAD RESOURCE local_bundle VERSION '/tmp/local_bundle'; | client-local commands are not allowed                       |
      | CORDON NODE node-1;                                       | cannot be queued in a transaction                           |
      | DROP NODE node-1;                                         | cannot be queued in a transaction                           |

  Scenario: Replicated transaction limits are enforced consistently
    Given the transaction statement limit is configured as 1
    And the transaction source byte limit is configured as 30
    And the concurrent transaction limit is configured as 1
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    Given client "owner" is connected to the leader node
    And client "other" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE RESOURCE r;
      """
    And client "other" fails to execute these NSPL commands
      """
      BEGIN;
      """
    Then the last command error contains
      """
      concurrent open transaction limit 1 reached
      """
    When client "owner" fails to execute these NSPL commands
      """
      CREATE SCHEMA exceeds_limit (
        value STRING
      );
      """
    Then the last command error contains
      """
      queued statement limit 1 reached
      """
    When client "owner" executes these NSPL commands
      """
      REVERT;
      """
    And client "other" executes these NSPL commands
      """
      BEGIN;
      """
    And client "other" fails to execute these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    Then the last command error contains
      """
      queued source byte limit 30 exceeded
      """

  Scenario: An orphaned transaction expires and retains its outcome
    Given the transaction idle timeout is configured as "250ms"
    And the transaction tombstone retention is configured as "1s"
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    And client "observer" is connected to node "{{new_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    And transaction "{{transaction_id}}" eventually has state "EXPIRED"
    When client "observer" fails to attach to transaction "{{transaction_id}}"
    Then the last command error contains
      """
      finished with outcome EXPIRED
      """
    And transaction "{{transaction_id}}" is eventually removed
    When client "observer" fails to attach to transaction "{{transaction_id}}"
    Then the last command error contains
      """
      is unknown
      """

  Scenario Outline: Open transactions are visible from replicated state
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      BEGIN;
      """
    Then the last command output contains
      """
      transaction started
      """
    When this NSPL command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      state=OPEN
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Implicit multi-command requests are rejected
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When this NSPL command request is executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA implicit_notification (
        user_id I64
      );
      """
    Then the last command error contains
      """
      multiple commands require BEGIN
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: COMMIT executes queued transaction commands
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      CREATE DOMAIN {{domain}};
      CREATE SCHEMA committed_notification (
        user_id I64
      );
      COMMIT
      """
    Then the last command output contains
      """
      stored model 'committed_notification'
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA committed_notification;
      """
    Then the last command output contains
      """
      CREATE SCHEMA committed_notification (user_id I64);
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: REVERT drops queued transaction commands
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      BEGIN;
      CREATE SCHEMA reverted_notification (
        user_id I64
      );
      REVERT;
      """
    Then the last command output contains
      """
      transaction reverted: dropped 1 command(s)
      """
    When these NSPL commands fail with "schema 'reverted_notification' does not exist"
      """
      SHOW CREATE SCHEMA reverted_notification;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Nested BEGIN is rejected
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands fail with "transaction is already active"
      """
      BEGIN;
      BEGIN;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mixed_schema_commit
  Scenario Outline: COMMIT atomically migrates interdependent schema and codec models
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA transaction_event (
        value STRING
      );
      CREATE WIRE JSON SCHEMA transaction_event_wire MODE STRICT (
        value string
      );
      CREATE CODEC transaction_event_codec
        FROM WIRE JSON SCHEMA transaction_event_wire
        TO SCHEMA transaction_event;
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA transaction_event_wire
        ALTER FIELD value SET TYPE number;
      ALTER SCHEMA transaction_event
        ALTER FIELD value SET TYPE F64;
      DROP CODEC transaction_event_codec;
      CREATE CODEC transaction_event_codec
        FROM WIRE JSON SCHEMA transaction_event_wire
        TO SCHEMA transaction_event;
      COMMIT
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA transaction_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA transaction_event (value F64);
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE JSON SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE JSON SCHEMA transaction_event_wire MODE STRICT (value NUMBER);
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @wire_schema_mode_alter
  Scenario Outline: Exact-format ALTER keeps same-name schema kinds independent
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA transaction_event_wire (
        value STRING
      );
      CREATE WIRE JSON SCHEMA transaction_event_wire MODE STRICT (
        value string
      );
      CREATE WIRE CBOR SCHEMA transaction_event_wire MODE STRICT (
        value string
      );
      CREATE CODEC transaction_json_codec
        FROM WIRE JSON SCHEMA transaction_event_wire
        TO SCHEMA transaction_event_wire;
      CREATE CODEC transaction_cbor_codec
        FROM WIRE CBOR SCHEMA transaction_event_wire
        TO SCHEMA transaction_event_wire;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER WIRE JSON SCHEMA transaction_event_wire MODE LOOSE;
      SHOW CREATE SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE SCHEMA transaction_event_wire (value STRING);
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE JSON SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE JSON SCHEMA transaction_event_wire MODE LOOSE (value STRING);
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE CBOR SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE CBOR SCHEMA transaction_event_wire MODE STRICT (value STRING);
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mixed_schema_commit
  Scenario Outline: A failing mixed COMMIT applies none of its mutations
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA atomic_event (
        value STRING
      );
      """
    When these NSPL commands fail with "field `missing` does not exist"
      """
      BEGIN;
      ALTER SCHEMA atomic_event
        ADD FIELD note STRING OPTIONAL;
      CREATE SCHEMA must_not_exist (
        value STRING
      );
      ALTER SCHEMA atomic_event
        DROP FIELD missing;
      COMMIT;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA atomic_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA atomic_event (value STRING);
      """
    When these NSPL commands fail with "schema 'must_not_exist' does not exist"
      """
      SHOW CREATE SCHEMA must_not_exist;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
