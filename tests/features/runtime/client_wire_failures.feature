Feature: Client wire failure regressions
  @client_wire_native_reconnect
  Scenario: The Rust client reconnects through its original seed after the leader stops
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "seed"
    Given client "recovering" is connected to node "{{seed}}"
    When client "recovering" executes these Plexus commands
      """
      CREATE SCHEMA recovered_client_record (value STRING);
      """
    And node "{{old_leader}}" is stopped
    Then node "{{seed}}" eventually observes a stable leader
    When client "recovering" executes these Plexus commands
      """
      SHOW CREATE SCHEMA recovered_client_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA recovered_client_record (
        value STRING
      );
      """

  # The forwarder stays stopped for twice the client's retry budget while the backup holds its
  # cut, so the client reaches its server again only after that budget has passed. The backup's
  # own wait covers every reconnect it makes, and the cut's budget covers the outage the cut waits
  # through at its gate on a loaded runner.
  @client_wire_backup_outage @deloxide_stress_restore
  Scenario Outline: A native backup waits through an outage longer than its retry budget on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the gRPC endpoint of node "{{leader}}" is forwarded from fixture address "127.0.0.1"
    And the backup cut for domain "{{domain}}" will pause before its capture
    When a native client with a 20 second retry budget and a 300 second backup wait begins backing up the cluster with a 240 second cut budget through "{{forwarded_grpc}}" into "outage.nvxb"
    Then the backup cut for domain "{{domain}}" has reached its pause
    When the TCP forwarder at "127.0.0.1" stays stopped for 40 seconds and then restarts
    And the backup cut pause for domain "{{domain}}" is released
    Then the native backup completes under its original execution reference

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_durable_admission
  Scenario: A command lost after durable admission is recovered by its request identity
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When the active session begins this Plexus command request with execution reference "durable-admission-{{test_id}}" in the background
      """
      CREATE SCHEMA durable_admission_record (
        value STRING
      );
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When the background command request connection is dropped
    And the durable command admission pause on node "{{leader}}" is released
    And this Plexus command request with execution reference "durable-admission-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA durable_admission_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA durable_admission_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA durable_admission_record (
        value STRING
      );
      """

  @client_wire_missing_command
  Scenario Outline: A command missing from a committed transaction cannot report aggregate success
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "owner" is connected to node "{{leader}}"
    And client "finisher" is connected to node "{{leader}}"
    When client "owner" executes these Plexus commands
      """
      BEGIN;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given command admission on node "{{leader}}" pauses before proposal
    When client "owner" begins executing these Plexus commands in the background
      """
      CREATE SCHEMA missing_command_record (
        value STRING
      );
      """
    Then the command admission pause on node "{{leader}}" is reached
    When client "finisher" attaches to transaction "{{transaction_id}}"
    And client "finisher" executes these Plexus commands
      """
      COMMIT;
      """
    And the command admission pause on node "{{leader}}" is released
    Then the background Plexus execution does not report success

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_held_recovery @command_reference_recovery
  Scenario: A held BEGIN recovers its transaction binding after leadership moves during admission
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA held_record (value I64);
      CREATE WIRE JSON SCHEMA held_wire MODE STRICT (value integer);
      CREATE WIRE JSON CODEC held_codec FROM WIRE SCHEMA held_wire TO SCHEMA held_record;
      CREATE RELAY held_records SCHEMA held_record UNBRANCHED;
      CREATE VHOST held_edge held-binding-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON held_edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR held_source
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_codec
        TO held_records INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "held_leader"
    And a node other than placeholder "held_leader" is saved as placeholder "held_successor"
    When these Plexus commands are executed on the active session
      """
      CREATE SUBSCRIPTION held_seen TO held_records;
      """
    And http payload is posted to node "{{held_leader}}" with host "held-binding-{{test_id}}.example.com" path "/records"
      """
      {"value":1}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"value":1}
      """
    Given leadership moves from node "{{held_leader}}" to node "{{held_successor}}" under its next durably admitted command
    When these Plexus commands are executed on the active session
      """
      BEGIN;
      """
    Then the leadership move under the durably admitted command completed
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only open transaction id is saved as placeholder "held_transaction"
    When these Plexus commands are executed on the active session
      """
      CREATE RESOURCE held_binding_resource;
      COMMIT;
      """
    Then transaction "{{held_transaction}}" eventually has state "COMMITTED"
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE held_binding_resource;
      """
    Then the last command output contains
      """
      resource: held_binding_resource
      """
    When http payload is posted to node "{{held_successor}}" with host "held-binding-{{test_id}}.example.com" path "/records"
      """
      {"value":2}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"value":2}
      """

  @client_wire_held_recovery @command_reference_recovery
  Scenario: A held REVERT recovers its original transaction after leadership moves during admission
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the active session
      """
      BEGIN;
      CREATE RESOURCE reverted_binding_resource;
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "reverted_transaction"
    Then the current leader node is saved as placeholder "held_leader"
    And a node other than placeholder "held_leader" is saved as placeholder "held_successor"
    Given leadership moves from node "{{held_leader}}" to node "{{held_successor}}" under its next durably admitted command
    When these Plexus commands are executed on the active session
      """
      REVERT;
      """
    Then the leadership move under the durably admitted command completed
    And transaction "{{reverted_transaction}}" eventually has state "REVERTED"
    When these Plexus commands are executed on the active session
      """
      BEGIN;
      CREATE RESOURCE subsequent_binding_resource;
      COMMIT;
      """
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE subsequent_binding_resource;
      """
    Then the last command output contains
      """
      resource: subsequent_binding_resource
      """
    When these Plexus commands fail with "resource 'reverted_binding_resource' does not exist"
      """
      DESCRIBE RESOURCE reverted_binding_resource;
      """

  @client_wire_lost_begin
  Scenario Outline: Replaying a BEGIN whose response was lost returns the original transaction
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session begins this Plexus command request with execution reference "lost-begin-{{test_id}}" in the background
      """
      BEGIN;
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the background command request connection is dropped
    And the command response delivery pause on node "{{leader}}" is released
    And this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "original_transaction"
    When this Plexus command request with execution reference "lost-begin-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      """
    Then the last command output contains
      """
      transaction started: id '{{original_transaction}}'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_duplicate_append @slots_2
  Scenario Outline: Replaying an accepted <append_kind> append does not preflight or append it again
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When these Plexus commands are executed on the active session
      """
      BEGIN;
      """
    And this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "transaction_id"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session sends this Plexus command request with execution reference "append-{{test_id}}" without reading its response
      """
      <append>
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the command response delivery pause on node "{{leader}}" is released
    And a new session attaches to transaction "{{transaction_id}}" and executes this Plexus command with execution reference "append-{{test_id}}" at transaction position 0
      """
      <append>
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      id={{transaction_id}}
      """
    And the last command output contains
      """
      pending=1
      """
    When these Plexus commands are executed on the active session
      """
      REVERT;
      """

    Examples:
      | cluster_size | append_kind  | append                             |
      | 1            | resource     | CREATE RESOURCE replayed_resource; |
      | 1            | domain start | START;                             |
      | 3            | resource     | CREATE RESOURCE replayed_resource; |
      | 3            | domain start | START;                             |

  @client_wire_duplicate_append @slots_2
  Scenario: A replay session that reaches a former leader attaches the transaction where the redirect leads
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "successor"
    When these Plexus commands are executed on the active session
      """
      BEGIN;
      """
    And this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "transaction_id"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session sends this Plexus command request with execution reference "append-{{test_id}}" without reading its response
      """
      CREATE RESOURCE replayed_resource;
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the command response delivery pause on node "{{leader}}" is released
    And leadership is transferred from node "{{leader}}" to node "{{successor}}"
    Then node "{{successor}}" eventually reports a leader other than "{{leader}}"
    And node "{{leader}}" eventually reports a leader other than "{{leader}}"
    When a new session on node "{{leader}}" attaches to transaction "{{transaction_id}}" and executes this Plexus command with execution reference "append-{{test_id}}" at transaction position 0
      """
      CREATE RESOURCE replayed_resource;
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      id={{transaction_id}}
      """
    And the last command output contains
      """
      pending=1
      """
    When these Plexus commands are executed on the active session
      """
      REVERT;
      """

  @client_wire_transaction_batch_leader_recovery
  Scenario: An exact transaction batch retry survives a leader change without repeating effects
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given command response delivery on node "{{old_leader}}" pauses after execution
    When the active session begins this Plexus command request with execution reference "leader-batch-{{test_id}}" in the background
      """
      BEGIN;
      CREATE RESOURCE leader_batch_resource;
      START;
      COMMIT;
      """
    Then the command response delivery pause on node "{{old_leader}}" is reached
    When the background command request connection is dropped
    And the command response delivery pause on node "{{old_leader}}" is released
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When this Plexus command request with execution reference "leader-batch-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      CREATE RESOURCE leader_batch_resource;
      START;
      COMMIT;
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE leader_batch_resource;
      """
    Then the last command output contains
      """
      resource: leader_batch_resource
      """

  @client_wire_transaction_batch_finalization_recovery @client_wire_probe_recovery @command_reference_recovery
  Scenario: A committed transaction batch keeps its one outcome when leadership moves while each attempt records it
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given command finalization on node "{{old_leader}}" pauses before its outcome is recorded
    And command finalization on node "{{new_leader}}" pauses before its outcome is recorded
    And command reconciliation of execution reference "finalized-batch-{{test_id}}" pauses on node "{{old_leader}}"
    And the leader refuses the admission of execution reference "finalized-batch-{{test_id}}" as a leader whose lease lapsed
    When the active session begins this Plexus command request with execution reference "finalized-batch-{{test_id}}" in the background
      """
      BEGIN;
      CREATE RESOURCE finalized_batch_resource;
      START;
      COMMIT;
      """
    Then the command finalization pause on node "{{old_leader}}" is reached
    And the leader refused the admission of execution reference "finalized-batch-{{test_id}}" as a leader whose lease lapsed
    And the transaction targeted by execution reference "finalized-batch-{{test_id}}" is saved as placeholder "batch_transaction"
    And transaction "{{batch_transaction}}" eventually has state "COMMITTED"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When the command finalization pause on node "{{old_leader}}" is released
    Then the background command request reports an unknown outcome because leadership moved
    When the command reconciliation pause for execution reference "finalized-batch-{{test_id}}" on node "{{old_leader}}" is released
    Then the command finalization pause on node "{{new_leader}}" is reached
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{old_leader}}"
    When the command finalization pause on node "{{new_leader}}" is released
    And this Plexus command request with execution reference "finalized-batch-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      CREATE RESOURCE finalized_batch_resource;
      START;
      COMMIT;
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "finalized-batch-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      CREATE RESOURCE finalized_batch_resource;
      START;
      COMMIT;
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE finalized_batch_resource;
      """
    Then the last command output contains
      """
      resource: finalized_batch_resource
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "completed_batch_transaction"
    And placeholder "completed_batch_transaction" equals placeholder "batch_transaction"
    And the last command output contains
      """
      state=COMMITTED
      """

  @client_wire_applying_probe_recovery
  Scenario: A committed transaction batch reaches its finalization gates after an applying lease refusal
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA applying_event (key I64);
      CREATE RELAY applying_input SCHEMA applying_event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "applying_leader"
    And a node other than placeholder "applying_leader" is saved as placeholder "applying_successor"
    Given command finalization on node "{{applying_leader}}" pauses before its outcome is recorded
    And command finalization on node "{{applying_successor}}" pauses before its outcome is recorded
    And command reconciliation of execution reference "applying-batch-{{test_id}}" pauses on node "{{applying_leader}}"
    And the leader refuses the next model publication in domain "{{domain}}" and the pause release after it with "a lapsed leader lease"
    When the active session begins this Plexus command request with execution reference "applying-batch-{{test_id}}" in the background
      """
      BEGIN;
      ALTER SCHEMA applying_event ADD FIELD note STRING OPTIONAL;
      CREATE RESOURCE applying_batch_resource;
      COMMIT;
      """
    Then the command finalization pause on node "{{applying_leader}}" is reached
    And the refusal of the model publication in domain "{{domain}}" and of its pause release was delivered
    And the transaction targeted by execution reference "applying-batch-{{test_id}}" is saved as placeholder "applying_transaction"
    When leadership is transferred from node "{{applying_leader}}" to node "{{applying_successor}}"
    Then node "{{applying_successor}}" eventually reports leader "{{applying_successor}}"
    When the command finalization pause on node "{{applying_leader}}" is released
    Then the background command request reports an unknown outcome because leadership moved
    When the command reconciliation pause for execution reference "applying-batch-{{test_id}}" on node "{{applying_leader}}" is released
    Then the command finalization pause on node "{{applying_successor}}" is reached
    When leadership is transferred from node "{{applying_successor}}" to node "{{applying_leader}}"
    Then node "{{applying_leader}}" eventually reports leader "{{applying_leader}}"
    When the command finalization pause on node "{{applying_successor}}" is released
    And this Plexus command request with execution reference "applying-batch-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      ALTER SCHEMA applying_event ADD FIELD note STRING OPTIONAL;
      CREATE RESOURCE applying_batch_resource;
      COMMIT;
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "applying-batch-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      ALTER SCHEMA applying_event ADD FIELD note STRING OPTIONAL;
      CREATE RESOURCE applying_batch_resource;
      COMMIT;
      """
    Then the last command request succeeded
    And transaction "{{applying_transaction}}" eventually has state "COMMITTED"
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA applying_event;
      """
    Then the last command output contains
      """
      note STRING OPTIONAL
      """
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE applying_batch_resource;
      """
    Then the last command output contains
      """
      resource: applying_batch_resource
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      id={{applying_transaction}}
      """

  @client_wire_transaction_batch_restart
  Scenario: An exact transaction batch retry survives a full durable restart without repeating effects
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session begins this Plexus command request with execution reference "restart-batch-{{test_id}}" in the background
      """
      BEGIN;
      CREATE RESOURCE restart_batch_resource;
      START;
      COMMIT;
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the background command request connection is dropped
    And the command response delivery pause on node "{{leader}}" is released
    And the cluster is restarted
    And this Plexus command request with execution reference "restart-batch-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      CREATE RESOURCE restart_batch_resource;
      START;
      COMMIT;
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE RESOURCE restart_batch_resource;
      """
    Then the last command output contains
      """
      resource: restart_batch_resource
      """

  @client_wire_execution_history
  Scenario Outline: A reclaimed command identity stays expired after a durable restart
    Given command retry identities are valid for "20s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this Plexus command request with execution reference "reclaimed-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA reclaimed_identity_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "reclaimed-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA reclaimed_identity_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "command_transaction"
    And command execution reference "reclaimed-{{test_id}}" is eventually reclaimed
    When the cluster is restarted
    And this Plexus command request with execution reference "reclaimed-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA reclaimed_identity_record (
        value STRING
      );
      """
    Then the last command error contains
      """
      has expired
      """
    And the last command request reports an expired execution reference
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA reclaimed_identity_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA reclaimed_identity_record (
        value STRING
      );
      """
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE TRANSACTION '{{command_transaction}}';
      """
    Then the last command output contains
      """
      transaction: {{command_transaction}}
      domain: {{domain}}
      state: COMMITTED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_execution_history
  Scenario Outline: A command identity outside its retry window starts no effect
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this Plexus command request with an execution reference <reference> is executed on the leader node
      """
      CREATE SCHEMA refused_identity_record (
        value STRING
      );
      """
    Then the last command error contains
      """
      <refusal>
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA refused_identity_record;
      """
    Then the last command error contains
      """
      schema 'refused_identity_record' does not exist in domain '{{domain}}'
      """

    Examples:
      | cluster_size | reference               | refusal                                            |
      | 1            | created "1h" before now | has expired                                        |
      | 1            | created "10m" after now | beyond the accepted client clock-skew boundary     |
      | 1            | without a creation time | does not carry a UUID version 7 creation timestamp |
      | 3            | created "1h" before now | has expired                                        |
      | 3            | created "10m" after now | beyond the accepted client clock-skew boundary     |
      | 3            | without a creation time | does not carry a UUID version 7 creation timestamp |

  @client_wire_execution_reference_conflict
  Scenario: A race across leaders recovers a typed execution reference conflict
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "first_leader"
    And a node other than placeholder "first_leader" is saved as placeholder "second_leader"
    Given command reference lookup on node "{{first_leader}}" pauses before proposal
    When this Plexus command request with execution reference "raced-{{test_id}}" begins executing in the background on the leader node
      """
      CREATE SCHEMA raced_first_record (value STRING);
      """
    Then the command reference lookup pause on node "{{first_leader}}" is reached
    When leadership is transferred from node "{{first_leader}}" to node "{{second_leader}}"
    Then node "{{second_leader}}" eventually reports leader "{{second_leader}}"
    When this Plexus command request with execution reference "raced-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA raced_second_record (value STRING);
      """
    Then the last command request succeeded
    When the command reference lookup pause on node "{{first_leader}}" is released
    Then the background command request reports a content conflict or unknown leadership outcome
    When this Plexus command request with execution reference "raced-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA raced_first_record (value STRING);
      """
    Then the last command request reports an execution reference content conflict
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA raced_second_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA raced_second_record (
        value STRING
      );
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA raced_first_record;
      """
    Then the last command error contains
      """
      schema 'raced_first_record' does not exist in domain '{{domain}}'
      """

  @client_wire_execution_history
  Scenario Outline: A full command history refuses new identities and keeps every retained result
    Given the command execution capacity is configured as 3
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this Plexus command request with execution reference "first-retained-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA first_retained_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "second-retained-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA second_retained_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "refused-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA refused_capacity_record (
        value STRING
      );
      """
    Then the last command error contains
      """
      command execution capacity of 3 is full
      """
    When this Plexus command request with execution reference "first-retained-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA first_retained_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA refused_capacity_record;
      """
    Then the last command error contains
      """
      schema 'refused_capacity_record' does not exist in domain '{{domain}}'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_concurrent_transaction_retry
  Scenario: Concurrent exact BEGIN retries join one durable execution
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When the active session begins this Plexus command request with execution reference "concurrent-begin-{{test_id}}" in the background
      """
      BEGIN;
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When an exact retry of this Plexus command request with execution reference "concurrent-begin-{{test_id}}" begins in parallel on the leader node
      """
      BEGIN;
      """
    And the durable command admission pause on node "{{leader}}" is released
    Then the background command request succeeds
    And the background Plexus execution succeeds
    When this Plexus command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "transaction_id"

  @client_wire_transaction_identity_conflict
  Scenario: Reusing a durable transaction identity with different content fails semantically
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this Plexus command request with execution reference "conflicting-transaction-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      """
    Then the last command request succeeded
    When this Plexus command request with execution reference "conflicting-transaction-{{test_id}}" is executed on the leader node
      """
      BEGIN;
      CREATE RESOURCE conflicting_resource;
      COMMIT;
      """
    Then the last command error contains
      """
      conflicts by content
      """

  @client_wire_stalled_commit
  Scenario: A stalled commit cannot block expiry, another domain, or tombstone cleanup
    Given the transaction idle timeout is configured as "15s"
    And the transaction tombstone retention is configured as "30s"
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN runnable_{{test_id}};
      """
    Given client "blocked" is connected to node "node-1"
    And client "expiring" is connected to node "node-1"
    And client "cleanup" is connected to node "node-1"
    And client "runnable" is connected to node "node-1"
    And client "observer" is connected to node "node-1"
    When client "runnable" selects domain "runnable_{{test_id}}"
    And client "cleanup" executes these Plexus commands
      """
      BEGIN;
      """
    Then client "cleanup" transaction id is saved as placeholder "cleanup_transaction"
    When client "cleanup" executes these Plexus commands
      """
      REVERT;
      """
    When client "blocked" executes these Plexus commands
      """
      BEGIN;
      CREATE SCHEMA blocked_commit_record (
        value STRING
      );
      """
    Then client "blocked" transaction id is saved as placeholder "blocked_transaction"
    When client "expiring" executes these Plexus commands
      """
      BEGIN;
      """
    Then client "expiring" transaction id is saved as placeholder "expiring_transaction"
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "blocked" begins executing these Plexus commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    When client "observer" executes these Plexus commands
      """
      SHOW TRANSACTIONS;
      """
    When client "runnable" executes these Plexus commands
      """
      BEGIN;
      CREATE SCHEMA runnable_record (value STRING);
      COMMIT;
      SHOW CREATE SCHEMA runnable_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA runnable_record (
        value STRING
      );
      """
    Then transaction "{{expiring_transaction}}" eventually has state "EXPIRED"
    And transaction "{{cleanup_transaction}}" is eventually removed
    When the transaction commit pause on node "node-1" after 1 statement is released
    Then the background Plexus execution succeeds

  @client_wire_stale_relocation
  Scenario: A relocation planned before a schedule revision cannot overwrite that revision
    Given Kafka is running
    Given runtime replication is configured with replica count 2 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And Kafka topic "relocation_offsets_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA relocation_record ( sequence I64 );
      CREATE WIRE JSON SCHEMA relocation_wire MODE STRICT ( sequence integer );
      CREATE WIRE JSON CODEC relocation_codec
        FROM WIRE SCHEMA relocation_wire
        TO SCHEMA relocation_record;
      CREATE SCHEMA relocation_tenant ( tenant STRING );
      CREATE BRANCH relocation_by_tenant SCHEMA relocation_tenant TTL 5m;
      CREATE RELAY relocation_input SCHEMA relocation_record UNBRANCHED;
      CREATE RELAY relocation_output SCHEMA relocation_record UNBRANCHED;
      CREATE RELAY relocation_offsets SCHEMA relocation_record BRANCHED BY relocation_by_tenant;
      CREATE KAFKA CLIENT relocation_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR relocation_offset_source
        FROM relocation_kafka TOPIC 'relocation_offsets_{{test_id}}'
          OFFSET BY DOMAIN INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING relocation_codec
        TIMESTAMP NOW
        TO relocation_offsets INHERIT ALL
        BRANCHED BY relocation_by_tenant
        SET tenant = 'tenant'
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "relocation_offset_source" on the leader node contains
      """
      kafka observed partitions: 0
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE JUNCTION relocation_route FROM relocation_input UNBRANCHED
        TO relocation_output INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "relocation_route" is saved as placeholder "source_owner"
    And the first replica for scheduled "junction" "relocation_route" in the last cluster status is saved as placeholder "relocation_target"
    Given relocation publication for domain "{{domain}}" pauses after planning
    When these Plexus commands begin executing in the background
      """
      RELOCATE JUNCTION relocation_route ONTO NODE {{relocation_target}} IGNORE PREFERENCES;
      """
    Then the relocation publication pause for domain "{{domain}}" is reached
    When Kafka topic "relocation_offsets_{{test_id}}" partition count is changed to 2
    Then within "5s" DESCRIBE INGESTOR "relocation_offset_source" on the leader node contains
      """
      kafka observed partitions: 0,1
      """
    When the relocation publication pause for domain "{{domain}}" is released
    Then the background Plexus execution fails with "schedule changed"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE INGESTOR relocation_offset_source;
      """
    Then the last command output contains
      """
      kafka observed partitions: 0,1
      """

  @client_wire_subscription_restore
  Scenario: A reconnected native client restores acknowledged subscriptions
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge client-wire-{{test_id}}.example.com;
      CREATE ENDPOINT wire_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR wire_source
        FROM wire_ingress MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "subscriber" is connected to node "{{old_leader}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And one http payload is posted with a completed graph acknowledgement to node "{{old_leader}}" host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When client "subscriber" executes these Plexus commands
      """
      DESCRIBE DOMAIN;
      """
    And node "{{old_leader}}" is stopped
    Then within "30s" client "subscriber" observes subscription "wire_seen" interrupted
    When client "subscriber" executes these Plexus commands
      """
      DESCRIBE DOMAIN;
      """
    Then client "subscriber" subscription "wire_seen" is active
    When within "30s" client "subscriber" receives one subscription row from one acknowledged http post to node "{{new_leader}}" host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then the last relay subscription payload contains
      """
      key={"tenant":"beta"}
      payload={"sequence":2,"tenant":"beta"}
      """

  @client_wire_subscription_restore
  @client_wire_subscription_timeout @deloxide_stress
  Scenario Outline: An unanswered subscription reopening lets a waiting native reader open another session
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge client-wire-{{test_id}}.example.com;
      CREATE ENDPOINT wire_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR wire_source
        FROM wire_ingress MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the gRPC endpoint of node "{{leader}}" is forwarded from fixture address "127.0.0.1"
    And client "subscriber" is connected to "{{forwarded_grpc}}" with request timeout "5s"
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And one http payload is posted with a completed graph acknowledgement to node "{{leader}}" host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    Given the next subscription opening reply on node "{{leader}}" is held
    When the TCP forwarder at "127.0.0.1" stops
    Then within "30s" client "subscriber" observes subscription "wire_seen" interrupted
    When client "subscriber" waits through the held reopening of subscription "wire_seen" on node "{{leader}}" then receives one http payload with host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then the last relay subscription payload contains
      """
      key={"tenant":"beta"}
      payload={"sequence":2,"tenant":"beta"}
      """
    And client "subscriber" subscription "wire_seen" is active
    And the subscription opening reply on node "{{leader}}" is held
    When the subscription opening reply on node "{{leader}}" is released

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_subscription_restore
  Scenario Outline: A native client deletes a subscription its lost session held and opens the name again
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE RELAY wire_records SCHEMA wire_record UNBRANCHED;
      CREATE VHOST edge client-wire-{{test_id}}.example.com;
      CREATE ENDPOINT wire_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR wire_source
        FROM wire_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING wire_record_codec
        TO wire_records INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the gRPC endpoint of node "{{leader}}" is forwarded from fixture address "127.0.0.1"
    And client "subscriber" is connected to "{{forwarded_grpc}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And the TCP forwarder at "127.0.0.1" stops
    Then within "30s" client "subscriber" observes subscription "wire_seen" interrupted
    When client "subscriber" executes these Plexus commands
      """
      DELETE SUBSCRIPTION wire_seen;
      """
    Then client "subscriber" no longer holds subscription "wire_seen"
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    Then client "subscriber" subscription "wire_seen" is active
    When within "30s" client "subscriber" receives a subscription payload from repeated http posts to node "{{leader}}" with host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then the last relay subscription payload contains
      """
      {"sequence":1,"tenant":"acme"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_subscription_restore
  Scenario Outline: A native client reports a refused subscription restoration and deletes the subscription without the server
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA watched_record (
        value STRING
      );
      CREATE RELAY watched SCHEMA watched_record UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the gRPC endpoint of node "{{leader}}" is forwarded from fixture address "127.0.0.1"
    And client "subscriber" is connected to "{{forwarded_grpc}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION watching TO watched;
      """
    And the TCP forwarder at "127.0.0.1" stops
    Then within "30s" client "subscriber" observes subscription "watching" interrupted
    When these Plexus commands are executed on the leader node
      """
      DROP RELAY watched;
      """
    And client "subscriber" executes these Plexus commands
      """
      DESCRIBE DOMAIN;
      """
    Then within "30s" client "subscriber" observes a failed restoration of subscription "watching"
      """
      stream 'watched' does not exist in domain '{{domain}}'
      """
    And client "subscriber" subscription "watching" is interrupted
    When client "subscriber" executes these Plexus commands
      """
      DELETE SUBSCRIPTION watching;
      """
    Then client "subscriber" no longer holds subscription "watching"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_subscription_restore
  Scenario Outline: A native client does not restore a subscription the server ended and opens its name again on request
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (
        tenant STRING,
        sequence I64
      );
      CREATE SCHEMA masked_wire_record (
        tenant STRING,
        sequence I64 SENSITIVE
      );
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE WIRE JSON CODEC wire_record_codec
        FROM WIRE SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE RELAY wire_records SCHEMA wire_record UNBRANCHED;
      CREATE VHOST edge client-wire-{{test_id}}.example.com;
      CREATE ENDPOINT wire_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR wire_source
        FROM wire_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING wire_record_codec
        TO wire_records
        SET tenant = message.tenant, sequence = message.sequence
        UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the gRPC endpoint of node "{{leader}}" is forwarded from fixture address "127.0.0.1"
    And client "subscriber" is connected to "{{forwarded_grpc}}" with cluster seeds
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And these Plexus commands are executed on the leader node
      """
      ALTER RELAY wire_records SET SCHEMA masked_wire_record;
      """
    Then within "30s" client "subscriber" observes subscription "wire_seen" ended with reason "RelayChanged"
    And client "subscriber" subscription "wire_seen" is ended
    When the TCP forwarder at "127.0.0.1" stops
    And client "subscriber" executes these Plexus commands
      """
      DESCRIBE DOMAIN;
      """
    Then client "subscriber" reports no event of subscription "wire_seen" within "5s"
    And client "subscriber" subscription "wire_seen" is ended
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    Then client "subscriber" subscription "wire_seen" is active
    When within "30s" client "subscriber" receives a subscription payload from repeated http posts to node "{{leader}}" with host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then the last relay subscription payload contains
      """
      "tenant":"acme"
      """
    And the last relay subscription payload masks field "sequence"
    When these Plexus commands are executed on the leader node
      """
      DROP INGESTOR wire_source;
      DROP RELAY wire_records;
      """
    Then within "30s" client "subscriber" observes subscription "wire_seen" ended with reason "RelayRemoved"
    When client "subscriber" executes these Plexus commands
      """
      DELETE SUBSCRIPTION wire_seen;
      """
    Then the last command output contains
      """
      subscription 'wire_seen' deleted; the server had already ended it
      """
    And client "subscriber" no longer holds subscription "wire_seen"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_wide_rows @slots_2
  Scenario Outline: A native client keeps a subscription active while it receives a row that fills most of a frame
    Given a <cluster_size> node nervix cluster is started
    And a repeated text placeholder "wide_note" of 3000000 bytes is prepared
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wide_record (
        sequence I64,
        note STRING
      );
      CREATE WIRE JSON SCHEMA wide_record_json MODE STRICT (
        sequence integer,
        note string
      );
      CREATE WIRE JSON CODEC wide_record_codec
        FROM WIRE SCHEMA wide_record_json
        TO SCHEMA wide_record;
      CREATE RELAY wide_records SCHEMA wide_record UNBRANCHED;
      CREATE VHOST edge wide-rows-{{test_id}}.example.com;
      CREATE ENDPOINT wide_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR wide_source
        FROM wide_ingress MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wide_record_codec
        TO wide_records
        SET sequence = message.sequence, note = message.note
        UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "subscriber" is connected to node "node-1"
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wide_seen TO wide_records;
      """
    And http payload is acknowledged within "30s" by node "node-1" with host "wide-rows-{{test_id}}.example.com" path "/records"
      """
      {"sequence":1,"note":"{{wide_note}}"}
      """
    Then within "30s" client "subscriber" receives a subscription payload
      """
      {"note":"{{wide_note}}","sequence":1}
      """
    And client "subscriber" subscription "wide_seen" is active

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_foreign_execution @deloxide_stress
  Scenario Outline: A native command recovers its own execution after a foreign command reply on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "recovering" is connected to node "{{leader}}"
    And the next successful reply to native command "CREATE SCHEMA recovered_identity (value STRING);" names another execution
    When client "recovering" executes these Plexus commands
      """
      CREATE SCHEMA recovered_identity (value STRING);
      """
    Then the native command recovered only its own execution reference
    When client "recovering" executes these Plexus commands
      """
      SHOW CREATE SCHEMA recovered_identity;
      """
    Then the last command output contains
      """
      recovered_identity
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_reply_kind @deloxide_stress
  Scenario Outline: A native command recovers its own execution after a reply of another kind on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "recovering" is connected to node "{{leader}}"
    And the next successful reply to native command "CREATE SCHEMA reply_kind_identity (value STRING);" is a reply of another kind
    When client "recovering" executes these Plexus commands
      """
      CREATE SCHEMA reply_kind_identity (value STRING);
      """
    Then the native command recovered its own execution reference after a reply of another kind
    When client "recovering" executes these Plexus commands
      """
      SHOW CREATE SCHEMA reply_kind_identity;
      """
    Then the last command output contains
      """
      reply_kind_identity
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
