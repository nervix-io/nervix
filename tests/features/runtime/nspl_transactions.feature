Feature: NSPL transactions
  @transaction_frozen_plan
  Scenario Outline: Commit admission freezes a plan before later authoritative state changes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "contender" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE IF NOT EXISTS RESOURCE frozen_plan_resource;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit admission on the leader node pauses before execution
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit admission pause on the leader node is reached
    When client "contender" executes these NSPL commands
      """
      CREATE RESOURCE frozen_plan_resource;
      """
    And the transaction commit admission pause on the leader node is released
    Then the background NSPL execution fails with "planning inputs changed"
    And transaction "{{transaction_id}}" eventually has state "FAILED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_stale_preview
  Scenario Outline: A commit fenced to a preview the transaction outgrew is refused and stays open
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "contender" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA staged_event ( user_id U32 );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "contender" executes these NSPL commands
      """
      CREATE SCHEMA unrelated_event ( order_id U32 );
      """
    And client "owner" attempts to commit its transaction
    Then client "owner" commit was refused because its expected preview is stale
    And client "owner" transaction state is "OPEN"
    And transaction "{{transaction_id}}" eventually has state "OPEN"
    When client "contender" fails to execute these NSPL commands
      """
      SHOW CREATE SCHEMA staged_event;
      """
    Then the last command error contains
      """
      schema 'staged_event' does not exist in domain '{{domain}}'
      """
    When client "owner" attempts to commit its transaction
    Then client "owner" commit was refused because its expected preview is stale
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /transaction/transaction_id = "{{transaction_id}}"
      /operation = 1
      /report/position = 1
      """
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then client "owner" transaction state is "COMMITTED"
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA staged_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA staged_event (
        user_id U32
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_inspection
  Scenario Outline: DESCRIBE TRANSACTION reads the open transaction without becoming its content
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA inspected_event ( user_id U32 );
      CREATE SCHEMA inspected_audit ( order_id U32 );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION;
      """
    Then the last command output contains
      """
      transaction: {{transaction_id}}
      domain: {{domain}}
      state: OPEN
      operations: 2 accepted, 0 applied, 2 pending
      report: COMPLETE
      """
    And the last command output contains
      """
      quiesce level: DYNAMIC
      pause: NO_PAUSE
      operation 1: CREATE_CONFIGURATION kind=schema name=inspected_event
        execution step: 1-2
      """
    And the last command output contains
      """
      operation 2: CREATE_CONFIGURATION kind=schema name=inspected_audit
        execution step: 1-2
      """
    And the last command output contains
      """
      execution step 1-2: planned DYNAMIC, actual DYNAMIC, outcome UNATTEMPTED
      """
    And the last inspection reports
      """
      transaction: {{transaction_id}}
      domain: {{domain}}
      state: OPEN
      accepted operations: 2
      applied operations: 0
      selected operation: none
      report operations: 2
      """
    When client "owner" executes these NSPL commands
      """
      describe transaction operation 2;
      """
    Then the last command output contains
      """
      inspected operation: 2
      operation 2: CREATE_CONFIGURATION kind=schema name=inspected_audit
        execution step: 1-2
        reason: CONFIGURATION kind=schema name=inspected_audit aspect=ENTITY_CREATED
        contribution: configuration CREATED kind=schema name=inspected_audit operations=2
      execution step 1-2: planned DYNAMIC, actual DYNAMIC, outcome UNATTEMPTED
      """
    And the last inspection reports
      """
      transaction: {{transaction_id}}
      selected operation: 2
      report operations: 2
      """
    And client "owner" transaction state is "OPEN"
    When client "owner" executes these NSPL commands
      """
      CREATE SCHEMA inspected_trace ( trace_id U32 );
      """
    Then the last accepted operation is 3
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then client "owner" transaction state is "COMMITTED"
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA inspected_trace;
      """
    Then the last command output contains
      """
      CREATE SCHEMA inspected_trace (
        trace_id U32
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_inspection
  Scenario Outline: DESCRIBE TRANSACTION by identity reads another transaction without adopting it
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA observed_event ( user_id U32 );
      CREATE SCHEMA observed_audit ( order_id U32 );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "observer" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /transaction/transaction_id = "{{transaction_id}}"
      /transaction/domain = "{{domain}}"
      /transaction/state = "OPEN"
      /transaction/accepted_operations = 2
      /transaction/applied_operations = 0
      /operation = 1
      /report/position = 2
      /report/completeness/status = "COMPLETE"
      /report/operations/0/number = 1
      /report/operations/0/operation/kind = "CREATE_CONFIGURATION"
      /report/operations/1/number = 2
      /report/execution_steps/0/actual/outcome/status = "UNATTEMPTED"
      """
    And the last inspection reports
      """
      transaction: {{transaction_id}}
      state: OPEN
      selected operation: 1
      report operations: 2
      """
    And client "observer" has no transaction
    When client "owner" executes these NSPL commands
      """
      CREATE SCHEMA observed_trace ( trace_id U32 );
      """
    Then the last accepted operation is 3
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When client "observer" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}';
      """
    Then the last command output contains
      """
      transaction: {{transaction_id}}
      domain: {{domain}}
      state: COMMITTED
      operations: 3 accepted, 3 applied, 0 pending
      """
    And the last command output contains
      """
      execution step 1-3: planned DYNAMIC, actual DYNAMIC, outcome APPLIED
      """
    And the last inspection reports
      """
      transaction: {{transaction_id}}
      state: COMMITTED
      accepted operations: 3
      applied operations: 3
      selected operation: none
      """
    And client "observer" has no transaction

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_inspection
  Scenario Outline: DESCRIBE TRANSACTION names why it read nothing and changes nothing
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE USER inspection_intruder WITH PASSWORD 'intruder-password';
      """
    Given client "owner" is connected to the leader node
    And client "observer" is connected to the leader node
    And client "intruder" is connected to the leader node as user "inspection_intruder" with password "intruder-password"
    When client "observer" fails to execute these NSPL commands
      """
      DESCRIBE TRANSACTION;
      """
    Then the last command error contains
      """
      no transaction is attached to this session
      """
    When client "observer" fails to execute these NSPL commands
      """
      DESCRIBE TRANSACTION 'reclaimed-transaction';
      """
    Then the last command error contains
      """
      transaction 'reclaimed-transaction' is unknown
      """
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA refused_event ( user_id U32 );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" fails to execute these NSPL commands
      """
      DESCRIBE TRANSACTION OPERATION 2;
      """
    Then the last command error contains
      """
      transaction '{{transaction_id}}' accepted 1 operation(s), so operation 2 does not exist
      """
    When client "intruder" fails to execute these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}';
      """
    Then the last command error contains
      """
      transaction '{{transaction_id}}' belongs to another user
      """
    When client "owner" submits this NSPL command request
      """
      CREATE SCHEMA refused_audit ( order_id U32 );
      DESCRIBE TRANSACTION;
      """
    Then the last command error contains
      """
      DESCRIBE TRANSACTION must be executed separately
      """
    When client "owner" fails to execute these NSPL commands
      """
      DESCRIBE DOMAIN;
      """
    Then the last command error contains
      """
      DESCRIBE cannot be queued in a transaction
      """
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION;
      """
    Then the last command output contains
      """
      state: OPEN
      operations: 1 accepted, 0 applied, 1 pending
      """
    And client "owner" transaction state is "OPEN"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_inspection
  Scenario: DESCRIBE TRANSACTION sent to a follower is answered by the leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    Given client "owner" is connected to the leader node
    And client "observer" is connected to node "{{follower}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA followed_event ( user_id U32 );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "observer" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' FORMAT TEXT;
      """
    Then the last command output contains
      """
      transaction: {{transaction_id}}
      domain: {{domain}}
      state: OPEN
      operations: 1 accepted, 0 applied, 1 pending
      """
    And client "observer" has no transaction
    And client "owner" transaction state is "OPEN"

  @transaction_inspection
  Scenario Outline: A standalone CLI inspection prints one JSON document and a refusal exits nonzero
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA cli_inspected_event ( user_id U32 );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When the CLI successfully executes this JSON inspection
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /transaction/transaction_id = "{{transaction_id}}"
      /transaction/state = "OPEN"
      /operation = 1
      /report/operations/0/number = 1
      """
    When the CLI successfully executes this text inspection
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT TEXT;
      """
    Then the last command output contains
      """
      transaction: {{transaction_id}}
      """
    And the last command output contains
      """
      inspected operation: 1
      operation 1: CREATE_CONFIGURATION kind=schema name=cli_inspected_event
      """
    When the CLI refuses this JSON inspection
      """
      DESCRIBE TRANSACTION 'missing-cli-transaction' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /error/code = "INSPECTION_REFUSED"
      /error/message = "transaction 'missing-cli-transaction' is unknown"
      """
    When the CLI refuses this JSON inspection
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 2 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /error/code = "INSPECTION_REFUSED"
      /error/message = "transaction '{{transaction_id}}' accepted 1 operation(s), so operation 2 does not exist"
      """
    When the CLI executes this JSON inspection with a missing CA file
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /error/code = "CLIENT_CONFIGURATION"
      """
    When the CLI executes this JSON inspection with an invalid server URL
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /error/code = "CONNECTION_FAILED"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @standalone_transaction_refresh
  Scenario: An ordinary command refreshes a frozen plan that loses its planning inputs
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "contender" is connected to the leader node
    Given transaction commit admission on the leader node pauses before execution
    When client "owner" begins executing these NSPL commands in the background
      """
      CREATE IF NOT EXISTS RESOURCE refreshed_command_resource;
      """
    Then the transaction commit admission pause on the leader node is reached
    When client "contender" executes these NSPL commands
      """
      CREATE RESOURCE refreshed_command_resource;
      """
    And the transaction commit admission pause on the leader node is released
    Then the background NSPL execution succeeds
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE refreshed_command_resource;
      """
    Then the last command output contains
      """
      resource: refreshed_command_resource
      """

  @transaction_report_restart
  Scenario: A queued transaction preview survives a full cluster restart
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA retained_before_restart (value STRING);
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER SCHEMA retained_before_restart
        ADD FIELD retained STRING OPTIONAL;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When the cluster is restarted
    Given client "resumed" is connected to the leader node
    When client "resumed" attaches to transaction "{{transaction_id}}"
    And client "resumed" attempts to commit its transaction
    Then client "resumed" commit was refused because its expected preview is stale
    And transaction "{{transaction_id}}" eventually has state "OPEN"
    When client "resumed" executes these NSPL commands
      """
      COMMIT;
      """
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA retained_before_restart;
      """
    Then the last command output contains
      """
      retained STRING OPTIONAL
      """

  Scenario: An open transaction survives leader failover and the client resumes it
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
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
      quiesce level: DYNAMIC
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA before_failover;
      SHOW CREATE SCHEMA after_failover;
      """
    Then the last command output contains
      """
      CREATE SCHEMA after_failover (
        value STRING
      );
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA closed_session_event (
        value STRING
      );
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

  @domain_mutation_ownership
  Scenario: An interrupted commit retains domain mutation ownership on the new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN independent_{{test_id}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    And client "contender" is connected to node "{{new_leader}}"
    And client "independent" is connected to node "{{new_leader}}"
    When client "independent" selects domain "independent_{{test_id}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE RESOURCE resumed_commit_bundle;
      CREATE SCHEMA resumed_commit (
        value STRING
      );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit on node "{{old_leader}}" pauses after 1 statement
    And transaction commit on node "{{new_leader}}" pauses after 2 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "{{old_leader}}" after 1 statement is reached
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    And the transaction commit pause on node "{{new_leader}}" after 2 statement is reached
    When client "contender" fails to execute these NSPL commands
      """
      CREATE SCHEMA competing_commit (value STRING);
      """
    Then the last command error contains
      """
      mutation is owned by transaction
      """
    When client "contender" fails to execute these NSPL commands
      """
      START;
      """
    Then the last command error contains
      """
      mutation is owned by transaction
      """
    When client "contender" fails to execute these NSPL commands
      """
      ALTER DOMAIN SET PLACEMENT REQUIRE COLOCATION;
      """
    Then the last command error contains
      """
      mutation is owned by transaction
      """
    When client "contender" fails to execute these NSPL commands
      """
      RELOCATE JUNCTION absent_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command error contains
      """
      mutation is owned by transaction
      """
    When client "contender" executes these NSPL commands
      """
      CREATE RESOURCE concurrent_bundle;
      """
    Then the last command output contains
      """
      created resource 'concurrent_bundle'
      """
    When client "independent" executes these NSPL commands
      """
      CREATE SCHEMA independent_commit (value STRING);
      SHOW CREATE SCHEMA independent_commit;
      """
    Then the last command output contains
      """
      CREATE SCHEMA independent_commit (
        value STRING
      );
      """
    When the transaction commit pause on node "{{new_leader}}" after 2 statement is released
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When client "contender" executes these NSPL commands
      """
      CREATE SCHEMA competing_commit (value STRING);
      SHOW CREATE SCHEMA resumed_commit;
      """
    Then the last command output contains
      """
      CREATE SCHEMA resumed_commit (
        value STRING
      );
      """
    When client "contender" executes these NSPL commands
      """
      SHOW CREATE SCHEMA competing_commit;
      """
    Then the last command output contains
      """
      CREATE SCHEMA competing_commit (
        value STRING
      );
      """
    When the transaction commit pause on node "{{old_leader}}" after 1 statement is released
    Then the background NSPL execution is discarded

  @command_completion
  Scenario: COMMIT remains applying until its durable model effect is activated
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to node "node-1"
    And client "observer" is connected to node "node-1"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA completion_record (value STRING);
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    And transaction "{{transaction_id}}" eventually has state "COMMITTING"
    When the transaction commit pause on node "node-1" after 1 statement is released
    Then the background NSPL execution succeeds
    When client "observer" executes these NSPL commands
      """
      SHOW CREATE SCHEMA completion_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA completion_record (
        value STRING
      );
      """

  @command_completion
  Scenario: A held commit does not block a commit in another domain
    Given a 1 node nervix cluster is started
    And the active domain is "blocked_{{test_id}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN blocked_{{test_id}};
      CREATE UNPACED DOMAIN runnable_{{test_id}};
      """
    Given client "blocked" is connected to node "node-1"
    And client "runnable" is connected to node "node-1"
    When client "blocked" selects domain "blocked_{{test_id}}"
    And client "runnable" selects domain "runnable_{{test_id}}"
    And client "blocked" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA blocked_record (value STRING);
      """
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "blocked" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    When client "runnable" executes these NSPL commands
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
    Given the active domain is "blocked_{{test_id}}"
    When the transaction commit pause on node "node-1" after 1 statement is released
    Then the background NSPL execution succeeds
    When client "blocked" executes these NSPL commands
      """
      SHOW CREATE SCHEMA blocked_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA blocked_record (
        value STRING
      );
      """

  @command_completion
  Scenario: Transactional START and STOP remain applying through activation
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to node "node-1"
    And client "observer" is connected to node "node-1"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      START;
      """
    Then client "owner" transaction id is saved as placeholder "start_transaction_id"
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    And transaction "{{start_transaction_id}}" eventually has state "COMMITTING"
    Then the background NSPL execution is discarded
    When client "owner" closes its session cleanly
    Given client "resumed" is connected to node "node-1"
    When client "resumed" attaches to transaction "{{start_transaction_id}}"
    And client "resumed" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    And the transaction commit pause on node "node-1" after 1 statement is released
    Then the background NSPL execution succeeds
    When client "observer" executes these NSPL commands
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: running
      """
    When client "resumed" executes these NSPL commands
      """
      BEGIN;
      STOP;
      """
    Then client "resumed" transaction id is saved as placeholder "stop_transaction_id"
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "resumed" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    And transaction "{{stop_transaction_id}}" eventually has state "COMMITTING"
    When the transaction commit pause on node "node-1" after 1 statement is released
    Then the background NSPL execution succeeds
    When client "observer" executes these NSPL commands
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      """

  @transaction_failed_resume
  Scenario: A failing resumed commit records the failing step and preserves its prefix
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    And client "observer" is connected to node "{{new_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE RESOURCE transaction_commit_prefix;
      CREATE RESOURCE transaction_commit_conflict;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit on node "{{old_leader}}" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "{{old_leader}}" after 1 statement is reached
    When client "observer" executes these NSPL commands
      """
      CREATE RESOURCE transaction_commit_conflict;
      """
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
      DESCRIBE RESOURCE transaction_commit_prefix;
      """
    Then the last command output contains
      """
      resource: transaction_commit_prefix
      """
    When the transaction commit pause on node "{{old_leader}}" after 1 statement is released
    Then the background NSPL execution is discarded

  Scenario: Attaching from a second session takes over an open transaction
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "taker" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA taken_over_event (
        value STRING
      );
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

  Scenario Outline: BEGIN requires an existing selected domain
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands fail with "domain '{{domain}}' does not exist"
      """
      BEGIN;
      """
    When this NSPL command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      no transactions
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_domain_binding
  Scenario Outline: A transaction is bound to the domain selected at BEGIN
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN transaction_other_domain;
      """
    Given client "owner" is connected to the leader node
    And client "taker" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA bound_domain_event (
        value STRING
      );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" selects domain "transaction_other_domain"
    And client "owner" fails to execute these NSPL commands
      """
      CREATE SCHEMA other_domain_event (
        value STRING
      );
      """
    Then the last command error contains
      """
      is bound to domain '{{domain}}'
      """
    When client "taker" executes these NSPL commands
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      domain={{domain}} state=OPEN pending=1
      """
    When client "taker" selects domain "transaction_other_domain"
    And client "taker" attaches to transaction "{{transaction_id}}"
    Then client "taker" active domain is "{{domain}}"
    When client "taker" executes these NSPL commands
      """
      COMMIT;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA bound_domain_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA bound_domain_event (
        value STRING
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_binding_recovery
  Scenario Outline: A session whose leader lost its binding re-attaches instead of failing
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA rebound_event (
        value STRING
      );
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the leader node forgets its transaction session bindings
    When client "owner" executes these NSPL commands
      """
      CREATE SCHEMA rebound_second_event (
        value STRING
      );
      COMMIT;
      """
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA rebound_second_event;
      """
    Then the last command output contains
      """
      CREATE SCHEMA rebound_second_event (
        value STRING
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Non-configuration statements are rejected while a transaction is open
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
      | CREATE DOMAIN transaction_extra_domain;                   | CREATE DOMAIN cannot be queued in a transaction             |
      | CREATE USER transaction_user WITH PASSWORD 'secret';      | CREATE USER cannot be queued in a transaction               |
      | CREATE SUBSCRIPTION tx_view TO missing_relay;             | session-scoped and client-local statements cannot be queued |
      | UPLOAD RESOURCE local_bundle VERSION '/tmp/local_bundle'; | client-local commands are not allowed                       |
      | CORDON NODE node-1;                                       | cannot be queued in a transaction                           |
      | DROP NODE node-1;                                         | cannot be queued in a transaction                           |

  @transaction_queue_preflight
  Scenario Outline: Queued statements are preflighted against the transaction prefix
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA queued_preflight (
        value STRING
      );
      """
    And client "owner" fails to execute these NSPL commands
      """
      ALTER SCHEMA queued_preflight
        DROP FIELD missing;
      """
    Then the last command error contains
      """
      field `missing` does not exist
      """
    When client "owner" fails to execute these NSPL commands
      """
      CREATE SCHEMA queued_preflight (
        value STRING
      );
      """
    Then the last command error contains
      """
      already exists
      """
    When client "observer" executes these NSPL commands
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      state=OPEN pending=1
      """
    When client "owner" executes these NSPL commands
      """
      ALTER SCHEMA queued_preflight
        ADD FIELD note STRING OPTIONAL;
      COMMIT;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA queued_preflight;
      """
    Then the last command output contains
      """
      CREATE SCHEMA queued_preflight (
        value STRING,
        note STRING OPTIONAL
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_batch_preflight
  Scenario Outline: Queue admission errors retain earlier request outcomes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      CREATE RESOURCE batch_preflight_bundle;
      CREATE RESOURCE batch_preflight_bundle;
      COMMIT;
      """
    Then the last command error contains
      """
      transaction started
      """
    And the last command error contains
      """
      resource 'batch_preflight_bundle' already exists
      """
    When these NSPL commands fail with "does not exist"
      """
      DESCRIBE RESOURCE batch_preflight_bundle;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_ordered_planning
  Scenario Outline: A later execution step cannot repair an incomplete model run
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to the leader node
    And client "observer" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE RELAY ordered_boundary_events
        SCHEMA ordered_boundary_schema
        UNBRANCHED
        CAPACITY 1;
      """
    And client "owner" fails to execute these NSPL commands
      """
      CREATE RESOURCE ordered_boundary_resource;
      """
    Then the last command error contains
      """
      ordered_boundary_schema
      """
    When client "observer" executes these NSPL commands
      """
      SHOW TRANSACTIONS;
      """
    Then the last command output contains
      """
      state=OPEN pending=1
      """
    When client "owner" executes these NSPL commands
      """
      CREATE SCHEMA ordered_boundary_schema (
        value STRING
      );
      CREATE RESOURCE ordered_boundary_resource;
      COMMIT;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE RELAY ordered_boundary_events;
      DESCRIBE RESOURCE ordered_boundary_resource;
      """
    Then the last command output contains
      """
      resource: ordered_boundary_resource
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_ordered_planning
  Scenario Outline: Atomic model runs use their base-to-final effect and ordered lifecycle state
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA ordered_effective_schema (
        value STRING
      );
      START;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Running"
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      DROP SCHEMA ordered_effective_schema;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When client "owner" executes these NSPL commands
      """
      CREATE SCHEMA ordered_effective_schema (
        value STRING,
        changed STRING OPTIONAL
      );
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      BEGIN;
      ALTER SCHEMA ordered_effective_schema
        ADD FIELD cancelled STRING OPTIONAL;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When client "owner" executes these NSPL commands
      """
      ALTER SCHEMA ordered_effective_schema
        DROP FIELD cancelled;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      BEGIN;
      STOP;
      ALTER SCHEMA ordered_effective_schema
        ADD FIELD stopped_change STRING OPTIONAL;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When client "owner" executes these NSPL commands
      """
      START;
      ALTER SCHEMA ordered_effective_schema
        DROP FIELD stopped_change;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When client "owner" executes these NSPL commands
      """
      CREATE IF NOT EXISTS SCHEMA ordered_effective_schema (
        value STRING,
        changed STRING OPTIONAL
      );
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Replicated transaction limits are enforced consistently
    Given the transaction statement limit is configured as 1
    And the transaction source byte limit is configured as 30
    And the concurrent transaction limit is configured as 1
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
      CREATE SCHEMA exceeds_byte_limit (
        value STRING
      );
      """
    Then the last command error contains
      """
      queued source byte limit 30 exceeded
      """

  Scenario: An orphaned transaction expires and retains its outcome
    Given the transaction idle timeout is configured as "250ms"
    # The scenario has to notice EXPIRED and then attach before the tombstone is swept, so the
    # retention has to cover a poll interval plus a client round trip. One second did not, and the
    # attach intermittently found the transaction already unknown. Removal still lands well inside
    # the ten seconds the "eventually removed" step allows.
    And the transaction tombstone retention is configured as "5s"
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
      domain={{domain}} state=OPEN
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      CREATE SCHEMA committed_notification (
        user_id I64
      );
      COMMIT
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA committed_notification;
      """
    Then the last command output contains
      """
      CREATE SCHEMA committed_notification (
        user_id I64
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_quiesce_output
  Scenario Outline: Transaction commands report planned quiescence and COMMIT reports only the executed aggregate
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA transaction_quiesce (
        value STRING
      );
      CREATE RELAY transaction_quiesce_events
        SCHEMA transaction_quiesce
        UNBRANCHED
        CAPACITY 1;
      START;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Running"
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER RELAY transaction_quiesce_events
        SET CAPACITY 2;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE RELAY transaction_quiesce_events;
      """
    Then the last command output contains
      """
      CAPACITY 1
      """
    When client "owner" executes these NSPL commands
      """
      ALTER SCHEMA transaction_quiesce
        ADD FIELD note STRING OPTIONAL;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
      CREATE SCHEMA transaction_event (
        value F64
      );
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE JSON SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE JSON SCHEMA transaction_event_wire MODE STRICT (
        value NUMBER
      );
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
      CREATE SCHEMA transaction_event_wire (
        value STRING
      );
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE JSON SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE JSON SCHEMA transaction_event_wire MODE LOOSE (
        value STRING
      );
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WIRE CBOR SCHEMA transaction_event_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE CBOR SCHEMA transaction_event_wire MODE STRICT (
        value STRING
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mixed_schema_commit
  Scenario Outline: A failing model queue preflight applies none of its mutations
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
      CREATE SCHEMA atomic_event (
        value STRING
      );
      """
    When these NSPL commands fail with "schema 'must_not_exist' does not exist"
      """
      SHOW CREATE SCHEMA must_not_exist;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
