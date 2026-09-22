Feature: Transaction actual quiescence
  @transaction_actual_quiescence
  Scenario Outline: A gate rejected before engagement stays dynamic in the transaction result
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_gate_event ( key I64, alternate I64 );
      CREATE RELAY actual_gate_incoming SCHEMA actual_gate_event UNBRANCHED;
      CREATE RELAY actual_gate_outgoing SCHEMA actual_gate_event UNBRANCHED;
      CREATE DEDUPLICATOR actual_gate_dedup
        FROM actual_gate_incoming
        DEDUPLICATE ON input.key
        MAX TIME 10m
        UNBRANCHED
        TO actual_gate_outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER DEDUPLICATOR actual_gate_dedup
        SET DEDUPLICATE ON input.alternate;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the next entity gate engagement in domain "{{domain}}" is rejected
    When client "owner" fails to execute these NSPL commands
      """
      COMMIT;
      """
    Then the last command error contains
      """
      quiesce level: DYNAMIC
      """
    And transaction "{{transaction_id}}" eventually has state "FAILED"
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "ENTITY_PAUSE", actual quiesce "DYNAMIC", execution "FAILED", and outcomes "REQUESTED,FAILED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_actual_quiescence
  Scenario Outline: A domain drain timeout retains the engaged domain pause and its release
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_domain_event ( key I64 );
      CREATE RELAY actual_domain_incoming SCHEMA actual_domain_event UNBRANCHED;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER SCHEMA actual_domain_event ADD FIELD note STRING OPTIONAL;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the next domain drain in domain "{{domain}}" is forced to time out
    When client "owner" fails to execute these NSPL commands
      """
      COMMIT;
      """
    Then the last command error contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    And transaction "{{transaction_id}}" eventually has state "FAILED"
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "DOMAIN_PAUSE", actual quiesce "DOMAIN_PAUSE", execution "FAILED", and outcomes "REQUESTED,CONFIRMED,FAILED,RELEASED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_actual_quiescence
  Scenario Outline: An entity drain failure retains the engaged entity gate and its release
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_entity_event ( key I64, alternate I64 );
      CREATE RELAY actual_entity_incoming SCHEMA actual_entity_event UNBRANCHED;
      CREATE RELAY actual_entity_outgoing SCHEMA actual_entity_event UNBRANCHED;
      CREATE DEDUPLICATOR actual_entity_dedup
        FROM actual_entity_incoming
        DEDUPLICATE ON input.key
        MAX TIME 10m
        UNBRANCHED
        TO actual_entity_outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER DEDUPLICATOR actual_entity_dedup
        SET DEDUPLICATE ON input.alternate;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When client "owner" fails to execute these NSPL commands
      """
      COMMIT;
      """
    Then the last command error contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And transaction "{{transaction_id}}" eventually has state "FAILED"
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "ENTITY_PAUSE", actual quiesce "ENTITY_PAUSE", execution "FAILED", and outcomes "REQUESTED,CONFIRMED,FAILED,RELEASED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_actual_quiescence
  Scenario: A timed-out remote gate response records uncertain engagement
    Given entity gate deadline is configured as "750ms"
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "remote_node"
    Given the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_remote_event ( key I64, alternate I64 );
      CREATE RELAY actual_remote_incoming SCHEMA actual_remote_event UNBRANCHED;
      CREATE RELAY actual_remote_outgoing SCHEMA actual_remote_event UNBRANCHED;
      CREATE DEDUPLICATOR actual_remote_dedup
        FROM actual_remote_incoming
        DEDUPLICATE ON input.key
        MAX TIME 10m
        UNBRANCHED
        TO actual_remote_outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER DEDUPLICATOR actual_remote_dedup
        SET DEDUPLICATE ON input.alternate;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the entity gate response from node "{{remote_node}}" for domain "{{domain}}" pauses after engagement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the entity gate response pause from node "{{remote_node}}" for domain "{{domain}}" is reached
    And the background NSPL execution fails with "failed to engage entity gates"
    When the entity gate response pause from node "{{remote_node}}" for domain "{{domain}}" is released
    Then transaction "{{transaction_id}}" eventually has state "FAILED"
    And the last command error contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "ENTITY_PAUSE", actual quiesce "ENTITY_PAUSE", execution "FAILED", and outcomes "REQUESTED,UNCERTAIN,RELEASED"

  @transaction_actual_quiescence
  Scenario: A successor records and releases a domain pause engaged by the former leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_failover_event ( key I64 );
      CREATE RELAY actual_failover_incoming SCHEMA actual_failover_event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "owner" is connected to node "{{old_leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER SCHEMA actual_failover_event ADD FIELD note STRING OPTIONAL;
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
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "DOMAIN_PAUSE", actual quiesce "DOMAIN_PAUSE", execution "APPLIED", and outcomes "REQUESTED,CONFIRMED,RELEASED"
    When the transaction commit pause on node "{{old_leader}}" after 1 statement is released
    Then the background NSPL execution is discarded

  @transaction_actual_quiescence
  Scenario Outline: A failed later step retains the actual pause of the committed prefix
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_partial_event ( key I64, alternate I64 );
      CREATE RELAY actual_partial_incoming SCHEMA actual_partial_event UNBRANCHED;
      CREATE RELAY actual_partial_outgoing SCHEMA actual_partial_event UNBRANCHED;
      CREATE DEDUPLICATOR actual_partial_dedup
        FROM actual_partial_incoming
        DEDUPLICATE ON input.key
        MAX TIME 10m
        UNBRANCHED
        TO actual_partial_outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER SCHEMA actual_partial_event ADD FIELD note STRING OPTIONAL;
      CREATE RESOURCE actual_partial_resource;
      ALTER DEDUPLICATOR actual_partial_dedup
        SET DEDUPLICATE ON input.alternate;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the next entity gate engagement in domain "{{domain}}" is rejected
    When client "owner" fails to execute these NSPL commands
      """
      COMMIT;
      """
    Then the last command error contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    And transaction "{{transaction_id}}" eventually has state "FAILED"
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "DOMAIN_PAUSE", actual quiesce "DOMAIN_PAUSE", execution "APPLIED", and outcomes "REQUESTED,CONFIRMED,RELEASED"
    And transaction "{{transaction_id}}" report step 3 eventually records planned quiesce "ENTITY_PAUSE", actual quiesce "DYNAMIC", execution "FAILED", and outcomes "REQUESTED,FAILED"
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE actual_partial_resource;
      """
    Then the last command output contains
      """
      resource: actual_partial_resource
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @transaction_actual_quiescence
  Scenario Outline: Entity swap fallback records the wider recovery pause and rebuild scope
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA actual_recovery_event ( key I64, alternate I64 );
      CREATE RELAY actual_recovery_incoming SCHEMA actual_recovery_event UNBRANCHED;
      CREATE RELAY actual_recovery_outgoing SCHEMA actual_recovery_event UNBRANCHED;
      CREATE DEDUPLICATOR actual_recovery_dedup
        FROM actual_recovery_incoming
        DEDUPLICATE ON input.key
        MAX TIME 10m
        UNBRANCHED
        TO actual_recovery_outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      ALTER DEDUPLICATOR actual_recovery_dedup
        SET DEDUPLICATE ON input.alternate;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given the next entity schedule swap in domain "{{domain}}" on the leader is forced to fail
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "ENTITY_PAUSE", actual quiesce "DOMAIN_PAUSE", execution "APPLIED", and outcomes "REQUESTED,CONFIRMED,RELEASED|REQUESTED,CONFIRMED,RELEASED"
    And transaction "{{transaction_id}}" report step 1 eventually records recovery rebuild effects

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
