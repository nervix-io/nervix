Feature: Client wire failure regressions
  @client_wire_durable_admission
  Scenario: A command lost after durable admission is recovered by its request identity
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When the active session begins this NSPL command request with execution reference "durable-admission-{{test_id}}" in the background
      """
      CREATE SCHEMA durable_admission_record (
        value STRING
      );
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When the background command request connection is dropped
    And the durable command admission pause on node "{{leader}}" is released
    And this NSPL command request with execution reference "durable-admission-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA durable_admission_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this NSPL command request is executed on the leader node
      """
      SHOW CREATE SCHEMA durable_admission_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA durable_admission_record (
        value STRING
      );
      """

  @client_wire_expected_failure @client_wire_missing_command
  Scenario Outline: A command missing from a committed transaction cannot report aggregate success
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "owner" is connected to node "{{leader}}"
    And client "finisher" is connected to node "{{leader}}"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given command admission on node "{{leader}}" pauses before proposal
    When client "owner" begins executing these NSPL commands in the background
      """
      CREATE SCHEMA missing_command_record (
        value STRING
      );
      """
    Then the command admission pause on node "{{leader}}" is reached
    When client "finisher" attaches to transaction "{{transaction_id}}"
    And client "finisher" executes these NSPL commands
      """
      COMMIT;
      """
    And the command admission pause on node "{{leader}}" is released
    Then the background NSPL execution does not report success

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_expected_failure @client_wire_lost_begin
  Scenario Outline: Replaying a BEGIN whose response was lost returns the original transaction
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session begins this NSPL command request with execution reference "lost-begin-{{test_id}}" in the background
      """
      BEGIN;
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the background command request connection is dropped
    And the command response delivery pause on node "{{leader}}" is released
    And this NSPL command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "original_transaction"
    When this NSPL command request with execution reference "lost-begin-{{test_id}}" is executed on the leader node
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

  @client_wire_expected_failure @client_wire_duplicate_append
  Scenario Outline: Replaying an admitted append does not preflight or append it again
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When these NSPL commands are executed on the active session
      """
      BEGIN;
      """
    And this NSPL command request is executed on the leader node
      """
      SHOW TRANSACTIONS;
      """
    Then the only transaction id is saved as placeholder "transaction_id"
    Given command response delivery on node "{{leader}}" pauses after execution
    When the active session sends this NSPL command request with execution reference "append-{{test_id}}" without reading its response
      """
      CREATE SCHEMA replayed_append_record (
        value STRING
      );
      """
    Then the command response delivery pause on node "{{leader}}" is reached
    When the command response delivery pause on node "{{leader}}" is released
    And a new session attaches to transaction "{{transaction_id}}" and executes this NSPL command with execution reference "append-{{test_id}}"
      """
      CREATE SCHEMA replayed_append_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this NSPL command request is executed on the leader node
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

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @client_wire_expected_failure @client_wire_stalled_commit
  Scenario: A stalled commit cannot block expiry of another orphaned transaction
    Given the transaction idle timeout is configured as "250ms"
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "blocked" is connected to node "node-1"
    And client "expiring" is connected to node "node-1"
    And client "observer" is connected to node "node-1"
    When client "blocked" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA blocked_commit_record (
        value STRING
      );
      """
    Then client "blocked" transaction id is saved as placeholder "blocked_transaction"
    When client "expiring" executes these NSPL commands
      """
      BEGIN;
      """
    Then client "expiring" transaction id is saved as placeholder "expiring_transaction"
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "blocked" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    Given the leader node forgets its transaction session bindings
    When client "observer" executes these NSPL commands
      """
      SHOW TRANSACTIONS;
      """
    And physical time passes for "1s"
    Then transaction "{{expiring_transaction}}" eventually has state "EXPIRED"

  @client_wire_expected_failure @client_wire_stale_relocation
  Scenario: A relocation planned before a schedule revision cannot overwrite that revision
    Given Kafka is running
    Given runtime replication is configured with replica count 2 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And Kafka topic "relocation_offsets_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA relocation_record ( sequence I64 );
      CREATE WIRE JSON SCHEMA relocation_wire MODE STRICT ( sequence integer );
      CREATE CODEC relocation_codec
        FROM WIRE JSON SCHEMA relocation_wire
        TO SCHEMA relocation_record;
      CREATE SCHEMA relocation_tenant ( tenant STRING );
      CREATE BRANCH relocation_by_tenant SCHEMA relocation_tenant TTL 5m;
      CREATE RELAY relocation_input SCHEMA relocation_record UNBRANCHED;
      CREATE RELAY relocation_output SCHEMA relocation_record UNBRANCHED;
      CREATE RELAY relocation_offsets SCHEMA relocation_record BRANCHED BY relocation_by_tenant;
      CREATE CLIENT relocation_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR relocation_offset_source
        FROM KAFKA relocation_kafka TOPIC relocation_offsets_{{test_id}}
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
      CREATE JUNCTION relocation_route FROM relocation_input UNBRANCHED
        TO relocation_output INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "relocation_route" is saved as placeholder "source_owner"
    And the first replica for scheduled "junction" "relocation_route" in the last cluster status is saved as placeholder "relocation_target"
    And within "5s" DESCRIBE INGESTOR "relocation_offset_source" on the leader node contains
      """
      kafka observed partitions: 0
      """
    Given relocation publication for domain "{{domain}}" pauses after planning
    When these NSPL commands begin executing in the background
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
    Then the background NSPL execution fails with "schedule changed"
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE INGESTOR relocation_offset_source;
      """
    Then the last command output contains
      """
      kafka observed partitions: 0,1
      """

  @client_wire_expected_failure @client_wire_subscription_restore
  Scenario: A reconnected native client restores acknowledged subscriptions
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
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
      CREATE CODEC wire_record_codec
        FROM WIRE JSON SCHEMA wire_record_json
        TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge client-wire-{{test_id}}.example.com;
      CREATE ENDPOINT wire_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE INGESTOR wire_source
        FROM ENDPOINT wire_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "subscriber" is connected to node "{{old_leader}}"
    When client "subscriber" executes these NSPL commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And http payload is posted to node "{{old_leader}}" with host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "10s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When client "subscriber" executes these NSPL commands
      """
      DESCRIBE DOMAIN;
      """
    And node "{{old_leader}}" is stopped
    And http payload is posted to node "{{new_leader}}" with host "client-wire-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "3s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"beta"} payload={"sequence":2,"tenant":"beta"}
      """
