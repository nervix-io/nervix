@transaction_quiesce_qualification
Feature: Transaction quiesce qualification
  Scenario: A multi-mebibyte report replicates without truncation and survives replay and restart
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "original_leader"
    And a node other than placeholder "original_leader" is saved as placeholder "catch_up_node"
    Given the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CORDON NODE {{catch_up_node}};
      """
    And a stopped transaction qualification graph with 120 relays is configured
    Given client "owner" is connected to node "{{original_leader}}" with cluster seeds
    When node "{{catch_up_node}}" is stopped
    Given the leader raft log position and retained bytes are remembered
    When client "owner" queues 128 transaction qualification schema changes
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last JSON inspection matches its typed result, exceeds 2097152 bytes, and reports 128 operations and 1 execution steps
    And the last inspection reports
      """
      transaction: {{transaction_id}}
      state: OPEN
      accepted operations: 128
      applied operations: 0
      selected operation: 1
      report operations: 128
      execution steps: 1
      applied steps: 0
      quiesce level: DYNAMIC
      """
    And the remembered raft log grew by more than 2097152 bytes
    When node "{{catch_up_node}}" is started
    Then within "60s" node "{{catch_up_node}}" retains transaction "{{transaction_id}}" with 128 report operations
    When leadership is transferred from node "{{original_leader}}" to node "{{catch_up_node}}"
    Then node "{{catch_up_node}}" eventually reports leader "{{catch_up_node}}"
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last JSON inspection matches its typed result, exceeds 2097152 bytes, and reports 128 operations and 1 execution steps
    When client "owner" attempts to commit its transaction
    Then client "owner" transaction state is "COMMITTED"
    Given client "observer" is connected to node "{{catch_up_node}}" with cluster seeds
    When client "observer" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT JSON;
      """
    Then the last JSON inspection matches its typed result, exceeds 2097152 bytes, and reports 128 operations and 1 execution steps
    And the last inspection reports
      """
      transaction: {{transaction_id}}
      state: COMMITTED
      accepted operations: 128
      applied operations: 128
      selected operation: 1
      report operations: 128
      execution steps: 1
      applied steps: 1
      quiesce level: DYNAMIC
      """
    And the last transaction inspection is remembered for restart comparison
    When the cluster is restarted
    Given client "restarted_observer" is connected to the leader node
    When client "restarted_observer" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT JSON;
      """
    Then the last JSON inspection matches its typed result, exceeds 2097152 bytes, and reports 128 operations and 1 execution steps
    And the last transaction inspection matches the report remembered before restart
