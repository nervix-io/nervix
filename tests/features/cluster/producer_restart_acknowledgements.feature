@remote_ack_owners
Feature: Remote acknowledgements across a producer restart

  @deloxide_stress
  Scenario: An acknowledgement addressed to a restarted producer's earlier process leaves its new records pending
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "restart_ack_before_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "restart_ack_after_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "restart_ack_out_{{test_id}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA restart_ack_event ( event_id STRING, branch_name STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA restart_ack_wire MODE STRICT (
        event_id string, branch_name string, sequence integer
      );
      CREATE CODEC restart_ack_codec
        FROM WIRE JSON SCHEMA restart_ack_wire TO SCHEMA restart_ack_event;
      CREATE SCHEMA restart_ack_key ( branch_name STRING );
      CREATE BRANCH restart_ack_branch SCHEMA restart_ack_key TTL 5m;
      CREATE RELAY restart_ack_before_records SCHEMA restart_ack_event
        BRANCHED BY restart_ack_branch;
      CREATE RELAY restart_ack_after_records SCHEMA restart_ack_event
        BRANCHED BY restart_ack_branch;
      CREATE CLIENT restart_ack_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR restart_ack_before_source
        FROM KAFKA restart_ack_kafka TOPIC restart_ack_before_in_{{test_id}}
          OFFSET BY CONSUMER GROUP restart_ack_before_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING restart_ack_codec
        TO restart_ack_before_records INHERIT ALL
          BRANCHED BY restart_ack_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR restart_ack_after_source
        FROM KAFKA restart_ack_kafka TOPIC restart_ack_after_in_{{test_id}}
          OFFSET BY CONSUMER GROUP restart_ack_after_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING restart_ack_codec
        TO restart_ack_after_records INHERIT ALL
          BRANCHED BY restart_ack_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER restart_ack_before_sink
        FROM restart_ack_before_records
        TO KAFKA restart_ack_kafka TOPIC restart_ack_out_{{test_id}}
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING restart_ack_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER restart_ack_after_sink
        FROM restart_ack_after_records
        TO KAFKA restart_ack_kafka TOPIC restart_ack_out_{{test_id}}
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING restart_ack_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR restart_ack_before_source ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE INGESTOR restart_ack_after_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY restart_ack_before_records ONTO NODE node-3 IGNORE PREFERENCES;
      RELOCATE RELAY restart_ack_after_records ONTO NODE node-3 IGNORE PREFERENCES;
      RELOCATE EMITTER restart_ack_before_sink ONTO NODE node-3 IGNORE PREFERENCES;
      RELOCATE EMITTER restart_ack_after_sink ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=restart_ack_before_source owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=ingestor name=restart_ack_after_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=restart_ack_before_records owner=node-3
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=restart_ack_after_records owner=node-3
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=restart_ack_before_sink owner=node-3
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=restart_ack_after_sink owner=node-3
      """
    And the last cluster status owner for scheduled "ingestor" "restart_ack_before_source" is saved as placeholder "before_source_owner"
    Given admitted remote relay dispatch for domain "{{domain}}" is paused
    When Kafka message is published to topic "restart_ack_before_in_{{test_id}}"
      """
      {"event_id":"before-restart","branch_name":"a","sequence":1}
      """
    Then the admitted remote relay dispatch pause for domain "{{domain}}" is reached
    When node "node-2" is stopped
    Then within "60s" node "node-1" eventually reports scheduled "ingestor" "restart_ack_before_source" owner different from placeholder "before_source_owner"
    When node "node-2" is started
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-3" eventually reports interconnect to "node-2" as "connected"
    And within "30s" node "node-3" observability metric "nervix_interconnect_relay_attempts" with labels eventually equals 0
      """
      """
    When these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR restart_ack_after_source ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=restart_ack_after_source owner=node-2
      """
    Given remote relay admission for branch '{"branch_name":"b"}' in domain "{{domain}}" is paused
    When emitter "restart_ack_after_sink" enters stall mode
    And Kafka message is published to topic "restart_ack_after_in_{{test_id}}"
      """
      {"event_id":"after-restart","branch_name":"b","sequence":2}
      """
    Then the remote relay admission pause for branch '{"branch_name":"b"}' in domain "{{domain}}" is reached
    When the admitted remote relay dispatch pause for domain "{{domain}}" is released
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"before-restart","branch_name":"a","sequence":1
      """
    When the remote relay admission pause for branch '{"branch_name":"b"}' in domain "{{domain}}" is released
    Then within "10s" Kafka consumer group "restart_ack_after_group_{{test_id}}" next offset for topic "restart_ack_after_in_{{test_id}}" partition 0 is "below 1"
    When emitter "restart_ack_after_sink" leaves fault mode
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"after-restart","branch_name":"b","sequence":2
      """
    And within "30s" Kafka consumer group "restart_ack_after_group_{{test_id}}" next offset for topic "restart_ack_after_in_{{test_id}}" partition 0 is "at least 1"
