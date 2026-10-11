@remote_ack_owners
Feature: Remote record acknowledgements lost between nodes

  @deloxide_stress
  Scenario: A record acknowledgement lost on its way back to the relay owner is redelivered instead of stalling its source
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "lost_ack_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "lost_ack_out_{{test_id}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA lost_ack_event ( event_id STRING, branch_name STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA lost_ack_wire MODE STRICT (
        event_id string, branch_name string, sequence integer
      );
      CREATE CODEC lost_ack_codec
        FROM WIRE JSON SCHEMA lost_ack_wire TO SCHEMA lost_ack_event;
      CREATE SCHEMA lost_ack_key ( branch_name STRING );
      CREATE BRANCH lost_ack_branch SCHEMA lost_ack_key TTL 5m;
      CREATE RELAY lost_ack_records SCHEMA lost_ack_event BRANCHED BY lost_ack_branch;
      CREATE CLIENT lost_ack_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR lost_ack_source
        FROM KAFKA lost_ack_kafka TOPIC lost_ack_in_{{test_id}}
          OFFSET BY CONSUMER GROUP lost_ack_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING lost_ack_codec
        TO lost_ack_records INHERIT ALL
          BRANCHED BY lost_ack_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER lost_ack_sink
        FROM lost_ack_records
        TO KAFKA lost_ack_kafka TOPIC lost_ack_out_{{test_id}}
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING lost_ack_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR lost_ack_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY lost_ack_records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER lost_ack_sink ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=lost_ack_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=lost_ack_records owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=lost_ack_sink owner=node-3
      """
    Given the next record acknowledgement node "node-3" returns to node "node-2" is lost
    When Kafka message is published to topic "lost_ack_in_{{test_id}}"
      """
      {"event_id":"lost-ack","branch_name":"a","sequence":1}
      """
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"lost-ack","branch_name":"a","sequence":1
      """
    And the record acknowledgement node "node-3" returned to node "node-2" was lost
    When Kafka message is published to topic "lost_ack_in_{{test_id}}"
      """
      {"event_id":"after-lost-ack","branch_name":"b","sequence":2}
      """
    Then within "90s" the observed broker receives payloads
      """
      "event_id":"after-lost-ack","branch_name":"b","sequence":2
      """
    And within "30s" Kafka consumer group "lost_ack_group_{{test_id}}" next offset for topic "lost_ack_in_{{test_id}}" partition 0 is "at least 2"
