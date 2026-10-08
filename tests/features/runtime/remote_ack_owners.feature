@remote_ack_owners
Feature: Attached record outcomes across independent branch deliveries

  Scenario Outline: Interleaved branches complete attached deliveries and commit their source offsets
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "ack_owners_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "ack_owners_out_{{test_id}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA ack_owner_event ( event_id STRING, branch_name STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA ack_owner_wire MODE STRICT (
        event_id string, branch_name string, sequence integer
      );
      CREATE CODEC ack_owner_codec FROM WIRE JSON SCHEMA ack_owner_wire TO SCHEMA ack_owner_event;
      CREATE SCHEMA ack_owner_key ( branch_name STRING );
      CREATE BRANCH ack_owner_branch SCHEMA ack_owner_key TTL 5m;
      CREATE RELAY ack_owner_records SCHEMA ack_owner_event BRANCHED BY ack_owner_branch;
      CREATE CLIENT ack_owner_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR ack_owner_source
        FROM KAFKA ack_owner_kafka TOPIC ack_owners_in_{{test_id}}
          OFFSET BY CONSUMER GROUP ack_owners_group_{{test_id}}
          MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING ack_owner_codec
        TO ack_owner_records INHERIT ALL
          BRANCHED BY ack_owner_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER ack_owner_sink
        FROM ack_owner_records
        TO KAFKA ack_owner_kafka TOPIC ack_owners_out_{{test_id}}
          MODE ACK PARALLEL MAX 8 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING ack_owner_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And Kafka message is published to topic "ack_owners_in_{{test_id}}"
      """
      {"event_id":"a-first","branch_name":"a","sequence":1}
      """
    And Kafka message is published to topic "ack_owners_in_{{test_id}}"
      """
      {"event_id":"b-first","branch_name":"b","sequence":2}
      """
    And Kafka message is published to topic "ack_owners_in_{{test_id}}"
      """
      {"event_id":"a-next","branch_name":"a","sequence":3}
      """
    And Kafka message is published to topic "ack_owners_in_{{test_id}}"
      """
      {"event_id":"b-next","branch_name":"b","sequence":4}
      """
    Then within "60s" the observed broker receives payloads
      """
      "event_id":"a-first","branch_name":"a","sequence":1
      "event_id":"b-first","branch_name":"b","sequence":2
      "event_id":"a-next","branch_name":"a","sequence":3
      "event_id":"b-next","branch_name":"b","sequence":4
      """
    And within "30s" Kafka consumer group "ack_owners_group_{{test_id}}" next offset for topic "ack_owners_in_{{test_id}}" partition 0 is "at least 4"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
