Feature: Kafka acknowledgements across relay-owner recovery

  Scenario: Attached Kafka records on two branches survive relay-owner loss
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "relay_handoff_out_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA relay_handoff_event ( event_id STRING, branch_name STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA relay_handoff_wire MODE STRICT (
        event_id string, branch_name string, sequence integer
      );
      CREATE CODEC relay_handoff_codec
        FROM WIRE JSON SCHEMA relay_handoff_wire TO SCHEMA relay_handoff_event;
      CREATE SCHEMA relay_handoff_key ( branch_name STRING );
      CREATE BRANCH relay_handoff_branch SCHEMA relay_handoff_key TTL 5m;
      CREATE RELAY relay_handoff_records SCHEMA relay_handoff_event
        BRANCHED BY relay_handoff_branch CAPACITY 1;
      CREATE CLIENT relay_handoff_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR relay_handoff_source
        FROM KAFKA relay_handoff_kafka TOPIC relay_handoff_in_{{test_id}}
          OFFSET BY CONSUMER GROUP relay_handoff_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING relay_handoff_codec
        TO relay_handoff_records INHERIT ALL
          BRANCHED BY relay_handoff_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER relay_handoff_sink
        FROM relay_handoff_records
        TO KAFKA relay_handoff_kafka TOPIC relay_handoff_out_{{test_id}}
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING relay_handoff_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR relay_handoff_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY relay_handoff_records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER relay_handoff_sink ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=relay_handoff_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=relay_handoff_records owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=relay_handoff_sink owner=node-3
      """
    And the last cluster status owner for scheduled "relay" "relay_handoff_records" is saved as placeholder "relay_owner"
    When Kafka message is published to topic "relay_handoff_in_{{test_id}}"
      """
      {"event_id":"before-a","branch_name":"a","sequence":1}
      """
    And Kafka message is published to topic "relay_handoff_in_{{test_id}}"
      """
      {"event_id":"before-b","branch_name":"b","sequence":2}
      """
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"before-a","branch_name":"a","sequence":1
      "event_id":"before-b","branch_name":"b","sequence":2
      """
    And within "30s" Kafka consumer group "relay_handoff_group_{{test_id}}" next offset for topic "relay_handoff_in_{{test_id}}" partition 0 is "at least 2"
    When emitter "relay_handoff_sink" enters stall mode
    And Kafka message is published to topic "relay_handoff_in_{{test_id}}"
      """
      {"event_id":"during-a","branch_name":"a","sequence":3}
      """
    And Kafka message is published to topic "relay_handoff_in_{{test_id}}"
      """
      {"event_id":"during-b","branch_name":"b","sequence":4}
      """
    Then within "2s" Kafka consumer group "relay_handoff_group_{{test_id}}" next offset for topic "relay_handoff_in_{{test_id}}" partition 0 is "below 3"
    When node "node-2" is stopped
    Then within "60s" node "node-1" eventually reports scheduled "relay" "relay_handoff_records" owner different from placeholder "relay_owner"
    When emitter "relay_handoff_sink" leaves fault mode
    Then within "60s" the observed broker receives payloads
      """
      "event_id":"during-a","branch_name":"a","sequence":3
      "event_id":"during-b","branch_name":"b","sequence":4
      """
    And within "30s" Kafka consumer group "relay_handoff_group_{{test_id}}" next offset for topic "relay_handoff_in_{{test_id}}" partition 0 is "at least 4"
    When Kafka message is published to topic "relay_handoff_in_{{test_id}}"
      """
      {"event_id":"after-a","branch_name":"a","sequence":5}
      """
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"after-a","branch_name":"a","sequence":5
      """
    And within "30s" Kafka consumer group "relay_handoff_group_{{test_id}}" next offset for topic "relay_handoff_in_{{test_id}}" partition 0 is "at least 5"
