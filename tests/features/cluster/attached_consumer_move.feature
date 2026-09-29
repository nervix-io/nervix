Feature: Attached acknowledgements across consumer moves

  Scenario: A Kafka record admitted on a node whose attached emitter moved away is emitted by the new owner
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "moved_sink_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "moved_sink_out_{{test_id}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA moved_sink_event ( event_id STRING, branch_name STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA moved_sink_wire MODE STRICT (
        event_id string, branch_name string, sequence integer
      );
      CREATE CODEC moved_sink_codec
        FROM WIRE JSON SCHEMA moved_sink_wire TO SCHEMA moved_sink_event;
      CREATE SCHEMA moved_sink_key ( branch_name STRING );
      CREATE BRANCH moved_sink_branch SCHEMA moved_sink_key TTL 5m;
      CREATE RELAY moved_sink_records SCHEMA moved_sink_event
        BRANCHED BY moved_sink_branch CAPACITY 1;
      CREATE CLIENT moved_sink_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR moved_sink_source
        FROM KAFKA moved_sink_kafka TOPIC moved_sink_in_{{test_id}}
          OFFSET BY CONSUMER GROUP moved_sink_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING moved_sink_codec
        TO moved_sink_records INHERIT ALL
          BRANCHED BY moved_sink_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER moved_sink
        FROM moved_sink_records
        TO KAFKA moved_sink_kafka TOPIC moved_sink_out_{{test_id}}
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING moved_sink_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR moved_sink_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY moved_sink_records ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE EMITTER moved_sink ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=moved_sink_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=moved_sink_records owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=moved_sink owner=node-3
      """
    When Kafka message is published to topic "moved_sink_in_{{test_id}}"
      """
      {"event_id":"before","branch_name":"a","sequence":1}
      """
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"before","branch_name":"a","sequence":1
      """
    And within "30s" Kafka consumer group "moved_sink_group_{{test_id}}" next offset for topic "moved_sink_in_{{test_id}}" partition 0 is "at least 1"
    Given admitted remote relay dispatch for domain "{{domain}}" is paused
    When Kafka message is published to topic "moved_sink_in_{{test_id}}"
      """
      {"event_id":"routed","branch_name":"a","sequence":2}
      """
    Then the admitted remote relay dispatch pause for domain "{{domain}}" is reached
    When these NSPL commands are executed on the leader node
      """
      RELOCATE EMITTER moved_sink ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=emitter name=moved_sink owner=node-2
      """
    When the admitted remote relay dispatch pause for domain "{{domain}}" is released
    Then within "60s" Kafka consumer group "moved_sink_group_{{test_id}}" next offset for topic "moved_sink_in_{{test_id}}" partition 0 is "at least 2"
    And within "30s" the observed broker receives payloads
      """
      "event_id":"routed","branch_name":"a","sequence":2
      """
    When Kafka message is published to topic "moved_sink_in_{{test_id}}"
      """
      {"event_id":"after","branch_name":"a","sequence":3}
      """
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"after","branch_name":"a","sequence":3
      """
    And within "30s" Kafka consumer group "moved_sink_group_{{test_id}}" next offset for topic "moved_sink_in_{{test_id}}" partition 0 is "at least 3"
