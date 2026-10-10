@remote_relay_memory_hold
Feature: Remote relay delivery held back while the relay memory budget is full

  Scenario: A delivery whose acknowledgement watches find the relay owner's memory full is held back and delivered once room returns
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "remote_hold_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "remote_hold_out_{{test_id}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA remote_hold_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA remote_hold_wire MODE STRICT ( event_id string );
      CREATE CODEC remote_hold_codec
        FROM WIRE JSON SCHEMA remote_hold_wire TO SCHEMA remote_hold_event;
      CREATE RELAY remote_hold_records SCHEMA remote_hold_event UNBRANCHED;
      CREATE CLIENT remote_hold_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR remote_hold_source
        FROM KAFKA remote_hold_kafka TOPIC remote_hold_in_{{test_id}}
          OFFSET BY CONSUMER GROUP remote_hold_group_{{test_id}}
          INSTANCES 1
          MODE NO_ACK PARALLEL
          ON QUIESCE SUSPEND DECODE USING remote_hold_codec
        TO remote_hold_records INHERIT ALL UNBRANCHED
          FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER remote_hold_sink
        FROM remote_hold_records
        TO KAFKA remote_hold_kafka TOPIC remote_hold_out_{{test_id}}
          MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING remote_hold_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed on the leader node
      """
      RELOCATE INGESTOR remote_hold_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY remote_hold_records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER remote_hold_sink ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=remote_hold_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=remote_hold_records owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=remote_hold_sink owner=node-2
      """
    When node "node-2" fills its relay memory before it charges its next acknowledgement watches
    And these Kafka messages are rapidly published to topic "remote_hold_in_{{test_id}}"
      """
      {"event_id":"held-1"}
      {"event_id":"held-2"}
      {"event_id":"held-3"}
      {"event_id":"held-4"}
      {"event_id":"held-5"}
      {"event_id":"held-6"}
      {"event_id":"held-7"}
      {"event_id":"held-8"}
      """
    Then within "60s" node "node-2" has filled its relay memory
    And within "30s" node "node-2" observability metric "nervix_interconnect_relay_attempts" with labels eventually equals 1
      """
      """
    When node "node-2" releases its filled relay memory
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"event_id":"held-1"}
      {"event_id":"held-2"}
      {"event_id":"held-3"}
      {"event_id":"held-4"}
      {"event_id":"held-5"}
      {"event_id":"held-6"}
      {"event_id":"held-7"}
      {"event_id":"held-8"}
      """
    And within "30s" node "node-2" observability metric "nervix_interconnect_relay_attempts" with labels eventually equals 0
      """
      """
    And node "node-2" observability metric "nervix_execution_memory_rejections_total" with labels eventually equals 0
      """
      class="relay"
      """
