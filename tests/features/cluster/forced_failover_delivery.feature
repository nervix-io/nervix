@forced_failover_delivery @deloxide_stress
Feature: Forced ownership recovery while a former owner holds relay deliveries

  Scenario: A forced failover and a later revision install while the former owner remains paused
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And entity gate deadline is configured as "60s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "forced_delivery_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "forced_delivery_out_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA forced_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA forced_wire MODE STRICT ( event_id string );
      CREATE CODEC forced_codec FROM WIRE JSON SCHEMA forced_wire TO SCHEMA forced_event;
      CREATE RELAY forced_records SCHEMA forced_event UNBRANCHED;
      CREATE CLIENT forced_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR forced_source
        FROM KAFKA forced_kafka TOPIC forced_delivery_in_{{test_id}}
          OFFSET BY CONSUMER GROUP forced_delivery_group_{{test_id}} INSTANCES 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING forced_codec
        TO forced_records INHERIT ALL UNBRANCHED
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER forced_sink FROM forced_records
        TO KAFKA forced_kafka TOPIC forced_delivery_out_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING forced_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      UNCORDON NODE node-3;
      START;
      RELOCATE EMITTER forced_sink ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=forced_records owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=forced_sink owner=node-3
      """
    When Kafka message is published to topic "forced_delivery_in_{{test_id}}"
      """
      {"event_id":"warmup"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"warmup"}
      """
    When node "node-3" fills its relay memory before it charges its next acknowledgement watches
    And Kafka message is published to topic "forced_delivery_in_{{test_id}}"
      """
      {"event_id":"held-during-failover"}
      """
    Then within "30s" node "node-3" has filled its relay memory
    When runtime preparation on node "node-3" is paused
    And application health responses from node "node-3" fail
    And gossip exchanges involving node "node-3" are blocked with a "0s" send delay
    Then within "30s" node "node-1" reports no scheduled work on "node-3"
    And node "node-3" reaches its runtime preparation pause
    When consensus connectivity for node "node-3" is blocked
    Then node "node-1" eventually observes a stable leader
    And within "20s" the observed broker receives exactly these payloads
      """
      {"event_id":"held-during-failover"}
      """
    And within "20s" these NSPL commands complete on node "node-1"
      """
      CREATE RELAY forced_canary SCHEMA forced_event UNBRANCHED;
      """
    When Kafka message is published to topic "forced_delivery_in_{{test_id}}"
      """
      {"event_id":"after-later-revision"}
      """
    Then within "20s" the observed broker receives exactly these payloads
      """
      {"event_id":"after-later-revision"}
      """
    When runtime preparation on node "node-3" is released
    And node "node-3" releases its filled relay memory
    And gossip exchanges involving node "node-3" are restored
    And consensus connectivity for node "node-3" is restored
