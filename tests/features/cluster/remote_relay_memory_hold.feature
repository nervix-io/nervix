@remote_relay_memory_hold
Feature: Remote relay delivery held back while the relay memory budget is full

  Scenario: A delivery whose acknowledgement watches find the relay owner's memory full is held back and delivered once room returns
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "remote_hold_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "remote_hold_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA remote_hold_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA remote_hold_wire MODE STRICT ( event_id string );
      CREATE CODEC remote_hold_codec
        FROM WIRE JSON SCHEMA remote_hold_wire TO SCHEMA remote_hold_event;
      CREATE RELAY remote_hold_records SCHEMA remote_hold_event UNBRANCHED;
      CREATE KAFKA CLIENT remote_hold_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR remote_hold_source
        FROM remote_hold_kafka TOPIC 'remote_hold_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'remote_hold_group_{{test_id}}'
          INSTANCES 1
          MODE NO ACK
          ON QUIESCE SUSPEND DECODE USING remote_hold_codec
        TO remote_hold_records INHERIT ALL UNBRANCHED
          FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER remote_hold_sink
        FROM remote_hold_records
        TO remote_hold_kafka TOPIC 'remote_hold_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING remote_hold_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
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

  @long_running
  Scenario: A grant asked while the relay owner's memory is entirely full is refused for capacity and delivered once room returns
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "full_hold_first_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "full_hold_second_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "full_hold_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA full_hold_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA full_hold_wire MODE STRICT ( event_id string );
      CREATE CODEC full_hold_codec
        FROM WIRE JSON SCHEMA full_hold_wire TO SCHEMA full_hold_event;
      CREATE RELAY full_hold_first SCHEMA full_hold_event UNBRANCHED;
      CREATE RELAY full_hold_second SCHEMA full_hold_event UNBRANCHED;
      CREATE KAFKA CLIENT full_hold_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR full_hold_first_source
        FROM full_hold_kafka TOPIC 'full_hold_first_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'full_hold_first_group_{{test_id}}'
          INSTANCES 1
          MODE NO ACK
          ON QUIESCE SUSPEND DECODE USING full_hold_codec
        TO full_hold_first INHERIT ALL UNBRANCHED
          FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR full_hold_second_source
        FROM full_hold_kafka TOPIC 'full_hold_second_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'full_hold_second_group_{{test_id}}'
          INSTANCES 1
          MODE NO ACK
          ON QUIESCE SUSPEND DECODE USING full_hold_codec
        TO full_hold_second INHERIT ALL UNBRANCHED
          FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER full_hold_first_sink
        FROM full_hold_first
        TO full_hold_kafka TOPIC 'full_hold_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING full_hold_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER full_hold_second_sink
        FROM full_hold_second
        TO full_hold_kafka TOPIC 'full_hold_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING full_hold_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR full_hold_first_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE INGESTOR full_hold_second_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY full_hold_first ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE RELAY full_hold_second ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER full_hold_first_sink ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER full_hold_second_sink ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=full_hold_first_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=ingestor name=full_hold_second_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=full_hold_first owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=full_hold_second owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=full_hold_first_sink owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=full_hold_second_sink owner=node-2
      """
    When node "node-2" fills its relay memory before it charges its next acknowledgement watches
    And these Kafka messages are rapidly published to topic "full_hold_first_in_{{test_id}}"
      """
      {"event_id":"first-1"}
      {"event_id":"first-2"}
      """
    Then within "60s" node "node-2" has filled its relay memory
    When these Kafka messages are rapidly published to topic "full_hold_second_in_{{test_id}}"
      """
      {"event_id":"second-1"}
      {"event_id":"second-2"}
      """
    Then within "60s" node "node-2" observability metric "nervix_interconnect_relay_grant_refusals_total" with labels eventually reaches at least 1
      """
      direction="inbound"
      reason="memory"
      """
    When node "node-2" keeps its relay memory filled for "8s" before releasing it
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"event_id":"first-1"}
      {"event_id":"first-2"}
      {"event_id":"second-1"}
      {"event_id":"second-2"}
      """
    And within "30s" node "node-2" observability metric "nervix_interconnect_relay_attempts" with labels eventually equals 0
      """
      """
