@remote_relay_sender_hold
Feature: Remote relay senders held back while relay memory is full

  Scenario: A delivery whose grant finds the relay owner's memory full is held past the dispatch bound and delivered once room returns
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "grant_hold_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "grant_hold_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA grant_hold_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA grant_hold_wire MODE STRICT ( event_id string );
      CREATE CODEC grant_hold_codec
        FROM WIRE JSON SCHEMA grant_hold_wire TO SCHEMA grant_hold_event;
      CREATE RELAY grant_hold_records SCHEMA grant_hold_event UNBRANCHED;
      CREATE KAFKA CLIENT grant_hold_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR grant_hold_source
        FROM grant_hold_kafka TOPIC 'grant_hold_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'grant_hold_group_{{test_id}}'
          INSTANCES 1
          MODE NO ACK
          ON QUIESCE SUSPEND DECODE USING grant_hold_codec
        TO grant_hold_records INHERIT ALL UNBRANCHED
          FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER grant_hold_sink
        FROM grant_hold_records
        TO grant_hold_kafka TOPIC 'grant_hold_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING grant_hold_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus statements are executed on the leader node
      """
      RELOCATE INGESTOR grant_hold_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY grant_hold_records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER grant_hold_sink ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=grant_hold_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=grant_hold_records owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=grant_hold_sink owner=node-2
      """
    When node "node-2" fills its relay memory, leaving room for no relay grant
    And these Kafka messages are rapidly published to topic "grant_hold_in_{{test_id}}"
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
    Then within "60s" node "node-2" observability metric "nervix_execution_memory_rejections_total" with labels eventually reaches at least 1
      """
      class="relay"
      """
    When node "node-2" keeps its relay memory filled for "8s" before releasing it
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
    And node "node-2" observability metric "nervix_interconnect_relay_grant_refusals_total" with labels eventually reaches at least 1
      """
      direction="inbound"
      reason="memory"
      """
    And node "node-1" observability metric "nervix_interconnect_relay_grant_refusals_total" with labels eventually reaches at least 1
      """
      direction="outbound"
      reason="memory"
      """
    And within "30s" node "node-2" observability metric "nervix_interconnect_relay_attempts" with labels eventually equals 0
      """
      """

  Scenario: A delivery whose sender has no relay memory for its record storage is held and delivered once room returns
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "storage_hold_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "storage_hold_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA storage_hold_event ( event_id STRING );
      CREATE WIRE JSON SCHEMA storage_hold_wire MODE STRICT ( event_id string );
      CREATE CODEC storage_hold_codec
        FROM WIRE JSON SCHEMA storage_hold_wire TO SCHEMA storage_hold_event;
      CREATE RELAY storage_hold_records SCHEMA storage_hold_event UNBRANCHED;
      CREATE KAFKA CLIENT storage_hold_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR storage_hold_source
        FROM storage_hold_kafka TOPIC 'storage_hold_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'storage_hold_group_{{test_id}}'
          INSTANCES 1
          MODE NO ACK
          ON QUIESCE SUSPEND DECODE USING storage_hold_codec
        TO storage_hold_records INHERIT ALL UNBRANCHED
          FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER storage_hold_sink
        FROM storage_hold_records
        TO storage_hold_kafka TOPIC 'storage_hold_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING storage_hold_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus statements are executed on the leader node
      """
      RELOCATE INGESTOR storage_hold_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY storage_hold_records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER storage_hold_sink ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=storage_hold_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=storage_hold_records owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=storage_hold_sink owner=node-2
      """
    When node "node-1" fills its relay memory before it stores the records of its next delivery
    And these Kafka messages are rapidly published to topic "storage_hold_in_{{test_id}}"
      """
      {"event_id":"stored-1"}
      {"event_id":"stored-2"}
      {"event_id":"stored-3"}
      {"event_id":"stored-4"}
      {"event_id":"stored-5"}
      {"event_id":"stored-6"}
      {"event_id":"stored-7"}
      {"event_id":"stored-8"}
      """
    Then within "60s" node "node-1" has filled its relay memory
    When node "node-1" keeps its relay memory filled for "2s" before releasing it
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"event_id":"stored-1"}
      {"event_id":"stored-2"}
      {"event_id":"stored-3"}
      {"event_id":"stored-4"}
      {"event_id":"stored-5"}
      {"event_id":"stored-6"}
      {"event_id":"stored-7"}
      {"event_id":"stored-8"}
      """
    And node "node-1" observability metric "nervix_execution_memory_rejections_total" with labels eventually equals 0
      """
      class="relay"
      """
