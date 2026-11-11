@inherited_partitions @chaos
Feature: Inherited partition growth with an unavailable state owner

  Scenario Outline: Growing inherited Kafka partitions while a processor owner is down preserves keyed state
    Given Kafka is running
    And runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "growing_input_{{test_id}}" exists with 12 partitions
    And Kafka topic "growing_output_{{test_id}}" exists with 16 partitions
    And Kafka topic "growing_output_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS INHERITED;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR inherited_reader
        FROM broker TOPIC 'growing_input_{{test_id}}'
        <offset_mode> INSTANCES <reader_instances>
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER inherited_writer FROM unique_events INSTANCES PER PARTITION
        TO broker TOPIC 'growing_output_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec PARTITION FROM BRANCH INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When Kafka message is published to topic "growing_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 2
      """
      {"tenant":"gamma","seq":1}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"alpha" | "seq":1
      "tenant":"gamma" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the owner of every partition of deduplicator "distinct_events" is saved with placeholder prefix "state_owner"
    # Partition 7 holds alpha. Lose that exact owner after its replica catches up, including
    # leadership loss when that owner is the leader.
    When physical time passes for "3s"
    And node "{{state_owner_7}}" is stopped
    And Kafka topic "growing_input_{{test_id}}" partition count is changed to 16
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":1}
      """
    Then the leader of the running nodes is saved as placeholder "topology_reader"
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: 16
      partition epoch: 2
      previous partitions: 12
      """
    And within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When these Plexus commands are executed on node "{{topology_reader}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists <readers_after_growth> instances of ingestor "inherited_reader" on <reader_owners> distinct owners
    And the last cluster status lists 16 instances of emitter "inherited_writer" on <owners> distinct owners
    When Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":2}
      """
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 2
      """
      {"tenant":"gamma","seq":2}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"alpha" | "seq":2
      "tenant":"gamma" | "seq":2
      """
    And the observed broker does not receive a payload within "500ms"

    Examples:
      | cluster_size | replicas | owners | offset_mode                                                      | reader_instances | readers_after_growth | reader_owners |
      | 3            | 1        | 2      | OFFSET BY DOMAIN                                                 | PER PARTITION    | 16                   | 2             |
      | 3            | 1        | 2      | OFFSET BY CONSUMER GROUP 'growing_{{test_id}}' START AT EARLIEST | 1                | 1                    | 1             |
