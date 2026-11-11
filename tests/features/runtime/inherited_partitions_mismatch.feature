@inherited_partitions @inherited_partition_lifecycle @deloxide_stress
Feature: Co-producing inherited readers agree on their external partition count

  Scenario Outline: Unequal Kafka co-producers suspend until their native partition counts agree
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "inherited_first_{{test_id}}" exists with 12 partitions
    And Kafka topic "inherited_second_{{test_id}}" exists with 16 partitions
    And Kafka topic "inherited_agreed_{{test_id}}" exists with 16 partitions
    And Kafka topic "inherited_agreed_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      BEGIN;
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS INHERITED;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR first_reader
        FROM broker TOPIC 'inherited_first_{{test_id}}'
        OFFSET BY DOMAIN INSTANCES PER PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR second_reader
        FROM broker TOPIC 'inherited_second_{{test_id}}'
        OFFSET BY DOMAIN INSTANCES PER PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER agreed_writer FROM events INSTANCES PER PARTITION
        TO broker TOPIC 'inherited_agreed_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec PARTITION FROM BRANCH INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      COMMIT;
      START;
      """
    Then within "60s" the active session observes a server error containing
      """
      observes different external partition counts
      """
    Then the current leader node is saved as placeholder "topology_reader"
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: awaiting topology
      """
    When Kafka message is published to topic "inherited_first_{{test_id}}" partition 7
      """
      {"tenant":"first","seq":1}
      """
    And Kafka message is published to topic "inherited_second_{{test_id}}" partition 13
      """
      {"tenant":"second","seq":1}
      """
    Then the observed broker does not receive a payload within "250ms"
    When Kafka topic "inherited_first_{{test_id}}" partition count is changed to 16
    Then within "60s" every instance of ingestor "first_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of ingestor "second_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "agreed_writer" reports running on its scheduled owner
    Then the current leader node is saved as placeholder "topology_reader"
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      external partitions: 16
      """
    And within "30s" the observed broker receives exactly these partitioned messages
      | payload                     | key base64 | partition |
      | {"tenant":"first","seq":1}  | none       | 7         |
      | {"tenant":"second","seq":1} | none       | 13        |
    And the observed broker does not receive a payload within "250ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
