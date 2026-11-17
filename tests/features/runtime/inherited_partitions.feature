@inherited_partitions
Feature: Branches inherit their external producers' partitions
  An inherited branch keeps the external partition as its routing provenance. Readers that follow
  partitions are placed with the corresponding relay partition, including after topology changes.

  Scenario Outline: An inherited partition declaration keeps its canonical form across restart
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <partitioning>;
      """
    And these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <partitioning>;
      """
    When the cluster is restarted
    And these Plexus commands are executed on node "<reader>"
      """
      SHOW CREATE BRANCH by_tenant;
      """
    Then the last command output contains
      """
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <partitioning>;
      """

    Examples:
      | cluster_size | reader | partitioning |
      | 1            | node-1 | INHERITED    |
      | 3            | node-2 | INHERITED    |
      | 1            | node-1 | 4 INHERITED  |
      | 3            | node-2 | 4 INHERITED  |

  @inherited_partition_lifecycle @deloxide_stress
  Scenario Outline: A Kafka domain-offset reader runs one instance with each <partitioning> partition
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "inherited_input_{{test_id}}" exists with 12 partitions
    And Kafka topic "inherited_output_{{test_id}}" exists with 16 partitions
    And Kafka topic "inherited_output_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <partitioning>;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker
        CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR inherited_reader
        FROM broker TOPIC 'inherited_input_{{test_id}}'
        OFFSET BY DOMAIN INSTANCES PER PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER inherited_writer FROM events INSTANCES PER PARTITION
        TO broker TOPIC 'inherited_output_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec PARTITION FROM BRANCH INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists <instances> instances of ingestor "inherited_reader" on <owners> distinct owners
    And the last cluster status colocates every instance of ingestor "inherited_reader" with the corresponding partition of relay "events"
    And the last cluster status lists <instances> instances of emitter "inherited_writer" on <owners> distinct owners
    And the last cluster status colocates every instance of emitter "inherited_writer" with the corresponding partition of relay "events"
    Then the current leader node is saved as placeholder "topology_reader"
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      idle partitions: <idle>
      """
    When Kafka message is published to topic "inherited_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "inherited_input_{{test_id}}" partition 2
      """
      {"tenant":"beta","seq":2}
      """
    And Kafka message is published to topic "inherited_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":3}
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                    | key base64 | partition         |
      | {"tenant":"alpha","seq":1} | none       | <alpha_partition> |
      | {"tenant":"beta","seq":2}  | none       | 2                 |
      | {"tenant":"alpha","seq":3} | none       | <alpha_partition> |
    When Kafka message is published to topic "inherited_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":4}
      """
    And Kafka message is published to topic "inherited_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":5}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"alpha" | "seq":4
      "tenant":"alpha" | "seq":5
      """
    And the observed broker does not receive a payload within "500ms"
    And cluster observability metric "nervix_interconnect_relay_admissions_total" with labels eventually totals 0
      """
      outcome="admitted"
      """

    Examples:
      | cluster_size | partitioning | instances | owners | alpha_partition | idle           |
      | 1            | INHERITED    | 12        | 1      | 7               | none           |
      | 3            | INHERITED    | 12        | 3      | 7               | none           |
      | 1            | 4 INHERITED  | 4         | 1      | 3               | none           |
      | 3            | 4 INHERITED  | 4         | 3      | 3               | none           |
      | 1            | 16 INHERITED | 16        | 1      | 7               | 12, 13, 14, 15 |
      | 3            | 16 INHERITED | 16        | 3      | 7               | 12, 13, 14, 15 |

  Scenario Outline: An HTTP producer cannot construct an inherited branch
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS INHERITED;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge inherited-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      """
    When these Plexus commands fail
      """
      CREATE HTTP ENDPOINT INGESTOR refused
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      requires an enumerable external partition set
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @inherited_partition_lifecycle @deloxide_stress
  Scenario Outline: A Pulsar <subscription_type> reader follows the inherited partition topics
    Given Pulsar is running
    And Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And Pulsar topic "inherited_pulsar_{{test_id}}" exists with 12 partitions
    And Kafka topic "inherited_pulsar_output_{{test_id}}" exists with 12 partitions
    And Kafka topic "inherited_pulsar_output_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE SCHEMA positioned_event ( tenant STRING, seq I64, native_partition I32 OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON SCHEMA positioned_wire MODE STRICT ( tenant string, seq integer, native_partition integer OPTIONAL );
      CREATE WIRE JSON CODEC positioned_codec FROM WIRE SCHEMA positioned_wire TO SCHEMA positioned_event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS INHERITED;
      CREATE RELAY events SCHEMA positioned_event BRANCHED BY by_tenant;
      CREATE PULSAR CLIENT broker CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE KAFKA CLIENT downstream CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE PULSAR TOPIC INGESTOR inherited_reader
        FROM broker TOPIC 'inherited_pulsar_{{test_id}}'
        SUBSCRIPTION 'inherited_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
        TYPE <subscription_type> INSTANCES PER PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET native_partition = metadata.partition BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER inherited_writer FROM events INSTANCES PER PARTITION
        TO downstream TOPIC 'inherited_pulsar_output_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING positioned_codec PARTITION FROM BRANCH INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 12 instances of ingestor "inherited_reader" on <owners> distinct owners
    And the last cluster status colocates every instance of ingestor "inherited_reader" with the corresponding partition of relay "events"
    When Pulsar message is published to topic "inherited_pulsar_{{test_id}}-partition-7"
      """
      {"tenant":"alpha","seq":1}
      """
    And Pulsar message is published to topic "inherited_pulsar_{{test_id}}-partition-2"
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                                         | key base64 | partition |
      | {"tenant":"alpha","seq":1,"native_partition":7} | none       | 7         |
      | {"tenant":"beta","seq":2,"native_partition":2}  | none       | 2         |
    And the observed broker does not receive a payload within "500ms"
    And cluster observability metric "nervix_interconnect_relay_admissions_total" with labels eventually totals 0
      """
      outcome="admitted"
      """

    Examples:
      | cluster_size | subscription_type | owners |
      | 1            | EXCLUSIVE         | 1      |
      | 3            | EXCLUSIVE         | 3      |
      | 1            | FAILOVER          | 1      |
      | 3            | FAILOVER          | 3      |

  @inherited_partition_lifecycle @deloxide_stress
  Scenario Outline: A growing Kafka topology preserves keyed state when a producer moves a key to a new shard
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
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <partitioning>;
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
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When Kafka topic "growing_input_{{test_id}}" partition count is changed to 16
    Then the entity gate pause for domain "{{domain}}" is reached
    Then the current leader node is saved as placeholder "topology_reader"
    And within "5s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: <partitions_before>
      external partitions: 12
      partition epoch: 1
      """
    When the entity gate pause for domain "{{domain}}" is released
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":1}
      """
    Then the current leader node is saved as placeholder "topology_reader"
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: <partitions_after>
      external partitions: 16
      partition epoch: 2
      previous partitions: <partitions_before>
      """
    And within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists <readers_after_growth> instances of ingestor "inherited_reader" on <reader_owners> distinct owners
    And the last cluster status lists <partitions_after> instances of emitter "inherited_writer" on <owners> distinct owners
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
    And within "30s" DESCRIBE INGESTOR "inherited_reader" metric "messages_total" "sent" relay "events" totals 6

    Examples:
      | cluster_size | replicas | owners | partitioning | partitions_before | partitions_after | offset_mode                                                      | reader_instances | readers_after_growth | reader_owners |
      | 1            | 0        | 1      | INHERITED    | 12                | 16               | OFFSET BY DOMAIN                                                 | PER PARTITION    | 16                   | 1             |
      | 3            | 1        | 3      | INHERITED    | 12                | 16               | OFFSET BY DOMAIN                                                 | PER PARTITION    | 16                   | 3             |
      | 1            | 0        | 1      | INHERITED    | 12                | 16               | OFFSET BY CONSUMER GROUP 'growing_{{test_id}}' START AT EARLIEST | 1                | 1                    | 1             |
      | 3            | 1        | 3      | INHERITED    | 12                | 16               | OFFSET BY CONSUMER GROUP 'growing_{{test_id}}' START AT EARLIEST | 1                | 1                    | 1             |
      | 1            | 0        | 1      | 4 INHERITED  | 4                 | 4                | OFFSET BY DOMAIN                                                 | PER PARTITION    | 4                    | 1             |
      | 3            | 1        | 3      | 4 INHERITED  | 4                 | 4                | OFFSET BY DOMAIN                                                 | PER PARTITION    | 4                    | 3             |

  @inherited_partition_lifecycle @deloxide_stress
  Scenario Outline: Regrouping Kafka shards transfers committed next offsets before replacement readers consume
    Given Kafka is running
    And runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "regroup_input_{{test_id}}" exists with 12 partitions
    And Kafka topic "regroup_output_{{test_id}}" exists with 6 partitions
    And Kafka topic "regroup_output_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4 INHERITED;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR inherited_reader
        FROM broker TOPIC 'regroup_input_{{test_id}}'
        OFFSET BY DOMAIN INSTANCES PER PARTITION
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER inherited_writer FROM events INSTANCES PER PARTITION
        TO broker TOPIC 'regroup_output_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec PARTITION FROM BRANCH INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When Kafka message is published to topic "regroup_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":1}
      """
    And Kafka message is published to topic "regroup_input_{{test_id}}" partition 2
      """
      {"tenant":"beta","seq":1}
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                    | key base64 | partition |
      | {"tenant":"alpha","seq":1} | none       | 3         |
      | {"tenant":"beta","seq":1}  | none       | 2         |
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 6 INHERITED;
      """
    Then the current leader node is saved as placeholder "topology_reader"
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: 6
      partition epoch: 2
      """
    And within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 6 instances of ingestor "inherited_reader" on <owners> distinct owners
    And the last cluster status lists 6 instances of emitter "inherited_writer" on <owners> distinct owners
    When Kafka message is published to topic "regroup_input_{{test_id}}" partition 7
      """
      {"tenant":"alpha","seq":2}
      """
    And Kafka message is published to topic "regroup_input_{{test_id}}" partition 2
      """
      {"tenant":"beta","seq":2}
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                    | key base64 | partition |
      | {"tenant":"alpha","seq":2} | none       | 1         |
      | {"tenant":"beta","seq":2}  | none       | 2         |
    And the observed broker does not receive a payload within "500ms"
    And within "30s" DESCRIBE INGESTOR "inherited_reader" metric "messages_total" "sent" relay "events" totals 4

    Examples:
      | cluster_size | replicas | owners |
      | 1            | 0        | 1      |
      | 3            | 1        | 3      |
