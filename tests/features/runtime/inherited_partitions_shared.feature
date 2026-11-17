@inherited_partitions @inherited_partition_lifecycle @deloxide_stress
Feature: Fixed reader counts retain native partition routing

  Scenario Outline: A fixed <broker> reader count inherits native partitions and hops to their owners
    Given Kafka is running
    And <broker> is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And <broker> topic "fixed_input_{{test_id}}" exists with 12 partitions
    And Kafka topic "fixed_output_{{test_id}}" exists with 12 partitions
    And Kafka topic "fixed_output_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS INHERITED;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE <connector> CLIENT upstream CONFIG { '<address_key>' = '<address>' };
      CREATE KAFKA CLIENT downstream CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE <connector> TOPIC INGESTOR inherited_reader
        FROM upstream TOPIC 'fixed_input_{{test_id}}'
        <delivery> INSTANCES 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER inherited_writer FROM events INSTANCES PER PARTITION
        TO downstream TOPIC 'fixed_output_{{test_id}}'
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
    Then the last cluster status lists 1 instances of ingestor "inherited_reader" on 1 distinct owners
    And the last cluster status lists 12 instances of emitter "inherited_writer" on <owners> distinct owners
    When each of the 12 partitions of <broker> topic "fixed_input_{{test_id}}" receives one distinct event
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                         | key base64 | partition |
      | {"tenant":"tenant-0","seq":0}   | none       | 0         |
      | {"tenant":"tenant-1","seq":1}   | none       | 1         |
      | {"tenant":"tenant-2","seq":2}   | none       | 2         |
      | {"tenant":"tenant-3","seq":3}   | none       | 3         |
      | {"tenant":"tenant-4","seq":4}   | none       | 4         |
      | {"tenant":"tenant-5","seq":5}   | none       | 5         |
      | {"tenant":"tenant-6","seq":6}   | none       | 6         |
      | {"tenant":"tenant-7","seq":7}   | none       | 7         |
      | {"tenant":"tenant-8","seq":8}   | none       | 8         |
      | {"tenant":"tenant-9","seq":9}   | none       | 9         |
      | {"tenant":"tenant-10","seq":10} | none       | 10        |
      | {"tenant":"tenant-11","seq":11} | none       | 11        |
    And the observed broker does not receive a payload within "500ms"
    And the cluster reports at least <hops> admitted relay hops

    Examples:
      | cluster_size | broker | connector | address_key       | address         | delivery                                                                         | owners | hops |
      | 1            | Kafka  | KAFKA     | bootstrap.servers | {{kafka_addr}}  | OFFSET BY CONSUMER GROUP 'fixed_{{test_id}}' START AT EARLIEST                   | 1      | 0    |
      | 3            | Kafka  | KAFKA     | bootstrap.servers | {{kafka_addr}}  | OFFSET BY CONSUMER GROUP 'fixed_{{test_id}}' START AT EARLIEST                   | 3      | 1    |
      | 1            | Pulsar | PULSAR    | addr              | {{pulsar_addr}} | SUBSCRIPTION 'fixed_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE SHARED | 1      | 0    |
      | 3            | Pulsar | PULSAR    | addr              | {{pulsar_addr}} | SUBSCRIPTION 'fixed_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE SHARED | 3      | 1    |
