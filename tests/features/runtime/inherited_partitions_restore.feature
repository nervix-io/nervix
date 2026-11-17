@inherited_partitions @inherited_partition_lifecycle @restore_installation @deloxide_stress_restore
Feature: Inherited topology and provenance survive a native backup and restore

  Scenario Outline: An inherited topology archive restores epochs, exact key origins and next offsets
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
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR inherited_reader
        FROM broker TOPIC 'growing_input_{{test_id}}'
        OFFSET BY DOMAIN INSTANCES PER PARTITION
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
    When Kafka topic "growing_input_{{test_id}}" partition count is changed to 16
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":1}
      """
    Then the current leader node is saved as placeholder "topology_reader"
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: 16
      partition epoch: 2
      previous partitions: 12
      """
    And within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 16 instances of ingestor "inherited_reader" on <owners> distinct owners
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

    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees deduplicator "distinct_events" hold 4 rows
    And client "reader" reads deduplicator "distinct_events" state one row per page with unchanged complete counts
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the last typed inspection has 2 returned rows and 2 complete rows
    And the last typed inspection rows match
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","seq":2}}
      {"branch":{"tenant":"gamma"},"fields":{"tenant":"gamma","seq":2}}
      """
    And the complete inspection row order is saved
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection has 1 returned rows and 2 complete rows
    And the inspection continuation is saved with its complete counts and rows
    When inspection client "reader" resumes relay "events" with the saved continuation
    Then the last typed inspection has 1 returned rows and 2 complete rows
    And the completed inspection has exactly 2 distinct rows with unchanged complete counts
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR BRANCH KEY { tenant = 'alpha' } LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection has 1 returned rows and 1 complete rows
    And the last typed inspection rows match
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","seq":2}}
      """
    And within "30s" DESCRIBE INGESTOR "inherited_reader" metric "messages_total" "sent" relay "events" totals 6
    And the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "inherited-topology.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    And Kafka topic "growing_output_{{test_id}}" is observed
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                    | key base64 | partition |
      | {"tenant":"alpha","seq":1} | none       | 7         |
      | {"tenant":"gamma","seq":1} | none       | 2         |
      | {"tenant":"alpha","seq":2} | none       | 13        |
      | {"tenant":"gamma","seq":2} | none       | 2         |
    And the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "inherited-topology.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 11 models
    And the CLI restore reports no warnings
    And within "60s" every instance of ingestor "inherited_reader" reports running and ready on its scheduled owner
    And within "60s" every instance of emitter "inherited_writer" reports running on its scheduled owner
    Then the current leader node is saved as placeholder "topology_reader"
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "{{topology_reader}}" contains
      """
      partitions: 16
      partition epoch: 2
      """
    Given client "restored_reader" is connected to node "restored-1"
    Then within "30s" client "restored_reader" sees deduplicator "distinct_events" hold 4 rows
    And client "restored_reader" reads deduplicator "distinct_events" state one row per page with unchanged complete counts
    When client "restored_reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 2 FORMAT JSON;
      """
    Then the last typed inspection has 2 returned rows and 2 complete rows
    And the last typed inspection rows match
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","seq":2}}
      {"branch":{"tenant":"gamma"},"fields":{"tenant":"gamma","seq":2}}
      """
    And the complete inspection row order is saved
    When client "restored_reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection has 1 returned rows and 2 complete rows
    And the inspection continuation is saved with its complete counts and rows
    When inspection client "restored_reader" resumes relay "events" with the saved continuation
    Then the last typed inspection has 1 returned rows and 2 complete rows
    And the completed inspection has exactly 2 distinct rows with unchanged complete counts
    When client "restored_reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR BRANCH KEY { tenant = 'alpha' } LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection has 1 returned rows and 1 complete rows
    And the last typed inspection rows match
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","seq":2}}
      """
    When Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":2}
      """
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 2
      """
      {"tenant":"gamma","seq":2}
      """
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 13
      """
      {"tenant":"alpha","seq":3}
      """
    And Kafka message is published to topic "growing_input_{{test_id}}" partition 2
      """
      {"tenant":"gamma","seq":3}
      """
    Then within "30s" the observed broker receives exactly these partitioned messages
      | payload                    | key base64 | partition |
      | {"tenant":"alpha","seq":3} | none       | 13        |
      | {"tenant":"gamma","seq":3} | none       | 2         |
    And the observed broker does not receive a payload within "1s"
    And within "30s" DESCRIBE INGESTOR "inherited_reader" metric "messages_total" "sent" relay "events" totals 4
    And within "30s" client "restored_reader" sees deduplicator "distinct_events" hold 6 rows

    Examples:
      | cluster_size | replicas | owners |
      | 1            | 0        | 1      |
      | 3            | 1        | 3      |
