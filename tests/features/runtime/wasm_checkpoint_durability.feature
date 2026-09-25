Feature: WASM guest-state checkpoint durability
  A WASM processor acknowledges its input only after the guest-state checkpoint that covers the
  input has reached its completion boundary: this node's stable storage, and every replica the
  schedule assigns to the processor when it has any. The scenarios ingest from Kafka in
  `ACK SEQUENTIAL` mode, so the consumer group's committed offset is the source acknowledgement.

  The fixture guest numbers the rows of each branch and keeps even-numbered rows. Its row count is
  the durable computation state it saves, so which rows a branch emits proves which state it
  continued from. A negatively acknowledged input is delivered again, possibly more than once, so
  every emitted row the scenarios assert belongs to an input the branch receives only after all of
  its earlier inputs were acknowledged.

  Scenario Outline: A WASM processor withholds source acknowledgement until its checkpoint reaches stable storage
    Given Kafka is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_checkpoint_local_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_checkpointed_filter;
      UPLOAD RESOURCE wasm_checkpointed_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_checkpoint_local_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_checkpoint_local_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_checkpointed_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    Then Kafka consumer group "wasm_checkpoint_local_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_checkpoint_local_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_checkpoint_local_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_checkpoint_local_group_{{test_id}}" next offset for topic "wasm_checkpoint_local_in_{{test_id}}" partition 0 is "at least 2"
    When WASM guest-state checkpoints fail to reach stable storage on every node
    And Kafka message is published to topic "wasm_checkpoint_local_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then within "30s" the active session observes a server error containing
      """
      wasm processor 'filter_even_rows' local state persistence failed (branch {"tenant":"alpha"}
      """
    And within "4s" Kafka consumer group "wasm_checkpoint_local_group_{{test_id}}" next offset for topic "wasm_checkpoint_local_in_{{test_id}}" partition 0 is "below 3"
    When WASM guest-state checkpoints reach stable storage again on every node
    And Kafka message is published to topic "wasm_checkpoint_local_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_checkpoint_local_group_{{test_id}}" next offset for topic "wasm_checkpoint_local_in_{{test_id}}" partition 0 is "at least 4"
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      key={"tenant":"beta"} | "tenant":"beta" | "value":12
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      failed checkpoints: 0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario: A WASM processor withholds source acknowledgement until its replica durably holds the checkpoint
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_checkpoint_replica_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_replicated_filter;
      UPLOAD RESOURCE wasm_replicated_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_checkpoint_replica_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_checkpoint_replica_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_replicated_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      kind=wasm_processor name=filter_even_rows owner=
      """
    And the first replica for scheduled "wasm_processor" "filter_even_rows" in the last cluster status is saved as placeholder "wasm_replica"
    And Kafka consumer group "wasm_checkpoint_replica_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_checkpoint_replica_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_checkpoint_replica_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_checkpoint_replica_group_{{test_id}}" next offset for topic "wasm_checkpoint_replica_in_{{test_id}}" partition 0 is "at least 2"
    When runtime state replica installations fail on every node
    And Kafka message is published to topic "wasm_checkpoint_replica_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then within "45s" the active session observes a server error containing
      """
      wasm processor 'filter_even_rows' state replication failed (branch {"tenant":"alpha"}
      """
    And the last server error contains
      """
      {{wasm_replica}}
      """
    And within "4s" Kafka consumer group "wasm_checkpoint_replica_group_{{test_id}}" next offset for topic "wasm_checkpoint_replica_in_{{test_id}}" partition 0 is "below 3"
    When runtime state replica installations succeed again on every node
    And Kafka message is published to topic "wasm_checkpoint_replica_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    Then within "90s" Kafka consumer group "wasm_checkpoint_replica_group_{{test_id}}" next offset for topic "wasm_checkpoint_replica_in_{{test_id}}" partition 0 is "at least 4"
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      key={"tenant":"beta"} | "tenant":"beta" | "value":12
      """

  @exclusive
  Scenario: Guest state acknowledged through a replica survives the loss of its owner
    Given Kafka is running
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "wasm_checkpoint_failover_in_{{test_id}}" exists with 1 partitions
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-1;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_failover_filter;
      UPLOAD RESOURCE wasm_failover_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest',
        'session.timeout.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE INGESTOR metric_source
        FROM KAFKA kafka_ingress TOPIC wasm_checkpoint_failover_in_{{test_id}}
          OFFSET BY CONSUMER GROUP wasm_checkpoint_failover_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_failover_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      START;
      """
    # Keep node-1 cordoned so the surviving replica owns recovery through the output assertion.
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "wasm_processor" "filter_even_rows" is saved as placeholder "failed_owner"
    And the first replica for scheduled "wasm_processor" "filter_even_rows" in the last cluster status is saved as placeholder "promoted_replica"
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      """
    Then Kafka consumer group "wasm_checkpoint_failover_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":11,"tenant":"beta"}
      """
    And Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":12,"tenant":"beta"}
      """
    And Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":3,"tenant":"alpha"}
      """
    And Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":13,"tenant":"beta"}
      """
    Then within "60s" Kafka consumer group "wasm_checkpoint_failover_group_{{test_id}}" next offset for topic "wasm_checkpoint_failover_in_{{test_id}}" partition 0 is "at least 6"
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      key={"tenant":"beta"} | "tenant":"beta" | "value":12
      """
    When runtime state replica installations fail on every node
    And Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":4,"tenant":"alpha"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":4
      """
    And within "4s" Kafka consumer group "wasm_checkpoint_failover_group_{{test_id}}" next offset for topic "wasm_checkpoint_failover_in_{{test_id}}" partition 0 is "below 7"
    When node "{{failed_owner}}" is stopped
    And runtime state replica installations succeed again on every node
    Then node "{{promoted_replica}}" eventually observes a stable leader
    And within "60s" node "{{promoted_replica}}" eventually reports scheduled "wasm_processor" "filter_even_rows" owner equals placeholder "promoted_replica"
    When Kafka message is published to topic "wasm_checkpoint_failover_in_{{test_id}}"
      """
      {"value":14,"tenant":"beta"}
      """
    Then within "90s" Kafka consumer group "wasm_checkpoint_failover_group_{{test_id}}" next offset for topic "wasm_checkpoint_failover_in_{{test_id}}" partition 0 is "at least 8"
    And within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "value":14
      """
