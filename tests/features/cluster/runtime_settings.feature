@runtime_settings
Feature: Declared data-plane runtime settings

  Runtime bounds have one typed owner. Cross-node acknowledgement policy is committed as one
  validated group, while local capacities are resolved before the node starts.

  # Two simultaneous capture/restore examples peaked at 26.7 GiB RSS; keep the shared-pool weight explicit.
  @runtime_materialized_capacity @long_running @slots_6 @deloxide_stress_restore
  Scenario Outline: Materialized metadata above eight MiB captures and restores on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And node-1 starts with runtime-ingest-group-max-rows = 64
    And node-2 starts with runtime-ingest-group-max-rows = 64
    And node-3 starts with runtime-ingest-group-max-rows = 64
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64, value I64);
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_key (id I64);
      CREATE BRANCH by_id SCHEMA event_key TTL 1h;
      CREATE RELAY events SCHEMA event BRANCHED BY by_id
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge runtime-materialized-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR populate FROM intake MODE ACK MAX 1 ACK TIMEOUT 2m
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_id SET id = message.id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When 98304 distinct materialized rows in batches of 64 are acknowledged through host "runtime-materialized-{{test_id}}.example.com" path "/events"
    And these Plexus commands are executed on the leader node
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /counts/rows = 98304
      """
    And the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 5m" from node "{{leader}}" into "runtime-materialized.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      STOP;
      DROP INGESTOR populate;
      DROP ENDPOINT intake;
      DROP VHOST edge;
      DROP RELAY events;
      """
    When the CLI restores "domain {{domain}} --as {{domain}}_copy --resume --request-timeout 5m" from "runtime-materialized.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 8 models
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      SHOW RELAY events MATERIALIZED STATE FOR ALL BRANCHES LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /counts/rows = 98304
      """
    Given client "reader" is connected to the leader node
    When client "reader" executes these Plexus commands
      """
      SHOW RELAY events MATERIALIZED STATE FOR BRANCH KEY { id = 98303 } LIMIT 1 FORMAT JSON;
      """
    Then the last typed inspection rows match
      """
      {"branch":{"id":98303},"fields":{"id":98303,"value":1671151}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @deloxide_stress
  Scenario Outline: A remote acknowledgement timing group commits together on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET REMOTE_ACK_ALIVE_INTERVAL = 20ms,
        REMOTE_ACK_DISPATCH_TIMEOUT = 100ms,
        REMOTE_ACK_SILENCE_TIMEOUT = 500ms,
        REMOTE_ACK_SWEEP_INTERVAL = 100ms,
        REMOTE_RELAY_TOTAL_TIMEOUT = 5s,
        REMOTE_RELAY_INSTANTIATION_WAIT = 1s;
      """
    Then node "node-1" eventually reports status containing "remote-ack-silence-timeout committed=500ms"
    And node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> remote-ack-silence-timeout running=500ms"
    When these Plexus statements fail with "remote-ack-silence-timeout"
      """
      ALTER CLUSTER SET REMOTE_ACK_SILENCE_TIMEOUT = 200ms;
      """
    Then node "node-1" eventually reports status containing "remote-ack-silence-timeout committed=500ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @deloxide_stress
  Scenario Outline: Local ingest grouping closes full three-row batches on <cluster_size> nodes
    Given Kafka is running
    And node-1 starts with runtime-ingest-group-max-rows = 3
    And node-1 starts with runtime-ingest-group-idle-flush = 30s
    And node-1 starts with runtime-unacknowledged-poll-batch = 3
    And node-2 starts with runtime-ingest-group-max-rows = 3
    And node-2 starts with runtime-ingest-group-idle-flush = 30s
    And node-2 starts with runtime-unacknowledged-poll-batch = 3
    And node-3 starts with runtime-ingest-group-max-rows = 3
    And node-3 starts with runtime-ingest-group-idle-flush = 30s
    And node-3 starts with runtime-unacknowledged-poll-batch = 3
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "runtime_groups_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id U32);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR grouped FROM broker TOPIC 'runtime_groups_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'runtime_groups_{{test_id}}' START AT EARLIEST
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER grouped_output FROM events
        TO broker TOPIC 'runtime_groups_output_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When Kafka message is published to topic "runtime_groups_{{test_id}}"
      """
      {"id":1}
      """
    And Kafka message is published to topic "runtime_groups_{{test_id}}"
      """
      {"id":2}
      """
    And Kafka message is published to topic "runtime_groups_{{test_id}}"
      """
      {"id":3}
      """
    And Kafka message is published to topic "runtime_groups_{{test_id}}"
      """
      {"id":4}
      """
    And Kafka message is published to topic "runtime_groups_{{test_id}}"
      """
      {"id":5}
      """
    And Kafka message is published to topic "runtime_groups_{{test_id}}"
      """
      {"id":6}
      """
    Then within "90s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "received" relay "events" across physical nodes totals 6
    And within "90s" DESCRIBE DOMAIN section "input_output" metric "batches_total" "received" relay "events" across physical nodes totals 2
    And within "90s" DESCRIBE EMITTER "grouped_output" on the leader node contains
      """
      payloads: 6 source_messages=6
      """
    And the last command output metric "messages_total" "received" relay "events" on any physical node has numeric values
      """
      total=6
      """
    And the last command output metric "batches_total" "received" relay "events" on any physical node has numeric values
      """
      total=2
      """
    And the last command output metric "messages_per_batch" "received" relay "events" on any physical node has numeric values
      """
      p50_1m=3
      p90_1m=3
      p99_1m=3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @deloxide_stress
  Scenario Outline: A domain drain cannot exceed a live node's shutdown budget on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements fail with "domain-drain-timeout"
      """
      ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 5m;
      """
    Then node "node-1" eventually reports status containing "domain-drain-timeout committed=30s"
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 20s, ENTITY_GATE_DEADLINE = 10s;
      """
    Then node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> domain-drain-timeout running=20s"
    When these Plexus statements fail with "entity-gate-deadline"
      """
      ALTER CLUSTER SET ENTITY_GATE_DEADLINE = 5m;
      """
    Then node "node-1" eventually reports status containing "entity-gate-deadline committed=10s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Forwarding between distinct owners is specific to three-node topology.
  @deloxide_stress
  Scenario: A committed silence policy expires a lost forwarded acknowledgement sooner
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET REMOTE_ACK_ALIVE_INTERVAL = 50ms,
        REMOTE_ACK_DISPATCH_TIMEOUT = 500ms,
        REMOTE_ACK_SILENCE_TIMEOUT = 2s,
        REMOTE_ACK_SWEEP_INTERVAL = 100ms,
        REMOTE_RELAY_TOTAL_TIMEOUT = 30s,
        REMOTE_RELAY_INSTANTIATION_WAIT = 5s;
      """
    Given Kafka topic "lost_ack_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "lost_ack_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA lost_ack_event ( event_id STRING, branch_name STRING, sequence I64 );
      CREATE WIRE JSON SCHEMA lost_ack_wire MODE STRICT (
        event_id string, branch_name string, sequence integer
      );
      CREATE WIRE JSON CODEC lost_ack_codec
        FROM WIRE SCHEMA lost_ack_wire TO SCHEMA lost_ack_event;
      CREATE SCHEMA lost_ack_key ( branch_name STRING );
      CREATE BRANCH lost_ack_branch SCHEMA lost_ack_key TTL 5m;
      CREATE RELAY lost_ack_records SCHEMA lost_ack_event BRANCHED BY lost_ack_branch;
      CREATE KAFKA CLIENT lost_ack_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR lost_ack_ingestor
        FROM lost_ack_kafka TOPIC 'lost_ack_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'lost_ack_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 2m RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING lost_ack_codec
        TO lost_ack_records INHERIT ALL
          BRANCHED BY lost_ack_branch SET branch_name = message.branch_name
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER lost_ack_emitter
        FROM lost_ack_records
        ATTACHED
        TO lost_ack_kafka TOPIC 'lost_ack_out_{{test_id}}'
          MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING lost_ack_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR lost_ack_ingestor ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY lost_ack_records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER lost_ack_emitter ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=lost_ack_ingestor unit=instance:0 owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=lost_ack_records owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=lost_ack_emitter owner=node-3
      """
    Given the next record acknowledgement node "node-3" returns to node "node-2" is lost
    When the forwarded acknowledgement measurement starts
    And Kafka message is published to topic "lost_ack_in_{{test_id}}"
      """
      {"event_id":"lost-ack","branch_name":"a","sequence":1}
      """
    Then within "30s" the observed broker receives payloads
      """
      "event_id":"lost-ack","branch_name":"a","sequence":1
      """
    And the record acknowledgement node "node-3" returned to node "node-2" was lost
    When Kafka message is published to topic "lost_ack_in_{{test_id}}"
      """
      {"event_id":"after-lost-ack","branch_name":"b","sequence":2}
      """
    Then within "90s" the observed broker receives payloads
      """
      "event_id":"after-lost-ack","branch_name":"b","sequence":2
      """
    And within "30s" Kafka consumer group "lost_ack_group_{{test_id}}" next offset for topic "lost_ack_in_{{test_id}}" partition 0 is "at least 2"
    And the lost acknowledgement recovers consumer group "lost_ack_group_{{test_id}}" topic "lost_ack_in_{{test_id}}" after at least "2s" and within "12s" of the measurement start
