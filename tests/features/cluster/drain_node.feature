Feature: Drain node

  Scenario: Draining a live owner uses the gated handoff
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA handoff_event ( id I64 );
      CREATE RELAY handoff_input SCHEMA handoff_event UNBRANCHED;
      CREATE RELAY handoff_output SCHEMA handoff_event UNBRANCHED;
      CREATE JUNCTION handoff_junction FROM handoff_input UNBRANCHED
        TO handoff_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "handoff_junction" is saved as placeholder "drained_owner"
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these NSPL commands begin executing in the background
      """
      DRAIN NODE {{drained_owner}};
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When the entity gate pause for domain "{{domain}}" is released
    Then the background NSPL execution succeeds
    And the last command output contains
      """
      drained node '{{drained_owner}}' (moved 2 of 2 scheduled graph node(s))
      quiesce level: ENTITY_PAUSE
      - kind=junction name=handoff_junction from={{drained_owner}} to=
      """

  @planned-handoff-timeout
  Scenario: A stalled schedule unit fails independently and can be retried
    Given entity gate deadline is configured as "250ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA drain_event ( seq I64 );
      CREATE WIRE JSON SCHEMA drain_event_wire MODE STRICT ( seq integer );
      CREATE CODEC drain_event_codec
        FROM WIRE JSON SCHEMA drain_event_wire
        TO SCHEMA drain_event;
      CREATE RELAY stalled_output SCHEMA drain_event UNBRANCHED CAPACITY 1;
      CREATE RELAY control_input SCHEMA drain_event UNBRANCHED;
      CREATE RELAY control_output SCHEMA drain_event UNBRANCHED;
      CREATE VHOST edge drain-{{test_id}}.example.com;
      CREATE ENDPOINT drain_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR drain_source
        FROM ENDPOINT drain_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING drain_event_codec
        TO stalled_output INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT drain_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER stalled_emitter FROM stalled_output
        TO ZEROMQ drain_sink MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING drain_event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION movable_junction FROM control_input UNBRANCHED
        TO control_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When emitter "stalled_emitter" enters stall mode
    And http payload is posted to node "node-1" with host "drain-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "5s" DESCRIBE EMITTER "stalled_emitter" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When these NSPL commands fail with "timed out draining domain"
      """
      DRAIN NODE node-1;
      """
    Then the last command error contains
      """
      drained node 'node-1' (moved 4 of 5 scheduled graph node(s))
      quiesce level: ENTITY_PAUSE
      - kind=emitter name=stalled_emitter owner=node-1 failed:
      """
    And the last command error contains
      """
      - kind=junction name=movable_junction from=node-1 to=
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "emitter" "stalled_emitter" is saved as placeholder "stalled_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "movable_junction" owner different from placeholder "stalled_owner"
    When emitter "stalled_emitter" leaves fault mode
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      DRAIN NODE node-1;
      """
    Then the last command output contains
      """
      drained node 'node-1' (moved 1 of 1 scheduled graph node(s))
      quiesce level: ENTITY_PAUSE
      - kind=emitter name=stalled_emitter from=node-1 to=
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then within "5s" node "node-1" eventually reports scheduled "emitter" "stalled_emitter" owner different from placeholder "stalled_owner"

  @planned-replica-handoff
  Scenario: Draining a primary promotes a live replica
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE UNPACED DOMAIN {{domain}};

      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I64
      );

      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        transaction_id string,
        amount integer
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );

      CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA transaction_id_branch TTL 5m;

      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_source_txns;

      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_source_txns;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;

      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_source_txns
        TO deduped
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;

      DESCRIBE DEDUPLICATOR dedup_txns;
      """
    Then the last command output owner is saved as placeholder "drained_primary_node"
    And the first replica in the last command output is saved as placeholder "expected_promoted_replica"
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE {{drained_primary_node}};
      DESCRIBE DEDUPLICATOR dedup_txns;
      """
    Then the last command output owner equals placeholder "expected_promoted_replica"
    And the last command output contains
      """
      replicas: {{drained_primary_node}}
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed on node "{{expected_promoted_replica}}"
      """
      CREATE SUBSCRIPTION after_handoff TO deduped;
      """
    When node "{{drained_primary_node}}" is stopped
    Then node "{{expected_promoted_replica}}" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"transaction_id":"after-handoff","amount":17}
      """
    And within "10s" the relay subscription receives a payload
      """
      "transaction_id":"after-handoff"
      """

  @planned-required-wait-handoff
  Scenario: A pending REQUIRED WAIT batch does not block its owner's drain
    Given entity gate deadline is configured as "5s"
    And Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wait_event ( id I64, source STRING );
      CREATE WIRE JSON SCHEMA wait_event_wire MODE STRICT (
        id integer,
        source string
      );
      CREATE CODEC wait_event_codec
        FROM WIRE JSON SCHEMA wait_event_wire
        TO SCHEMA wait_event;
      CREATE RELAY event_state SCHEMA wait_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY waiting_input SCHEMA wait_event UNBRANCHED;
      CREATE RELAY waiting_output SCHEMA wait_event UNBRANCHED;
      CREATE CLIENT wait_kafka
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR event_source
        FROM KAFKA wait_kafka TOPIC wait_events_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_wait_events_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 500ms
        ON QUIESCE SUSPEND DECODE USING wait_event_codec
        TO waiting_input
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR state_source
        FROM KAFKA wait_kafka TOPIC wait_state_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_wait_state_{{test_id}}
          MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING wait_event_codec
        TO event_state
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION wait_for_state FROM waiting_input UNBRANCHED
        USING MATERIALIZED STATE event_state REQUIRED WAIT
        TO waiting_output
        INHERIT ALL
        SET source = relay_state.event_state.source
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_waiting_ack_local
        FROM event_source
        TO wait_for_state
        REQUIRE COLOCATION;
      CREATE PLACEMENT keep_waiting_state_local
        FROM state_source
        TO wait_for_state
        REQUIRE COLOCATION;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=event_source owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=wait_for_state owner=node-1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION waiting_input_seen TO waiting_input;
      """
    And Kafka message is published to topic "wait_events_{{test_id}}"
      """
      {"id":7,"source":"input"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"id":7,"source":"input"}
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION waiting_input_seen;
      CREATE SUBSCRIPTION waiting_output_seen TO waiting_output;
      """
    Then the relay subscription does not receive a payload within "1s"
    When these NSPL commands begin executing in the background
      """
      DRAIN NODE node-1;
      """
    Then the background NSPL execution succeeds
    And the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And the last command output contains
      """
      - kind=ingestor name=event_source from=node-1 to=
      """
    And the last command output contains
      """
      - kind=junction name=wait_for_state from=node-1 to=
      """
    When Kafka message is published to topic "wait_state_{{test_id}}"
      """
      {"id":7,"source":"state"}
      """
    Then within "15s" the relay subscription receives a payload
      """
      {"id":7,"source":"state"}
      """
    And the relay subscription does not receive a payload within "2s"

  Scenario: Draining a node cordons it and moves scheduled graph nodes away
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE UNPACED DOMAIN {{domain}};

      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE INGESTOR kafka_a
        FROM KAFKA kafka_main TOPIC notifications_a_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_a_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR kafka_b
        FROM KAFKA kafka_main TOPIC notifications_b_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_b_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR kafka_c
        FROM KAFKA kafka_main TOPIC notifications_c_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_c_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      DRAIN NODE node-2;
      """
    Then the last command output contains
      """
      drained node 'node-2' (moved
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      raft.cordoned_nodes: node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR kafka_a;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR kafka_b;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR kafka_c;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """
