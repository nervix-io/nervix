@shutdown_qualification
Feature: Graceful shutdown

  Scenario: Graceful shutdown preserves an operator cordon across restart
    Given graceful shutdown drain is enabled
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When leadership is transferred to node "node-2"
    And these NSPL commands are executed on the leader node
      """
      CORDON NODE node-2;
      """
    And node "node-2" is gracefully stopped
    And node "node-2" is started
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      raft.cordoned_nodes: node-2
      """

  Scenario: Placement excludes a terminating process and admits its next incarnation
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA shutdown_event ( id I64 );
      CREATE RELAY shutdown_input SCHEMA shutdown_event UNBRANCHED;
      CREATE RELAY shutdown_output SCHEMA shutdown_event UNBRANCHED;
      CREATE JUNCTION shutdown_route FROM shutdown_input UNBRANCHED
        TO shutdown_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "shutdown_route" is saved as placeholder "terminating_owner"
    And a node other than placeholder "terminating_owner" is saved as placeholder "drain_leader"
    When leadership is transferred to node "{{drain_leader}}"
    Given ownership handoff for domain "{{domain}}" pauses after preparation
    When node "{{terminating_owner}}" begins stopping
    Then node "{{drain_leader}}" eventually reports status containing "terminating: true"
    When these NSPL commands are attempted on node "{{drain_leader}}"
      """
      DESCRIBE RELOCATION JUNCTION shutdown_route
        ONTO NODE {{terminating_owner}} FOLLOW PREFERENCES;
      """
    And the ownership handoff preparation pause for domain "{{domain}}" is released
    Then the last command error contains
      """
      node '{{terminating_owner}}' is terminating
      """
    When node "{{terminating_owner}}" is stopped
    And node "{{terminating_owner}}" is started
    And these NSPL commands are executed on node "{{drain_leader}}"
      """
      DESCRIBE RELOCATION JUNCTION shutdown_route
        ONTO NODE {{terminating_owner}} FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocation onto node '{{terminating_owner}}'
      """

  Scenario: A delayed remote acknowledgement completes while its source owner drains
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA shutdown_ack_event ( id I64 );
      CREATE WIRE JSON SCHEMA shutdown_ack_wire MODE STRICT ( id integer );
      CREATE CODEC shutdown_ack_codec
        FROM WIRE JSON SCHEMA shutdown_ack_wire
        TO SCHEMA shutdown_ack_event;
      CREATE RELAY shutdown_ack_input SCHEMA shutdown_ack_event UNBRANCHED;
      CREATE RELAY shutdown_ack_output SCHEMA shutdown_ack_event UNBRANCHED;
      CREATE VHOST edge shutdown-ack-{{test_id}}.example.com;
      CREATE ENDPOINT shutdown_ack_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR shutdown_ack_source
        FROM ENDPOINT shutdown_ack_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING shutdown_ack_codec
        TO shutdown_ack_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION shutdown_ack_route FROM shutdown_ack_input UNBRANCHED
        TO shutdown_ack_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CLIENT shutdown_ack_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER shutdown_ack_output_sink FROM shutdown_ack_output
        TO ZEROMQ shutdown_ack_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING shutdown_ack_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PLACEMENT keep_shutdown_ack_downstream_local
        FROM shutdown_ack_route TO shutdown_ack_output_sink REQUIRE COLOCATION;
      START;
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      SHOW CLUSTER STATUS;
      """
    Then node "node-1" eventually reports status containing "kind=ingestor name=shutdown_ack_source owner=- replicas=node"
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the first replica for scheduled "ingestor" "shutdown_ack_source" in the last cluster status is saved as placeholder "shutdown_ack_source_owner"
    And a node other than placeholder "shutdown_ack_source_owner" is saved as placeholder "shutdown_ack_downstream_owner"
    When these NSPL commands are executed on the leader node
      """
      RELOCATE JUNCTION shutdown_ack_route
        ONTO NODE {{shutdown_ack_downstream_owner}} IGNORE PREFERENCES;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "shutdown_ack_route" owner equals placeholder "shutdown_ack_downstream_owner"
    Given remote relay admission for domain "{{domain}}" is paused
    When http payload begins posting in the background to node "{{shutdown_ack_source_owner}}" with host "shutdown-ack-{{test_id}}.example.com" path "/events"
      """
      {"id":7}
      """
    Then the remote relay admission pause for domain "{{domain}}" is reached
    When node "{{shutdown_ack_source_owner}}" begins stopping
    Then node "{{shutdown_ack_downstream_owner}}" eventually reports status containing "terminating: true"
    When the remote relay admission pause for domain "{{domain}}" is released
    Then the observed broker receives a payload
      """
      "id":7
      """
    When node "{{shutdown_ack_source_owner}}" is stopped while timing shutdown
    Then the last cluster operation completes within "5s"

  Scenario: A single node publishes work held by long cadences before it stops
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA held_event ( sequence I64, tenant STRING );
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT ( sequence integer, tenant string );
      CREATE CODEC held_event_codec
        FROM WIRE JSON SCHEMA held_event_wire
        TO SCHEMA held_event;
      CREATE RELAY held_ingested SCHEMA held_event UNBRANCHED;
      CREATE RELAY held_routed SCHEMA held_event UNBRANCHED;
      CREATE VHOST edge held-shutdown-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR held_source
        FROM ENDPOINT held_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_event_codec
        TIMESTAMP NOW
        TO held_ingested
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION held_route FROM held_ingested UNBRANCHED
        TO held_routed INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE CLIENT held_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER held_output FROM held_routed
        TO ZEROMQ held_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING held_event_codec
        INHERIT ALL
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    And http payload is posted to node "node-1" with host "held-shutdown-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then the observed broker does not receive a payload within "2s"
    When node "node-1" begins stopping
    Then within "20s" the observed broker receives payloads
      """
      {"sequence":1,"tenant":"alpha"}
      """
    When node "node-1" is stopped while timing shutdown
    Then the last cluster operation completes within "30s"
    When node "node-1" is started
    Then node "node-1" eventually observes a stable leader

  Scenario: The last schedulable node completes interleaved branch work and commits its source offsets
    Given Kafka is running
    And graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "shutdown_orders_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA shutdown_order ( order_id I64, tenant STRING );
      CREATE WIRE JSON SCHEMA shutdown_order_wire MODE STRICT ( order_id integer, tenant string );
      CREATE CODEC shutdown_order_codec
        FROM WIRE JSON SCHEMA shutdown_order_wire
        TO SCHEMA shutdown_order;
      CREATE SCHEMA shutdown_tenant ( tenant STRING );
      CREATE BRANCH by_shutdown_tenant SCHEMA shutdown_tenant TTL 5m;
      CREATE RELAY shutdown_orders SCHEMA shutdown_order BRANCHED BY by_shutdown_tenant;
      CREATE RELAY shutdown_unique_orders SCHEMA shutdown_order BRANCHED BY by_shutdown_tenant;
      CREATE CLIENT shutdown_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR shutdown_order_source
        FROM KAFKA shutdown_kafka TOPIC shutdown_orders_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_shutdown_orders_{{test_id}}
          MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING shutdown_order_codec
        TO shutdown_orders
          INHERIT ALL
          BRANCHED BY by_shutdown_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR shutdown_order_dedup FROM shutdown_orders
        DEDUPLICATE ON input.order_id
        MAX TIME 10m
        BRANCHED BY by_shutdown_tenant
        TO shutdown_unique_orders
          INHERIT ALL
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE CLIENT shutdown_order_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER shutdown_order_output FROM shutdown_unique_orders
        TO ZEROMQ shutdown_order_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING shutdown_order_codec
        INHERIT ALL
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION shutdown_orders_admitted TO shutdown_orders;
      """
    And these Kafka messages are rapidly published to topic "shutdown_orders_{{test_id}}"
      """
      {"order_id":11,"tenant":"acme"}
      {"order_id":11,"tenant":"beta"}
      {"order_id":11,"tenant":"acme"}
      {"order_id":22,"tenant":"beta"}
      {"order_id":12,"tenant":"acme"}
      """
    Then within "30s" the relay subscription receives payloads
      """
      payload={"order_id":11,"tenant":"acme"}
      payload={"order_id":11,"tenant":"beta"}
      payload={"order_id":11,"tenant":"acme"}
      payload={"order_id":22,"tenant":"beta"}
      payload={"order_id":12,"tenant":"acme"}
      """
    And the observed broker does not receive a payload within "2s"
    And within "2s" Kafka consumer group "nervix_cucumber_shutdown_orders_{{test_id}}" next offset for topic "shutdown_orders_{{test_id}}" partition 0 is "below 1"
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "30s"
    And within "10s" the observed broker receives payloads
      """
      {"order_id":11,"tenant":"acme"}
      {"order_id":11,"tenant":"beta"}
      {"order_id":22,"tenant":"beta"}
      {"order_id":12,"tenant":"acme"}
      """
    And the observed broker does not receive a payload within "2s"
    And within "10s" Kafka consumer group "nervix_cucumber_shutdown_orders_{{test_id}}" next offset for topic "shutdown_orders_{{test_id}}" partition 0 is "at least 5"

  Scenario: A node whose only uncordoned peer is terminating completes its admitted work in place
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA eligible_event ( sequence I64, tenant STRING );
      CREATE WIRE JSON SCHEMA eligible_event_wire MODE STRICT ( sequence integer, tenant string );
      CREATE CODEC eligible_event_codec
        FROM WIRE JSON SCHEMA eligible_event_wire
        TO SCHEMA eligible_event;
      CREATE RELAY eligible_ingested SCHEMA eligible_event UNBRANCHED;
      CREATE RELAY eligible_routed SCHEMA eligible_event UNBRANCHED;
      CREATE RELAY handoff_input SCHEMA eligible_event UNBRANCHED;
      CREATE RELAY handoff_output SCHEMA eligible_event UNBRANCHED;
      CREATE VHOST edge eligible-shutdown-{{test_id}}.example.com;
      CREATE ENDPOINT eligible_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR eligible_source
        FROM ENDPOINT eligible_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING eligible_event_codec
        TO eligible_ingested
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION eligible_route FROM eligible_ingested UNBRANCHED
        TO eligible_routed INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE JUNCTION handoff_route FROM handoff_input UNBRANCHED
        TO handoff_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CLIENT eligible_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER eligible_output FROM eligible_routed
        TO ZEROMQ eligible_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING eligible_event_codec
        INHERIT ALL
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      UNCORDON NODE node-2;
      """
    And these NSPL commands are executed on the leader node
      """
      RELOCATE JUNCTION handoff_route ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then node "node-1" eventually reports status containing "kind=junction name=handoff_route owner=node-2"
    When leadership is transferred to node "node-3"
    Given ownership handoff for domain "{{domain}}" pauses after preparation
    When node "node-2" begins stopping
    Then the ownership handoff preparation pause for domain "{{domain}}" is reached
    And node "node-1" eventually reports status containing "terminating: true"
    When http payload is posted to node "node-1" with host "eligible-shutdown-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1,"tenant":"alpha"}
      """
    Then the observed broker does not receive a payload within "2s"
    When node "node-1" begins stopping
    Then within "20s" the observed broker receives payloads
      """
      {"sequence":1,"tenant":"alpha"}
      """
    When the ownership handoff preparation pause for domain "{{domain}}" is released
    And node "node-1" is stopped
    And node "node-2" is stopped

  Scenario: Every node terminating at once completes its admitted work and commits its source offsets
    Given Kafka is running
    And graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "shutdown_everyone_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA everyone_event ( event_id I64 );
      CREATE WIRE JSON SCHEMA everyone_event_wire MODE STRICT ( event_id integer );
      CREATE CODEC everyone_event_codec
        FROM WIRE JSON SCHEMA everyone_event_wire
        TO SCHEMA everyone_event;
      CREATE RELAY everyone_ingested SCHEMA everyone_event UNBRANCHED;
      CREATE RELAY everyone_routed SCHEMA everyone_event UNBRANCHED;
      CREATE CLIENT everyone_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR everyone_source
        FROM KAFKA everyone_kafka TOPIC shutdown_everyone_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_shutdown_everyone_{{test_id}}
          MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING everyone_event_codec
        TO everyone_ingested
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION everyone_route FROM everyone_ingested UNBRANCHED
        TO everyone_routed INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE CLIENT everyone_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER everyone_output FROM everyone_routed
        TO ZEROMQ everyone_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING everyone_event_codec
        INHERIT ALL
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PLACEMENT everyone_together FROM everyone_source TO everyone_output REQUIRE COLOCATION;
      START;
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION everyone_admitted TO everyone_ingested;
      """
    And these Kafka messages are rapidly published to topic "shutdown_everyone_{{test_id}}"
      """
      {"event_id":1}
      {"event_id":2}
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"event_id":1}
      {"event_id":2}
      """
    And the observed broker does not receive a payload within "2s"
    When all nodes are gracefully stopped
    Then the last cluster operation completes within "120s"
    And within "10s" the observed broker receives payloads
      """
      {"event_id":1}
      {"event_id":2}
      """
    And within "10s" Kafka consumer group "nervix_cucumber_shutdown_everyone_{{test_id}}" next offset for topic "shutdown_everyone_{{test_id}}" partition 0 is "at least 2"

  Scenario: A generator releases its buffered output and stops producing when its node drains
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And a 1 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA tenant_amount ( amount I64, tenant STRING );
      CREATE SCHEMA tenant_total ( tenant STRING, total I64 );
      CREATE WIRE JSON SCHEMA tenant_amount_wire MODE STRICT ( amount integer, tenant string );
      CREATE WIRE JSON SCHEMA tenant_total_wire MODE STRICT ( tenant string, total integer );
      CREATE CODEC tenant_amount_codec
        FROM WIRE JSON SCHEMA tenant_amount_wire
        TO SCHEMA tenant_amount;
      CREATE CODEC tenant_total_codec
        FROM WIRE JSON SCHEMA tenant_total_wire
        TO SCHEMA tenant_total;
      CREATE SCHEMA generated_tenant ( tenant STRING );
      CREATE BRANCH by_generated_tenant SCHEMA generated_tenant TTL 5m;
      CREATE RELAY tenant_amounts SCHEMA tenant_amount BRANCHED BY by_generated_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generated_totals SCHEMA tenant_total BRANCHED BY by_generated_tenant;
      CREATE VHOST edge generated-shutdown-{{test_id}}.example.com;
      CREATE ENDPOINT tenant_amount_ingress ON edge PATH '/amounts' TYPE HTTP;
      CREATE INGESTOR tenant_amount_source
        FROM ENDPOINT tenant_amount_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING tenant_amount_codec
        TO tenant_amounts
          INHERIT ALL
          BRANCHED BY by_generated_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR tenant_totals
        USING MATERIALIZED STATE tenant_amounts
        EACH 100ms
        BRANCHED BY by_generated_tenant
        TO generated_totals
          SET tenant = relay_state.tenant_amounts.tenant,
              total = relay_state.tenant_amounts.amount
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE CLIENT generated_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER generated_output FROM generated_totals
        TO ZEROMQ generated_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING tenant_total_codec
        INHERIT ALL
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "generated-shutdown-{{test_id}}.example.com" path "/amounts"
      """
      {"amount":10,"tenant":"acme"}
      """
    And http payload is posted to node "node-1" with host "generated-shutdown-{{test_id}}.example.com" path "/amounts"
      """
      {"amount":20,"tenant":"beta"}
      """
    Then the observed broker does not receive a payload within "2s"
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "20s"
    And within "10s" the observed broker receives payloads
      """
      {"tenant":"acme","total":10}
      {"tenant":"beta","total":20}
      """

  Scenario: A pending REQUIRED WAIT message does not hold a graceful shutdown drain open
    Given Kafka is running
    And graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And a 1 node nervix cluster is started
    And Kafka topic "shutdown_waiting_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA wait_event ( id I64, source STRING );
      CREATE WIRE JSON SCHEMA wait_event_wire MODE STRICT ( id integer, source string );
      CREATE CODEC wait_event_codec
        FROM WIRE JSON SCHEMA wait_event_wire
        TO SCHEMA wait_event;
      CREATE RELAY wait_state SCHEMA wait_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY waiting_input SCHEMA wait_event UNBRANCHED;
      CREATE RELAY waiting_output SCHEMA wait_event UNBRANCHED;
      CREATE CLIENT wait_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR wait_event_source
        FROM KAFKA wait_kafka TOPIC shutdown_waiting_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_shutdown_waiting_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING wait_event_codec
        TO waiting_input
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE VHOST edge wait-state-{{test_id}}.example.com;
      CREATE ENDPOINT wait_state_ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR wait_state_source
        FROM ENDPOINT wait_state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING wait_event_codec
        TO wait_state
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION wait_for_state FROM waiting_input UNBRANCHED
        USING MATERIALIZED STATE wait_state REQUIRED WAIT
        TO waiting_output
          INHERIT ALL
          SET source = relay_state.wait_state.source
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE CLIENT wait_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER wait_output FROM waiting_output
        TO ZEROMQ wait_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING wait_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION waiting_input_seen TO waiting_input;
      START;
      """
    And Kafka message is published to topic "shutdown_waiting_{{test_id}}"
      """
      {"id":7,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"id":7,"source":"input"}
      """
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "15s"
    And within "2s" Kafka consumer group "nervix_cucumber_shutdown_waiting_{{test_id}}" next offset for topic "shutdown_waiting_{{test_id}}" partition 0 is "below 1"
    When node "node-1" is started
    Then node "node-1" eventually accepts http traffic for host "wait-state-{{test_id}}.example.com" path "/state"
      """
      {"id":0,"source":"state"}
      """
    And within "60s" the observed broker receives payloads
      """
      {"id":7,"source":"state"}
      """
    And within "30s" Kafka consumer group "nervix_cucumber_shutdown_waiting_{{test_id}}" next offset for topic "shutdown_waiting_{{test_id}}" partition 0 is "at least 1"

  Scenario: A drain that reaches its timeout leaves Kafka offsets uncommitted for redelivery after restart
    Given Kafka is running
    And graceful shutdown drain is enabled
    And drain timeout is configured as "3s"
    And a 1 node nervix cluster is started
    And Kafka topic "shutdown_stalled_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA stalled_event ( event_id I64 );
      CREATE WIRE JSON SCHEMA stalled_event_wire MODE STRICT ( event_id integer );
      CREATE CODEC stalled_event_codec
        FROM WIRE JSON SCHEMA stalled_event_wire
        TO SCHEMA stalled_event;
      CREATE RELAY stalled_events SCHEMA stalled_event UNBRANCHED;
      CREATE CLIENT stalled_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR stalled_source
        FROM KAFKA stalled_kafka TOPIC shutdown_stalled_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_shutdown_stalled_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING stalled_event_codec
        TO stalled_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT stalled_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER stalled_output FROM stalled_events
        TO ZEROMQ stalled_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING stalled_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And emitter "stalled_output" enters stall mode
    And Kafka message is published to topic "shutdown_stalled_{{test_id}}"
      """
      {"event_id":7}
      """
    Then within "30s" DESCRIBE EMITTER "stalled_output" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "20s"
    And within "2s" Kafka consumer group "nervix_cucumber_shutdown_stalled_{{test_id}}" next offset for topic "shutdown_stalled_{{test_id}}" partition 0 is "below 1"
    And the observed broker does not receive a payload within "1s"
    When emitter "stalled_output" leaves stall mode
    And node "node-1" is started
    Then within "60s" the observed broker receives payloads
      """
      {"event_id":7}
      """
    And within "30s" Kafka consumer group "nervix_cucumber_shutdown_stalled_{{test_id}}" next offset for topic "shutdown_stalled_{{test_id}}" partition 0 is "at least 1"

  Scenario: Graceful shutdown drain timeout bounds full cluster termination
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "1ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
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
      """
    When all nodes are gracefully stopped
    Then the last cluster operation completes within "5s"

  Scenario: A transaction commit stuck at its shutdown deadline is cancelled and completed after restart
    Given graceful shutdown drain is enabled
    And shutdown timeout is configured as "5s"
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given client "owner" is connected to node "node-1"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA deadline_record (value STRING);
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given transaction commit on node "node-1" pauses after 1 statement
    When client "owner" begins executing these NSPL commands in the background
      """
      COMMIT;
      """
    Then the transaction commit pause on node "node-1" after 1 statement is reached
    When node "node-1" is stopped while timing shutdown
    Then the last cluster operation takes at least "5s"
    And the last cluster operation completes within "60s"
    And node "node-1" reports that its last shutdown passed its deadline
    When the transaction commit pause on node "node-1" after 1 statement is released
    And node "node-1" is started
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"

  Scenario: A command held inside its session does not keep a stopping node past its shutdown deadline
    Given graceful shutdown drain is enabled
    And shutdown timeout is configured as "5s"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given command admission on node "node-1" pauses before proposal
    When this NSPL command request begins executing in the background on the leader node
      """
      CREATE SCHEMA held_admission_event ( id I64 );
      """
    Then the command admission pause on node "node-1" is reached
    When node "node-1" is stopped while timing shutdown
    Then the last cluster operation takes at least "5s"
    And the last cluster operation completes within "60s"
    And node "node-1" reports that its last shutdown passed its deadline
    When the command admission pause on node "node-1" is released
    And node "node-1" is started
    Then node "node-1" eventually observes a stable leader
