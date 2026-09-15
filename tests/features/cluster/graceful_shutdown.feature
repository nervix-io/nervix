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

  Scenario: Graceful shutdown skips drain when no replacement node exists
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "5s"
    And a 1 node nervix cluster is started
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "2s"
    When node "node-1" is started
    Then node "node-1" eventually observes a stable leader

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
