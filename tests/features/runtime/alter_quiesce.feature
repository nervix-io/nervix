Feature: Model alteration quiesce classification
  Background:
    Given schema change drain timeout is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA alter_event ( seq I64 );
      CREATE WIRE JSON SCHEMA alter_event_wire MODE STRICT ( seq integer );
      CREATE CODEC alter_event_codec
        FROM WIRE JSON SCHEMA alter_event_wire
        TO SCHEMA alter_event;
      CREATE RELAY alter_events SCHEMA alter_event UNBRANCHED CAPACITY 3;

      CREATE CLIENT alter_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
      CREATE VHOST alter_edge http-{{test_id}}-quiesce.example.com;
      CREATE ENDPOINT alter_ingress ON alter_edge PATH '/alter-quiesce' TYPE HTTP;
      CREATE INGESTOR alter_source
        FROM ENDPOINT alter_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING alter_event_codec
        TO alter_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER alter_out
        FROM alter_events
        TO ZEROMQ alter_sink MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING alter_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And emitter "alter_out" enters stall mode
    And http payload is posted to node "node-1" with host "http-{{test_id}}-quiesce.example.com" path "/alter-quiesce"
      """
      {"seq":1}
      """

  @noop_alter_is_dynamic
  Scenario: A no-op ALTER is dynamic even while the domain cannot drain
    When these NSPL commands are executed on the leader node
      """
      ALTER RELAY alter_events SET CAPACITY 3;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And emitter "alter_out" leaves stall mode
    Then the observed broker receives a payload
      """
      "seq":1
      """

  @concurrent_alter_rejected
  Scenario: A second model alteration is rejected while the domain ALTER lock is held
    When these NSPL commands begin executing in the background
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA alter_event_wire
        ADD FIELD note string OPTIONAL;
      ALTER SCHEMA alter_event
        ADD FIELD note STRING OPTIONAL;
      COMMIT;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Paused"
    When these NSPL commands fail with "already has a model alteration in progress"
      """
      ALTER RELAY alter_events SET CAPACITY 4;
      """
    And emitter "alter_out" leaves stall mode
    Then the background NSPL execution succeeds
