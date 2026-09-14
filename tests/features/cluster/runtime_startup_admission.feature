Feature: Runtime startup admission

  Scenario: A restarting node waits for linearizable consensus catch-up before executing persisted ownership
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "runtime_admission_out_{{test_id}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-1;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA admission_event (
        sequence U64
      );

      CREATE WIRE JSON SCHEMA admission_event_wire MODE STRICT (
        sequence integer
      );

      CREATE CODEC admission_event_codec
        FROM WIRE JSON SCHEMA admission_event_wire
        TO SCHEMA admission_event;

      CREATE RELAY admission_input SCHEMA admission_event UNBRANCHED;
      CREATE RELAY admission_output SCHEMA admission_event UNBRANCHED;

      CREATE VHOST admission_edge http-admission-{{test_id}}.example.com;
      CREATE ENDPOINT admission_ingress
        ON admission_edge
        PATH '/events'
        TYPE HTTP;

      CREATE INGESTOR admission_source
        FROM ENDPOINT admission_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING admission_event_codec
        TO admission_input
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE JUNCTION admission_forwarder FROM admission_input
        UNBRANCHED
        TO admission_output
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE EMITTER admission_emitter
        FROM admission_output
        TO KAFKA kafka_main TOPIC runtime_admission_out_{{test_id}}
        MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING admission_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=admission_forwarder owner=node-2
      """
    And the last cluster status owner for scheduled "junction" "admission_forwarder" is saved as placeholder "persisted_owner"
    And node "node-2" eventually accepts http traffic for host "http-admission-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    And the observed broker receives a payload
      """
      {"sequence":1}
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      """
    And node "node-2" is stopped
    Then within "20s" node "node-1" eventually reports scheduled "junction" "admission_forwarder" owner different from placeholder "persisted_owner"
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=admission_forwarder owner=node-1
      """
    When node "node-2" is started while consensus connectivity is blocked
    Then node "node-2" eventually reports interconnect to "node-1" as "connected"
    When these NSPL commands are executed on node "node-2"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      raft.id: node-2
      """
    When http payload is posted to node "node-2" with host "http-admission-{{test_id}}.example.com" path "/events" and is not routed
      """
      {"sequence":2}
      """
    Then the observed broker does not receive a payload within "2s"
    When consensus connectivity for node "node-2" is restored
    Then within "20s" node "node-2" eventually reports scheduled "junction" "admission_forwarder" owner different from placeholder "persisted_owner"
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=admission_forwarder owner=node-1
      """
    And node "node-2" eventually forwards http traffic for host "http-admission-{{test_id}}.example.com" path "/events" to the observed broker
      """
      {"sequence":3}
      """
