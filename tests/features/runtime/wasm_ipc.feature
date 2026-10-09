Feature: Bounded generated WASM Arrow IPC

  Scenario Outline: A guest declaring a body outside its output bytes returns a typed runtime error
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has a WASM fixture declaring <declared> Arrow body bytes to relay "generated" in resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE malformed_pool;
      UPLOAD RESOURCE malformed_pool VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( value I32 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input_events SCHEMA event UNBRANCHED;
      CREATE RELAY generated SCHEMA event UNBRANCHED;
      CREATE VHOST edge wasm-ipc-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR generate FROM input_events
        USING RESOURCE malformed_pool VERSION 1 FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000 MAX MEMORY 64MiB UNBRANCHED
        TO generated SET value = value ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      START;
      """
    When http payload is posted to host "wasm-ipc-{{test_id}}.example.com" path "/events"
      """
      {"value":1}
      """
    Then within "30s" the active session observes a server error
    And the last server error contains
      """
      WASM output group has invalid generated Arrow IPC
      """
    When these NSPL commands are executed on the leader node
      """
      LIST CLUSTER STATUS;
      """
    Then the last command output contains "node-1"
    And every node still answers cluster status

    Examples:
      | cluster_size | declared            |
      | 1            | 1152921504606846976 |
      | 3            | 1152921504606846976 |
      | 1            | -1                  |
      | 3            | -1                  |
