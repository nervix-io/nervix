Feature: Config entity quiesce classification
  Background:
    Given schema change drain timeout is configured as "20s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA config_event ( seq I64 );
      CREATE WIRE JSON SCHEMA config_event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON SCHEMA config_event_wire_v2 MODE STRICT ( seq integer );
      CREATE CODEC config_event_codec
        FROM WIRE JSON SCHEMA config_event_wire
        TO SCHEMA config_event;
      CREATE RELAY config_events SCHEMA config_event UNBRANCHED;

      CREATE CLIENT config_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST config_edge http-{{test_id}}-config.example.com;
      CREATE ENDPOINT config_ingress ON config_edge PATH '/config-quiesce' TYPE HTTP;

      CREATE INGESTOR config_source
        FROM ENDPOINT config_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING config_event_codec
        TO config_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE EMITTER config_out
        FROM config_events
        TO ZEROMQ config_sink ENCODE USING config_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-config.example.com" path "/config-quiesce"
      """
      {"seq":1}
      """
    Then the observed broker receives a payload
      """
      "seq":1
      """

  @codec_recreate_pauses_the_domain
  Scenario: Recreating a codec with a changed definition quiesces the domain
    When emitter "config_out" enters stall mode
    And http payload is posted to node "node-1" with host "http-{{test_id}}-config.example.com" path "/config-quiesce"
      """
      {"seq":2}
      """
    And these NSPL commands begin executing in the background
      """
      BEGIN;
      DROP CODEC config_event_codec;
      CREATE CODEC config_event_codec
        FROM WIRE JSON SCHEMA config_event_wire_v2
        TO SCHEMA config_event;
      COMMIT;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Paused"
    When emitter "config_out" leaves stall mode
    Then the observed broker receives a payload
      """
      "seq":2
      """
    And the background NSPL execution succeeds
    When http payload is posted to node "node-1" with host "http-{{test_id}}-config.example.com" path "/config-quiesce"
      """
      {"seq":3}
      """
    Then the observed broker receives a payload
      """
      "seq":3
      """

  @client_recreate_pauses_the_domain
  Scenario: Recreating a client with a changed configuration quiesces the domain
    When emitter "config_out" enters stall mode
    And http payload is posted to node "node-1" with host "http-{{test_id}}-config.example.com" path "/config-quiesce"
      """
      {"seq":2}
      """
    And these NSPL commands begin executing in the background
      """
      BEGIN;
      DROP EMITTER config_out;
      DROP CLIENT config_sink;
      CREATE CLIENT config_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false',
          'send_timeout' = '5s'
        };
      CREATE EMITTER config_out
        FROM config_events
        TO ZEROMQ config_sink ENCODE USING config_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      COMMIT;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Paused"
    When emitter "config_out" leaves stall mode
    Then the observed broker receives a payload
      """
      "seq":2
      """
    And the background NSPL execution succeeds
    When http payload is posted to node "node-1" with host "http-{{test_id}}-config.example.com" path "/config-quiesce"
      """
      {"seq":3}
      """
    Then the observed broker receives a payload
      """
      "seq":3
      """
