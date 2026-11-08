Feature: Retained emitter lifecycle
  A transient failure retains the emitter and its prepared work through recovery.

  Scenario Outline: An emitter recovers a transient failure without reconstructing its connector
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And constructions of emitter "retained" are observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}', 'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER retained FROM events ATTACHED
        TO destination MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And producer "events" submits batch "first" with rows
      | seq |
      | 1   |
    Then batch "first" completes
    And emitter "retained" has been constructed exactly 1 times
    When emitter client for emitter "retained" enters unavailable fault mode
    And producer "events" submits batch "recovering" with rows
      | seq |
      | 2   |
    Then within "30s" DESCRIBE EMITTER "retained" on the leader node contains
      """
      transient error: emitter fault injector returned an unavailable client
      """
    When emitter client for emitter "retained" leaves fault mode
    Then batch "recovering" completes
    And within "30s" the observed broker receives exactly these payloads
      """
      {"seq":1}
      {"seq":2}
      """
    And emitter "retained" has been constructed exactly 1 times
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then emitter "retained" has been constructed exactly 1 times

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
