Feature: Altering ingestors
  @alter_ingestor_show_create_roundtrip
  Scenario Outline: ALTER INGESTOR applies ordered operations and SHOW CREATE renders the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC event_codec_v2 FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY audit SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-ingestor-roundtrip.example.com;
      CREATE ENDPOINT ingress_a ON edge PATH '/a' TYPE HTTP;
      CREATE ENDPOINT ingress_b ON edge PATH '/b' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT ingress_a MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH EACH 30s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      ALTER INGESTOR event_source
        SET FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL,
        SET DECODE USING event_codec_v2,
        SET TIMESTAMP NOW,
        SET FILTER WHERE input.seq > 0,
        REPLACE ROUTE TO outgoing SET seq = input.seq + 1 UNBRANCHED
          FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE,
        ADD ROUTE TO audit INHERIT ALL UNBRANCHED
          FLUSH EACH 10ms MAX BATCH SIZE 512KiB ON MESSAGE ERROR LOG,
        SET GENERAL ERROR IGNORE;
      SHOW CREATE INGESTOR event_source;
      """
    Then the last command output contains
      """
      CREATE INGESTOR event_source FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL DECODE USING event_codec_v2 TIMESTAMP NOW FILTER WHERE (input.seq > 0)
      """
    And the last command output contains
      """
      TO outgoing SET seq = (input.seq + 1) UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR IGNORE
      """
    And the last command output contains
      """
      TO audit INHERIT ALL UNBRANCHED FLUSH EACH 10ms MAX BATCH SIZE 512KiB ON MESSAGE ERROR LOG ON GENERAL ERROR IGNORE;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_pause_ingestor_source_swap
  Scenario Outline: An ingestor source swap stops intake, replaces the source, and resumes flow
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-ingestor-swap.example.com;
      CREATE ENDPOINT ingress_a ON edge PATH '/a' TYPE HTTP;
      CREATE ENDPOINT ingress_b ON edge PATH '/b' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT ingress_a MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-ingestor-swap.example.com" path "/a"
      """
      {"seq":1}
      """
    Then the relay subscription receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER INGESTOR event_source
        SET FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-ingestor-swap.example.com" path "/b"
      """
      {"seq":2}
      """
    Then the relay subscription receives a payload
      """
      "seq":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @paced_ingestor_timestamp_alter_rejected
  Scenario Outline: A paced domain rejects dropping an ingestor timestamp source
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100000h;
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 1.0;

      CREATE SCHEMA paced_event ( seq I64 );
      CREATE WIRE JSON SCHEMA paced_event_wire MODE STRICT ( seq integer );
      CREATE CODEC paced_event_codec
        FROM WIRE JSON SCHEMA paced_event_wire
        TO SCHEMA paced_event;
      CREATE RELAY paced_events SCHEMA paced_event UNBRANCHED;

      CREATE CLIENT paced_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST paced_edge http-{{test_id}}-paced-alter.example.com;
      CREATE ENDPOINT paced_ingress ON paced_edge PATH '/paced-alter' TYPE HTTP;

      CREATE INGESTOR paced_source
        FROM ENDPOINT paced_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING paced_event_codec
        TIMESTAMP NOW
        TO paced_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE EMITTER paced_out
        FROM paced_events
        TO ZEROMQ paced_sink MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING paced_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-paced-alter.example.com" path "/paced-alter"
      """
      {"seq":1}
      """
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these NSPL commands fail with "requires ingestor 'paced_source' to declare TIMESTAMP NOW"
      """
      ALTER INGESTOR paced_source DROP TIMESTAMP;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE INGESTOR paced_source;
      """
    Then the last command output contains
      """
      TIMESTAMP NOW
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-paced-alter.example.com" path "/paced-alter"
      """
      {"seq":2}
      """
    Then the observed broker receives a payload
      """
      "seq":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
