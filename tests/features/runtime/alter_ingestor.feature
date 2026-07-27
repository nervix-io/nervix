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
      CREATE STRICT WIRE JSON SCHEMA event_wire ( seq integer );
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
      CREATE STRICT WIRE JSON SCHEMA event_wire ( seq integer );
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
