Feature: Altering junctions
  @alter_junction_show_create_roundtrip
  Scenario Outline: ALTER JUNCTION applies ordered operations and SHOW CREATE renders the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY incoming_a SCHEMA event UNBRANCHED;
      CREATE RELAY incoming_b SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY audit SCHEMA event UNBRANCHED;
      CREATE JUNCTION route_events
        FROM incoming_a
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      ALTER JUNCTION route_events
        ADD FROM incoming_b,
        SET COLLECT FOR 10ms MAX BATCH SIZE 1MiB,
        SET FILTER WHERE input.seq > 0,
        SET DETACHED,
        ADD ROUTE TO audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE JUNCTION route_events;
      """
    Then the last command output contains
      """
      CREATE DETACHED JUNCTION route_events FROM incoming_a, incoming_b COLLECT FOR 10ms MAX BATCH SIZE 1MiB FILTER WHERE (input.seq > 0) UNBRANCHED
      """
    And the last command output contains
      """
      TO audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_junction_filter_behavior
  Scenario Outline: A junction filter ALTER is classified dynamic and changes behavior
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
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-junction-filter.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events
        FROM incoming
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter-junction-filter.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the relay subscription receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events SET FILTER WHERE input.seq >= 10;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-junction-filter.example.com" path "/events"
      """
      {"seq":2}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-junction-filter.example.com" path "/events"
      """
      {"seq":10}
      """
    Then the relay subscription receives a payload
      """
      "seq":10
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_junction_structural_entity_pause
  Scenario Outline: A structural junction ALTER uses entity pause and flow resumes
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
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-junction-structural.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events
        FROM incoming
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events SET DETACHED;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-junction-structural.example.com" path "/events"
      """
      {"seq":42}
      """
    Then the relay subscription receives a payload
      """
      "seq":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
