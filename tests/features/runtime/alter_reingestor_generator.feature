Feature: Altering reingestors and generators
  @entity_pause_reingestor_rewire
  Scenario Outline: ALTER REINGESTOR rewires its task under entity pause
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( key I64, alternate I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( key integer, alternate integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-reingestor.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REINGESTOR repartition
        FROM incoming
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to host "http-{{test_id}}-alter-reingestor.example.com" path "/events"
      """
      {"key":1,"alternate":2}
      """
    Then the relay subscription receives a payload
      """
      {"alternate":2,"key":1}
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER REINGESTOR repartition
        SET DETACHED,
        SET COLLECT FOR 10ms MAX BATCH SIZE 1MiB,
        SET FILTER WHERE input.key > 0,
        REPLACE ROUTE TO outgoing
          SET key = message.alternate, alternate = 99
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE REINGESTOR repartition;
      """
    Then the last command output contains
      """
      CREATE DETACHED REINGESTOR repartition FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB FILTER WHERE (input.key > 0)
      """
    When http payload is posted to host "http-{{test_id}}-alter-reingestor.example.com" path "/events"
      """
      {"key":3,"alternate":4}
      """
    Then the relay subscription receives a payload
      """
      {"alternate":99,"key":4}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_pause_generator_respawn
  Scenario Outline: ALTER GENERATOR respawns its timed task under entity pause
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( key I64, alternate I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( key integer, alternate integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY state_events SCHEMA event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generated SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-generator.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TIMESTAMP NOW
        TO state_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE GENERATOR synth
        USING MATERIALIZED STATE state_events
        EACH 100ms
        UNBRANCHED
        TO generated
          SET key = relay_state.state_events.key,
              alternate = relay_state.state_events.alternate
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION generated_subscription TO generated;
      START;
      """
    When http payload is posted to host "http-{{test_id}}-alter-generator.example.com" path "/events"
      """
      {"key":5,"alternate":6}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"alternate":6,"key":5}
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER GENERATOR synth SET EACH 1h;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER GENERATOR synth
        SET EACH 200ms,
        REPLACE ROUTE TO generated
          SET key = relay_state.state_events.alternate,
              alternate = relay_state.state_events.key
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE GENERATOR synth;
      """
    Then the last command output contains
      """
      CREATE GENERATOR synth USING MATERIALIZED STATE state_events EACH 200ms UNBRANCHED
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"alternate":5,"key":6}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
