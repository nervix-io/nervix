Feature: Altering relay materialized state
  @relay_materialized_state_add_no_gap
  Scenario Outline: Adding materialized state has no post-commit state gap
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE STRICT WIRE JSON SCHEMA event_wire ( tenant string, seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_branch_schema ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch_schema TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge http-{{test_id}}-materialized-add.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO events INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER RELAY events SET MATERIALIZED STATE LAST BY TIMESTAMP;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-materialized-add.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-materialized-add.example.com" path "/events"
      """
      {"tenant":"globex","seq":2}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"acme" | "seq":1
      "tenant":"globex" | "seq":2
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "events" containing
      """
      key={"tenant":"acme"} payload={"seq":1,"tenant":"acme"}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "events" containing
      """
      key={"tenant":"globex"} payload={"seq":2,"tenant":"globex"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @relay_materialized_state_drop
  Scenario Outline: Dropping materialized state purges state without interrupting relay flow
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
      CREATE RELAY events SCHEMA event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY projected SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-materialized-drop.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION materialized_consumer FROM events UNBRANCHED
        USING MATERIALIZED STATE events REQUIRED SKIP
        TO projected INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    When these NSPL commands fail with "must declare materialized state"
      """
      ALTER RELAY events DROP MATERIALIZED STATE;
      """
    And these NSPL commands are executed on the leader node
      """
      DROP JUNCTION materialized_consumer;
      """
    And these NSPL commands are executed on the leader node
      """
      ALTER RELAY events DROP MATERIALIZED STATE;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION events_after_drop TO events;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-materialized-drop.example.com" path "/events"
      """
      {"seq":9}
      """
    Then the relay subscription receives a payload
      """
      "seq":9
      """
    When these NSPL commands fail with "is not materialized"
      """
      SHOW RELAY events MATERIALIZED STATE;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
