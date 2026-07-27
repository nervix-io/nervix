Feature: Entity-pause model alterations
  @entity_pause_junction_swap_sibling_uninterrupted
  Scenario Outline: A junction swap preserves sibling delivery and concrete branch identity
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
      CREATE SCHEMA routed_event ( tenant STRING, seq I64, route STRING );
      CREATE STRICT WIRE JSON SCHEMA event_wire ( tenant string, seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_branch_schema ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch_schema TTL 5m;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY outgoing SCHEMA routed_event BRANCHED BY by_tenant;
      CREATE VHOST edge http-{{test_id}}-entity-swap.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION altered_path FROM incoming BRANCHED BY by_tenant
        TO outgoing
        SET tenant = input.tenant, seq = input.seq, route = 'altered'
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION sibling_path FROM incoming BRANCHED BY by_tenant
        TO outgoing
        SET tenant = input.tenant, seq = input.seq, route = 'sibling'
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-entity-swap.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-entity-swap.example.com" path "/events"
      """
      {"tenant":"globex","seq":2}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"acme" | "seq":1 | "route":"altered"
      "tenant":"acme" | "seq":1 | "route":"sibling"
      "tenant":"globex" | "seq":2 | "route":"altered"
      "tenant":"globex" | "seq":2 | "route":"sibling"
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION altered_path SET DETACHED;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-entity-swap.example.com" path "/events"
      """
      {"tenant":"acme","seq":3}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-entity-swap.example.com" path "/events"
      """
      {"tenant":"globex","seq":4}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"acme" | "seq":3 | "route":"altered"
      "tenant":"acme" | "seq":3 | "route":"sibling"
      "tenant":"globex" | "seq":4 | "route":"altered"
      "tenant":"globex" | "seq":4 | "route":"sibling"
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_pause_drain_timeout_rollback
  Scenario Outline: A timed-out entity drain leaves the old junction model active
    Given entity gate deadline is configured as "250ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE STRICT WIRE JSON SCHEMA event_wire ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY incoming SCHEMA event UNBRANCHED CAPACITY 1;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED CAPACITY 1;
      CREATE VHOST edge http-{{test_id}}-entity-timeout.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION altered_path FROM incoming UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER blocked_sink FROM outgoing
        ENCODE USING event_codec
        TO ZEROMQ sink INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And emitter "blocked_sink" enters stall mode
    When http payload is posted to node "node-1" with host "http-{{test_id}}-entity-timeout.example.com" path "/events"
      """
      {"seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-entity-timeout.example.com" path "/events"
      """
      {"seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-entity-timeout.example.com" path "/events"
      """
      {"seq":3}
      """
    When these NSPL commands fail with "timed out draining domain"
      """
      ALTER JUNCTION altered_path SET DETACHED;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE JUNCTION altered_path;
      """
    Then the last command output contains
      """
      CREATE ATTACHED JUNCTION altered_path
      """
    When emitter "blocked_sink" leaves stall mode
    Then within "5s" the observed broker receives payloads
      """
      "seq":1
      "seq":2
      "seq":3
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION altered_path SET DETACHED;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
