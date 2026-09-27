Feature: Dynamically applying junction ALTER operations
  @dynamic_junction_filter_zero_interruption
  Scenario Outline: A dynamic junction filter change preserves the running path
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
      CREATE VHOST edge http-{{test_id}}-dynamic-filter.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events FROM incoming UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-filter.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the relay subscription receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events SET FILTER WHERE input.seq % 2 != 0;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-filter.example.com" path "/events"
      """
      {"seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-filter.example.com" path "/events"
      """
      {"seq":3}
      """
    Then the relay subscription receives a payload
      """
      "seq":3
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @dynamic_junction_construction_change
  Scenario Outline: A dynamic route construction change is used by the next message
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA input_event ( seq I64 );
      CREATE SCHEMA output_event ( seq I64, label STRING );
      CREATE WIRE JSON SCHEMA input_event_wire MODE STRICT ( seq integer );
      CREATE CODEC input_event_codec FROM WIRE JSON SCHEMA input_event_wire TO SCHEMA input_event;
      CREATE RELAY incoming SCHEMA input_event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA output_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-dynamic-construction.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING input_event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events FROM incoming UNBRANCHED
        TO outgoing SET seq = input.seq, label = 'old'
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-construction.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the relay subscription receives a payload
      """
      "label":"old","seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events
        REPLACE ROUTE TO outgoing SET seq = input.seq, label = 'new'
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-construction.example.com" path "/events"
      """
      {"seq":2}
      """
    Then the relay subscription receives a payload
      """
      "label":"new","seq":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @dynamic_flush_policy_change
  Scenario Outline: Changing a route flush policy preserves and releases buffered output
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
      CREATE VHOST edge http-{{test_id}}-dynamic-flush.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events FROM incoming UNBRANCHED
        TO outgoing INHERIT ALL FLUSH EACH 30s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-flush.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events
        REPLACE ROUTE TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And the relay subscription receives a payload
      """
      "seq":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @dynamic_branched_junction_revision
  Scenario Outline: A dynamic junction revision reaches existing and newly appearing branches
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA input_event ( tenant STRING, seq I64 );
      CREATE SCHEMA output_event ( tenant STRING, seq I64, label STRING );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_event_source SCHEMA tenant_branch TTL 5m;
      CREATE WIRE JSON SCHEMA input_event_wire MODE STRICT ( tenant string, seq integer );
      CREATE CODEC input_event_codec FROM WIRE JSON SCHEMA input_event_wire TO SCHEMA input_event;
      CREATE RELAY incoming SCHEMA input_event BRANCHED BY by_event_source;
      CREATE RELAY outgoing SCHEMA output_event BRANCHED BY by_event_source;
      CREATE VHOST edge http-{{test_id}}-dynamic-branch-revision.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING input_event_codec
        TO incoming INHERIT ALL BRANCHED BY by_event_source SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events FROM incoming BRANCHED BY by_event_source
        TO outgoing SET tenant = input.tenant, seq = input.seq, label = 'before'
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-branch-revision.example.com" path "/events"
      """
      {"tenant":"existing","seq":1}
      """
    Then the relay subscription receives a payload
      """
      "label":"before","seq":1,"tenant":"existing"
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events
        REPLACE ROUTE TO outgoing
        SET tenant = input.tenant, seq = input.seq, label = 'after'
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-branch-revision.example.com" path "/events"
      """
      {"tenant":"existing","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-dynamic-branch-revision.example.com" path "/events"
      """
      {"tenant":"new","seq":3}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"existing"} | "label":"after","seq":2,"tenant":"existing"
      key={"tenant":"new"} | "label":"after","seq":3,"tenant":"new"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
