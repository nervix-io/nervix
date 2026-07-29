Feature: Altering deduplicators and reorderers
  @alter_deduplicator_show_create_roundtrip
  Scenario Outline: ALTER DEDUPLICATOR applies ordered operations and SHOW CREATE renders the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( key I64, alternate I64 );
      CREATE RELAY incoming_a SCHEMA event UNBRANCHED;
      CREATE RELAY incoming_b SCHEMA event UNBRANCHED;
      CREATE RELAY state_events SCHEMA event UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY audit SCHEMA event UNBRANCHED;
      CREATE DEDUPLICATOR dedup_events
        FROM incoming_a
        DEDUPLICATE ON input.key
        MAX TIME 10m
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      ALTER DEDUPLICATOR dedup_events
        ADD FROM incoming_b WHERE input.key > 0,
        SET COLLECT FOR 10ms MAX BATCH SIZE 1MiB,
        SET FILTER WHERE input.alternate > 0,
        SET DEDUPLICATE ON input.alternate, input.key,
        SET MAX TIME 20m,
        SET DETACHED,
        ADD MATERIALIZED STATE state_events REQUIRED SKIP,
        ADD ROUTE TO audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE DEDUPLICATOR dedup_events;
      """
    Then the last command output contains
      """
      CREATE DETACHED DEDUPLICATOR dedup_events FROM incoming_a, incoming_b WHERE (input.key > 0) COLLECT FOR 10ms MAX BATCH SIZE 1MiB FILTER WHERE (input.alternate > 0) DEDUPLICATE ON input.alternate, input.key MAX TIME 20m UNBRANCHED
      """
    And the last command output contains
      """
      USING MATERIALIZED STATE state_events REQUIRED SKIP TO outgoing
      """
    And the last command output contains
      """
      TO audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_reorderer_show_create_roundtrip
  Scenario Outline: ALTER REORDERER applies ordered operations and SHOW CREATE renders the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( primary_key I64, secondary_key I64 );
      CREATE RELAY incoming_a SCHEMA event UNBRANCHED;
      CREATE RELAY incoming_b SCHEMA event UNBRANCHED;
      CREATE RELAY state_events SCHEMA event UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY audit SCHEMA event UNBRANCHED;
      CREATE REORDERER order_events
        FROM incoming_a
        BY input.primary_key
        MAX TIME 10m
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      ALTER REORDERER order_events
        ADD FROM incoming_b WHERE input.primary_key > 0,
        SET COLLECT FOR 10ms MAX BATCH SIZE 1MiB,
        SET FILTER WHERE input.secondary_key > 0,
        SET BY input.secondary_key, input.primary_key,
        SET MAX TIME 20m,
        SET DETACHED,
        ADD MATERIALIZED STATE state_events REQUIRED SKIP,
        ADD ROUTE TO audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE REORDERER order_events;
      """
    Then the last command output contains
      """
      CREATE DETACHED REORDERER order_events FROM incoming_a, incoming_b WHERE (input.primary_key > 0) COLLECT FOR 10ms MAX BATCH SIZE 1MiB FILTER WHERE (input.secondary_key > 0) BY input.secondary_key, input.primary_key MAX TIME 20m UNBRANCHED
      """
    And the last command output contains
      """
      USING MATERIALIZED STATE state_events REQUIRED SKIP TO outgoing
      """
    And the last command output contains
      """
      TO audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @dynamic_deduplicator_max_time
  Scenario Outline: Deduplicator MAX TIME changes dynamically without losing its state
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
      CREATE VHOST edge http-{{test_id}}-alter-dedup-dynamic.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_events
        FROM incoming
        DEDUPLICATE ON input.key
        MAX TIME 1h
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to host "http-{{test_id}}-alter-dedup-dynamic.example.com" path "/events"
      """
      {"key":1,"alternate":1}
      """
    Then the relay subscription receives a payload
      """
      "key":1
      """
    When http payload is posted to host "http-{{test_id}}-alter-dedup-dynamic.example.com" path "/events"
      """
      {"key":1,"alternate":2}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When these NSPL commands are executed on the leader node
      """
      ALTER DEDUPLICATOR dedup_events SET MAX TIME 1ms;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    Then within "5s" repeatedly posting http payload to host "http-{{test_id}}-alter-dedup-dynamic.example.com" path "/events" yields a relay subscription payload
      """
      {"key":1,"alternate":3}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_pause_deduplicator_keyspace
  Scenario Outline: Deduplicator key changes use entity pause and purge the old keyspace
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
      CREATE VHOST edge http-{{test_id}}-alter-dedup-key.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_events
        FROM incoming
        DEDUPLICATE ON input.key
        MAX TIME 1h
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to host "http-{{test_id}}-alter-dedup-key.example.com" path "/events"
      """
      {"key":1,"alternate":1}
      """
    Then the relay subscription receives a payload
      """
      "key":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER DEDUPLICATOR dedup_events SET DEDUPLICATE ON input.alternate;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to host "http-{{test_id}}-alter-dedup-key.example.com" path "/events"
      """
      {"key":2,"alternate":1}
      """
    Then the relay subscription receives a payload
      """
      "key":2
      """
    When http payload is posted to host "http-{{test_id}}-alter-dedup-key.example.com" path "/events"
      """
      {"key":3,"alternate":1}
      """
    Then the relay subscription does not receive a payload within "300ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @dynamic_reorderer_max_time
  Scenario Outline: Reorderer MAX TIME changes dynamically and releases buffered output
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( primary_key I64, secondary_key I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( primary_key integer, secondary_key integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-reorder-dynamic.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER order_events
        FROM incoming
        BY input.primary_key
        MAX TIME 1h
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When http payload is posted to host "http-{{test_id}}-alter-reorder-dynamic.example.com" path "/events"
      """
      {"primary_key":1,"secondary_key":1}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When these NSPL commands are executed on the leader node
      """
      ALTER REORDERER order_events SET MAX TIME 1ms;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    Then the relay subscription receives a payload
      """
      "primary_key":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_pause_reorderer_ordering
  Scenario Outline: Reorderer ordering changes use entity pause and the new order takes effect
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( primary_key I64, secondary_key I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( primary_key integer, secondary_key integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-reorder-key.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER order_events
        FROM incoming
        BY input.primary_key
        MAX TIME 1s
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER REORDERER order_events SET BY input.secondary_key;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to host "http-{{test_id}}-alter-reorder-key.example.com" path "/events"
      """
      {"primary_key":1,"secondary_key":2}
      """
    And http payload is posted to host "http-{{test_id}}-alter-reorder-key.example.com" path "/events"
      """
      {"primary_key":2,"secondary_key":1}
      """
    Then within "5s" the relay subscription receives payloads in order
      """
      "primary_key":2
      "primary_key":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
