Feature: Placement policies

  Scenario Outline: Placement rules and domain defaults are inspectable through NSPL
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}} PLACEMENT PREFER COLOCATION;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage_one SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage_two SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage_one INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_middle FROM placement_stage_one UNBRANCHED
        TO placement_stage_two INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage_two UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT corridor_policy
        FROM corridor_source
        TO corridor_sink
        PREFER COLOCATION
        RANK 3;
      START;
      SHOW CREATE PLACEMENT corridor_policy;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT corridor_policy FROM corridor_source TO corridor_sink PREFER COLOCATION RANK 3;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      corridor_policy
      """
    And the last command output contains
      """
      PREFER COLOCATION
      """
    And the last command output contains
      """
      rank=3
      """
    And the last command output contains
      """
      coverage=effective
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE PLACEMENT corridor_policy;
      """
    Then the last command output contains
      """
      placement: corridor_policy
      """
    And the last command output contains
      """
      policy: PREFER COLOCATION
      """
    And the last command output contains
      """
      rank: 3
      """
    And the last command output contains
      """
      connected: true
      """
    And the last command output contains
      """
      covered: corridor_source, corridor_middle, corridor_sink
      """
    And the last command output contains
      """
      witness: corridor_source -> corridor_middle -> corridor_sink
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      placement:
      """
    And the last command output contains
      """
      default policy: PREFER COLOCATION
      """
    And the last command output contains
      """
      rule count: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: REQUIRE COLOCATION keeps a complete corridor on one cluster node
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}} PLACEMENT SUGGEST SEPARATION;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage_one SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage_two SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage_one INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_middle FROM placement_stage_one UNBRANCHED
        TO placement_stage_two INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage_two UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_corridor_local
        FROM corridor_source
        TO corridor_sink
        REQUIRE COLOCATION
        RANK 1;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "corridor_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_middle" owner equals placeholder "corridor_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "corridor_owner"
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      members: corridor_middle, corridor_sink, corridor_source
      """
    And the last command output contains
      """
      host: {{corridor_owner}}
      """

  Scenario: A materialized-state dependency forms a placement corridor
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}} PLACEMENT SUGGEST SEPARATION;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY state_writer_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY state_cache SCHEMA placement_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY state_reader_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY state_reader_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION state_writer FROM state_writer_input UNBRANCHED
        TO state_cache INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION state_reader FROM state_reader_input UNBRANCHED
        USING MATERIALIZED STATE state_cache REQUIRED SKIP
        TO state_reader_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT state_dependency_local
        FROM state_cache
        TO state_reader
        REQUIRE COLOCATION
        RANK 1;
      START;
      DESCRIBE PLACEMENT state_dependency_local;
      """
    Then the last command output contains
      """
      pair: state_cache -> state_reader
      """
    And the last command output contains
      """
      connected: true
      """
    And the last command output contains
      """
      covered: state_cache, state_reader
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "materializer" "state_cache" is saved as placeholder "state_group_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "state_reader" owner equals placeholder "state_group_owner"

  Scenario: PREFER COLOCATION overrides the spreading domain default for a new assignment
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}} PLACEMENT SUGGEST SEPARATION;
      BEGIN;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE RELAY control_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY control_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY control_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION control_source FROM control_input UNBRANCHED
        TO control_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION control_sink FROM control_stage UNBRANCHED
        TO control_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT prefer_local
        FROM corridor_source
        TO corridor_sink
        PREFER COLOCATION;
      COMMIT;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "source_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "source_owner"
    And the last cluster status owner for scheduled "junction" "control_source" is saved as placeholder "control_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "control_sink" owner different from placeholder "control_owner"

  Scenario: SUGGEST SEPARATION overrides upstream locality for a new assignment
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}} PLACEMENT PREFER COLOCATION;
      BEGIN;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT spread_corridor
        FROM corridor_source
        TO corridor_sink
        SUGGEST SEPARATION;
      COMMIT;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "source_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner different from placeholder "source_owner"

  Scenario: Changing a soft placement policy does not migrate existing assignments
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      BEGIN;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT existing_soft_policy
        FROM corridor_source
        TO corridor_sink
        SUGGEST SEPARATION;
      COMMIT;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "original_source_owner"
    And the last cluster status owner for scheduled "junction" "corridor_sink" is saved as placeholder "original_sink_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner different from placeholder "original_source_owner"
    When these NSPL commands are executed on the leader node
      """
      ALTER PLACEMENT existing_soft_policy SET POLICY PREFER COLOCATION;
      SHOW CLUSTER STATUS;
      """
    Then within "5s" node "node-1" eventually reports scheduled "junction" "corridor_source" owner equals placeholder "original_source_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "original_sink_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner different from placeholder "original_source_owner"

  Scenario Outline: A stronger rank overrides a claim and equal-rank policy claims conflict
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_local
        FROM corridor_source
        TO corridor_sink
        REQUIRE COLOCATION
        RANK 2;
      CREATE PLACEMENT carve_out
        FROM corridor_source
        TO corridor_sink
        NEUTRAL
        RANK 1;
      DESCRIBE PLACEMENT keep_local;
      """
    Then the last command output contains
      """
      overridden by: carve_out
      """
    And the last command output contains
      """
      effective policy: NEUTRAL
      """
    When these NSPL commands fail
      """
      ALTER PLACEMENT carve_out SET RANK 2;
      """
    Then the last command error contains
      """
      keep_local
      """
    And the last command error contains
      """
      carve_out
      """
    And the last command error contains
      """
      corridor_source
      """
    And the last command error contains
      """
      corridor_sink
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT carve_out;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT carve_out FROM corridor_source TO corridor_sink NEUTRAL RANK 1;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A placement with no connected endpoint pair is valid and visibly empty
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY left_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY left_output SCHEMA placement_event UNBRANCHED;
      CREATE RELAY right_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY right_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION disconnected_source FROM left_input UNBRANCHED
        TO left_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION disconnected_sink FROM right_input UNBRANCHED
        TO right_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT disconnected_corridor
        FROM disconnected_source
        TO disconnected_sink
        REQUIRE COLOCATION;
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      disconnected_corridor
      """
    And the last command output contains
      """
      coverage=empty
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE PLACEMENT disconnected_corridor;
      """
    Then the last command output contains
      """
      connected: false
      """
    And the last command output contains
      """
      covered: (none)
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Placement coverage grows and shrinks with topology edits
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY left_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY left_output SCHEMA placement_event UNBRANCHED;
      CREATE RELAY right_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY right_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION disconnected_source FROM left_input UNBRANCHED
        TO left_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION disconnected_sink FROM right_input UNBRANCHED
        TO right_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT changing_corridor
        FROM disconnected_source
        TO disconnected_sink
        REQUIRE COLOCATION
        RANK 1;
      START;
      DESCRIBE PLACEMENT changing_corridor;
      """
    Then the last command output contains
      """
      connected: false
      """
    And the last command output contains
      """
      covered: (none)
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION corridor_bridge FROM left_output UNBRANCHED
        TO right_input INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      DESCRIBE PLACEMENT changing_corridor;
      """
    Then the last command output contains
      """
      connected: true
      """
    And the last command output contains
      """
      covered: disconnected_source, corridor_bridge, disconnected_sink
      """
    And the last command output contains
      """
      witness: disconnected_source -> corridor_bridge -> disconnected_sink
      """
    When these NSPL commands are executed on the leader node
      """
      DROP JUNCTION corridor_bridge;
      DESCRIBE PLACEMENT changing_corridor;
      """
    Then the last command output contains
      """
      connected: false
      """
    And the last command output contains
      """
      covered: (none)
      """

  Scenario Outline: ALTER and DROP PLACEMENT preserve operation order and member pins
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT managed_corridor
        FROM corridor_source
        TO corridor_sink
        REQUIRE COLOCATION
        RANK 3;
      ALTER PLACEMENT managed_corridor
        SET POLICY PREFER COLOCATION,
        SET RANK 2,
        RENAME TO renamed_corridor,
        DROP RANK;
      SHOW CREATE PLACEMENT renamed_corridor;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT renamed_corridor FROM corridor_source TO corridor_sink PREFER COLOCATION;
      """
    When these NSPL commands fail
      """
      DROP JUNCTION corridor_sink;
      """
    Then the last command error contains
      """
      corridor_sink
      """
    And the last command error contains
      """
      renamed_corridor
      """
    When these NSPL commands are executed on the leader node
      """
      DROP PLACEMENT renamed_corridor;
      DROP JUNCTION corridor_sink;
      SHOW PLACEMENTS;
      """
    Then the last command output does not contain
      """
      renamed_corridor
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Invalid placement ranks and members report semantic diagnostics
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE WIRE JSON SCHEMA placement_event_wire MODE STRICT ( id integer );
      CREATE CODEC placement_event_codec
        FROM WIRE JSON SCHEMA placement_event_wire
        TO SCHEMA placement_event;
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE VHOST edge placement-{{test_id}}.example.com;
      CREATE ENDPOINT placement_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR endpoint_source
        FROM ENDPOINT placement_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING placement_event_codec
        TO placement_input INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these NSPL commands fail
      """
      CREATE PLACEMENT bad_rank
        FROM corridor_source
        TO corridor_sink
        REQUIRE COLOCATION
        RANK 0;
      """
    Then the last command error contains
      """
      RANK 0
      """
    When these NSPL commands fail
      """
      CREATE PLACEMENT unknown_member
        FROM missing_runtime_node
        TO corridor_sink
        REQUIRE COLOCATION;
      """
    Then the last command error contains
      """
      missing_runtime_node
      """
    When these NSPL commands fail
      """
      CREATE PLACEMENT relay_member
        FROM placement_input
        TO corridor_sink
        REQUIRE COLOCATION;
      """
    Then the last command error contains
      """
      placement_input
      """
    And the last command error contains
      """
      not materialized
      """
    When these NSPL commands fail
      """
      CREATE PLACEMENT endpoint_member
        FROM endpoint_source
        TO corridor_sink
        REQUIRE COLOCATION;
      """
    Then the last command error contains
      """
      endpoint_source
      """
    And the last command error contains
      """
      not placement-eligible
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Changing the domain default to REQUIRE COLOCATION consolidates the pipeline
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}} PLACEMENT SUGGEST SEPARATION;
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      ALTER DOMAIN SET PLACEMENT REQUIRE COLOCATION;
      """
    Then the last command output contains
      """
      planned relocations:
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "consolidated_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "consolidated_owner"

  Scenario: Draining a REQUIRE COLOCATION host relocates the group atomically
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_corridor_local
        FROM corridor_source TO corridor_sink REQUIRE COLOCATION RANK 1;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "drained_group_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "drained_group_owner"
    When these NSPL commands are executed on the leader node
      """
      DRAIN NODE {{drained_group_owner}};
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "relocated_group_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_source" owner different from placeholder "drained_group_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "relocated_group_owner"

  Scenario: Failover relocates a REQUIRE COLOCATION group atomically
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA placement_event ( id I64 );
      CREATE RELAY placement_input SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_stage SCHEMA placement_event UNBRANCHED;
      CREATE RELAY placement_output SCHEMA placement_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM placement_input UNBRANCHED
        TO placement_stage INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM placement_stage UNBRANCHED
        TO placement_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_corridor_local
        FROM corridor_source TO corridor_sink REQUIRE COLOCATION RANK 1;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "failed_group_owner"
    And within "5s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "failed_group_owner"
    And a node other than placeholder "failed_group_owner" is saved as placeholder "failover_query_node"
    When node "{{failed_group_owner}}" is stopped
    Then node "{{failover_query_node}}" eventually observes a stable leader
    And within "20s" node "{{failover_query_node}}" eventually reports scheduled "junction" "corridor_source" owner different from placeholder "failed_group_owner"
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "failover_group_owner"
    And within "5s" node "{{failover_query_node}}" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "failover_group_owner"
