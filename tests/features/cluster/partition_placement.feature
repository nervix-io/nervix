@partition_placement
Feature: Placement policies apply to corresponding execution units

  Scenario Outline: A hard corridor keeps matching partitions together while partitions spread
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, id I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, id integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY incoming SCHEMA event BRANCHED BY tenants;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge placement-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR reader FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION forward FROM incoming BRANCHED BY tenants
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT partition_corridor FROM incoming TO outgoing
        REQUIRE COLOCATION;
      CREATE SUBSCRIPTION observed TO outgoing;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of junction "forward" on <owners> distinct owners
    And the last cluster status colocates every partition of junction "forward" with the corresponding partition of relay "incoming"
    And the last cluster status colocates every partition of junction "forward" with the corresponding partition of relay "outgoing"
    And the owner of every partition of junction "forward" is saved with placeholder prefix "before"
    When http payload is posted to node "node-1" with host "placement-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","id":1}
      """
    And http payload is posted to node "node-1" with host "placement-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","id":2}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"id":1,"tenant":"eta"}
      key={"tenant":"acme"} payload={"id":2,"tenant":"acme"}
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT partition_corridor;
      """
    Then the last command output contains
      """
      class=identity
      """
    When these Plexus commands fail
      """
      RELOCATE JUNCTION forward PARTITION 2
        ONTO NODE <destination> IGNORE PREFERENCES;
      """
    Then the last command error contains
      """
      partition 2
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RELOCATION JUNCTION forward PARTITION 0
        ONTO NODE <destination> IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      kind=junction name=forward unit=partition:0
      """
    And the last command output does not contain
      """
      unit=partition:1
      """
    When these Plexus commands are executed on the leader node
      """
      RELOCATE JUNCTION forward PARTITION 0
        ONTO NODE <destination> IGNORE PREFERENCES;
      DESCRIBE RELOCATION JUNCTION forward PARTITION 0
        ONTO NODE <destination> IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      owner=<destination> moves=no
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status colocates every partition of junction "forward" with the corresponding partition of relay "incoming"
    And the last cluster status colocates every partition of junction "forward" with the corresponding partition of relay "outgoing"
    And the owner of every partition of junction "forward" is saved with placeholder prefix "after"
    And placeholder "after_1" equals placeholder "before_1"
    When http payload is posted to node "node-1" with host "placement-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","id":3}
      """
    And http payload is posted to node "node-1" with host "placement-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","id":4}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"id":3,"tenant":"eta"}
      key={"tenant":"acme"} payload={"id":4,"tenant":"acme"}
      """

    Examples:
      | cluster_size | owners | destination |
      | 1            | 1      | node-1      |
      | 3            | 2      | node-3      |

  Scenario Outline: A domain-wide rule is inspectable and participates in ordinary rank precedence
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE JUNCTION forward FROM incoming UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT domain_policy DOMAIN WIDE REQUIRE COLOCATION RANK 2;
      CREATE PLACEMENT corridor_policy FROM incoming TO outgoing NEUTRAL RANK 1;
      START;
      SHOW CREATE PLACEMENT domain_policy;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT domain_policy DOMAIN WIDE REQUIRE COLOCATION RANK 2;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT domain_policy;
      """
    Then the last command output contains
      """
      overridden by: corridor_policy
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER PLACEMENT corridor_policy SET DOMAIN WIDE;
      SHOW CREATE PLACEMENT corridor_policy;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT corridor_policy DOMAIN WIDE NEUTRAL RANK 1;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      scope=domain-wide policy=REQUIRE COLOCATION
      """
    And the last command output contains
      """
      class=identity
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A data-dependent intake hop accepts only an effective neutral policy
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, id I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, id integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY incoming SCHEMA event BRANCHED BY tenants;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC INGESTOR reader FROM broker TOPIC 'events'
        OFFSET BY CONSUMER GROUP 'readers' START AT LATEST INSTANCES 2
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      CREATE PLACEMENT invalid_intake FROM reader TO incoming <policy>;
      """
    Then the last command error contains
      """
      data-dependent
      """
    And the last command error contains
      """
      reader
      """
    And the last command error contains
      """
      incoming
      """
    When these Plexus commands fail
      """
      CREATE PLACEMENT domain_policy DOMAIN WIDE REQUIRE COLOCATION RANK 2;
      """
    Then the last command error contains
      """
      data-dependent
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE PLACEMENT intake_boundary FROM reader TO incoming NEUTRAL RANK 1;
      CREATE PLACEMENT domain_policy DOMAIN WIDE REQUIRE COLOCATION RANK 2;
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      domain_policy
      """
    And the last command output contains
      """
      class=data-dependent effective=NEUTRAL
      """

    Examples:
      | cluster_size | policy             |
      | 1            | REQUIRE COLOCATION |
      | 3            | REQUIRE COLOCATION |
      | 1            | PREFER COLOCATION  |
      | 3            | PREFER COLOCATION  |
      | 1            | SUGGEST SEPARATION |
      | 3            | SUGGEST SEPARATION |

  Scenario Outline: Altering instancing revalidates the existing rule against the candidate graph
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC INGESTOR reader FROM broker TOPIC 'events'
        OFFSET BY CONSUMER GROUP 'readers' START AT LATEST INSTANCES 1
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PLACEMENT intake_local FROM reader TO incoming REQUIRE COLOCATION;
      """
    When these Plexus commands fail
      """
      ALTER KAFKA TOPIC INGESTOR reader SET FROM broker TOPIC 'events'
        OFFSET BY CONSUMER GROUP 'readers' START AT LATEST INSTANCES 3
        MODE NO ACK ON QUIESCE SUSPEND;
      """
    Then the last command error contains
      """
      single↔many
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR reader;
      """
    Then the last command output contains
      """
      OFFSET BY CONSUMER GROUP 'readers' START AT LATEST MODE NO ACK
      """
    When these Plexus commands are executed on the leader node
      """
      DROP PLACEMENT intake_local;
      ALTER KAFKA TOPIC INGESTOR reader SET FROM broker TOPIC 'events'
        OFFSET BY CONSUMER GROUP 'readers' START AT LATEST INSTANCES 3
        MODE NO ACK ON QUIESCE SUSPEND;
      """
    When these Plexus commands fail
      """
      CREATE PLACEMENT intake_local FROM reader TO incoming REQUIRE COLOCATION;
      """
    Then the last command error contains
      """
      single↔many
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE PLACEMENT intake_spread FROM reader TO incoming SUGGEST SEPARATION;
      """
    When these Plexus commands fail
      """
      ALTER PLACEMENT intake_spread SET POLICY PREFER COLOCATION;
      """
    Then the last command error contains
      """
      single↔many
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT intake_spread;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT intake_spread FROM reader TO incoming SUGGEST SEPARATION;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "reader" on <owners> distinct owners
    And the owner of every instance of ingestor "reader" is saved with placeholder prefix "instance_before"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RELOCATION INGESTOR reader INSTANCE 2
        ONTO NODE node-1 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      kind=ingestor name=reader unit=instance:2
      """
    And the last command output does not contain
      """
      unit=instance:0
      """
    And the last command output does not contain
      """
      unit=instance:1
      """
    When these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR reader INSTANCE 2
        ONTO NODE node-1 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      relocated <moved> of 1 execution unit(s) onto node 'node-1'
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the owner of every instance of ingestor "reader" is saved with placeholder prefix "instance_after"
    And placeholder "instance_after_0" equals placeholder "instance_before_0"
    And placeholder "instance_after_1" equals placeholder "instance_before_1"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RELOCATION INGESTOR reader INSTANCE 2
        ONTO NODE node-1 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      owner=node-1 moves=no
      """
    When these Plexus commands fail
      """
      RELOCATE INGESTOR reader INSTANCE 3
        ONTO NODE node-1 IGNORE PREFERENCES;
      """
    Then the last command error contains
      """
      instance 3
      """

    Examples:
      | cluster_size | owners | moved |
      | 1            | 1      | 0     |
      | 3            | 3      | 1     |

  Scenario Outline: Recovering one partition preserves the healthy WASM partition's saved state
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a state-counting WASM reset graph with 2 branch partitions is running
    When these Plexus commands are executed on the leader node
      """
      CREATE PLACEMENT guest_partitions FROM counted_input_events TO counted_events
        <policy>;
      SHOW CLUSTER STATUS;
      """
    Then the current leader node is saved as placeholder "survivor"
    And the last cluster status lists 2 partitions of wasm_processor "counting_guest" on 2 distinct owners
    And the owner of every partition of wasm_processor "counting_guest" is saved with placeholder prefix "guest_before"
    And a partition owner of wasm_processor "counting_guest" other than placeholder "survivor" is saved as placeholder "failed_owner"
    When http payload is posted to node "{{survivor}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","sequence":1}
      """
    And http payload is posted to node "{{survivor}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with 0 confirmed replicas
    When node "{{failed_owner}}" is stopped
    Then within "90s" node "{{survivor}}" eventually reports wasm_processor "counting_guest" partitions moved from node "{{failed_owner}}" and all other owners unchanged from placeholder prefix "guest_before"
    When the cluster is restarted
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION after_recovery TO counted_events;
      """
    And http payload is posted to node "node-1" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","sequence":2}
      """
    And http payload is posted to node "node-1" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","sequence":2}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "note":"even"
      """

    Examples:
      | policy             |
      | NEUTRAL            |
      | REQUIRE COLOCATION |
