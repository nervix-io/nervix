Feature: Node rules
  A placement rule can select the cluster nodes a runtime node's units may run on, or should
  prefer, by the facts in each node's committed record. Every placement decision honours it, a
  running domain refuses a change a running unit would break, and a stopped domain's START places
  every unit on a node its rules admit, waiting while no node does.

  Scenario Outline: A REQUIRE rule places its member on the node its selector admits on a cluster of <cluster_size>
    Given node-<cluster_size> starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
      START;
      SHOW CREATE PLACEMENT eu_only;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
      """
    And within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-<cluster_size>"
    When these Plexus commands are executed on the leader node
      """
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      eu_only scope=members policy=REQUIRE NODES rank=unranked coverage=effective
      """
    And the last command output contains
      """
      eu_only member=scorer effective=REQUIRE NODES
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT eu_only;
      """
    Then the last command output contains
      """
      selector: tag('zone') = 'eu'
      """
    And the last command output contains
      """
      matching nodes: node-<cluster_size>
      """
    And the last command output contains
      """
      unit: junction scorer owner=node-<cluster_size> satisfaction=satisfied
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      eu_only scope=members policy=REQUIRE NODES
      """
    And the last command output contains
      """
      units violating node rules: none
      """
    And the last command output contains
      """
      units waiting for node rules: none
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: RELOCATE refuses a destination a REQUIRE rule does not admit, in its preview as in the move
    Given node-2 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
      START;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-2"
    When these Plexus commands fail with "placement 'eu_only' does not admit node 'node-3' for junction 'scorer'"
      """
      DESCRIBE RELOCATION JUNCTION scorer ONTO NODE node-3 IGNORE PREFERENCES;
      """
    And these Plexus commands fail with "placement 'eu_only' does not admit node 'node-3' for junction 'scorer'"
      """
      RELOCATE JUNCTION scorer ONTO NODE node-3 IGNORE PREFERENCES;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-2"

  Scenario: The three-node random scheduler places a REQUIRE rule's member on an admitted node
    Given node-2 starts with node-tags = zone=eu
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT eu_everywhere DOMAIN WIDE REQUIRE NODES WHERE tag('zone') = 'eu';
      START;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-2"
    And within "10s" node "node-1" eventually reports scheduled "relay" "rule_input" owner "node-2"
    And within "10s" node "node-1" eventually reports scheduled "relay" "rule_output" owner "node-2"

  Scenario: A PREFER rule ranks the admitted node first and an ANY NODE rule frees a member
    Given node-3 starts with node-tags = pool=fast
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_middle SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_middle INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION archiver FROM rule_middle UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT prefer_fast FOR scorer PREFER NODES WHERE tag('pool') = 'fast';
      CREATE PLACEMENT fast_only DOMAIN WIDE REQUIRE NODES WHERE tag('pool') = 'fast' RANK 2;
      CREATE PLACEMENT archiver_anywhere FOR archiver ANY NODE RANK 1;
      START;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-3"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT fast_only;
      """
    Then the last command output contains
      """
      member: archiver overridden by archiver_anywhere
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT prefer_fast;
      """
    Then the last command output contains
      """
      unit: junction scorer owner=node-3 satisfaction=preferred
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT archiver_anywhere;
      """
    Then the last command output contains
      """
      unit: junction archiver owner=node-
      """
    And the last command output contains
      """
      satisfaction=satisfied
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      archiver_anywhere scope=members policy=ANY NODE rank=1 coverage=effective
      """
    And the last command output contains
      """
      fast_only scope=domain-wide policy=REQUIRE NODES rank=2 coverage=partial
      """

  Scenario: A selector that fails on a node does not match it, and DESCRIBE PLACEMENT names the failure
    Given node-1 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 1 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT numeric_zone FOR scorer PREFER NODES WHERE (tag('zone') AS I64) = 1;
      START;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-1"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE PLACEMENT numeric_zone;
      """
    Then the last command output contains
      """
      matching nodes: (none)
      """
    And the last command output contains
      """
      unit: junction scorer owner=node-1 satisfaction=not preferred
      """
    And the last command output contains
      """
      selector failed on 'node-1':
      """

  Scenario Outline: Conflicting or invalid node rules are refused on a cluster of <cluster_size>
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT named_node FOR scorer REQUIRE NODES WHERE node.name = 'node-1' RANK 1;
      """
    And these Plexus commands fail with "placements 'anywhere' and 'named_node' conflict on runtime node junction 'scorer'"
      """
      CREATE PLACEMENT anywhere FOR scorer ANY NODE RANK 1;
      """
    And these Plexus commands fail with "node.disk.free"
      """
      CREATE PLACEMENT disks FOR scorer REQUIRE NODES WHERE node.disk.free;
      """
    And these Plexus commands fail with "placement 'named_node' is a node rule; SET FROM ... TO ... applies only to a colocation rule"
      """
      ALTER PLACEMENT named_node SET FROM scorer TO scorer;
      """
    And these Plexus commands fail with "placement 'named_node' is a node rule; SET POLICY needs a node policy"
      """
      ALTER PLACEMENT named_node SET POLICY PREFER COLOCATION;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A running domain refuses a node rule its running unit breaks on a cluster of <cluster_size>
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-1"
    When these Plexus commands fail with "placement 'mars_only' does not admit node 'node-1', which runs junction 'scorer'"
      """
      CREATE PLACEMENT mars_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'mars';
      """
    And these Plexus commands fail with "no cluster node satisfies placement 'moon_only' for junction 'lunar'"
      """
      BEGIN;
      CREATE JUNCTION lunar FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT moon_only FOR lunar REQUIRE NODES WHERE tag('zone') = 'moon';
      COMMIT;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      no placements
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A unit keeps running when its node leaves and returns without the tag its rule requires
    Given node-3 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING );
      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string
      );
      CREATE WIRE JSON CODEC transaction_codec
        FROM WIRE SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge rules-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/rules';
      CREATE HTTP ENDPOINT INGESTOR ingestor_txns
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING transaction_codec
        TO inbound INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION scorer FROM inbound BRANCHED BY by_tenant
        TO scored INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
      START;
      CREATE SUBSCRIPTION scored_seen TO scored;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-3"
    When http payload is posted to node "node-1" with host "rules-{{test_id}}.example.com" path "/rules"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "rules-{{test_id}}.example.com" path "/rules"
      """
      {"tenant":"beta","transaction_id":"txn-2"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"beta","transaction_id":"txn-2"}
      """
    When node "node-3" is stopped
    Then within "30s" node "node-1" eventually reports scheduled "junction" "scorer" with "waiting_for=eu_only"
    And within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-3"
    And node "node-1" observability metric "nervix_placement_waiting_units" with labels eventually equals 1
      """
      domain="{{domain}}"
      """
    Given node-3 starts with node-tags = zone=us
    When node "node-3" is started
    Then within "30s" node "node-1" eventually reports scheduled "junction" "scorer" with "violates=eu_only"
    And node "node-1" observability metric "nervix_placement_violating_units" with labels eventually equals 1
      """
      domain="{{domain}}"
      """
    And node "node-1" observability metric "nervix_placement_waiting_units" with labels eventually equals 0
      """
      domain="{{domain}}"
      """
    When this Plexus command request is executed on the leader node
      """
      DESCRIBE PLACEMENT eu_only;
      """
    Then the last command output contains
      """
      unit: junction scorer owner=node-3 satisfaction=violates
      """
    And within "10s" DESCRIBE DOMAIN on node "node-1" contains
      """
      units violating node rules: junction scorer on node-3 (eu_only)
      """
    When http payload is posted to node "node-1" with host "rules-{{test_id}}.example.com" path "/rules"
      """
      {"tenant":"beta","transaction_id":"txn-3"}
      """
    And http payload is posted to node "node-1" with host "rules-{{test_id}}.example.com" path "/rules"
      """
      {"tenant":"acme","transaction_id":"txn-4"}
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"tenant":"beta","transaction_id":"txn-3"}
      {"tenant":"acme","transaction_id":"txn-4"}
      """
    And within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-3"
    When these Plexus commands are executed on the leader node
      """
      DROP PLACEMENT eu_only;
      """
    Then node "node-1" observability metric "nervix_placement_violating_units" with labels is eventually absent
      """
      domain="{{domain}}"
      """
    And node "node-1" observability metric "nervix_placement_waiting_units" with labels is eventually absent
      """
      domain="{{domain}}"
      """

  Scenario: A stopped domain accepts a node rule and START moves the unit with its state to the admitted node
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And node-2 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING );
      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string
      );
      CREATE WIRE JSON CODEC transaction_codec
        FROM WIRE SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge stopped-rules-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/stopped-rules';
      CREATE HTTP ENDPOINT INGESTOR ingestor_txns
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING transaction_codec
        TO inbound INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION deduped_seen TO deduped;
      """
    Then within "10s" node "node-1" eventually reports scheduled "deduplicator" "dedup_txns" owner "node-1"
    When http payload is posted to node "node-1" with host "stopped-rules-{{test_id}}.example.com" path "/stopped-rules"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "stopped-rules-{{test_id}}.example.com" path "/stopped-rules"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    When these Plexus commands are executed on the active session
      """
      STOP;
      CREATE PLACEMENT eu_dedup FOR dedup_txns REQUIRE NODES WHERE tag('zone') = 'eu';
      """
    Then within "10s" node "node-1" eventually reports scheduled "deduplicator" "dedup_txns" with "violates=eu_dedup"
    When these Plexus commands are executed on the active session
      """
      START;
      """
    Then the last command output contains
      """
      starting domain '{{domain}}'
      """
    And within "10s" node "node-1" eventually reports scheduled "deduplicator" "dedup_txns" owner "node-2"
    When http payload is posted to node "node-1" with host "stopped-rules-{{test_id}}.example.com" path "/stopped-rules"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "stopped-rules-{{test_id}}.example.com" path "/stopped-rules"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    Then the relay subscription does not receive a payload within "5s"

  Scenario: START waits until a node satisfies the rule of every unit
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT mars_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'mars';
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      mars_only scope=members policy=REQUIRE NODES rank=unranked coverage=unmatched
      """
    And within "10s" node "node-1" eventually reports scheduled "junction" "scorer" with "waiting_for=mars_only"
    When these Plexus commands begin executing in the background
      """
      START;
      """
    Then the background Plexus execution is still running after "3s"
    When these Plexus commands are executed through the client on node "node-2"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      """
    And the last command output contains
      """
      units waiting for node rules: junction scorer (mars_only)
      """
    When these Plexus commands are executed through the client on node "node-2"
      """
      DESCRIBE PLACEMENT mars_only;
      """
    Then the last command output contains
      """
      matching nodes: (none)
      """
    And the last command output contains
      """
      satisfaction=waiting
      """
    When node "node-3" is stopped
    Given node-3 starts with node-tags = zone=mars
    When node "node-3" is started
    Then within "60s" the background Plexus execution reaches a successful outcome
    And within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-3"

  Scenario: START queued beside a node rule is refused, and the rule commits without it
    Given the production sticky scheduler is configured
    And a 1 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Given client "operator" is connected to the leader node
    When client "operator" fails to execute these Plexus commands
      """
      BEGIN;
      CREATE PLACEMENT mars_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'mars';
      START;
      """
    Then the last command error contains
      """
      START cannot be queued in a transaction; START and STOP run on their own, after COMMIT
      """
    And client "operator" transaction state is "OPEN"
    When client "operator" executes these Plexus commands
      """
      COMMIT;
      """
    Then client "operator" transaction state is "COMMITTED"
    When these Plexus commands are executed on the leader node
      """
      SHOW PLACEMENTS;
      """
    Then the last command output contains
      """
      mars_only
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      """

  Scenario: A node that restarts without quorum reports its status for a domain with node rules
    Given node-3 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
      START;
      """
    Then within "10s" node "node-1" eventually reports scheduled "junction" "scorer" owner "node-3"
    When node "node-1" is stopped
    And node "node-2" is stopped
    And node "node-3" is stopped
    And node "node-3" is started until public observation is available
    Then node "node-3" eventually reports status containing "node.initialization=initializing"
    When node "node-1" is started
    Then node "node-3" eventually reports status containing "node.initialization=ready"
    And within "30s" node "node-3" eventually reports scheduled "junction" "scorer" owner "node-3"

  Scenario: A backup restores a domain's node rules
    Given node-1 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA rule_event ( id I64 );
      CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
      CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
      CREATE JUNCTION scorer FROM rule_input UNBRANCHED
        TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT eu_only FOR scorer PREFER NODES WHERE tag('zone') = 'eu' AND NOT node.cuda.available RANK 4;
      CREATE PLACEMENT eu_required DOMAIN WIDE REQUIRE NODES WHERE tag('zone') IN ('eu', 'uk');
      """
    When the CLI backs up "domain {{domain}}" from node "node-1" into "rules.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "rules.nvxb" on node "node-1" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 6 models
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT eu_only;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT eu_only FOR scorer PREFER NODES WHERE tag('zone') = 'eu' AND NOT node.cuda.available RANK 4;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE PLACEMENT eu_required;
      """
    Then the last command output contains
      """
      CREATE PLACEMENT eu_required DOMAIN WIDE REQUIRE NODES WHERE tag('zone') IN ('eu', 'uk');
      """
