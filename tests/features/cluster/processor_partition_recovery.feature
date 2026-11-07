@processor_partition_recovery
Feature: Partitioned processor recovery and handoff
  A moved processor partition retains its durable branch state. The sibling partition keeps its
  owner and continues processing through node loss and a planned drain.

  Background:
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64, amount I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer, amount integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge partition-recovery-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY tenants
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When these Plexus commands are executed through the client on node "{{leader}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of deduplicator "distinct_events" on 2 distinct owners
    And the last cluster status colocates every partition of deduplicator "distinct_events" with the corresponding partition of relay "events"
    And the owner of every partition of deduplicator "distinct_events" is saved with placeholder prefix "owner_before"
    And a partition owner of deduplicator "distinct_events" other than placeholder "leader" is saved as placeholder "moving_owner"
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observed TO unique_events;
      """
    When http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":10}
      """
    And http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":20}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":10,"seq":1,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":20,"seq":1,"tenant":"acme"}
      """

  Scenario: Losing one processor partition owner preserves both units state and sibling progress
    # Deduplicator state is published periodically rather than on its output acknowledgement.
    # Allow the 100ms publication and the replica's one-second catch-up rounds before owner loss,
    # as in branch_replica_catch_up.feature; planned handoff below captures its own final state.
    When physical time passes for "3s"
    And node "{{moving_owner}}" is stopped
    Then within "90s" node "{{leader}}" eventually reports deduplicator "distinct_events" partitions moved from node "{{moving_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    When http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":11}
      """
    And http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":21}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2,"amount":12}
      """
    And http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2,"amount":22}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":12,"seq":2,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":22,"seq":2,"tenant":"acme"}
      """

  Scenario: Draining one processor partition owner hands off its state and keeps sibling owners
    When these Plexus commands are executed on the leader node
      """
      DRAIN NODE {{moving_owner}};
      """
    Then within "90s" node "{{leader}}" eventually reports deduplicator "distinct_events" partitions moved from node "{{moving_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    When http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":11}
      """
    And http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":21}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2,"amount":12}
      """
    And http payload is posted to node "{{leader}}" with host "partition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2,"amount":22}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":12,"seq":2,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":22,"seq":2,"tenant":"acme"}
      """

