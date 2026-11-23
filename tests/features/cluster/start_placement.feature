Feature: START placement when the cluster changes under it
  A stopped domain's START moves each unit that sits on a node its node rules do not admit onto a
  node they do admit, and waits while some unit has no admitted node. A destination that has just
  restarted is not ready to take a unit yet, so START looks again once the cluster may have changed.
  A START that waits answers with a redirect when its node stops leading, and with a refusal when
  its node shuts down.

  Scenario: START looks again after a destination that has just restarted could not take the unit, then moves it with its state
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And node-2 starts with node-tags = zone=eu
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When leadership is transferred to node "node-1"
    And these Plexus commands are executed through the client on node "node-1"
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
      CREATE VHOST edge restart-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/restart';
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
    When http payload is posted to node "node-1" with host "restart-{{test_id}}.example.com" path "/restart"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "restart-{{test_id}}.example.com" path "/restart"
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
    When node "node-2" is stopped
    And runtime installation on node "node-2" is paused
    And node "node-2" is started
    Then node "node-2" reaches its runtime installation pause
    Given START placement on node "node-1" pauses at its deferral boundary
    When these Plexus commands begin executing in the background
      """
      START;
      """
    Then START placement on node "node-1" reaches its deferral boundary
    When runtime installation on node "node-2" is released
    And START placement on node "node-1" leaves its deferral boundary
    Then within "60s" the background Plexus execution reaches a successful outcome
    And within "10s" node "node-1" eventually reports scheduled "deduplicator" "dedup_txns" owner "node-2"
    When http payload is posted to node "node-1" with host "restart-{{test_id}}.example.com" path "/restart"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "restart-{{test_id}}.example.com" path "/restart"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "restart-{{test_id}}.example.com" path "/restart"
      """
      {"tenant":"beta","transaction_id":"txn-2"}
      """
    And http payload is posted to node "node-1" with host "restart-{{test_id}}.example.com" path "/restart"
      """
      {"tenant":"acme","transaction_id":"txn-2"}
      """
    Then within "10s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "transaction_id":"txn-2"
      "tenant":"acme" | "transaction_id":"txn-2"
      """

  Scenario: START that waits for an admitted node answers with a redirect when its node stops leading, and the new leader starts the domain
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "successor"
    And a node other than placeholders "leader" and "successor" is saved as placeholder "mars_node"
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
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge redirect-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/redirect';
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
      CREATE PLACEMENT mars_dedup FOR dedup_txns REQUIRE NODES WHERE tag('zone') = 'mars';
      """
    Then within "10s" node "{{leader}}" eventually reports scheduled "deduplicator" "dedup_txns" with "waiting_for=mars_dedup"
    Given START placement on node "{{leader}}" pauses at its wait boundary
    When this Plexus command request with execution reference "redirected-start" begins executing in the background on the leader node
      """
      START;
      """
    Then START placement on node "{{leader}}" reaches its wait boundary
    When leadership is transferred from node "{{leader}}" to node "{{successor}}"
    Then node "{{successor}}" eventually reports leader "{{successor}}"
    And node "{{leader}}" eventually reports leader "{{successor}}"
    When START placement on node "{{leader}}" leaves its wait boundary
    Then the background command request reports that leadership moved to node "{{successor}}" while it was applying
    When this Plexus command request with execution reference "redirected-start" begins executing in the background on the leader node
      """
      START;
      """
    And node "{{mars_node}}" is stopped
    Given {{mars_node}} starts with node-tags = zone=mars
    When node "{{mars_node}}" is started
    Then the background command request succeeds
    And within "10s" node "{{successor}}" eventually reports scheduled "deduplicator" "dedup_txns" owner "{{mars_node}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION deduped_seen TO deduped;
      """
    And http payload is posted to node "{{successor}}" with host "redirect-{{test_id}}.example.com" path "/redirect"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "{{successor}}" with host "redirect-{{test_id}}.example.com" path "/redirect"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"beta","transaction_id":"txn-1"}
      """

  Scenario: START that waits for an admitted node answers with a refusal when its node shuts down
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT mars_dedup FOR dedup_txns REQUIRE NODES WHERE tag('zone') = 'mars';
      """
    Then within "10s" node "{{leader}}" eventually reports scheduled "deduplicator" "dedup_txns" with "waiting_for=mars_dedup"
    Given START placement on node "{{leader}}" pauses at its wait boundary
    When this Plexus command request with execution reference "refused-start" begins executing in the background on the leader node
      """
      START;
      """
    Then START placement on node "{{leader}}" reaches its wait boundary
    When START placement on node "{{leader}}" leaves its wait boundary
    And node "{{leader}}" is stopped
    And node "{{leader}}" is started
    And this Plexus command request with execution reference "refused-start" is executed on the leader node
      """
      START;
      """
    Then the last command error contains
      """
      START of domain '{{domain}}' stopped waiting for its node rules because the node is shutting down
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      """
