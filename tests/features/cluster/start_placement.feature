Feature: START placement when the cluster changes under it
  A stopped domain's START moves each unit that sits on a node its node rules do not admit onto a
  node they do admit, and waits while some unit has no admitted node. START puts a move off and
  looks again once the cluster may have changed when the destination has just restarted and is not
  ready to take the unit yet, when the cluster changes after the move began, and when the cluster
  refuses to record the moved schedule because it changed after START checked it. A START that
  waits answers with a redirect when its node stops leading. When its node shuts down instead, it
  sends no answer: the leader that follows continues it like any command in progress, and the
  statement repeated under the same execution reference learns its outcome.

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

  Scenario: START looks again when the cluster changes after its move began, then moves the unit with its state
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
      CREATE VHOST edge moving-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/moving';
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
    When http payload is posted to node "node-1" with host "moving-{{test_id}}.example.com" path "/moving"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "moving-{{test_id}}.example.com" path "/moving"
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
    Given START placement on node "node-1" pauses at its move boundary
    And START placement on node "node-1" pauses at its deferral boundary
    When these Plexus commands begin executing in the background
      """
      START;
      """
    Then START placement on node "node-1" reaches its move boundary
    When these Plexus commands are executed on the active session
      """
      CORDON NODE node-3;
      """
    And START placement on node "node-1" leaves its move boundary
    Then START placement on node "node-1" reaches its deferral boundary
    When START placement on node "node-1" leaves its deferral boundary
    Then within "60s" the background Plexus execution reaches a successful outcome
    And within "10s" node "node-1" eventually reports scheduled "deduplicator" "dedup_txns" owner "node-2"
    When http payload is posted to node "node-1" with host "moving-{{test_id}}.example.com" path "/moving"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "moving-{{test_id}}.example.com" path "/moving"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "moving-{{test_id}}.example.com" path "/moving"
      """
      {"tenant":"beta","transaction_id":"txn-2"}
      """
    And http payload is posted to node "node-1" with host "moving-{{test_id}}.example.com" path "/moving"
      """
      {"tenant":"acme","transaction_id":"txn-2"}
      """
    Then within "10s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "transaction_id":"txn-2"
      "tenant":"acme" | "transaction_id":"txn-2"
      """

  Scenario: START looks again when the cluster refuses to record its moved schedule, then moves the unit with its state
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
      CREATE VHOST edge publication-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/publication';
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
    When http payload is posted to node "node-1" with host "publication-{{test_id}}.example.com" path "/publication"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "publication-{{test_id}}.example.com" path "/publication"
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
    Given START placement on node "node-1" pauses at its publication boundary
    And START placement on node "node-1" pauses at its deferral boundary
    When these Plexus commands begin executing in the background
      """
      START;
      """
    Then START placement on node "node-1" reaches its publication boundary
    When these Plexus commands are executed on the active session
      """
      CORDON NODE node-3;
      """
    And START placement on node "node-1" leaves its publication boundary
    Then START placement on node "node-1" reaches its deferral boundary
    When START placement on node "node-1" leaves its deferral boundary
    Then within "60s" the background Plexus execution reaches a successful outcome
    And within "10s" node "node-1" eventually reports scheduled "deduplicator" "dedup_txns" owner "node-2"
    When http payload is posted to node "node-1" with host "publication-{{test_id}}.example.com" path "/publication"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "publication-{{test_id}}.example.com" path "/publication"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "publication-{{test_id}}.example.com" path "/publication"
      """
      {"tenant":"beta","transaction_id":"txn-2"}
      """
    And http payload is posted to node "node-1" with host "publication-{{test_id}}.example.com" path "/publication"
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

  Scenario: START that waits for an admitted node continues on the leader that follows when its node shuts down, and starts the domain once a node is admitted
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
      CREATE VHOST edge continued-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/continued';
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
    When this Plexus command request with execution reference "continued-start" begins executing in the background on the leader node
      """
      START;
      """
    Then START placement on node "{{leader}}" reaches its wait boundary
    When START placement on node "{{leader}}" leaves its wait boundary
    And node "{{leader}}" is stopped while timing shutdown
    Then the last cluster operation completes within "20s"
    And node "{{leader}}" reports that its last shutdown finished cleanly
    And the background command request ends without an answer
    When node "{{leader}}" is started
    Then the current leader node is saved as placeholder "successor"
    And a node other than placeholders "leader" and "successor" is saved as placeholder "mars_node"
    When this Plexus command request with execution reference "continued-start" begins executing in the background on the leader node
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
    And http payload is posted to node "{{successor}}" with host "continued-{{test_id}}.example.com" path "/continued"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "{{successor}}" with host "continued-{{test_id}}.example.com" path "/continued"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      {"tenant":"beta","transaction_id":"txn-1"}
      """
