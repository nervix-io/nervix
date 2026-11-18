@window_collection_recovery @deloxide_stress_restore
Feature: Collected window sequences survive state lifecycle changes
  The exact scalar and multidimensional lists identify retained rows, order and branch isolation.
  Every fault begins after the replica holds the owner's complete populated checkpoints.

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
      CREATE SCHEMA reading (tenant STRING, value I64, pair ARRAY<I64, 2>);
      CREATE SCHEMA history (tenant STRING, values ARRAY<I64, 3>, vectors VEC<I64>, pairs ARRAY<I64, 3, 2>);
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (tenant string, value integer, pair array);
      CREATE WIRE JSON CODEC reading_codec FROM WIRE SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge collection-recovery-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR collection_window FROM readings
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_tenant
        TO histories SET tenant = FIRST(input.tenant), values = COLLECT_ARRAY(input.value),
          vectors = COLLECT_VEC(input.value), pairs = COLLECT_ARRAY(input.pair)
        ON MESSAGE ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "window_processor" "collection_window" is saved as placeholder "owner"
    And the first replica for scheduled "window_processor" "collection_window" in the last cluster status is saved as placeholder "replica"
    When http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":10,"pair":[10,11]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":100,"pair":[100,101]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":20,"pair":[20,21]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":200,"pair":[200,201]}
      """
    Given window processor "collection_window" has complete checkpoints for both branches on replica "{{replica}}" of owner "{{owner}}"

  Scenario: STOP and START retain both collecting windows
    When these Plexus commands are executed on node "{{replica}}"
      """
      STOP;
      START;
      """
    Then node "{{replica}}" eventually observes a stable leader
    When these Plexus commands are executed on node "{{replica}}"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  Scenario: Restarting the cluster retains scalar and list argument columns
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "node-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "node-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  Scenario: Restarting the window owner retains both branch sequences
    When node "{{owner}}" is stopped
    And node "{{owner}}" is started
    Then node "{{replica}}" eventually observes a stable leader
    And within "60s" node "{{owner}}" answers node "{{replica}}" the revisions node "{{replica}}" made ready
    When these Plexus commands are executed on node "{{replica}}"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  Scenario: A planned handoff retains incomplete multidimensional collections
    When these Plexus commands are executed on node "{{replica}}"
      """
      RELOCATE WINDOW PROCESSOR collection_window ONTO NODE {{replica}} IGNORE PREFERENCES;
      """
    Then within "90s" node "{{replica}}" eventually reports scheduled "window_processor" "collection_window" owner equals placeholder "replica"
    Then node "{{replica}}" eventually observes a stable leader
    When these Plexus commands are executed on node "{{replica}}"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  Scenario: A promoted replica continues both exact collected sequences
    When node "{{owner}}" is stopped
    Then within "90s" node "{{replica}}" eventually reports scheduled "window_processor" "collection_window" owner equals placeholder "replica"
    Then node "{{replica}}" eventually observes a stable leader
    When these Plexus commands are executed on node "{{replica}}"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "{{replica}}" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  @restore_installation
  Scenario: RESUME restores scalar and list argument columns
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "collection-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh 3 node cluster whose nodes are named "restored"
    And the restore coordinator uses remote placement in a multi-node cluster
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume" from "collection-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 12 models
    Then node "restored-1" eventually observes a stable leader
    When these Plexus commands are executed on node "restored-1"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  @restore_installation
  Scenario: A plain START continues restored collecting windows
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "collection-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh 3 node cluster whose nodes are named "restored"
    And the restore coordinator uses remote placement in a multi-node cluster
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} " from "collection-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 12 models
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then node "restored-1" eventually observes a stable leader
    When these Plexus commands are executed on node "restored-1"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[10,20,30] | "vectors":[10,20,30] | "pairs":[[10,11],[20,21],[30,31]]
      key={"tenant":"beta"} | "values":[100,200,300] | "vectors":[100,200,300] | "pairs":[[100,101],[200,201],[300,301]]
      """

  @restore_installation
  Scenario: WITHOUT STATE starts both collecting windows empty
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "collection-state.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh 3 node cluster whose nodes are named "restored"
    And the restore coordinator uses remote placement in a multi-node cluster
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume --without-state" from "collection-state.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 12 models
    Then node "restored-1" eventually observes a stable leader
    When these Plexus commands are executed on node "restored-1"
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":30,"pair":[30,31]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":300,"pair":[300,301]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":40,"pair":[40,41]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":400,"pair":[400,401]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":50,"pair":[50,51]}
      """
    And http payload is posted to node "restored-1" with host "collection-recovery-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":500,"pair":[500,501]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "values":[30,40,50] | "vectors":[30,40,50] | "pairs":[[30,31],[40,41],[50,51]]
      key={"tenant":"beta"} | "values":[300,400,500] | "vectors":[300,400,500] | "pairs":[[300,301],[400,401],[500,501]]
      """
