@forced_recovery_schedule
Feature: Forced recovery during destination startup

  @deloxide_stress_restore
  Scenario: A promoted replica retains its guest saves when recovery precedes schedule installation
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a non-node-1-owned branched state-counting WASM reset graph is running
    When these Plexus commands are executed on node "node-1"
      """
      CORDON NODE node-1;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "wasm_processor" "counting_guest" is saved as placeholder "former_owner"
    And the first replica for scheduled "wasm_processor" "counting_guest" in the last cluster status is saved as placeholder "promoted_replica"
    When http payload is posted to node "{{former_owner}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to node "{{former_owner}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with 1 confirmed replicas
    When runtime installation on node "{{promoted_replica}}" is paused
    And node "{{promoted_replica}}" resumes its runtime installation when forced recovery finds no schedule
    And the cluster is restarted
    Then node "{{promoted_replica}}" reaches its runtime installation pause
    When application health responses from node "{{former_owner}}" fail
    Then forced recovery on node "{{promoted_replica}}" observes its missing schedule
    And within "60s" node "node-1" eventually reports scheduled "wasm_processor" "counting_guest" owner equals placeholder "promoted_replica"
    And the last command output contains
      """
      transition_from={{former_owner}} state_recovery=unverified
      """
    When these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION recovered_events TO counted_events;
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """
