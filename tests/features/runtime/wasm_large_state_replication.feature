@wasm_state_qualification
Feature: WASM guest saves across the replication message limit
  A guest save fits the host's buffer even when it cannot fit in one replication message. Each
  branch must reach the replica-confirmed boundary before the next callback completes.

  Scenario Outline: <save_mib> MiB saves reach the checkpoint boundary for interleaved branches
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM reset fixture with <save_mib> MiB saves in resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with <replicas> confirmed replicas
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 2 with <replicas> confirmed replicas
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    And within "30s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      failed checkpoints: 0
      """

    Examples:
      | cluster_size | replicas | save_mib |
      | 1            | 0        | 3        |
      | 3            | 1        | 3        |
      | 3            | 1        | 40       |

  Scenario: A promoted replica continues both large guest saves
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM reset fixture with 3 MiB saves in resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    # Keep the HTTP source on a surviving node while the WASM owner is lost.
    When these NSPL commands are executed on the leader node
      """
      RELOCATE WASM PROCESSOR counting_guest ONTO NODE node-2 IGNORE PREFERENCES;
      """
    When these NSPL commands are executed on the leader node
      """
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
    Then within "60s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with 1 confirmed replicas
    When node "{{former_owner}}" is stopped
    Then node "{{promoted_replica}}" eventually observes a stable leader
    And within "60s" node "{{promoted_replica}}" eventually reports scheduled "wasm_processor" "counting_guest" owner equals placeholder "promoted_replica"
    When these NSPL commands are executed on node "{{promoted_replica}}"
      """
      CREATE SUBSCRIPTION counted_events_subscription TO counted_events;
      """
    When http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "60s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 2 with 1 confirmed replicas
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    And the relay subscription does not receive a payload within "1s"

  @deadlock_diagnostics
  Scenario: Draining a large-state owner prepares its successor through bulk checkpoint streams
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM reset fixture with 3 MiB saves in resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "60s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with 1 confirmed replicas
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "wasm_processor" "counting_guest" is saved as placeholder "former_owner"
    When these NSPL commands are executed on the leader node
      """
      DRAIN NODE {{former_owner}};
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "wasm_processor" "counting_guest" is saved as placeholder "successor"
    And within "60s" node "{{successor}}" eventually reports scheduled "wasm_processor" "counting_guest" owner different from placeholder "former_owner"
    When these NSPL commands are executed on node "{{successor}}"
      """
      CREATE SUBSCRIPTION counted_events_subscription TO counted_events;
      """
    When http payload is posted to node "{{successor}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to node "{{successor}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    # Drain checkpoints each branch once before the successor handles its next callback.
    Then within "60s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 3 with 1 confirmed replicas
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    And the relay subscription does not receive a payload within "1s"
