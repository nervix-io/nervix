Feature: Coordinated WASM processor state reset
  A reset starts a new durable guest-state lifetime for exactly the requested processor scope.
  Records already completed before the reset remain completed, and records admitted afterwards
  can observe only the new lifetime.

  Scenario Outline: Resetting one WASM branch survives restart and leaves its sibling lifetime intact
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a node-1-owned branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION counted_events_subscription TO counted_events;
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Resetting every WASM branch replaces two interleaved branch lifetimes together
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM processor "counting_guest" state is reset for all branches
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Resetting an unbranched WASM processor starts one explicit fresh lifetime
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And an unbranched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"root","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM processor "counting_guest" unbranched state is reset
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"root","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"root","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"unbranched" | "note":"even"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Reset drains accepted buffered output once and cancels the old branch timer
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has timeout-buffering WASM processor fixture resource directory "wasm_processor"
    And a branched timeout-buffering WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    And WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    Then within "10s" the relay subscription receives payloads in order
      """
      "tenant":"alpha"
      "tenant":"beta"
      """
    And the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1s"
    And within "5s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"released"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario: Fresh guest initialization failure preserves the pre-publication lifetime
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When fresh WASM reset guest initialization fails on every node
    And WASM processor "counting_guest" state reset for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      failed to initialize fresh guest state for WASM processor 'counting_guest'
      """
    When fresh WASM reset guest initialization succeeds again on every node
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """
    When WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then the relay subscription does not receive a payload within "1500ms"

  Scenario: A committed reset retries its initial checkpoint without starting another lifetime
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM guest-state checkpoints fail to reach stable storage on every node
    And WASM processor "counting_guest" state reset for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      reset was committed but its new lifetime is not usable
      """
    When WASM guest-state checkpoints reach stable storage again on every node
    And WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

  Scenario: A reset survives owner failover and cannot be resurrected by the former owner
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a non-node-1-owned branched state-counting WASM reset graph is running
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
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to node "{{former_owner}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    When WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And node "{{former_owner}}" is stopped
    Then node "{{promoted_replica}}" eventually observes a stable leader
    And within "60s" node "{{promoted_replica}}" eventually reports scheduled "wasm_processor" "counting_guest" owner equals placeholder "promoted_replica"
    When http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":3}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":4}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    When node "{{former_owner}}" is started
    Then node "{{promoted_replica}}" eventually observes a stable leader
    And within "60s" node "{{promoted_replica}}" eventually reports scheduled "wasm_processor" "counting_guest" owner equals placeholder "promoted_replica"
    When http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":4}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":5}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":6}
      """
    And http payload is posted to node "{{promoted_replica}}" with host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":5}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """
