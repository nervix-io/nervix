Feature: Coordinated WASM processor state reset
  A reset starts a new durable guest-state lifetime for exactly the requested processor scope.
  Records already completed before the reset remain completed, and records admitted afterwards
  can observe only the new lifetime.

  Scenario Outline: NSPL resets one WASM branch through the public command path
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
    When these NSPL commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: READY, branch, generation 2
      state reset reason: TRANSACTION
      state reset readiness: READY
      """
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      stage=REPLICA_CONFIRMED required_replicas=<replica_count> confirmed_replicas=<replica_count>
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
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

  Scenario Outline: Retrying an NSPL reset keeps the selected branch's new lifetime
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And this NSPL command request with execution reference "retry-reset-{{test_id}}" is executed on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then the last command request succeeded
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And this NSPL command request with execution reference "retry-reset-{{test_id}}" is executed on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then the last command request succeeded
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

  Scenario: A reset reply lost across a leader change keeps its command identity
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given command response delivery on node "{{old_leader}}" pauses after execution
    When the active session begins this NSPL command request with execution reference "lost-reset-{{test_id}}" in the background
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then the command response delivery pause on node "{{old_leader}}" is reached
    When the background command request connection is dropped
    And the command response delivery pause on node "{{old_leader}}" is released
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And this NSPL command request with execution reference "lost-reset-{{test_id}}" is executed on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then the last command request succeeded
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION counted_events_subscription TO counted_events;
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

  Scenario Outline: An expired reset identity cannot start a guest-state lifetime
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And this NSPL command request with an execution reference created "1h" before now is executed on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then the last command error contains
      """
      has expired
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Invalid NSPL reset selections leave the active branch unchanged
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And this NSPL command request is executed on the leader node
      """
      <command>
      """
    Then the last command error contains
      """
      <error>
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

    Examples:
      | command                                                                                                   | error                                       |
      | RESET WASM PROCESSOR missing_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };     | does not exist                              |
      | RESET WASM PROCESSOR counting_guest STATE IN DOMAIN absent_domain FOR BRANCH VALUES { tenant = 'alpha' }; | names domain                                |
      | RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR UNBRANCHED;                            | invalid branch scope                        |
      | RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 7 };          | requires an exact STRING literal            |
      | RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'unseen' };   | no active execution for the selected branch |

  Scenario Outline: NSPL all-branch reset replaces each active branch together
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
    When these NSPL commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR ALL BRANCHES;
      """
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

  Scenario Outline: NSPL unbranched reset starts one fresh guest lifetime
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And an unbranched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"root","sequence":1}
      """
    When these NSPL commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR UNBRANCHED;
      """
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

  Scenario Outline: An ordered transaction reports and applies its WASM reset effect
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      RESET WASM PROCESSOR counting_guest STATE IN DOMAIN {{domain}} FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /report/operations/0/operation/kind = "RESET_WASM_STATE"
      /report/execution_steps/0/actual/outcome/status = "APPLIED"
      """
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}';
      """
    Then the last command output contains
      """
      processor=counting_guest
      """
    And the last command output contains
      """
      WASM_STATE_RESET
      """
    When client "owner" executes these NSPL commands
      """
      DESCRIBE WASM PROCESSOR counting_guest;
      """
    Then the last client outcome reports WASM reset phase "READY" at generation 2
    And the last command output does not contain
      """
      {"tenant":"alpha"}
      """
    When client "owner" executes these NSPL commands
      """
      DESCRIBE WASM PROCESSOR counting_guest FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /resource_version = 1
      /reset_readiness = "Ready"
      /reset/reset/reason = "Transaction"
      """
    And the last command output does not contain
      """
      {"tenant":"alpha"}
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
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: READY, branch, generation 2
      state reset reason: OPERATOR
      state reset readiness: READY
      """
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

  Scenario: Failed generation publication aborts preparation and preserves the current lifetime
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When the next schedule publication for domain "{{domain}}" fails
    And WASM processor "counting_guest" state reset for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      failed to publish a new guest-state generation for WASM processor 'counting_guest'
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
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

  Scenario: One request reference cannot select two reset scopes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    When WASM processor "counting_guest" state reset for branch fails
      """
      {"unexpected":"alpha"}
      """
    Then the last command error contains
      """
      reset target does not match WASM processor 'counting_guest' branching
      """
    When WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And WASM processor "counting_guest" state reset for branch fails
      """
      {"tenant":"beta"}
      """
    Then the last command error contains
      """
      conflicts with the reset already publishing for WASM processor 'counting_guest'
      """

  Scenario: A follower forwards reset validation and coordination to the leader
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When leadership is transferred to node "node-2"
    Then node "node-1" eventually reports leader "node-2"
    When WASM processor "counting_guest" state reset through node "node-1" for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      domain '{{domain}}' does not exist
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And WASM processor "counting_guest" state reset through node "node-1" for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      domain '{{domain}}' is not running
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    And WASM processor "counting_guest" state reset through node "node-1" for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      WASM processor 'counting_guest' does not exist in domain '{{domain}}'
      """
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Given node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running in the existing domain
    When WASM processor "counting_guest" state reset through node "node-1" for branch fails
      """
      {"unexpected":"alpha"}
      """
    Then the last command error contains
      """
      reset target does not match WASM processor 'counting_guest' branching
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":0}
      """
    When WASM processor "counting_guest" state is reset through node "node-1" for branch
      """
      {"tenant":"alpha"}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      """

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
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: PUBLISHING, branch, generation 2
      """
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset readiness: RESETTING
      """
    When WASM processor "counting_guest" state reset for branch with a different request fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      conflicts with the reset already publishing for WASM processor 'counting_guest'
      """
    When WASM guest-state checkpoints reach stable storage again on every node
    And the next schedule publication for domain "{{domain}}" fails
    And WASM processor "counting_guest" state reset for branch fails
      """
      {"tenant":"alpha"}
      """
    Then the last command error contains
      """
      reset was committed but its new lifetime is not usable
      """
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: PUBLISHING, branch, generation 2
      """
    And within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset readiness: AWAITING_USABLE_EXECUTION
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM processor "counting_guest" state is reset for branch
      """
      {"tenant":"alpha"}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":4}
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

  Scenario Outline: A WASM guest replaces the state lifetime of its own branch and leaves its sibling counting
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_guest_reset_filter;
      UPLOAD RESOURCE wasm_guest_reset_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE VHOST edge guest-reset-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO raw_metrics
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_guest_reset_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    And http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"alpha"}
      """
    And http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"beta"}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":-500,"tenant":"alpha"}
      """
    Then within "60s" WASM processor "filter_even_rows" completes a guest-requested state reset
    When http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"tenant":"beta"}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "value":2
      """
    When http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3,"tenant":"alpha"}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4,"tenant":"alpha"}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":4
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A guest request from a timeout callback discards the output that callback buffered
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has guest-requested-reset WASM processor fixture resource directory "wasm_processor"
    And an unbranched timeout-buffering WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"root","sequence":1}
      """
    Then within "60s" WASM processor "counting_guest" completes a guest-requested state reset
    And the relay subscription does not receive a payload within "5s"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"root","sequence":2}
      """
    Then within "15s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"unbranched" | "note":"released"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario: A failed guest-requested reset is reported and leaves the previous lifetime usable
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_guest_reset_filter;
      UPLOAD RESOURCE wasm_guest_reset_filter VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE VHOST edge guest-reset-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR metric_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO raw_metrics
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_guest_reset_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    And http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"alpha"}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When fresh WASM reset guest initialization fails on every node
    And http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":-500,"tenant":"alpha"}
      """
    Then within "30s" the active session observes a server error containing
      """
      wasm processor 'filter_even_rows' in domain '{{domain}}' could not replace the guest-state lifetime its guest requested
      """
    When fresh WASM reset guest initialization succeeds again on every node
    And http payload is posted to host "guest-reset-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      """


  Scenario Outline: Rebinding a WASM module starts a fresh lifetime in every branch
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
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 2;
      """
    Then the last command output contains
      """
      rebound 1 of 1 usage(s) of resource 'wasm_reset_guest' to version 2
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
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

  Scenario Outline: An upload and a no-op rebind keep every WASM guest lifetime
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
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 1;
      """
    Then the last command output contains
      """
      rebound 0 of 1 usage(s) of resource 'wasm_reset_guest' to version 1
      """
    And the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
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

  Scenario Outline: A rebound WASM module keeps only its new lifetime through a cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
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
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 2;
      """
    Then the last command output contains
      """
      rebound 1 of 1 usage(s) of resource 'wasm_reset_guest' to version 2
      """
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports status containing "{{domain}} status=Running"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION counted_events_subscription TO counted_events;
      """
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
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WASM PROCESSOR counting_guest;
      """
    Then the last command output contains
      """
      USING RESOURCE wasm_reset_guest VERSION 2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A rebind rejected by an unusable module keeps the previous binding and its guest lifetimes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_unusable"
    And resource directory "wasm_unusable" additionally contains
      """
      {
        "processors/filter_even.wasm": "not a WebAssembly module"
      }
      """
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
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_unusable}}';
      """
    And these NSPL commands fail with "invalid WASM PROCESSOR 'counting_guest' in domain '{{domain}}'"
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 2;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WASM PROCESSOR counting_guest;
      """
    Then the last command output contains
      """
      USING RESOURCE wasm_reset_guest VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION counted_events_retained TO counted_events;
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
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

  Scenario Outline: A rebinding of two usages resets guest lifetimes only when the whole batch activates
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_unusable"
    And resource directory "wasm_unusable" additionally contains
      """
      {
        "processors/filter_even.wasm": "not a WebAssembly module"
      }
      """
    And a branched state-counting WASM reset graph with a second usage of its resource is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_unusable}}';
      """
    And these NSPL commands fail with "invalid WASM PROCESSOR"
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 2;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WASM PROCESSOR counting_guest;
      """
    Then the last command output contains
      """
      USING RESOURCE wasm_reset_guest VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WASM PROCESSOR secondary_guest;
      """
    Then the last command output contains
      """
      USING RESOURCE wasm_reset_guest VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION counted_events_retained TO counted_events;
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":3}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 3;
      """
    Then the last command output contains
      """
      rebound 2 of 2 usage(s) of resource 'wasm_reset_guest' to version 3
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":4}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":4}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":5}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":5}
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

  Scenario Outline: A coordinated reset after a rebinding starts another lifetime on the rebound module
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
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE wasm_reset_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE wasm_reset_guest TO VERSION 2;
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When WASM processor "counting_guest" state is reset for all branches
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":3}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":4}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":4}
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
