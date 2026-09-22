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
