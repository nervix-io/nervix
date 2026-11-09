Feature: Typed WASM processor state inspection
  SHOW WASM PROCESSOR ... STATE reads every execution unit's committed guest-state facts through the
  typed state inspection envelope: the pinned binding, each unit's lifetimes, reset and complete
  checkpoint and recovery counts, and one entry per selected branch, bounded by the envelope's paging.

  @wasm_state_inspection
  Scenario Outline: Branch checkpoints and unit lifetimes travel as typed tables
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
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with <replica_count> confirmed replicas
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR ALL BRANCHES FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /entity/name = "counting_guest"
      /outcome = "page"
      /counts/branches = 2
      /unit_count = 1
      /continuation = null
      /wasm_binding/resource = "wasm_reset_guest"
      /wasm_binding/version = 1
      /wasm_binding/file = "processors/filter_even.wasm"
      /units/0/wasm/default_generation = 1
      /units/0/wasm/reset = null
      /units/0/wasm/checkpoints/total = 2
      /units/0/wasm/checkpoints/replica_confirmed = 2
      /units/0/wasm/checkpoints/failed = 0
      /units/0/wasm/recoveries/attempted = 0
      /units/0/wasm/recoveries/recovered = 0
      /units/0/wasm/recoveries/failed = 0
      /units/0/wasm/branches/0/checkpoint/generation = 1
      /units/0/wasm/branches/0/checkpoint/committed_revision = 1
      /units/0/wasm/branches/0/checkpoint/latest_revision = 1
      /units/0/wasm/branches/0/checkpoint/stage = "replica_confirmed"
      /units/0/wasm/branches/0/checkpoint/required_replicas = <replica_count>
      /units/0/wasm/branches/0/checkpoint/confirmed_replicas = <replica_count>
      /units/0/wasm/branches/0/recovery = null
      /units/0/wasm/branches/1/checkpoint/stage = "replica_confirmed"
      /units/0/wasm/branches/1/recovery = null
      """
    And the last WASM state inspection JSON lists 2 branch entries
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE;
      """
    Then the last command output contains
      """
      kind: WASM PROCESSOR
      entity: counting_guest
      """
    And the last command output contains
      """
      binding: resource=wasm_reset_guest version=1 file=processors/filter_even.wasm
      """
    And the last command output contains
      """
      state default generation: 1
      state structures: 2
      checkpoints awaiting local storage: 0
      checkpoints awaiting replicas: 0
      failed checkpoints: 0
      """
    And the last command output contains
      """
      generation=1 committed_revision=1 latest_revision=1 stage=REPLICA_CONFIRMED required_replicas=<replica_count> confirmed_replicas=<replica_count>
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE WASM PROCESSOR counting_guest;
      """
    Then the last command output contains
      """
      resource: wasm_reset_guest
      resource version: 1
      file: processors/filter_even.wasm
      """
    And the last command output contains
      """
      state default generation: 1
      state structures: 2
      checkpoints awaiting local storage: 0
      checkpoints awaiting replicas: 0
      failed checkpoints: 0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: The envelope's paging bounds branch entries and resumes after the last returned branch
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
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"gamma","sequence":1}
      """
    Then within "30s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      state structures: 3
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 2 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 3
      /units/0/wasm/checkpoints/total = 3
      """
    And the last WASM state inspection JSON lists 2 branch entries
    And the last WASM state inspection JSON branch fingerprints are saved as placeholder "first_page"
    And the last command output JSON value at "/continuation" is saved as placeholder "continuation"
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 2 AFTER '{{continuation}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 3
      /units/0/wasm/checkpoints/total = 3
      /continuation = null
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are disjoint from placeholder "first_page"
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR BRANCH VALUES { tenant = 'beta' } LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 1
      /units/0/wasm/checkpoints/total = 1
      /units/0/wasm/branches/0/checkpoint/stage = "replica_confirmed"
      /continuation = null
      """
    And the last WASM state inspection JSON lists 1 branch entries
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 1 FORMAT JSON;
      """
    Then the last command output JSON value at "/continuation" is saved as placeholder "stale_continuation"
    When these Plexus commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE FOR BRANCH VALUES { tenant = 'gamma' };
      """
    Given client "reader" is connected to node "node-1"
    When client "reader" submits this Plexus command request
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 1 AFTER '{{stale_continuation}}' FORMAT JSON;
      """
    Then the typed inspection refuses the read with "stale_continuation"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: A unit's latest reset carries its request, scope, phase, generation and readiness
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
    When these Plexus commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE FOR BRANCH VALUES { tenant = 'alpha' };
      """
    Then within "10s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      state reset: READY, branch, generation 2
      state reset reason: TRANSACTION
      state reset readiness: READY
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR BRANCH VALUES { tenant = 'alpha' } FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 1
      /units/0/wasm/default_generation = 1
      /units/0/wasm/reset/scope/kind = "branch"
      /units/0/wasm/reset/phase = "ready"
      /units/0/wasm/reset/reason = "transaction"
      /units/0/wasm/reset/generation = 2
      /units/0/wasm/reset/readiness = "ready"
      /units/0/wasm/branches/0/checkpoint/generation = 2
      """
    And the last WASM state inspection JSON reset scope names its only branch entry
    And the last WASM state inspection JSON reset names a request reference

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: An unbranched processor reports one unbranched entry
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And an unbranched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    Then within "30s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      stage=REPLICA_CONFIRMED required_replicas=<replica_count> confirmed_replicas=<replica_count>
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 0
      /units/0/wasm/checkpoints/total = 1
      /units/0/wasm/branches/0/branch = null
      /units/0/wasm/branches/0/checkpoint/stage = "replica_confirmed"
      /continuation = null
      """
    And the last WASM state inspection JSON lists 1 branch entries

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: Partitioned units are read at their owners and paged across units
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a state-counting WASM reset graph with 2 branch partitions is running
    When these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of wasm_processor "counting_guest" on <owners> distinct owners
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "30s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      generation=1 committed_revision=1 latest_revision=1 stage=REPLICA_CONFIRMED
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 2
      /unit_count = 2
      /units/0/wasm/checkpoints/total = 1
      /units/1/wasm/checkpoints/total = 1
      /continuation = null
      """
    And the last WASM state inspection JSON lists 2 branch entries
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 1 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /counts/branches = 2
      /unit_count = 2
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are saved as placeholder "first_page"
    And the last command output JSON value at "/continuation" is saved as placeholder "continuation"
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 1 AFTER '{{continuation}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /counts/branches = 2
      /unit_count = 2
      /continuation = null
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are disjoint from placeholder "first_page"

    Examples:
      | cluster_size | replica_count | owners |
      | 1            | 0             | 1      |
      | 3            | 1             | 2      |

  @wasm_state_inspection
  Scenario Outline: Inspection refusals are typed outcomes of the envelope
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    Given client "reader" is connected to node "node-1"
    When client "reader" submits this Plexus command request
      """
      SHOW WASM PROCESSOR absent_guest STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "unknown_entity"
    When client "reader" submits this Plexus command request
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR UNBRANCHED FORMAT JSON;
      """
    Then the typed inspection refuses the read with "invalid_selection"
    When client "reader" submits this Plexus command request
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 18446744073709551615 FORMAT JSON;
      """
    Then the typed inspection refuses the read with "invalid_limit"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
