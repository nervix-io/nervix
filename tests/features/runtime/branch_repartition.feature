@branch_repartition
Feature: Changing a running branch's partition count
  ALTER BRANCH SET PARTITIONS on a running domain begins a new partition epoch without pausing the
  domain. New partitions start at once, and a key that lands in another partition under the new
  count keeps its state: the first record that reaches its new partition claims the key from the
  partition that holds it, which finishes the key's pending work and hands over every state
  component the key has. A key that never reappears expires in its old partition, and the epoch
  retires once no partition holds a key outside its place. Under four partitions eta and gamma
  lie in partition 0 and 1 and stay there under eight; beta, lambda, xi and kappa lie in
  partitions 1, 2, 0 and 3 and move to 5, 6, 4 and 7. Under eight they lie in 5, 6, 4 and 7 and
  move back under four.

  Scenario Outline: Growing a running branch keeps each moved key's deduplicator keys, materialized row and order to an emitter
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA profile ( tenant STRING, tier STRING );
      CREATE WIRE JSON SCHEMA profile_wire MODE STRICT ( tenant string, tier string );
      CREATE WIRE JSON CODEC profile_codec FROM WIRE SCHEMA profile_wire TO SCHEMA profile;
      CREATE SCHEMA enriched_event ( tenant STRING, seq I64, tier STRING );
      CREATE WIRE JSON SCHEMA enriched_wire MODE STRICT ( tenant string, seq integer, tier string );
      CREATE WIRE JSON CODEC enriched_codec FROM WIRE SCHEMA enriched_wire TO SCHEMA enriched_event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY profiles SCHEMA profile BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY enriched_events SCHEMA enriched_event BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT profile_ingress ON edge PATH '/profiles';
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR profile_ingestor FROM profile_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING profile_codec TIMESTAMP NOW
        TO profiles INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION enrich FROM unique_events BRANCHED BY by_tenant
        USING MATERIALIZED STATE profiles REQUIRED SKIP
        TO enriched_events INHERIT ALL SET tier = relay_state.profiles.tier
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE ZEROMQ CLIENT repartition_out
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
      CREATE ZEROMQ PUSH EMITTER enriched_out
        FROM enriched_events TO repartition_out MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING enriched_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"eta","tier":"gold"}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"gamma","tier":"silver"}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"beta","tier":"bronze"}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"lambda","tier":"gold"}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"xi","tier":"silver"}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"kappa","tier":"bronze"}
      """
    Then within "30s" node "<reader>" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"kappa"},"fields":{"tenant":"kappa","tier":"bronze"}}
      """
    And within "30s" node "<reader>" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"xi"},"fields":{"tenant":"xi","tier":"silver"}}
      """
    When http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"gamma","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1 | "tier":"gold"
      "tenant":"gamma" | "seq":1 | "tier":"silver"
      "tenant":"beta" | "seq":1 | "tier":"bronze"
      "tenant":"lambda" | "seq":1 | "tier":"gold"
      "tenant":"xi" | "seq":1 | "tier":"silver"
      "tenant":"kappa" | "seq":1 | "tier":"bronze"
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "<reader>" contains
      """
      partitions: 8
      partition epoch: 2
      previous partitions: 4
      """
    When these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 8 partitions of relay "events"
    When http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"gamma","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":3}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":3}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":2 | "tier":"gold"
      "tenant":"gamma" | "seq":2 | "tier":"silver"
      "tenant":"beta" | "seq":2 | "tier":"bronze"
      "tenant":"beta" | "seq":3 | "tier":"bronze"
      "tenant":"lambda" | "seq":2 | "tier":"gold"
      "tenant":"xi" | "seq":2 | "tier":"silver"
      "tenant":"xi" | "seq":3 | "tier":"silver"
      "tenant":"kappa" | "seq":2 | "tier":"bronze"
      """
    And the observed broker does not receive a payload within "2s"
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "<reader>" reports no partition transition
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "<reader>" contains
      """
      partitions: 8
      partition epoch: 2
      """
    When http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":3}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":4}
      """
    And http payload is posted to node "node-1" with host "repartition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":4}
      """
    Then within "30s" the observed broker receives payloads keeping the order of each key
      """
      "tenant":"beta" | "seq":4 | "tier":"bronze"
      "tenant":"xi" | "seq":4 | "tier":"silver"
      """
    And the observed broker does not receive a payload within "2s"

    Examples:
      | cluster_size | replica_count | reader |
      | 1            | 0             | node-1 |
      | 3            | 0             | node-2 |
      | 3            | 1             | node-3 |

  Scenario Outline: Growing a running branch continues each moved key's window and WASM guest state
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the active domain is "{{domain}}"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE wasm_counter;
      UPLOAD RESOURCE wasm_counter VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE WIRE JSON CODEC metric_codec FROM WIRE SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA counted ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE SCHEMA summary (
        tenant STRING,
        samples I64,
        first_value I32 OPTIONAL,
        last_value I32 OPTIONAL
      );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY counted_metrics SCHEMA counted BRANCHED BY by_tenant;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT metric_ingress ON edge PATH '/metrics';
      CREATE HTTP ENDPOINT INGESTOR metric_ingestor FROM metric_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR pair_window FROM metrics
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        BRANCHED BY by_tenant
        TO summaries
          SET tenant = FIRST(input.tenant), samples = COUNT(input.value),
              first_value = FIRST(input.value), last_value = LAST(input.value)
          ON MESSAGE ERROR LOG;
      CREATE WASM PROCESSOR even_rows FROM metrics
        USING RESOURCE wasm_counter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO counted_metrics
        SET tenant = branch.tenant, note = coalesce(note, "even")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION summaries_seen TO summaries;
      CREATE SUBSCRIPTION counted_seen TO counted_metrics;
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"eta","value":10}
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","value":11}
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"lambda","value":12}
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"kappa","value":13}
      """
    Then the relay subscription does not receive a payload within "3s"
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    # The change may run in another node's session than the subscriptions, which replaces theirs.
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION summaries_after_change TO summaries;
      CREATE SUBSCRIPTION counted_after_change TO counted_metrics;
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"eta","value":70}
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","value":71}
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"lambda","value":72}
      """
    And http payload is posted to node "node-1" with host "repartition-state-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"kappa","value":73}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"eta" | "samples":2 | "first_value":10 | "last_value":70
      "tenant":"beta" | "samples":2 | "first_value":11 | "last_value":71
      "tenant":"lambda" | "samples":2 | "first_value":12 | "last_value":72
      "tenant":"kappa" | "samples":2 | "first_value":13 | "last_value":73
      "tenant":"eta" | "note":"even"
      "tenant":"beta" | "note":"even"
      "tenant":"lambda" | "note":"even"
      "tenant":"kappa" | "note":"even"
      """
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "node-1" reports no partition transition

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Shrinking a running branch claims every moved key from the partitions it drops and then removes them
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 8;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-shrink-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION unique_seen TO unique_events;
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"eta" | "seq":1
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      "tenant":"xi" | "seq":1
      "tenant":"kappa" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 4;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "node-1" contains
      """
      partitions: 4
      partition epoch: 2
      previous partitions: 8
      """
    When these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 partitions of relay "events"
    And the last cluster status schedules 8 partitions of deduplicator "distinct_events"
    # The status ran through its own client session, which replaced the subscription's.
    When these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION unique_after_shrink TO unique_events;
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":2
      "tenant":"xi" | "seq":2
      "tenant":"kappa" | "seq":2
      """
    And the relay subscription does not receive a payload within "2s"
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "node-1" reports no partition transition
    When these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 partitions of relay "events"
    And the last cluster status schedules 4 partitions of deduplicator "distinct_events"
    When these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION unique_after_retirement TO unique_events;
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":3}
      """
    And http payload is posted to node "node-1" with host "repartition-shrink-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":3}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "seq":3
      "tenant":"kappa" | "seq":3
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A moved key that never reappears expires in its old partition and the epoch retires
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5s PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-expiry-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION unique_seen TO unique_events;
      """
    And http payload is posted to node "node-1" with host "repartition-expiry-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"xi" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "node-1" contains
      """
      previous partitions: 4
      """
    And within "90s" DESCRIBE BRANCH "by_tenant" on node "node-1" reports no partition transition
    When http payload is posted to node "node-1" with host "repartition-expiry-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"xi" | "seq":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A second partition count change waits until the previous change has moved its keys
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-twice-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION unique_seen TO unique_events;
      """
    And http payload is posted to node "node-1" with host "repartition-twice-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"kappa" | "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    And these Plexus commands fail with "is still moving keys placed under its previous partition count"
      """
      ALTER BRANCH by_tenant SET PARTITIONS 16;
      """
    When these Plexus commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION unique_after_refusal TO unique_events;
      """
    And http payload is posted to node "node-1" with host "repartition-twice-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"kappa" | "seq":2
      """
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "node-1" reports no partition transition
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 16;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "node-1" contains
      """
      partitions: 16
      partition epoch: 3
      """

  Scenario: A claim whose holding partition lost its owner follows forced recovery and keeps the key's replicated state
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
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-recovery-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When these Plexus commands are executed on node "{{leader}}"
      """
      CREATE SUBSCRIPTION unique_seen TO unique_events;
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      "tenant":"xi" | "seq":1
      "tenant":"kappa" | "seq":1
      """
    # The deduplicator publishes its keys periodically. Allow the 100ms publication and the
    # replica's one-second catch-up rounds before the change, as in branch_replica_catch_up.feature.
    When physical time passes for "3s"
    And these Plexus commands are executed on node "{{leader}}"
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    And these Plexus commands are executed through the client on node "{{leader}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the owner of every partition of deduplicator "distinct_events" is saved with placeholder prefix "owner_before"
    And a partition owner of deduplicator "distinct_events" other than placeholder "leader" is saved as placeholder "lost_owner"
    When node "{{lost_owner}}" is stopped
    Then within "90s" node "{{leader}}" eventually reports deduplicator "distinct_events" partitions moved from node "{{lost_owner}}" and all other owners unchanged from placeholder prefix "owner_before"
    # The status ran through its own client session, which replaced the subscription's.
    When these Plexus commands are executed on node "{{leader}}"
      """
      CREATE SUBSCRIPTION unique_after_recovery TO unique_events;
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "repartition-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":2}
      """
    Then within "90s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":2
      "tenant":"xi" | "seq":2
      "tenant":"kappa" | "seq":2
      """
    And the relay subscription does not receive a payload within "2s"
    And within "90s" DESCRIBE BRANCH "by_tenant" on node "{{leader}}" reports no partition transition

  @long_running
  Scenario Outline: A backup cut while keys are still moving restores every key into its partition under the new count
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA metric ( tenant STRING, latency I64 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( tenant string, latency integer );
      CREATE WIRE JSON CODEC metric_codec FROM WIRE SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA summary (
        tenant STRING,
        samples I64,
        first_latency I64 OPTIONAL,
        last_latency I64 OPTIONAL
      );
      CREATE SCHEMA profile ( tenant STRING, tier STRING );
      CREATE WIRE JSON SCHEMA profile_wire MODE STRICT ( tenant string, tier string );
      CREATE WIRE JSON CODEC profile_codec FROM WIRE SCHEMA profile_wire TO SCHEMA profile;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m PARTITIONS 4;
      CREATE RELAY profiles SCHEMA profile BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-backup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT metric_ingress ON edge PATH '/metrics';
      CREATE HTTP ENDPOINT profile_ingress ON edge PATH '/profiles';
      CREATE HTTP ENDPOINT INGESTOR profile_ingestor FROM profile_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING profile_codec TIMESTAMP NOW
        TO profiles INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR metric_ingestor FROM metric_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 30m BRANCHED BY by_tenant
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR pair_window FROM metrics
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        BRANCHED BY by_tenant
        TO summaries
          SET tenant = FIRST(input.tenant), samples = COUNT(input.latency),
              first_latency = FIRST(input.latency), last_latency = LAST(input.latency)
          ON MESSAGE ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION source_unique TO unique_events;
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"eta" | "seq":1
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      "tenant":"xi" | "seq":1
      "tenant":"kappa" | "seq":1
      """
    When http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"eta","tier":"gold"}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"beta","tier":"bronze"}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"kappa","tier":"silver"}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"eta"},"fields":{"tenant":"eta","tier":"gold"}}
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"bronze"}}
      {"branch":{"tenant":"kappa"},"fields":{"tenant":"kappa","tier":"silver"}}
      """
    When http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":10}
      """
    And http payload is posted to host "repartition-backup-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"kappa","latency":20}
      """
    And these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "node-1" contains
      """
      partitions: 8
      previous partitions: 4
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "repartition.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --resume" from "repartition.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 26 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And the CLI restore reports no warnings
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "{{leader}}" contains
      """
      partitions: 8
      partition epoch: 1
      """
    And within "30s" node "restored-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"eta"},"fields":{"tenant":"eta","tier":"gold"}}
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"bronze"}}
      {"branch":{"tenant":"kappa"},"fields":{"tenant":"kappa","tier":"silver"}}
      """
    And within "60s" node "restored-1" accepts http payload for host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"probe","seq":1}
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_unique TO unique_events;
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"xi","seq":1}
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":1}
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"kappa","seq":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "seq":2
      "tenant":"kappa" | "seq":2
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_summaries TO summaries;
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":70}
      """
    And http payload is posted to node "restored-1" with host "repartition-backup-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"kappa","latency":80}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "samples":2 | "first_latency":10 | "last_latency":70
      "tenant":"kappa" | "samples":2 | "first_latency":20 | "last_latency":80
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Growing a running branch continues each moved key's inference state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-infer-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores TO scored;
      START;
      """
    And http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"eta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"eta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "score":[11.0]
      key={"tenant":"eta"} | "score":[1100.0]
      key={"tenant":"beta"} | "score":[33.0]
      key={"tenant":"eta"} | "score":[3300.0]
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS 8;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "node-1" contains
      """
      partitions: 8
      partition epoch: 2
      previous partitions: 4
      """
    # The leader's session keeps the subscription, since the change ran through it.
    When http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[3.0,30.0]}
      """
    And http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"eta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "score":[66.0]
      key={"tenant":"eta"} | "score":[6600.0]
      """
    And within "60s" DESCRIBE BRANCH "by_tenant" on node "node-1" reports no partition transition
    When http payload is posted to node "node-1" with host "repartition-infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[4.0,40.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "score":[110.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  # beta lies in partition 1 of four and 5 of eight, and eta in partition 0 of both. No record of
  # beta reaches its new partition before the cut, so the archive holds beta's inference state in
  # the partition the previous count gave it: under four for a grow, in a holding partition the new
  # count drops for a shrink.
  @long_running
  Scenario Outline: A backup cut before a moved key's inference state is claimed restores the state in the key's partition under the new count
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30m PARTITIONS <from>;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge repartition-infer-backup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    And http payload is posted to node "node-1" with host "repartition-infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "repartition-infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"eta","vector":[100.0,1000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "score":[11.0]
      key={"tenant":"eta"} | "score":[1100.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      ALTER BRANCH by_tenant SET PARTITIONS <to>;
      """
    Then within "30s" DESCRIBE BRANCH "by_tenant" on node "node-1" contains
      """
      partitions: <to>
      previous partitions: <from>
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "repartition-inference.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "repartition-inference.nvxb" as json
    Then the described backup has exactly 2 "inferencer" state sections
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    And the restore coordinator uses remote placement in a multi-node cluster
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume" from "repartition-inference.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 1 resource versions and 12 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And the CLI restore reports no warnings
    And within "30s" DESCRIBE BRANCH "by_tenant" on node "{{leader}}" contains
      """
      partitions: <to>
      partition epoch: 1
      """
    And within "60s" node "restored-1" accepts http payload for host "repartition-infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"probe","vector":[0.0,0.0]}
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "restored-1" with host "repartition-infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[2.0,20.0]}
      """
    And http payload is posted to node "restored-1" with host "repartition-infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"eta","vector":[200.0,2000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "score":[33.0]
      key={"tenant":"eta"} | "score":[3300.0]
      """

    Examples:
      | cluster_size | replica_count | from | to | input_type    | output_type   | tensor_type       |
      | 1            | 0             | 4    | 8  | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | 4    | 8  | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 1            | 0             | 8    | 4  | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | 8    | 4  | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
