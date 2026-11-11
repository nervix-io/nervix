@processor_partitions
Feature: Partitioned processors and generators
  Processors keep one supervisor and branch registry per partition. The partition's state and
  materialized reads follow that partition's owner.

  Scenario Outline: A deduplicator runs each branch partition on its scheduled owner
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64, amount I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT
        (tenant string, seq integer, amount integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge partition-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR distinct_events FROM events
        DEDUPLICATE ON input.seq MAX TIME 10m BRANCHED BY tenants
        TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed TO unique_events;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      kind=deduplicator name=distinct_events unit=partition:0
      """
    And the last cluster status lists 2 partitions of deduplicator "distinct_events" on <owners> distinct owners
    And the last cluster status colocates every partition of deduplicator "distinct_events" with the corresponding partition of relay "events"
    When http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":10}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":20}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":10,"seq":1,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":20,"seq":1,"tenant":"acme"}
      """
    When these Plexus commands are executed through the client on the leader node
      """
      DESCRIBE DEDUPLICATOR distinct_events;
      """
    Then the last command output contains
      """
      messages_total received relay=events partition=0
      """
    And the last command output contains
      """
      messages_total received relay=events partition=1
      """
    When the cluster is restarted
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observed_after_restart TO unique_events;
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":11}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":21}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2,"amount":12}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2,"amount":22}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":12,"seq":2,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":22,"seq":2,"tenant":"acme"}
      """

    When these Plexus commands are executed through the client on the leader node
      """
      RELOCATE DEDUPLICATOR distinct_events ONTO NODE <destination> IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of deduplicator "distinct_events" on 1 distinct owners
    When http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2,"amount":13}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2,"amount":23}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":3,"amount":14}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":3,"amount":24}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"eta"} | "tenant":"eta" | "seq":3 | "amount":14
      key={"tenant":"acme"} | "tenant":"acme" | "seq":3 | "amount":24
      """

    Examples:
      | cluster_size | owners | destination |
      | 1            | 1      | node-1      |
      | 3            | 2      | node-3      |

  Scenario Outline: A generator reads and emits only the keys of its own partition
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64, amount I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT
        (tenant string, seq integer, amount integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generated_events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge partition-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec TIMESTAMP NOW
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE GENERATOR sampled_events USING MATERIALIZED STATE events EACH 100ms
        BRANCHED BY tenants
        TO generated_events SET tenant = branch.tenant,
          seq = relay_state.events.seq, amount = relay_state.events.amount
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed TO generated_events;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of generator "sampled_events" on <owners> distinct owners
    And the last cluster status colocates every partition of generator "sampled_events" with the corresponding partition of relay "events"
    When http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":10}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":20}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":10,"seq":1,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":20,"seq":1,"tenant":"acme"}
      """

    When these Plexus commands are executed through the client on the leader node
      """
      RELOCATE GENERATOR sampled_events ONTO NODE <destination> IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of generator "sampled_events" on 1 distinct owners
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION observed_after_move TO generated_events;
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2,"amount":12}
      """
    And http payload is posted to node "node-1" with host "partition-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2,"amount":22}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"eta"} | "tenant":"eta" | "seq":2 | "amount":12
      key={"tenant":"acme"} | "tenant":"acme" | "seq":2 | "amount":22
      """

    Examples:
      | cluster_size | owners | destination |
      | 1            | 1      | node-1      |
      | 3            | 2      | node-3      |

  Scenario Outline: A partitioned window restores each units incomplete window after cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );
        CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64
      );
        CREATE WIRE JSON SCHEMA metric_wire MODE STRICT (
        tenant string,
        latency integer
      );
        CREATE WIRE JSON CODEC metric_codec
        FROM WIRE SCHEMA metric_wire
        TO SCHEMA metric;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_metric_ingestor SCHEMA tenant_branch TTL 5m PARTITIONS 2;
        CREATE RELAY metrics SCHEMA metric BRANCHED BY by_metric_ingestor;
        CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_metric_ingestor;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE HTTP ENDPOINT ingress ON edge PATH '/metrics';
        CREATE HTTP ENDPOINT INGESTOR metric_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics
        INHERIT ALL
        BRANCHED BY by_metric_ingestor
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE WINDOW PROCESSOR restart_latency FROM metrics
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        BRANCHED BY by_metric_ingestor
        TO metric_summaries
        SET tenant = FIRST(input.tenant), sample_count = COUNT(input.latency), total_latency = SUM(input.latency)
        ON MESSAGE ERROR LOG;
        START;
      """
    When these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of window_processor "restart_latency" on <owners> distinct owners
    And the last cluster status colocates every partition of window_processor "restart_latency" with the corresponding partition of relay "metrics"
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"eta","latency":100}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":20}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"eta","latency":200}
      """
    Then within "5s" DESCRIBE DOMAIN section "processed" metric "messages_total" "received" relay "metrics" across physical nodes totals 4
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION metric_summaries_subscription TO metric_summaries;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"eta","latency":300}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "sample_count":3 | "total_latency":60
      key={"tenant":"eta"} | "tenant":"eta" | "sample_count":3 | "total_latency":600
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count | owners |
      | 1            | 0             | 1      |
      | 3            | 0             | 2      |
      | 3            | 1             | 2      |


  @execution_resource_plan
  Scenario Outline: A partitioned WASM processor resets only the selected units branch lifetime
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
    When these Plexus commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'eta' };
      """
    Then within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: READY, branch, generation 2
      state reset reason: TRANSACTION
      state reset readiness: READY
      """
    And within "10s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      stage=REPLICA_CONFIRMED required_replicas=<replica_count> confirmed_replicas=<replica_count>
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","sequence":2}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "note":"even"
      """
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","sequence":3}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"eta"} | "tenant":"eta" | "note":"even"
      """

    When these Plexus commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'acme' };
      """
    Then within "10s" DESCRIBE WASM PROCESSOR "counting_guest" on the leader node contains
      """
      state reset: READY, branch, generation 2
      state reset reason: TRANSACTION
      state reset readiness: READY
      """
    And within "10s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      stage=REPLICA_CONFIRMED required_replicas=<replica_count> confirmed_replicas=<replica_count>
      """

    Examples:
      | cluster_size | replica_count | owners |
      | 1            | 0             | 1      |
      | 3            | 1             | 2      |


  Scenario Outline: A partitioned correlator matches interleaved keys only inside their partition
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64, amount I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer, amount integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY left_events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY right_events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY joined_events SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge correlation-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT left_ingress ON edge PATH '/left';
      CREATE HTTP ENDPOINT right_ingress ON edge PATH '/right';
      CREATE HTTP ENDPOINT INGESTOR left_ingestor FROM left_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO left_events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR right_ingestor FROM right_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO right_events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CORRELATOR join_events LEFT FROM left_events RIGHT FROM right_events
        CORRELATE WHERE left.seq = right.seq MATCH EARLIEST MAX TIME 5m
        ON CORRELATION TIMEOUT DROP, DROP BRANCHED BY tenants
        TO joined_events SET tenant = left.tenant, seq = left.seq, amount = left.amount + right.amount
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed TO joined_events;
      START;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of correlator "join_events" on <owners> distinct owners
    When http payload is posted to node "node-1" with host "correlation-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"eta","seq":1,"amount":10}
      """
    And http payload is posted to node "node-1" with host "correlation-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"acme","seq":1,"amount":200}
      """
    And http payload is posted to node "node-1" with host "correlation-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","seq":1,"amount":20}
      """
    And http payload is posted to node "node-1" with host "correlation-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"eta","seq":1,"amount":100}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"eta"} | "tenant":"eta" | "seq":1 | "amount":110
      key={"tenant":"acme"} | "tenant":"acme" | "seq":1 | "amount":220
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size | owners |
      | 1            | 1      |
      | 3            | 2      |

  Scenario Outline: Partitioned dependency reads resolve each keys materialized owner locally and remotely
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64, amount I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer, amount integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m PARTITIONS 2;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY preferences SCHEMA event BRANCHED BY tenants
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY enriched SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge partition-read-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT state_ingress ON edge PATH '/preferences';
      CREATE HTTP ENDPOINT INGESTOR state_ingestor FROM state_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec TIMESTAMP NOW
        TO preferences INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION enrich FROM events BRANCHED BY tenants
        USING MATERIALIZED STATE preferences REQUIRED WAIT
        TO enriched INHERIT ALL SET amount = relay_state.preferences.amount
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed TO enriched;
      START;
      """
    And http payload is posted to node "node-1" with host "partition-read-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":1,"amount":10}
      """
    And http payload is posted to node "node-1" with host "partition-read-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1,"amount":20}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "partition-read-{{test_id}}.example.com" path "/preferences"
      """
      {"tenant":"eta","seq":1,"amount":100}
      """
    And http payload is posted to node "node-1" with host "partition-read-{{test_id}}.example.com" path "/preferences"
      """
      {"tenant":"acme","seq":1,"amount":200}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":100,"seq":1,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":200,"seq":1,"tenant":"acme"}
      """
    When these Plexus commands are executed through the client on the leader node
      """
      RELOCATE JUNCTION enrich ONTO NODE <destination> IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 partitions of junction "enrich" on 1 distinct owners
    When http payload is posted to node "node-1" with host "partition-read-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":2,"amount":11}
      """
    And http payload is posted to node "node-1" with host "partition-read-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":2,"amount":21}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"eta"} payload={"amount":100,"seq":2,"tenant":"eta"}
      key={"tenant":"acme"} payload={"amount":200,"seq":2,"tenant":"acme"}
      """

    Examples:
      | cluster_size | destination |
      | 1            | node-1      |
      | 3            | node-3      |

