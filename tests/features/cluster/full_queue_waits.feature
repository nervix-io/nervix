@full_queue_waits
Feature: Work no caller waits on waits for a place in a full worker class
  A worker class whose workers are all busy and whose wait queue is full refuses only a request
  whose caller still holds it. Work the node or a peer already accepted, which no caller waits to
  be answered about, waits for a place instead: relay slices and subscription deliveries on the
  data workers, checkpoints, state captures, partition moves and the compilation of a committed
  change on the bulk workers, and checkpoint and resource files on the filesystem workers. Each
  scenario brings that work up to its full class, sees a job of the class wait for a place while
  no node refuses one, releases the class and sees the work complete once.

  # A batch the receiving node's relay bound cannot take whole is sent again as slices, and every
  # slice is encoded again on the sending node's data workers. Slicing exists only between nodes.
  @full_queue_relay_slicing
  Scenario: A relay slice the sending node's data workers cannot take now waits for a place and arrives once
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-2 starts with interconnect-relay-batch-bytes = 2KiB
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY records SCHEMA incoming BRANCHED BY tenants;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA incoming MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND TIMESTAMP NOW
        TO records INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH EACH 250ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER output FROM records
        TO SCHEMA incoming MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE INGESTOR input ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY records ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER output ONTO NODE node-3 IGNORE PREFERENCES;
      """
    Then node "node-1" eventually describes node "node-2" accepting relay bodies of "2KiB"
    Given client "app" is connected to node "node-3"
    When client "app" opens consumer "output" on emitter "output" expecting fields "id STRING, amount I64, tenant STRING"
    And client "app" opens producer "input" on ingestor "input" expecting fields "id STRING, amount I64, tenant STRING"
    Given relay payloads for domain "{{domain}}" are held before their grant requests
    When producer "input" submits 8 interleaved rows with 512-byte ids as batch "sliced"
    Then a relay payload for domain "{{domain}}" is waiting before its grant request
    When data execution on node "node-1" is saturated
    And relay payload grant requests for domain "{{domain}}" are released
    Then within "30s" a data job waits for a place on node "node-1"
    And batch "sliced" remains pending for "200ms"
    When data execution is released on every node
    Then consumer "output" receives 8 interleaved rows with 512-byte ids exactly once and acknowledges them
    And batch "sliced" completes
    And relay payloads to node "node-2" in domain "{{domain}}" respect "2KiB" with largest slice 2
    And no node refused a data job while it was saturated

  # A relay's owner encodes a batch once, on its data workers, for every session on another node
  # that subscribes to the relay. A remote subscriber exists only between nodes.
  @full_queue_subscription_fanout
  Scenario: A subscription delivery the relay owner's data workers cannot take now waits for a place and reaches a session on another node once
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading (id STRING, value I64);
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (id string, value integer);
      CREATE WIRE JSON CODEC reading_codec FROM WIRE SCHEMA reading_wire TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE VHOST edge fanout-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING reading_codec
        TO readings INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY readings ONTO NODE node-1 IGNORE PREFERENCES;
      """
    When these Plexus commands are executed on node "node-3"
      """
      CREATE SUBSCRIPTION remote_readings TO readings;
      """
    And within "30s" the relay subscription receives a payload from repeated http posts to node "node-1" with host "fanout-{{test_id}}.example.com" path "/readings"
      """
      {"id":"warm","value":0}
      """
    Given relay owner fan-out for domain "{{domain}}" is paused before dispatch
    When http payload is posted to node "node-1" with host "fanout-{{test_id}}.example.com" path "/readings"
      """
      {"id":"held","value":1}
      """
    Then the relay owner fan-out pause for domain "{{domain}}" is reached
    When data execution on node "node-1" is saturated
    And the relay owner fan-out pause for domain "{{domain}}" is released
    Then within "30s" a data job waits for a place on node "node-1"
    When data execution is released on every node
    Then within "30s" the relay subscription receives a payload
      """
      "id":"held"
      """
    When http payload is posted to node "node-1" with host "fanout-{{test_id}}.example.com" path "/readings"
      """
      {"id":"barrier","value":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"barrier" | "value":2
      """
    And no node refused a data job while it was saturated

  # A STOP seals the final checkpoint of every state owner on the bulk workers before it answers.
  # With the periodic snapshot ten minutes away, that final checkpoint is the only state the
  # domain's next START restores.
  @full_queue_stop_checkpoint
  Scenario Outline: A STOP waits for a bulk place to seal a window's final checkpoint on a <cluster_size> node cluster
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading ( value I64 );
      CREATE SCHEMA reading_summary ( samples I64, total I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC reading_codec FROM WIRE SCHEMA reading_wire TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY reading_summaries SCHEMA reading_summary UNBRANCHED;
      CREATE VHOST edge stop-checkpoint-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING reading_codec
        TO readings INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR sliding_readings FROM readings
        WIDTH 2 MESSAGES STEP 1 MESSAGES UNBRANCHED
        TO reading_summaries SET samples = COUNT(input.value), total = SUM(input.value)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION reading_summaries_subscription TO reading_summaries;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "window_processor" "sliding_readings" is saved as placeholder "window_owner"
    And node "node-1" eventually accepts http traffic for host "stop-checkpoint-{{test_id}}.example.com" path "/readings"
      """
      {"value":10}
      """
    When http payload is posted to node "node-1" with host "stop-checkpoint-{{test_id}}.example.com" path "/readings"
      """
      {"value":20}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "samples":2,"total":30
      """
    When bulk execution is saturated on every node
    And these Plexus commands begin executing in the background
      """
      STOP;
      """
    Then within "30s" a bulk job waits for a place on node "{{window_owner}}"
    And the background Plexus execution is still running after "1s"
    When bulk execution is released on every node
    Then the background Plexus execution succeeds
    And no node refused a bulk job while it was saturated
    When these Plexus commands are executed on the leader node
      """
      START;
      CREATE SUBSCRIPTION restarted_summaries_subscription TO reading_summaries;
      """
    Then node "node-1" eventually accepts http traffic for host "stop-checkpoint-{{test_id}}.example.com" path "/readings"
      """
      {"value":30}
      """
    And within "30s" the relay subscription receives a payload
      """
      "samples":2,"total":50
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A materialized relay's checkpoint writer seals its rows into snapshot staging files and
  # publishes them to the owner's state store, on the filesystem workers, at each snapshot interval
  # its state changed in. The node's own maintenance also waits in a full filesystem class, so the
  # checkpoint is held before it seals, and the job it adds once let go is the one that must wait.
  # A refused checkpoint would be tried again later, so the refusal count tells waiting from
  # refusing; a restarted node reads the rows back.
  @full_queue_checkpoint_files
  Scenario Outline: A relay's checkpoint waits for filesystem places and a restarted <cluster_size> node cluster reads its rows back
    Given runtime replication is configured with replica count 0 and snapshot interval "200ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA profile ( tenant STRING, tier STRING );
      CREATE WIRE JSON SCHEMA profile_wire MODE STRICT ( tenant string, tier string );
      CREATE WIRE JSON CODEC profile_codec FROM WIRE SCHEMA profile_wire TO SCHEMA profile;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY profiles SCHEMA profile BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge checkpoint-files-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT profile_ingress ON edge PATH '/profiles';
      CREATE HTTP ENDPOINT INGESTOR profile_ingestor FROM profile_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING profile_codec TIMESTAMP NOW
        TO profiles INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "relay" "profiles" is saved as placeholder "relay_owner"
    And node "node-1" eventually accepts http traffic for host "checkpoint-files-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"alpha","tier":"gold"}
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","tier":"gold"}}
      """
    Given the next checkpoint of relay "profiles" pauses before it seals its rows
    When http payload is posted to node "node-1" with host "checkpoint-files-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"beta","tier":"silver"}
      """
    Then the checkpoint of relay "profiles" reaches its pause
    When filesystem storage execution is saturated on every node
    Then within "30s" a filesystem storage job waits for a place on node "{{relay_owner}}"
    When the checkpoint of relay "profiles" is released from its pause
    Then within "30s" another filesystem storage job waits for a place on node "{{relay_owner}}"
    When filesystem storage execution is released on every node
    Then no node refused a filesystem storage job while it was saturated
    And within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"silver"}}
      """
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    And within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","tier":"gold"}}
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"silver"}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A planned move captures the unit's state on its source node, on the bulk workers, before the
  # destination installs it. A move needs a second node.
  @full_queue_handoff_capture
  Scenario: A relay's move waits for a bulk place on its source to capture its materialized state and keeps every row
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "10m"
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA profile ( tenant STRING, tier STRING );
      CREATE WIRE JSON SCHEMA profile_wire MODE STRICT ( tenant string, tier string );
      CREATE WIRE JSON CODEC profile_codec FROM WIRE SCHEMA profile_wire TO SCHEMA profile;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY profiles SCHEMA profile BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge handoff-capture-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT profile_ingress ON edge PATH '/profiles';
      CREATE HTTP ENDPOINT INGESTOR profile_ingestor FROM profile_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING profile_codec TIMESTAMP NOW
        TO profiles INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY profiles ONTO NODE node-1 IGNORE PREFERENCES;
      """
    When http payload is posted to node "node-1" with host "handoff-capture-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"alpha","tier":"gold"}
      """
    And http payload is posted to node "node-1" with host "handoff-capture-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"beta","tier":"silver"}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","tier":"gold"}}
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"silver"}}
      """
    When bulk execution on node "node-1" is saturated
    And these Plexus commands begin executing in the background
      """
      RELOCATE RELAY profiles ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then within "30s" a bulk job waits for a place on node "node-1"
    And the background Plexus execution is still running after "1s"
    When bulk execution is released on every node
    Then the background Plexus execution succeeds
    And no node refused a bulk job while it was saturated
    And within "30s" node "node-2" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"alpha"},"fields":{"tenant":"alpha","tier":"gold"}}
      """
    And within "30s" node "node-2" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"silver"}}
      """

  # Under four partitions beta lies in partition 1, and under eight in partition 5, whose unit
  # claims beta's profile from the unit that holds it as soon as the new count applies. The holder
  # encodes the row it releases on the bulk workers, and the claimant decodes it on them.
  @full_queue_partition_move
  Scenario Outline: A moved key's materialized row waits for a bulk place at its <side> and moves once on a <cluster_size> node cluster
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA profile ( tenant STRING, tier STRING );
      CREATE WIRE JSON SCHEMA profile_wire MODE STRICT ( tenant string, tier string );
      CREATE WIRE JSON CODEC profile_codec FROM WIRE SCHEMA profile_wire TO SCHEMA profile;
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA enriched_event ( tenant STRING, seq I64, tier STRING );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 5m PARTITIONS 4;
      CREATE RELAY profiles SCHEMA profile BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY enriched_events SCHEMA enriched_event BRANCHED BY by_tenant;
      CREATE VHOST edge partition-move-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT profile_ingress ON edge PATH '/profiles';
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR profile_ingestor FROM profile_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING profile_codec TIMESTAMP NOW
        TO profiles INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR event_ingestor FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION enrich FROM events BRANCHED BY by_tenant
        USING MATERIALIZED STATE profiles REQUIRED SKIP
        TO enriched_events INHERIT ALL SET tier = relay_state.profiles.tier
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION enriched_subscription TO enriched_events;
      START;
      """
    When http payload is posted to node "node-1" with host "partition-move-{{test_id}}.example.com" path "/profiles"
      """
      {"tenant":"beta","tier":"bronze"}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "profiles" containing
      """
      {"branch":{"tenant":"beta"},"fields":{"tenant":"beta","tier":"bronze"}}
      """
    Given the <side> of a moved row of relay "profiles" pauses before handling it
    When these Plexus commands are executed on the leader node
      """
      ALTER PARAMETRIZER by_tenant SET PARTITIONS 8;
      CREATE SUBSCRIPTION repartitioned_subscription TO enriched_events;
      """
    Then the <side> of a moved row of relay "profiles" reaches its pause
    When bulk execution is saturated on every node
    And the <side> of a moved row of relay "profiles" is released from its pause
    Then within "30s" a bulk job waits for a place on some node
    When bulk execution is released on every node
    And http payload is posted to node "node-1" with host "partition-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"beta" | "seq":1 | "tier":"bronze"
      """
    And no node refused a bulk job while it was saturated

    Examples:
      | cluster_size | side     |
      | 1            | holder   |
      | 3            | holder   |
      | 1            | claimant |
      | 3            | claimant |

  # A committed change compiles each user-defined function it adds on the bulk workers. The change
  # is already decided, so a full bulk class holds the compilation back rather than refusing it.
  @full_queue_committed_compilation
  Scenario Outline: A committed CREATE UDF waits for a bulk place to compile and its function runs once on a <cluster_size> node cluster
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string, value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY accepted SCHEMA event UNBRANCHED;
      CREATE RELAY delivered SCHEMA event UNBRANCHED;
      CREATE VHOST edge compile-udf-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO accepted INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR IGNORE;
      CREATE SUBSCRIPTION output TO delivered;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When bulk execution is saturated on every node
    And these Plexus commands begin executing in the background
      """
      CREATE UDF add_one WITH ROTO_0_13 ARGS (value I64) RETURNS I64
        CODE $roto$ fn add_one(value: I64Column) -> I64Column { value.add_s(1) } $roto$;
      CREATE JUNCTION transform FROM accepted UNBRANCHED
        TO delivered SET id = input.id, value = udf::add_one(input.value)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then within "30s" a bulk job waits for a place on node "{{leader}}"
    And the background Plexus execution is still running after "1s"
    When bulk execution is released on every node
    Then the background Plexus execution succeeds
    And no node refused a bulk job while it was saturated
    When http payload is posted to node "node-1" with host "compile-udf-{{test_id}}.example.com" path "/events"
      """
      {"id":"compiled","value":41}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"compiled" | "value":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A committed change also compiles each WASM module a processor it adds runs, on the bulk
  # workers of every node that installs the processor.
  @full_queue_committed_compilation
  Scenario Outline: A committed CREATE WASM PROCESSOR waits for a bulk place to compile its module and filters once on a <cluster_size> node cluster
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE WIRE JSON CODEC metric_codec FROM WIRE SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE VHOST edge compile-wasm-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/metrics';
      CREATE HTTP ENDPOINT INGESTOR metric_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING metric_codec
        TO raw_metrics INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When bulk execution is saturated on every node
    And these Plexus commands begin executing in the background
      """
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      """
    Then within "30s" a bulk job waits for a place on node "{{leader}}"
    And the background Plexus execution is still running after "1s"
    When bulk execution is released on every node
    Then the background Plexus execution succeeds
    And no node refused a bulk job while it was saturated
    When http payload is posted to node "node-1" with host "compile-wasm-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"alpha"}
      """
    And http payload is posted to node "node-1" with host "compile-wasm-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Every job of a node's resource store waits for a place on the filesystem workers: the
  # installation of an upload on the node that receives it, and on three nodes its replication to
  # each peer, which the upload waits for. The node's own maintenance also waits in a full
  # filesystem class, so the upload must add a waiting job of its own.
  @full_queue_resource_store
  Scenario Outline: A resource upload waits for filesystem places and is installed once on every node of a <cluster_size> node cluster
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}",
        "assets/lookup.csv": "id,name\n1,Alice\n2,Bob\n"
      }
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And filesystem storage execution is saturated on every node
    Then within "30s" a filesystem storage job waits for a place on node "{{leader}}"
    When client "uploader" begins uploading resource "proto" from "{{proto_dir}}" with identity "full-store" in the background
    Then within "30s" another filesystem storage job waits for a place on node "{{leader}}"
    And the background Plexus execution is still running after "1s"
    When filesystem storage execution is released on every node
    Then the background Plexus execution succeeds
    And no node refused a filesystem storage job while it was saturated
    When these Plexus commands are executed on node "node-1"
      """
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      versions: 1
      """
    When these Plexus commands are executed on node "node-1"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    Then the last command output contains
      """
      - node-1 topology=alive state=ready
      """
    And the last command output contains
      """
      <peers_ready>
      """

    Examples:
      | cluster_size | peers_ready                         |
      | 1            | - node-1 topology=alive state=ready |
      | 3            | - node-3 topology=alive state=ready |
