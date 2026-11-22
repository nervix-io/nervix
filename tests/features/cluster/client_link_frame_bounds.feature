Feature: Client link frames within both nodes' relay bounds
  A client may open a producer or a consumer through any node. When the ingestor or emitter it
  attaches to executes on another node, the two nodes carry the attachment over the interconnect,
  and every producer one node forwards to another shares one link. Every frame on such a link stays
  within the sending node's relay encoded limit and the receiving node's committed relay batch
  bound, so one batch or delivery never ends a link and the attachments that share it. See
  docs/src/interconnect.md#client-producer-links and
  docs/src/interconnect.md#client-consumer-streams.

  @client_link_frame_bound
  Scenario: A forwarded producer is granted batches its owning node's relay bound carries, and a larger batch is refused without ending the link
    Given HTTP receiver "emitter" is running
    And HTTP receiver "emitter" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And node-2 starts with execution-relay-encoded-bytes = 1MiB
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA records (id STRING, amount I64);
      CREATE RELAY outbox SCHEMA records UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA records
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO outbox INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT sink_api CONFIG {
        'endpoint' = '{{http_receiver.emitter}}', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST EMITTER published FROM outbox
        TO sink_api
          METHOD 'POST' PATH '/records'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # Only the leader serves a console session, so node-1 leads and serves both sessions, while
    # node-2, whose relay bound is smaller than the default 4 MiB session frame, executes the graph.
    When leadership is transferred to node "node-1"
    And these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR input ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE RELAY outbox ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER published ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then within "30s" node "node-1" eventually reports scheduled "ingestor" "input" owner "node-2"
    Given WebSocket session "app" is connected to node "node-1"
    And client "steady" is connected to node "node-1"
    When within "30s" WebSocket session "app" opens producer "large" on ingestor "input" expecting fields "id STRING, amount I64"
    And within "30s" client "steady" opens producer "steady" on ingestor "input" expecting fields "id STRING, amount I64"
    # The session frame admits this batch, so only the link's bounds stand in its way.
    And producer "large" submits batch "a full session frame" with one 4000000-byte id
    Then batch "a full session frame" is not admitted because "invalid batch: too large"
    When producer "steady" submits batch "after the refusal" with one 1000-byte id
    Then batch "after the refusal" completes
    When producer "large" submits batch "within the bound" with one 500000-byte id
    Then batch "within the bound" completes
    And producer "large" is granted batches of just under "1MiB"
    And producer "steady" is granted batches of just under "1MiB"
    And within "30s" the leader node describes ingestor "input" with
      """
      owner: node-2
      producers: 2
      forwarded producers: 2
      """

  # A producer on the node that executes its ingestor crosses no link, so its node's relay bound
  # does not limit its batches; decoding still holds them to the relay decoded limit.
  @client_link_frame_bound
  Scenario: A producer on the node that executes its ingestor is granted batches as large as its session frame carries, whatever that node's relay bound
    Given HTTP receiver "emitter" is running
    And HTTP receiver "emitter" answers unscripted requests with "respond 200"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-1 starts with execution-relay-encoded-bytes = 1MiB
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA records (id STRING, amount I64);
      CREATE RELAY outbox SCHEMA records UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA records
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO outbox INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT sink_api CONFIG {
        'endpoint' = '{{http_receiver.emitter}}', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST EMITTER published FROM outbox
        TO sink_api
          METHOD 'POST' PATH '/records'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Given WebSocket session "app" is connected to node "node-1"
    When within "30s" WebSocket session "app" opens producer "local" on ingestor "input" expecting fields "id STRING, amount I64"
    And producer "local" submits batch "a full session frame" with one 4000000-byte id
    Then batch "a full session frame" completes
    And producer "local" is granted batches of just under "4MiB"

  @client_link_frame_bound
  Scenario: A forwarded consumer receives a delivery larger than its serving node's relay bound in chunks that bound carries
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And node-1 starts with execution-relay-encoded-bytes = 512KiB
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA records (id STRING, amount I64);
      CREATE RELAY outbox SCHEMA records UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA records MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND TIMESTAMP NOW
        TO outbox INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER output FROM outbox
        TO SCHEMA records MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 2MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE INGESTOR input ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE RELAY outbox ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER output ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then within "30s" node "node-1" eventually reports scheduled "emitter" "output" owner "node-2"
    And within "30s" node "node-2" eventually reports scheduled "relay" "outbox" owner "node-2"
    And within "30s" node "node-2" eventually reports scheduled "ingestor" "input" owner "node-2"
    # node-1 serves the consumer, so the executing node-2 sends it the delivery over the
    # interconnect, where node-1 receives frames of at most 512 KiB.
    Given client "reader" is connected to node "node-1"
    And client "writer" is connected to node "node-2"
    When client "reader" opens consumer "output" on emitter "output" expecting fields "id STRING, amount I64"
    And client "writer" opens producer "input" on ingestor "input" expecting fields "id STRING, amount I64"
    And producer "input" submits batch "a large row" with one 1500000-byte id
    And consumer "output" reads output batch "the large row"
    Then output batch "the large row" carries one 1500000-byte id
    When output batch "the large row" is acknowledged
    Then batch "a large row" completes

  # 64 bytes cannot hold a Submit or DeliveryChunk frame's envelope beside a byte of data.
  @client_link_frame_bound
  Scenario: A node whose relay batch bound cannot carry one batch or chunk refuses forwarded producers and consumers as node capacity exhausted
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And node-2 starts with interconnect-relay-batch-bytes = 64B
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA records (id STRING, amount I64);
      CREATE RELAY outbox SCHEMA records UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR input
        FROM SCHEMA records MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND TIMESTAMP NOW
        TO outbox INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER output FROM outbox
        TO SCHEMA records MODE ACK SEQUENTIAL ACK TIMEOUT 1m
          RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE INGESTOR input ONTO NODE node-2 IGNORE PREFERENCES;
      RELOCATE EMITTER output ONTO NODE node-1 IGNORE PREFERENCES;
      """
    Then within "30s" node "node-1" eventually reports scheduled "ingestor" "input" owner "node-2"
    And within "30s" node "node-2" eventually reports scheduled "emitter" "output" owner "node-1"
    # node-2 executes the ingestor, so it would receive every forwarded batch.
    Given client "app" is connected to node "node-1"
    Then client "app" cannot open a producer on ingestor "input" expecting fields "id STRING, amount I64" because "node capacity exhausted"
    # node-2 serves this consumer, so it would receive every delivery chunk.
    Given client "reader" is connected to node "node-2"
    Then client "reader" cannot open a consumer on emitter "output" expecting fields "id STRING, amount I64" because "node capacity exhausted"
