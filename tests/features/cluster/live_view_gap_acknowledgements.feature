Feature: Forwarded record acknowledgements across a gap in the receiver's live view
  A node that holds the acknowledgement of a record another node forwarded reports it to that
  node until it resolves. A failure detector can briefly drop a slow node from another node's live
  view while its run goes on. Such a gap does not end the run, so the receiver keeps the
  acknowledgement and returns its outcome once the relay owner is back, and the source commits the
  record once instead of sending it again.

  # The forwarded record's acknowledgement is held on the processor's node by its paused checkpoint
  # while the processor's node misses the relay owner for one look. The source is sequential: it
  # reads no later record before the held one resolves, so once the later record is committed every
  # resend of the held one has already reached the relay, and the exact relay total settles whether
  # the source sent it again.
  Scenario: A forwarded record's acknowledgement survives a brief gap in the receiver's live view
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "live_view_gap_in_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE live_view_gap_filter;
      UPLOAD RESOURCE live_view_gap_filter VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32, tenant STRING );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer, tenant string );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY raw_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY filtered_metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_ingress CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR metric_source
        FROM kafka_ingress TOPIC 'live_view_gap_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'live_view_gap_group_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING metric_codec
        TO raw_metrics
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE live_view_gap_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        BRANCHED BY by_tenant
        TO filtered_metrics
        SET value = value, tenant = tenant
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR metric_source ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY raw_metrics ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE WASM PROCESSOR filter_even_rows ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=raw_metrics owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=wasm_processor name=filter_even_rows owner=node-2
      """
    And Kafka consumer group "live_view_gap_group_{{test_id}}" eventually has 1 consumers
    When the next guest-state checkpoint of WASM processor "filter_even_rows" pauses after_local_durability
    And Kafka message is published to topic "live_view_gap_in_{{test_id}}"
      """
      {"value":1,"tenant":"alpha"}
      """
    Then a guest-state checkpoint of WASM processor "filter_even_rows" is held after_local_durability
    When node "node-2" briefly misses node "node-1" in its live view
    And every held WASM guest-state checkpoint is released
    And Kafka message is published to topic "live_view_gap_in_{{test_id}}"
      """
      {"value":2,"tenant":"alpha"}
      """
    Then within "90s" Kafka consumer group "live_view_gap_group_{{test_id}}" next offset for topic "live_view_gap_in_{{test_id}}" partition 0 is "at least 2"
    And within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "raw_metrics" across physical nodes totals 2
