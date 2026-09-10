Feature: Positive count settings
  Every NSPL count whose meaning requires at least one — relay capacity, ingest instances,
  confirmation windows, database batch limits, branch retention, guest fuel and memory, tensor
  dimensions, placement rank — is rejected where the statement is read, before any Model exists.

  Background:
    Given a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """

  Scenario: Relay CAPACITY must be a positive count
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( tenant STRING );
      CREATE RELAY notifications SCHEMA notification UNBRANCHED CAPACITY 3;
      """
    And these NSPL commands fail with "invalid relay capacity '0'; expected a positive integer"
      """
      CREATE RELAY empty_capacity SCHEMA notification UNBRANCHED CAPACITY 0;
      """
    And these NSPL commands fail with "invalid relay capacity '0'; expected a positive integer"
      """
      ALTER RELAY notifications SET CAPACITY 0;
      """
    And these NSPL commands are executed
      """
      SHOW CREATE RELAY notifications;
      """
    Then the last command output contains
      """
      CREATE RELAY notifications SCHEMA notification UNBRANCHED CAPACITY 3;
      """

  Scenario: Branch MAX INSTANCES must be a positive count
    When these NSPL commands fail with "MAX INSTANCES must be greater than 0"
      """
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m MAX INSTANCES 0 EVICT LRU;
      """

  Scenario: Ingest INSTANCES and confirmation windows must be positive counts
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( tenant STRING );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( tenant string );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    And these NSPL commands fail with "instances must be greater than 0"
      """
      CREATE INGESTOR no_instances
        FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP counts INSTANCES 0
          MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these NSPL commands fail with "parallel max in-flight must be greater than zero"
      """
      CREATE INGESTOR empty_ack_window
        FROM KAFKA kafka_main TOPIC notifications OFFSET BY CONSUMER GROUP counts
          MODE ACK PARALLEL MAX 0 BATCH TIMEOUT 1s ACK TIMEOUT 1s
          RETRY POLICY BACKOFF 10ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

  Scenario: Database emitter WITH MAX BATCH must be a positive count
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( tenant STRING );
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT postgres_main TYPE POSTGRES POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable'
      };
      """
    And these NSPL commands fail with "max batch size must be greater than zero"
      """
      CREATE EMITTER postgres_out FROM notifications
        TO POSTGRES postgres_main INSERT TO TABLE notifications
        VALUES { "tenant" = input.tenant }
        WITH MAX BATCH 0
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

  Scenario: WASM processor MAX FUEL and MAX MEMORY must be positive counts
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( tenant STRING );
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY processed SCHEMA notification UNBRANCHED;
      CREATE RESOURCE wasm_guest;
      """
    And these NSPL commands fail with "MAX FUEL must be greater than zero"
      """
      CREATE WASM PROCESSOR no_fuel FROM notifications
        USING RESOURCE wasm_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 0
        MAX MEMORY 64MiB
        UNBRANCHED
        TO processed SET tenant = tenant
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      """
    And these NSPL commands fail with "MAX MEMORY must be greater than zero"
      """
      CREATE WASM PROCESSOR no_memory FROM notifications
        USING RESOURCE wasm_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000
        MAX MEMORY 0B
        UNBRANCHED
        TO processed SET tenant = tenant
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      """

  Scenario: Array lengths and inferencer tensor dimensions must be positive counts
    When these NSPL commands fail with "array length must be a positive unsigned integer"
      """
      CREATE SCHEMA empty_embedding ( vector ARRAY<F32, 0> );
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA embedding ( vector ARRAY<F32, 2> );
      CREATE SCHEMA scored ( score ARRAY<F32, 1> );
      CREATE RELAY embeddings SCHEMA embedding UNBRANCHED;
      CREATE RELAY scores SCHEMA scored UNBRANCHED;
      CREATE RESOURCE fraud_model;
      """
    And these NSPL commands fail with "tensor dimension must be a positive integer, DYNAMIC, or BATCH"
      """
      CREATE INFERENCER empty_dimension FROM embeddings
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS { "features" DENSE TENSOR<F32>[0] = input.vector }
        OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
        UNBRANCHED
        TO scores
        SET score = score
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """

  Scenario: Placement RANK must be a positive count
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( tenant STRING );
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY forwarded SCHEMA notification UNBRANCHED;
      CREATE JUNCTION forwarder FROM notifications UNBRANCHED
        TO forwarded INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "RANK 0"
      """
      CREATE PLACEMENT ranked
        FROM forwarder
        TO forwarder
        PREFER COLOCATION
        RANK 0;
      """
