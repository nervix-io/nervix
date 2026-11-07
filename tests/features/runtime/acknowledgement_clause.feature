Feature: Input acknowledgement mode is a Plexus body clause

  @acknowledgement_clause
  Scenario Outline: A junction body mode survives SHOW CREATE and ALTER
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE JUNCTION routed FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED FILTER WHERE input.value > 0 UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE JUNCTION routed;
      """
    Then the last command output contains
      """
      CREATE JUNCTION routed
        FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED
        FILTER WHERE input.value > 0
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE JUNCTION routed;
      """
    Then the last command output contains
      """
      mode: DETACHED
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER JUNCTION routed SET ATTACHED;
      SHOW CREATE JUNCTION routed;
      """
    Then the last command output contains
      """
      CREATE JUNCTION routed
        FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        ATTACHED
        FILTER WHERE input.value > 0
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE JUNCTION routed;
      """
    Then the last command output contains
      """
      mode: ATTACHED
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @acknowledgement_clause
  Scenario Outline: A <kind> body mode follows the complete input contract in SHOW CREATE
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY other SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE IF NOT EXISTS <head> tested <inputs> DETACHED <body>;
      SHOW CREATE <kind> tested;
      """
    Then the last command output contains
      """
      CREATE <head> tested
      """
    And the last command output contains
      """
      <last_input>
        DETACHED
      """

    Examples:
      | cluster_size | head                    | kind             | inputs                                                                                   | last_input                                            | body                                                                                                                                                                                            |
      | 1            | DEDUPLICATOR            | DEDUPLICATOR     | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | DEDUPLICATE ON input.value MAX TIME 1m UNBRANCHED TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                  |
      | 3            | DEDUPLICATOR            | DEDUPLICATOR     | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | DEDUPLICATE ON input.value MAX TIME 1m UNBRANCHED TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                  |
      | 1            | REORDERER               | REORDERER        | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | BY input.value MAX TIME 1m UNBRANCHED TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                              |
      | 3            | REORDERER               | REORDERER        | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | BY input.value MAX TIME 1m UNBRANCHED TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                              |
      | 1            | CORRELATOR              | CORRELATOR       | LEFT FROM incoming COLLECT FOR 5ms RIGHT FROM other COLLECT FOR 10ms MAX BATCH SIZE 1MiB | RIGHT FROM other COLLECT FOR 10ms MAX BATCH SIZE 1MiB | CORRELATE WHERE left.value = right.value MATCH EARLIEST MAX TIME 1m ON CORRELATION TIMEOUT DROP, DROP UNBRANCHED TO outgoing SET value = left.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG        |
      | 3            | CORRELATOR              | CORRELATOR       | LEFT FROM incoming COLLECT FOR 5ms RIGHT FROM other COLLECT FOR 10ms MAX BATCH SIZE 1MiB | RIGHT FROM other COLLECT FOR 10ms MAX BATCH SIZE 1MiB | CORRELATE WHERE left.value = right.value MATCH EARLIEST MAX TIME 1m ON CORRELATION TIMEOUT DROP, DROP UNBRANCHED TO outgoing SET value = left.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG        |
      | 1            | WINDOW PROCESSOR        | WINDOW PROCESSOR | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | WIDTH 3 MESSAGES STEP 3 MESSAGES UNBRANCHED TO outgoing SET value = COUNT(input.value) ON MESSAGE ERROR LOG                                                                                     |
      | 3            | WINDOW PROCESSOR        | WINDOW PROCESSOR | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | WIDTH 3 MESSAGES STEP 3 MESSAGES UNBRANCHED TO outgoing SET value = COUNT(input.value) ON MESSAGE ERROR LOG                                                                                     |
      | 1            | REINGESTOR              | REINGESTOR       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                                                         |
      | 3            | REINGESTOR              | REINGESTOR       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG                                                                                                                         |
      | 1            | CLIENT CONSUMER EMITTER | EMITTER          | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | TO SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |
      | 3            | CLIENT CONSUMER EMITTER | EMITTER          | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB                                       | FROM incoming COLLECT FOR 10ms MAX BATCH SIZE 1MiB    | TO SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG |

  @acknowledgement_clause
  Scenario Outline: Resource-backed processors place the body mode before their resource binding
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE model;
      UPLOAD RESOURCE model VERSION '{{onnx_model}}';
      CREATE RESOURCE guest;
      UPLOAD RESOURCE guest VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features (vector <vector_type>);
      CREATE SCHEMA scored (score <score_type>);
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER scoring FROM features COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED USING RESOURCE model VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS {'features' <tensor_type>[2] = input.vector}
        OUTPUT SCHEMA {'score' <tensor_type>[1]}
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      SHOW CREATE INFERENCER scoring;
      """
    Then the last command output contains
      """
      CREATE INFERENCER scoring
        FROM features COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED
        USING RESOURCE model VERSION 1
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric (value I32);
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE WASM PROCESSOR filtering FROM raw_metrics COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED USING RESOURCE guest VERSION 1 FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000 MAX MEMORY 64MiB
        UNBRANCHED TO filtered_metrics SET value = value
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      SHOW CREATE WASM PROCESSOR filtering;
      """
    Then the last command output contains
      """
      CREATE WASM PROCESSOR filtering
        FROM raw_metrics COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED
        USING RESOURCE guest VERSION 1
      """

    Examples:
      | cluster_size | vector_type   | score_type    | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
