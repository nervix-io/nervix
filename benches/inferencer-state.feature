Feature: Cluster benchmark for one model with and without carried inference state
  Scenario Outline: <variant> on a <cluster_size> node cluster
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <input_type> );
      CREATE SCHEMA scores ( score <output_type> );
      CREATE WIRE JSON SCHEMA feature_wire MODE STRICT ( vector array );
      CREATE WIRE JSON CODEC feature_codec FROM WIRE SCHEMA feature_wire TO SCHEMA features;
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scores SCHEMA scores UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST benchmark inference-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON benchmark PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR requests FROM intake MODE ACK MAX 16 ACK TIMEOUT 10s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING feature_codec
        TO features INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector <state_input> }
        <state_clause>
        OUTPUT SCHEMA { 'score' <tensor_type>[1] <state_output> }
        UNBRANCHED TO scores SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT local_inference FROM features TO scores REQUIRE COLOCATION;
      START;
      RELOCATE RELAY features ONTO NODE node-1 IGNORE PREFERENCES;
      """
    And inference case "<variant>, cluster size <cluster_size>" is timed at 8 concurrent requests after 200 warmup requests for 2000 measured requests to host "inference-{{test_id}}.example.com" path "/features"
    Then within "15s" node "node-1" eventually reports materialized state for relay "scores" containing
      """
      {"fields":{"score":[<final_score>]}}
      """

    Examples:
      | variant       | cluster_size | input_type    | output_type   | tensor_type       | state_input                                              | state_clause                                                             | state_output                       | final_score |
      | Without STATE | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | , 'state_in' DENSE TENSOR<F32>[2] = [0 AS F32, 0 AS F32] |                                                                          | , 'state_out' DENSE TENSOR<F32>[2] | 1.0         |
      | With STATE    | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |                                                          | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } |                                    | 2201.0      |
      | Without STATE | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | , 'state_in' DENSE TENSOR<F32>[2] = [0 AS F32, 0 AS F32] |                                                                          | , 'state_out' DENSE TENSOR<F32>[2] | 1.0         |
      | With STATE    | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |                                                          | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } |                                    | 2201.0      |
