Feature: Inferencer configuration and state summaries

  @inferencer_describe
  Scenario Outline: Inferencers describe their pinned declarations while stopped and register zero state metrics at startup
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the active domain is saved as placeholder "active_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <input_type>, mask <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER scoring FROM features
        USING RESOURCE inference VERSION 1 FILE '<model_file>'
        INPUTS { 'features' <tensor_type>[<input_axes>] = input.vector <extra_input> }
        <state_clause>
        OUTPUT SCHEMA { '<output_tensor>' <tensor_type>[<output_axes>] }
        UNBRANCHED TO scored SET score = <output_tensor> FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE INFERENCER scoring;
      """
    Then the last command output contains
      """
      inferencer: scoring
      resource: inference@1
      file: <model_file>
      execution: <execution_mode>
      state bytes per branch: <state_bytes>
      """
    And the last command output contains
      """
      INPUTS
      """
    And the last command output contains
      """
      OUTPUT SCHEMA
      """
    When these Plexus commands are executed on the leader node
      """
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "scoring" is saved as placeholder "inference_owner"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_state_bytes{"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_snapshot_failures_total{"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_restored_branches_total{"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_reset_branches_total{"
    And node "{{inference_owner}}" observability metric "nervix_inferencer_state_bytes" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_snapshot_failures_total" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_restored_branches_total" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_reset_branches_total" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE INFERENCER scoring;
      """
    Then the last command output contains
      """
      branches: 0
      """
    And the last command output contains
      """
      state bytes: 0
      """

    Examples:
      | cluster_size | model_file               | state_clause                                                             | state_bytes | tensor_type       | input_type    | output_type   | input_axes | output_axes | output_tensor | extra_input                                       | execution_mode |
      | 1            | models/simple_score.onnx |                                                                          | 0           | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> | 2          | 1           | score         |                                                   | per message    |
      | 3            | models/simple_score.onnx |                                                                          | 0           | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> | 2          | 1           | score         |                                                   | per message    |
      | 1            | models/running_sum.onnx  | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } | 8           | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> | 2          | 1           | score         |                                                   | per message    |
      | 3            | models/running_sum.onnx  | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } | 8           | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> | 2          | 1           | score         |                                                   | per message    |
      | 1            | models/batch_score.onnx  |                                                                          | 0           | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 2> | BATCH, 2   | BATCH, 2    | scores        | , 'mask' DENSE TENSOR<F32>[BATCH, 2] = input.mask | batched        |
      | 3            | models/batch_score.onnx  |                                                                          | 0           | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 2> | BATCH, 2   | BATCH, 2    | scores        | , 'mask' DENSE TENSOR<F32>[BATCH, 2] = input.mask | batched        |

  @inferencer_describe @stateful_inferencer_persistence @deloxide_stress_restore
  Scenario Outline: Every node reads state counts and progress while interleaved branches keep their values private
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the active domain is saved as placeholder "active_domain"
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
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-describe-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER scoring FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores TO scored;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "scoring" is saved as placeholder "inference_owner"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_state_bytes{"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_restored_branches_total{"
    And node "{{inference_owner}}" observability path "/metrics" eventually responds with 200 and contains "nervix_inferencer_reset_branches_total{"
    Then node "{{inference_owner}}" observability metric "nervix_inferencer_state_bytes" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    When http payload is posted to node "node-1" with host "infer-describe-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"private_acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-describe-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"private_beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-describe-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"private_acme","vector":[2.0,20.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"private_acme"} | "score":[11.0]
      key={"tenant":"private_beta"} | "score":[1100.0]
      key={"tenant":"private_acme"} | "score":[33.0]
      """
    And within "60s" every node describes inferencer "scoring" with 2 branches and 16 state bytes and <replica_count> replicas
    And the inferencer summary reports published and persisted revisions
    And the last command output does not contain
      """
      private_acme
      """
    And the last command output does not contain
      """
      private_beta
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_state_bytes" with labels eventually equals 16
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_snapshot_failures_total" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_reset_branches_total" with labels eventually equals 2
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_restored_branches_total" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores;
      STOP;
      START;
      CREATE SUBSCRIPTION scores_after TO scored;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "scoring" is saved as placeholder "inference_owner"
    When http payload is posted to node "node-1" with host "infer-describe-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"private_acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "node-1" with host "infer-describe-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"private_beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"private_acme"} | "score":[66.0]
      key={"tenant":"private_beta"} | "score":[3300.0]
      """
    And within "60s" every node describes inferencer "scoring" with 2 branches and 16 state bytes and <replica_count> replicas
    And node "{{inference_owner}}" observability metric "nervix_inferencer_restored_branches_total" with labels eventually equals 2
      """
      domain="{{active_domain}}"
      target="scoring"
      """
    And node "{{inference_owner}}" observability metric "nervix_inferencer_reset_branches_total" with labels eventually equals 0
      """
      domain="{{active_domain}}"
      target="scoring"
      """

    Examples:
      | cluster_size | replica_count | tensor_type       | input_type    | output_type   |
      | 1            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | 1             | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
