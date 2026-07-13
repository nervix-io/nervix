Feature: Inferencer resources
  Scenario Outline: Inferencer creation uses a static ONNX resource model
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE fraud_model;
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_model}}';
      CREATE SCHEMA features (
        tenant STRING,
        vector <vector_type>
      );
      CREATE SCHEMA scored (
        tenant STRING,
        score <score_type>
      );
      CREATE STRICT WIRE JSON SCHEMA features_wire (
        tenant string,
        vector array
      );
      CREATE CODEC features_codec
        FROM WIRE JSON SCHEMA features_wire
        TO SCHEMA features;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_feature_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_feature_source;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_feature_source;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress
        ON edge
        PATH '/features'
        TYPE HTTP;
      CREATE INGESTOR feature_source
        TO features
        DECODE USING features_codec
        BRANCHED BY by_feature_source VALUES { tenant = features.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER score_model
        FROM features
        TO scored SET scored.tenant = features.tenant
        BRANCHED BY by_feature_source
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = features.vector }
        OUTPUTS { "score" <tensor_type>[1] = scored.score }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      stored model 'score_model'
      """

    Examples:
      | cluster_size | replica_count | vector_type   | score_type    | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  Scenario Outline: Inferencer creation rejects incompatible ONNX bindings
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "<expected_error>"
      """
      CREATE RESOURCE fraud_model;
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_model}}';
      CREATE SCHEMA features (
        tenant STRING,
        vector <vector_type>
      );
      CREATE SCHEMA scored (
        tenant STRING,
        score <score_type>
      );
      CREATE STRICT WIRE JSON SCHEMA features_wire (
        tenant string,
        vector array
      );
      CREATE CODEC features_codec
        FROM WIRE JSON SCHEMA features_wire
        TO SCHEMA features;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_feature_source SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_feature_source;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_feature_source;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress
        ON edge
        PATH '/features'
        TYPE HTTP;
      CREATE INGESTOR feature_source
        TO features
        DECODE USING features_codec
        BRANCHED BY by_feature_source VALUES { tenant = features.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER score_model
        FROM features
        TO scored SET scored.tenant = features.tenant
        BRANCHED BY by_feature_source
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/<model_file>'
        INPUTS { "<input_tensor>" <tensor_type>[<input_dimensions>] = features.vector }
        OUTPUTS { "<output_tensor>" <tensor_type>[<output_dimensions>] = scored.score }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count | vector_type   | score_type    | model_file        | input_tensor | output_tensor | input_dimensions | output_dimensions | tensor_type       | expected_error                       |
      | 1            | 0             | ARRAY<F32, 3> | ARRAY<F32, 1> | simple_score.onnx | features     | score         | 3                | 1                 | DENSE TENSOR<F32> | incompatible shape                   |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | simple_score.onnx | features     | score         | 1, 2             | 1                 | DENSE TENSOR<F32> | incompatible shape                   |
      | 1            | 0             | ARRAY<F64, 2> | ARRAY<F32, 1> | simple_score.onnx | features     | score         | 2                | 1                 | DENSE TENSOR<F32> | incompatible element type            |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 2> | f64_score.onnx    | features     | score         | 2                | 2                 | DENSE TENSOR<F32> | incompatible element type            |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | simple_score.onnx | missing      | score         | 2                | 1                 | DENSE TENSOR<F32> | missing ONNX input tensor 'missing'  |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | simple_score.onnx | features     | missing       | 2                | 1                 | DENSE TENSOR<F32> | missing ONNX output tensor 'missing' |

  Scenario Outline: Inferencer creation rejects invalid batch declarations
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "<expected_error>"
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features (
        features <field_type>,
        mask <field_type>
      );
      CREATE SCHEMA scored ( scores <field_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER invalid_batch_schema
        FROM features TO scored UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[<features_dimensions>] = features.features,
          "mask" <tensor_type>[<mask_dimensions>] = features.mask
        }
        OUTPUTS { "scores" <tensor_type>[<output_dimensions>] = scored.scores }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | field_type    | tensor_type       | features_dimensions | mask_dimensions | output_dimensions | expected_error                    |
      | ARRAY<F32, 2> | DENSE TENSOR<F32> | BATCH, 2            | 1, 2            | BATCH, 2          | mixes batched and per-message     |
      | ARRAY<F32, 2> | DENSE TENSOR<F32> | BATCH, BATCH, 2     | BATCH, 2        | BATCH, 2          | contains more than one BATCH axis |

  Scenario Outline: Inferencer syntax rejects unsupported ONNX value schemas
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail
      """
      CREATE INFERENCER unsupported_schema
        FROM features TO scored UNBRANCHED
        USING RESOURCE inference FILE 'model.onnx'
        INPUTS { "features" <unsupported_tensor_type>[2] = features.features }
        OUTPUTS { "scores" <supported_tensor_type>[2] = scored.scores }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | unsupported_tensor_type | supported_tensor_type |
      | SPARSE TENSOR<F32>      | DENSE TENSOR<F32>     |
      | DENSE TENSOR<F64>       | DENSE TENSOR<F32>     |

  Scenario Outline: Inferencer creation requires complete ONNX port bindings
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "missing INPUTS binding for ONNX input tensor 'mask'"
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( features <field_type> );
      CREATE SCHEMA scored ( scores <field_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER incomplete_bindings
        FROM features TO scored UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS { "features" <tensor_type>[BATCH, 2] = features.features }
        OUTPUTS { "scores" <tensor_type>[BATCH, 2] = scored.scores }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | field_type    | tensor_type       |
      | ARRAY<F32, 2> | DENSE TENSOR<F32> |

  Scenario Outline: Dynamic ONNX axes may be instantiated with fixed sizes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features (
        features <field_type>,
        mask <field_type>
      );
      CREATE SCHEMA scored ( scores <field_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER fixed_dynamic_axis
        FROM features TO scored UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[3, 2] = features.features,
          "mask" <tensor_type>[3, 2] = features.mask
        }
        OUTPUTS { "scores" <tensor_type>[3, 2] = scored.scores }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      stored model 'fixed_dynamic_axis'
      """

    Examples:
      | field_type    | tensor_type       |
      | ARRAY<F32, 6> | DENSE TENSOR<F32> |

  Scenario Outline: Per-message inferencer invokes ONNX once for every collected message
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <input_field_type> );
      CREATE SCHEMA scored ( score <output_field_type> );
      CREATE STRICT WIRE JSON SCHEMA features_wire ( vector array );
      CREATE CODEC features_codec FROM WIRE JSON SCHEMA features_wire TO SCHEMA features;
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE VHOST edge infer-per-message-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE INGESTOR feature_source
        TO features DECODE USING features_codec UNBRANCHED FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER score_messages
        FROM features TO scored UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = features.vector }
        OUTPUTS { "score" <tensor_type>[1] = scored.score }
        FLUSH EACH 500ms MAX BATCH SIZE 16mb ON MESSAGE ERROR LOG;
      """
    And these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO scored;
      START;
      """
    And http payload is posted to host "infer-per-message-{{test_id}}.example.com" path "/features"
      """
      {"vector":[1.0,0.0]}
      """
    And http payload is posted to host "infer-per-message-{{test_id}}.example.com" path "/features"
      """
      {"vector":[0.0,1.0]}
      """
    And http payload is posted to host "infer-per-message-{{test_id}}.example.com" path "/features"
      """
      {"vector":[2.0,1.0]}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"score":[0.875]}
      {"score":[-0.375]}
      {"score":[1.125]}
      """

    Examples:
      | cluster_size | replica_count | tensor_type       | input_field_type | output_field_type |
      | 1            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2>    | ARRAY<F32, 1>     |
      | 3            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2>    | ARRAY<F32, 1>     |

  Scenario Outline: Batched inferencer invokes ONNX once and preserves row order
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features (
        features <input_field_type>,
        mask <input_field_type>
      );
      CREATE SCHEMA scored ( scores <output_field_type> );
      CREATE STRICT WIRE JSON SCHEMA features_wire (
        features array,
        mask array
      );
      CREATE CODEC features_codec FROM WIRE JSON SCHEMA features_wire TO SCHEMA features;
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE VHOST edge infer-batch-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE INGESTOR feature_source
        TO features DECODE USING features_codec UNBRANCHED FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER batch_score_messages
        FROM features TO scored UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[BATCH, 2] = features.features,
          "mask" <tensor_type>[BATCH, 2] = features.mask
        }
        OUTPUTS { "scores" <tensor_type>[BATCH, 2] = scored.scores }
        FLUSH EACH 500ms MAX BATCH SIZE 16mb ON MESSAGE ERROR LOG;
      """
    And these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO scored;
      START;
      """
    And http payload is posted to host "infer-batch-{{test_id}}.example.com" path "/features"
      """
      {"features":[1.0,10.0],"mask":[100.0,1000.0]}
      """
    And http payload is posted to host "infer-batch-{{test_id}}.example.com" path "/features"
      """
      {"features":[2.0,20.0],"mask":[200.0,2000.0]}
      """
    And http payload is posted to host "infer-batch-{{test_id}}.example.com" path "/features"
      """
      {"features":[3.0,30.0],"mask":[300.0,3000.0]}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"scores":[103.0,1030.0]}
      {"scores":[204.0,2040.0]}
      {"scores":[305.0,3050.0]}
      """

    Examples:
      | cluster_size | replica_count | tensor_type       | input_field_type | output_field_type |
      | 1            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2>    | ARRAY<F32, 2>     |
      | 3            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2>    | ARRAY<F32, 2>     |

  Scenario Outline: Batched inferencer isolates interleaved concrete branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features (
        tenant STRING,
        features <input_field_type>,
        mask <input_field_type>
      );
      CREATE SCHEMA scored ( scores <output_field_type> );
      CREATE STRICT WIRE JSON SCHEMA features_wire (
        tenant string,
        features array,
        mask array
      );
      CREATE CODEC features_codec FROM WIRE JSON SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-branch-batch-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE INGESTOR feature_source
        TO features DECODE USING features_codec
        BRANCHED BY by_tenant VALUES { tenant = features.tenant }
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER branch_batch_score
        FROM features TO scored BRANCHED BY by_tenant
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[BATCH, 2] = features.features,
          "mask" <tensor_type>[BATCH, 2] = features.mask
        }
        OUTPUTS { "scores" <tensor_type>[BATCH, 2] = scored.scores }
        FLUSH EACH 500ms MAX BATCH SIZE 16mb ON MESSAGE ERROR LOG;
      """
    And these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO scored;
      START;
      """
    And http payload is posted to host "infer-branch-batch-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[1.0,10.0],"mask":[100.0,1000.0]}
      """
    And http payload is posted to host "infer-branch-batch-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[100.0,1000.0],"mask":[1.0,10.0]}
      """
    And http payload is posted to host "infer-branch-batch-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[3.0,30.0],"mask":[300.0,3000.0]}
      """
    And http payload is posted to host "infer-branch-batch-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[300.0,3000.0],"mask":[3.0,30.0]}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "scores":[103.0,1030.0]
      key={"tenant":"acme"} | "scores":[305.0,3050.0]
      key={"tenant":"beta"} | "scores":[301.0,3010.0]
      key={"tenant":"beta"} | "scores":[503.0,5030.0]
      """

    Examples:
      | cluster_size | replica_count | tensor_type       | input_field_type | output_field_type |
      | 1            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2>    | ARRAY<F32, 2>     |
      | 3            | 0             | DENSE TENSOR<F32> | ARRAY<F32, 2>    | ARRAY<F32, 2>     |
