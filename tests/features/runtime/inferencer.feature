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
        TO features FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        DECODE USING features_codec
        BRANCHED BY by_feature_source VALUES { tenant = features.tenant }

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER score_model
        FROM features
        TO scored FLUSH IMMEDIATE SET scored.tenant = features.tenant, scored.score = inner_output.score
          UNSET features.vector ON MESSAGE ERROR LOG
        BRANCHED BY by_feature_source
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = features.vector }
        OUTPUT SCHEMA { "score" <tensor_type>[1] };
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
        TO features FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        DECODE USING features_codec
        BRANCHED BY by_feature_source VALUES { tenant = features.tenant }

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER score_model
        FROM features
        TO scored FLUSH IMMEDIATE SET scored.tenant = features.tenant, scored.score = inner_output.<output_tensor>
          UNSET features.vector ON MESSAGE ERROR LOG
        BRANCHED BY by_feature_source
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/<model_file>'
        INPUTS { "<input_tensor>" <tensor_type>[<input_dimensions>] = features.vector }
        OUTPUT SCHEMA { "<output_tensor>" <tensor_type>[<output_dimensions>] };
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
        FROM features TO scored FLUSH IMMEDIATE SET scored.scores = inner_output.scores
          UNSET features.features, features.mask ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[<features_dimensions>] = features.features,
          "mask" <tensor_type>[<mask_dimensions>] = features.mask
        }
        OUTPUT SCHEMA { "scores" <tensor_type>[<output_dimensions>] };
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
        FROM features TO scored FLUSH IMMEDIATE SET scored.scores = inner_output.scores
          UNSET features.features ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference FILE 'model.onnx'
        INPUTS { "features" <unsupported_tensor_type>[2] = features.features }
        OUTPUT SCHEMA { "scores" <supported_tensor_type>[2] };
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
        FROM features TO scored FLUSH IMMEDIATE SET scored.scores = inner_output.scores
          UNSET features.features ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS { "features" <tensor_type>[BATCH, 2] = features.features }
        OUTPUT SCHEMA { "scores" <tensor_type>[BATCH, 2] };
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
        FROM features TO scored FLUSH IMMEDIATE SET scored.scores = inner_output.scores
          UNSET features.features, features.mask ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[3, 2] = features.features,
          "mask" <tensor_type>[3, 2] = features.mask
        }
        OUTPUT SCHEMA { "scores" <tensor_type>[3, 2] };
      """
    Then the last command output contains
      """
      stored model 'fixed_dynamic_axis'
      """

    Examples:
      | field_type       | tensor_type       |
      | ARRAY<F32, 3, 2> | DENSE TENSOR<F32> |

  Scenario Outline: Per-message inferencer preserves multidimensional tensor shape
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
      CREATE SCHEMA matrices ( matrix <matrix_type> );
      CREATE SCHEMA transformed_matrices ( transformed <matrix_type> );
      CREATE STRICT WIRE JSON SCHEMA matrices_wire ( matrix array );
      CREATE CODEC matrices_codec FROM WIRE JSON SCHEMA matrices_wire TO SCHEMA matrices;
      CREATE RELAY matrices SCHEMA matrices UNBRANCHED;
      CREATE RELAY transformed_matrices SCHEMA transformed_matrices UNBRANCHED;
      CREATE VHOST edge infer-matrix-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/matrix' TYPE HTTP;
      CREATE INGESTOR matrix_source
        TO matrices FLUSH IMMEDIATE ON MESSAGE ERROR LOG DECODE USING matrices_codec UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER transform_matrix
        FROM matrices
        TO transformed_matrices FLUSH IMMEDIATE SET transformed_matrices.transformed = inner_output.transformed
          UNSET matrices.matrix ON MESSAGE ERROR LOG
        UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/matrix_identity.onnx'
        INPUTS { "matrix" <tensor_type>[2, 3] = matrices.matrix }
        OUTPUT SCHEMA { "transformed" <tensor_type>[2, 3] };
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION transformed_matrices_subscription TO transformed_matrices;
      START;
      """
    And http payload is posted to host "infer-matrix-{{test_id}}.example.com" path "/matrix"
      """
      {"matrix":[[1.0,2.0,3.0],[4.0,5.0,6.0]]}
      """
    Then the relay subscription receives a payload
      """
      {"transformed":[[1.0,2.0,3.0],[4.0,5.0,6.0]]}
      """

    Examples:
      | cluster_size | matrix_type      | tensor_type       |
      | 1            | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |

  Scenario Outline: Per-message inferencer maps dynamic tensor axes to vectors
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
      CREATE SCHEMA sequences ( features <sequence_type>, mask <sequence_type> );
      CREATE SCHEMA scored_sequences ( scores <sequence_type> );
      CREATE STRICT WIRE JSON SCHEMA sequences_wire ( features array, mask array );
      CREATE CODEC sequences_codec FROM WIRE JSON SCHEMA sequences_wire TO SCHEMA sequences;
      CREATE RELAY sequences SCHEMA sequences UNBRANCHED;
      CREATE RELAY scored_sequences SCHEMA scored_sequences UNBRANCHED;
      CREATE VHOST edge infer-dynamic-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/sequence' TYPE HTTP;
      CREATE INGESTOR sequence_source
        TO sequences FLUSH IMMEDIATE ON MESSAGE ERROR LOG DECODE USING sequences_codec UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER score_sequence
        FROM sequences TO scored_sequences FLUSH IMMEDIATE SET scored_sequences.scores = inner_output.scores
          UNSET sequences.features, sequences.mask ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[DYNAMIC, 2] = sequences.features,
          "mask" <tensor_type>[DYNAMIC, 2] = sequences.mask
        }
        OUTPUT SCHEMA { "scores" <tensor_type>[DYNAMIC, 2] };
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_sequences_subscription TO scored_sequences;
      START;
      """
    And http payload is posted to host "infer-dynamic-{{test_id}}.example.com" path "/sequence"
      """
      {"features":[[1.0,10.0],[2.0,20.0]],"mask":[[100.0,1000.0],[200.0,2000.0]]}
      """
    Then the relay subscription receives a payload
      """
      {"scores":[[102.5,1025.0],[203.5,2035.0]]}
      """

    Examples:
      | cluster_size | sequence_type      | tensor_type       |
      | 1            | VEC<ARRAY<F32, 2>> | DENSE TENSOR<F32> |
      | 3            | VEC<ARRAY<F32, 2>> | DENSE TENSOR<F32> |

  Scenario Outline: Batched inferencer groups dynamic vectors by concrete shape
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
      CREATE SCHEMA sequences ( features <sequence_type>, mask <sequence_type> );
      CREATE SCHEMA scored_sequences ( scores <sequence_type> );
      CREATE STRICT WIRE JSON SCHEMA sequences_wire ( features array, mask array );
      CREATE CODEC sequences_codec FROM WIRE JSON SCHEMA sequences_wire TO SCHEMA sequences;
      CREATE RELAY sequences SCHEMA sequences UNBRANCHED;
      CREATE RELAY scored_sequences SCHEMA scored_sequences UNBRANCHED;
      CREATE VHOST edge infer-dynamic-batch-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/sequence' TYPE HTTP;
      CREATE INGESTOR sequence_source
        TO sequences FLUSH IMMEDIATE ON MESSAGE ERROR LOG DECODE USING sequences_codec UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER score_sequence_batch
        FROM sequences TO scored_sequences FLUSH EACH 500ms MAX BATCH SIZE 16mb SET scored_sequences.scores = inner_output.scores
          UNSET sequences.features, sequences.mask ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/dynamic_batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[BATCH, DYNAMIC, 2] = sequences.features,
          "mask" <tensor_type>[BATCH, DYNAMIC, 2] = sequences.mask
        }
        OUTPUT SCHEMA { "scores" <tensor_type>[BATCH, DYNAMIC, 2] };
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_sequences_subscription TO scored_sequences;
      START;
      """
    And http payload is posted to host "infer-dynamic-batch-{{test_id}}.example.com" path "/sequence"
      """
      {"features":[[1.0,10.0]],"mask":[[100.0,1000.0]]}
      """
    And http payload is posted to host "infer-dynamic-batch-{{test_id}}.example.com" path "/sequence"
      """
      {"features":[[2.0,20.0],[4.0,40.0]],"mask":[[200.0,2000.0],[400.0,4000.0]]}
      """
    And http payload is posted to host "infer-dynamic-batch-{{test_id}}.example.com" path "/sequence"
      """
      {"features":[[3.0,30.0]],"mask":[[300.0,3000.0]]}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"scores":[[103.0,1030.0]]}
      {"scores":[[204.0,2040.0],[408.0,4080.0]]}
      {"scores":[[305.0,3050.0]]}
      """

    Examples:
      | cluster_size | sequence_type      | tensor_type       |
      | 1            | VEC<ARRAY<F32, 2>> | DENSE TENSOR<F32> |
      | 3            | VEC<ARRAY<F32, 2>> | DENSE TENSOR<F32> |

  Scenario Outline: Per-message inferencer inherits inputs and maps inner values per output route
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
      CREATE SCHEMA scored (
        vector <input_field_type>,
        score <output_field_type>
      );
      CREATE SCHEMA audited (
        vector <input_field_type>,
        model_input <input_field_type>
      );
      CREATE STRICT WIRE JSON SCHEMA features_wire ( vector array );
      CREATE CODEC features_codec FROM WIRE JSON SCHEMA features_wire TO SCHEMA features;
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE RELAY audited SCHEMA audited UNBRANCHED;
      CREATE VHOST edge infer-per-message-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE INGESTOR feature_source
        TO features FLUSH IMMEDIATE ON MESSAGE ERROR LOG DECODE USING features_codec UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER score_messages
        FROM features
        TO scored FLUSH EACH 500ms MAX BATCH SIZE 16mb SET scored.score = inner_output.score ON MESSAGE ERROR LOG
        TO audited FLUSH EACH 500ms MAX BATCH SIZE 16mb SET audited.model_input = inner_input.features ON MESSAGE ERROR LOG
        UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = features.vector }
        OUTPUT SCHEMA { "score" <tensor_type>[1] };
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
      CREATE SUBSCRIPTION audited_subscription TO audited;
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
      {"score":[0.875],"vector":[1.0,0.0]}
      {"model_input":[1.0,0.0],"vector":[1.0,0.0]}
      {"score":[-0.375],"vector":[0.0,1.0]}
      {"model_input":[0.0,1.0],"vector":[0.0,1.0]}
      {"score":[1.125],"vector":[2.0,1.0]}
      {"model_input":[2.0,1.0],"vector":[2.0,1.0]}
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
        TO features FLUSH IMMEDIATE ON MESSAGE ERROR LOG DECODE USING features_codec UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER batch_score_messages
        FROM features TO scored FLUSH EACH 500ms MAX BATCH SIZE 16mb SET scored.scores = inner_output.scores
          UNSET features.features, features.mask ON MESSAGE ERROR LOG UNBRANCHED
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[BATCH, 2] = features.features,
          "mask" <tensor_type>[BATCH, 2] = features.mask
        }
        OUTPUT SCHEMA { "scores" <tensor_type>[BATCH, 2] };
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
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
        TO features FLUSH IMMEDIATE ON MESSAGE ERROR LOG DECODE USING features_codec
        BRANCHED BY by_tenant VALUES { tenant = features.tenant }

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE INFERENCER branch_batch_score
        FROM features TO scored FLUSH EACH 500ms MAX BATCH SIZE 16mb SET scored.scores = inner_output.scores
          UNSET features.tenant, features.features, features.mask ON MESSAGE ERROR LOG BRANCHED BY by_tenant
        USING RESOURCE inference VERSION 1 FILE 'models/batch_score.onnx'
        INPUTS {
          "features" <tensor_type>[BATCH, 2] = features.features,
          "mask" <tensor_type>[BATCH, 2] = features.mask
        }
        OUTPUT SCHEMA { "scores" <tensor_type>[BATCH, 2] };
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
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
