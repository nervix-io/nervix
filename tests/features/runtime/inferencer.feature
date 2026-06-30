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
      CREATE RELAY features SCHEMA features PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY scored SCHEMA scored PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/features'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE IF NOT EXISTS BRANCH by_feature_source PARAMETERIZED BY tenant_branch VALUES { tenant = features.tenant } TTL 5m; CREATE INGESTOR feature_source
        TO features
        DECODE USING features_codec
        BRANCHED BY by_feature_source
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE INFERENCER score_model
        FROM features
        TO scored SET scored.tenant = features.tenant
        BRANCHED BY by_feature_source
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS { "features" = features.vector }
        OUTPUTS { "score" = scored.score }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command output contains
      """
      stored model 'score_model'
      """

    Examples:
      | cluster_size | replica_count | vector_type   | score_type    |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> |

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
      CREATE RELAY features SCHEMA features PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY scored SCHEMA scored PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/features'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE IF NOT EXISTS BRANCH by_feature_source PARAMETERIZED BY tenant_branch VALUES { tenant = features.tenant } TTL 5m; CREATE INGESTOR feature_source
        TO features
        DECODE USING features_codec
        BRANCHED BY by_feature_source
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE INFERENCER score_model
        FROM features
        TO scored SET scored.tenant = features.tenant
        BRANCHED BY by_feature_source
        USING RESOURCE fraud_model VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS { "<input_tensor>" = features.vector }
        OUTPUTS { "<output_tensor>" = scored.score }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count | vector_type   | score_type    | input_tensor | output_tensor | expected_error                       |
      | 1            | 0             | ARRAY<F32, 3> | ARRAY<F32, 1> | features     | score         | incompatible shape                   |
      | 1            | 0             | ARRAY<F64, 2> | ARRAY<F32, 1> | features     | score         | incompatible element type            |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | missing      | score         | missing ONNX input tensor 'missing'  |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | features     | missing       | missing ONNX output tensor 'missing' |
