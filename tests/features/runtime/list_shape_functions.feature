Feature: List shape functions
  Scenario Outline: A retained sequence is reshaped for a multidimensional inferencer input
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      """
    And client "model_uploader" is connected to the leader node
    When client "model_uploader" uploads resource "inference" from "{{onnx_model}}" with identity "list-shape-model"
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA samples (tenant STRING, sequence <sequence_type>);
      CREATE SCHEMA scored (transformed <matrix_type>);
      CREATE WIRE JSON SCHEMA samples_wire MODE STRICT (tenant string, sequence array);
      CREATE WIRE JSON CODEC samples_codec FROM WIRE SCHEMA samples_wire TO SCHEMA samples;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY samples SCHEMA samples BRANCHED BY by_tenant;
      CREATE RELAY retained SCHEMA samples BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge list-inference-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/samples';
      CREATE HTTP ENDPOINT INGESTOR ingest FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING samples_codec
        TO samples INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR retain_sequence FROM samples
        WIDTH 1 MESSAGES STEP 1 MESSAGES BRANCHED BY by_tenant
        TO retained SET tenant = FIRST(input.tenant), sequence = FIRST(input.sequence)
          ON MESSAGE ERROR LOG;
      CREATE INFERENCER shaped_input FROM retained
        USING RESOURCE inference VERSION 1 FILE 'models/matrix_identity.onnx'
        INPUTS { 'matrix' <tensor_type>[2, 3] = list_reshape(input.sequence, 2, 3) }
        OUTPUT SCHEMA { 'transformed' <tensor_type>[2, 3] }
        BRANCHED BY by_tenant TO scored SET transformed = transformed
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scored_subscription TO scored;
      START;
      """
    And http payload is posted to node "node-1" with host "list-inference-{{test_id}}.example.com" path "/samples"
      """
      {"tenant":"a","sequence":[1.0,2.0,3.0,4.0,5.0,6.0]}
      """
    And http payload is posted to node "node-1" with host "list-inference-{{test_id}}.example.com" path "/samples"
      """
      {"tenant":"b","sequence":[10.0,20.0,30.0,40.0,50.0,60.0]}
      """
    And http payload is posted to node "node-1" with host "list-inference-{{test_id}}.example.com" path "/samples"
      """
      {"tenant":"a","sequence":[6.0,5.0,4.0,3.0,2.0,1.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"a"} | "transformed":[[1.0,2.0,3.0],[4.0,5.0,6.0]]
      key={"tenant":"b"} | "transformed":[[10.0,20.0,30.0],[40.0,50.0,60.0]]
      key={"tenant":"a"} | "transformed":[[6.0,5.0,4.0],[3.0,2.0,1.0]]
      """

    Examples:
      | cluster_size | sequence_type | matrix_type      | tensor_type       |
      | 1            | ARRAY<F32, 6> | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 6> | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |

  Scenario Outline: List shape functions preserve ordered elements and nested access
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA source (
        id STRING,
        matrix <matrix_type> OPTIONAL,
        ragged <ragged_type> OPTIONAL
      );
      CREATE SCHEMA result (
        id STRING,
        reshaped <reshape_type> OPTIONAL,
        flattened <flat_type> OPTIONAL,
        transposed <transpose_type> OPTIONAL,
        first_row <row_type> OPTIONAL,
        last_row <row_type> OPTIONAL,
        second_row <row_type> OPTIONAL,
        flat_ragged <vector_type> OPTIONAL,
        first_ragged <vector_type> OPTIONAL,
        absent BOOL
      );
      CREATE JAQ JSON CODEC source_codec TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE VHOST edge list-shapes-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/shapes';
      CREATE HTTP ENDPOINT INGESTOR ingest
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources INHERIT ALL UNBRANCHED
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION shapes FROM sources UNBRANCHED
        TO results
          SET id = input.id,
              reshaped = list_reshape(input.matrix, 1, 3, 2),
              flattened = list_flatten(input.matrix),
              transposed = list_transpose(input.matrix),
              first_row = first(input.matrix),
              last_row = last(input.matrix),
              second_row = nth(input.matrix, 1),
              flat_ragged = list_flatten(input.ragged),
              first_ragged = first(input.ragged),
              absent = is_null(list_transpose(input.matrix))
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION results_subscription TO results;
      START;
      """
    And http payload is posted to node "node-1" with host "list-shapes-{{test_id}}.example.com" path "/shapes"
      """
      [{"id":"ordered","matrix":[[1,2,3],[4,5,6]],"ragged":[[7,8],[],[9]]},{"id":"null","matrix":null,"ragged":null},{"id":"empty","matrix":[[10,11,12],[13,14,15]],"ragged":[]}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ordered" | "reshaped":[[[1,2],[3,4],[5,6]]] | "flattened":[1,2,3,4,5,6] | "transposed":[[1,4],[2,5],[3,6]] | "first_row":[1,2,3] | "last_row":[4,5,6] | "second_row":[4,5,6] | "flat_ragged":[7,8,9] | "first_ragged":[7,8] | "absent":false
      "id":"null" | "absent":true
      "id":"empty" | "flat_ragged":[] | "absent":false
      """

    Examples:
      | cluster_size | matrix_type      | ragged_type   | reshape_type        | flat_type     | transpose_type   | row_type      | vector_type |
      | 1            | ARRAY<I64, 2, 3> | VEC<VEC<I64>> | ARRAY<I64, 1, 3, 2> | ARRAY<I64, 6> | ARRAY<I64, 3, 2> | ARRAY<I64, 3> | VEC<I64>    |
      | 3            | ARRAY<I64, 2, 3> | VEC<VEC<I64>> | ARRAY<I64, 1, 3, 2> | ARRAY<I64, 6> | ARRAY<I64, 3, 2> | ARRAY<I64, 3> | VEC<I64>    |

  Scenario Outline: Invalid list shapes and sensitive outputs are refused when applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA source (matrix <matrix_type>, flat <flat_type>, ragged <ragged_type>, width I64, secret <matrix_type> SENSITIVE);
      CREATE SCHEMA result (items <flat_type>);
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      """
    When these Plexus commands fail with "list_reshape"
      """
      CREATE JUNCTION invalid_product FROM sources UNBRANCHED TO results
        SET items = list_reshape(input.matrix, 5) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_reshape"
      """
      CREATE JUNCTION dynamic_dimension FROM sources UNBRANCHED TO results
        SET items = list_reshape(input.matrix, input.width) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_reshape"
      """
      CREATE JUNCTION zero_dimension FROM sources UNBRANCHED TO results
        SET items = list_reshape(input.matrix, 0) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_reshape"
      """
      CREATE JUNCTION negative_dimension FROM sources UNBRANCHED TO results
        SET items = list_reshape(input.matrix, -1) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_reshape"
      """
      CREATE JUNCTION variable_reshape FROM sources UNBRANCHED TO results
        SET items = list_reshape(input.ragged, 6) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_transpose"
      """
      CREATE JUNCTION variable_transpose FROM sources UNBRANCHED TO results
        SET items = list_transpose(input.ragged) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_transpose"
      """
      CREATE JUNCTION rank_one FROM sources UNBRANCHED TO results
        SET items = list_transpose(input.flat) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "list_flatten"
      """
      CREATE JUNCTION scalar_elements FROM sources UNBRANCHED TO results
        SET items = list_flatten(input.flat) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "would store sensitive data in a non-sensitive output field"
      """
      CREATE JUNCTION sensitive_shape FROM sources UNBRANCHED TO results
        SET items = list_reshape(input.secret, 6) FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | matrix_type      | flat_type     | ragged_type   |
      | 1            | ARRAY<I64, 2, 3> | ARRAY<I64, 6> | VEC<VEC<I64>> |
      | 3            | ARRAY<I64, 2, 3> | ARRAY<I64, 6> | VEC<VEC<I64>> |
