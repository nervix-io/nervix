Feature: List conversions
  # A Scenario Outline reads every <name> as an Examples column, so each list type is written in
  # the Examples tables below.

  Scenario Outline: AS and TRY_CAST convert the elements and the containers of a list
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA raw_list (
        id STRING,
        counts <i64_vec> OPTIONAL,
        grid <i64_grid>,
        texts <text_vec>
      );
      CREATE SCHEMA tolerant_list (
        id STRING,
        counts <f32_vec> OPTIONAL,
        counts_converted BOOL,
        grid <f64_grid>,
        grid_rows <i64_rows>,
        triple <i64_triple> OPTIONAL,
        triple_converted BOOL,
        numbers <i64_vec> OPTIONAL,
        numbers_converted BOOL
      );
      CREATE SCHEMA strict_list (
        id STRING,
        route STRING,
        triple <i64_triple> OPTIONAL,
        triple_converted BOOL,
        numbers <i64_vec> OPTIONAL
      );
      CREATE SCHEMA list_error (
        input_id STRING,
        error_message STRING
      );
      CREATE JAQ JSON CODEC raw_list_batch_codec
        TO SCHEMA raw_list
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_lists SCHEMA raw_list UNBRANCHED;
      CREATE RELAY tolerant_lists SCHEMA tolerant_list UNBRANCHED;
      CREATE RELAY strict_lists SCHEMA strict_list UNBRANCHED;
      CREATE RELAY list_errors SCHEMA list_error UNBRANCHED;
      CREATE VHOST edge list-conversions-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT list_ingress ON edge PATH '/lists';
      CREATE HTTP ENDPOINT INGESTOR list_ingestor
        FROM list_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_list_batch_codec
        TO raw_lists
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION convert_lists
        FROM raw_lists
        UNBRANCHED
        TO tolerant_lists
          SET id = input.id,
              counts = input.counts AS <f32_vec>,
              counts_converted = NOT is_null(output.counts),
              grid = input.grid AS <f64_grid>,
              grid_rows = input.grid AS <i64_rows>,
              triple = TRY_CAST(input.counts AS <i64_triple>),
              triple_converted = NOT is_null(output.triple),
              numbers = TRY_CAST(input.texts AS <i64_vec>),
              numbers_converted = NOT is_null(output.numbers)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO strict_lists
          SET id = input.id,
              route = 'strict',
              triple = input.counts AS <i64_triple>,
              triple_converted = NOT is_null(output.triple),
              numbers = input.texts AS <i64_vec>
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO list_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION tolerant_lists_subscription TO tolerant_lists;
      CREATE SUBSCRIPTION strict_lists_subscription TO strict_lists;
      CREATE SUBSCRIPTION list_errors_subscription TO list_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "list-conversions-{{test_id}}.example.com" path "/lists"
      """
      [{"id":"fits","counts":[1,2,3],"grid":[[1,2],[3,4]],"texts":["7","8"]},{"id":"short","counts":[1,2],"grid":[[5,6],[7,8]],"texts":["9"]},{"id":"missing","grid":[[0,-1],[-2,0]],"texts":[]},{"id":"malformed","counts":[4,5,6],"grid":[[1,1],[1,1]],"texts":["1","x"]}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"fits" | "counts":[1.0,2.0,3.0] | "counts_converted":true | "grid":[[1.0,2.0],[3.0,4.0]] | "grid_rows":[[1,2],[3,4]] | "triple":[1,2,3] | "triple_converted":true | "numbers":[7,8] | "numbers_converted":true
      "id":"short" | "counts":[1.0,2.0] | "counts_converted":true | "grid":[[5.0,6.0],[7.0,8.0]] | "grid_rows":[[5,6],[7,8]] | "triple_converted":false | "numbers":[9] | "numbers_converted":true
      "id":"missing" | "counts_converted":false | "grid":[[0.0,-1.0],[-2.0,0.0]] | "grid_rows":[[0,-1],[-2,0]] | "triple_converted":false | "numbers":[] | "numbers_converted":true
      "id":"malformed" | "counts":[4.0,5.0,6.0] | "counts_converted":true | "grid":[[1.0,1.0],[1.0,1.0]] | "grid_rows":[[1,1],[1,1]] | "triple":[4,5,6] | "triple_converted":true | "numbers_converted":false
      "id":"fits" | "route":"strict" | "triple":[1,2,3] | "triple_converted":true | "numbers":[7,8]
      "id":"missing" | "route":"strict" | "triple_converted":false | "numbers":[]
      "input_id":"short" | cast_failed: cannot cast a list of length 2 to an ARRAY of length 3
      "input_id":"malformed" | cast_failed: cannot cast list element to Int64
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE JUNCTION convert_lists;
      """
    Then the last command output contains
      """
      counts = input.counts AS <f32_vec>,
      """
    And the last command output contains
      """
      grid = input.grid AS <f64_grid>,
      """
    And the last command output contains
      """
      grid_rows = input.grid AS <i64_rows>,
      """
    And the last command output contains
      """
      triple = TRY_CAST(input.counts AS <i64_triple>),
      """
    And the last command output contains
      """
      numbers = TRY_CAST(input.texts AS <i64_vec>)
      """

    Examples:
      | cluster_size | replica_count | i64_vec  | text_vec    | f32_vec  | i64_grid         | f64_grid         | i64_rows      | i64_triple    |
      | 1            | 0             | VEC<I64> | VEC<STRING> | VEC<F32> | ARRAY<I64, 2, 2> | ARRAY<F64, 2, 2> | VEC<VEC<I64>> | ARRAY<I64, 3> |
      | 3            | 0             | VEC<I64> | VEC<STRING> | VEC<F32> | ARRAY<I64, 2, 2> | ARRAY<F64, 2, 2> | VEC<VEC<I64>> | ARRAY<I64, 3> |

  # A CASE selects only scalar results, so each arm reduces the list it converts.
  Scenario Outline: A conditional arm converts a list only for the messages that select it
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA routed_list (
        id STRING,
        kind STRING,
        counts <i64_vec>
      );
      CREATE SCHEMA routed_total (
        id STRING,
        total I64 OPTIONAL,
        converted BOOL
      );
      CREATE SCHEMA routed_error (
        input_id STRING,
        error_message STRING
      );
      CREATE JAQ JSON CODEC routed_list_batch_codec
        TO SCHEMA routed_list
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY routed_lists SCHEMA routed_list UNBRANCHED;
      CREATE RELAY routed_totals SCHEMA routed_total UNBRANCHED;
      CREATE RELAY routed_errors SCHEMA routed_error UNBRANCHED;
      CREATE VHOST edge list-arms-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT routed_ingress ON edge PATH '/routed-lists';
      CREATE HTTP ENDPOINT INGESTOR routed_list_ingestor
        FROM routed_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING routed_list_batch_codec
        TO routed_lists
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_lists
        FROM routed_lists
        UNBRANCHED
        TO routed_totals
          SET id = input.id,
              total = CASE
                WHEN input.kind = 'tolerant' THEN sum(TRY_CAST(input.counts AS <i64_triple>))
                WHEN input.kind = 'strict' THEN sum(input.counts AS <i64_triple>)
                ELSE -1
              END,
              converted = NOT is_null(output.total)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO routed_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION routed_totals_subscription TO routed_totals;
      CREATE SUBSCRIPTION routed_errors_subscription TO routed_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "list-arms-{{test_id}}.example.com" path "/routed-lists"
      """
      [{"id":"tolerant-valid","kind":"tolerant","counts":[1,2,3]},{"id":"tolerant-invalid","kind":"tolerant","counts":[1,2]},{"id":"strict-valid","kind":"strict","counts":[4,5,6]},{"id":"strict-invalid","kind":"strict","counts":[4]},{"id":"unselected","kind":"other","counts":[9]}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"tolerant-valid" | "total":6 | "converted":true
      "id":"tolerant-invalid" | "converted":false
      "id":"strict-valid" | "total":15 | "converted":true
      "input_id":"strict-invalid" | cast_failed: cannot cast a list of length 1 to an ARRAY of length 3
      "id":"unselected" | "total":-1 | "converted":true
      """

    Examples:
      | cluster_size | replica_count | i64_vec  | i64_triple    |
      | 1            | 0             | VEC<I64> | ARRAY<I64, 3> |
      | 3            | 0             | VEC<I64> | ARRAY<I64, 3> |

  Scenario Outline: A converted list binds an inferencer input tensor
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA integer_grid (
        id STRING,
        rows <i64_rows>
      );
      CREATE SCHEMA transformed_grid (
        transformed <f32_grid>
      );
      CREATE JAQ JSON CODEC integer_grid_batch_codec
        TO SCHEMA integer_grid
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY integer_grids SCHEMA integer_grid UNBRANCHED;
      CREATE RELAY transformed_grids SCHEMA transformed_grid UNBRANCHED;
      CREATE VHOST edge list-inference-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/grids';
      CREATE HTTP ENDPOINT INGESTOR integer_grid_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING integer_grid_batch_codec
        TO integer_grids
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER transform_grid FROM integer_grids
        USING RESOURCE inference VERSION 1
        FILE 'models/matrix_identity.onnx'
        INPUTS { "matrix" <tensor_type>[2, 3] = input.rows AS <f32_grid> }
        OUTPUT SCHEMA { "transformed" <tensor_type>[2, 3] }
        UNBRANCHED
        TO transformed_grids
          SET transformed = transformed
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION transformed_grids_subscription TO transformed_grids;
      START;
      """
    And http payload is posted to node "node-1" with host "list-inference-{{test_id}}.example.com" path "/grids"
      """
      [{"id":"grid","rows":[[1,2,3],[4,-5,6]]}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      {"transformed":[[1.0,2.0,3.0],[4.0,-5.0,6.0]]}
      """

    Examples:
      | cluster_size | replica_count | i64_rows      | f32_grid         | tensor_type       |
      | 1            | 0             | VEC<VEC<I64>> | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |
      | 3            | 0             | VEC<VEC<I64>> | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |

  Scenario Outline: A list conversion that can never succeed is refused when it is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA list_source (
        id STRING,
        counts <i64_vec>,
        pair <i64_pair>,
        grid <i64_grid>,
        chunks <bytes_vec>
      );
      CREATE SCHEMA converted_value (
        id STRING,
        text STRING OPTIONAL,
        number I64 OPTIONAL,
        texts <text_vec> OPTIONAL,
        nested <i64_rows> OPTIONAL,
        triple <i64_triple> OPTIONAL,
        wide_grid <i64_wide_grid> OPTIONAL,
        payloads <bytes_vec> OPTIONAL
      );
      CREATE RELAY list_sources SCHEMA list_source UNBRANCHED;
      CREATE RELAY converted_values SCHEMA converted_value UNBRANCHED;
      """
    And these Plexus commands fail with "an ARRAY or VEC value converts only to an ARRAY or VEC type"
      """
      CREATE JUNCTION list_to_text FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, text = <open>input.counts AS STRING<close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "an ARRAY or VEC value converts only to an ARRAY or VEC type"
      """
      CREATE JUNCTION array_to_number FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, number = <open>input.pair AS I64<close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "only an ARRAY or VEC value converts to an ARRAY or VEC type"
      """
      CREATE JUNCTION text_to_list FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, texts = <open>input.id AS <text_vec><close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "an ARRAY or VEC value nested 1 deep converts only to a type nested 1 deep, found one nested 2 deep"
      """
      CREATE JUNCTION deeper_list FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, nested = <open>input.counts AS <i64_rows><close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "an ARRAY of length 2 never converts to an ARRAY of length 3"
      """
      CREATE JUNCTION longer_array FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, triple = <open>input.pair AS <i64_triple><close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "an ARRAY of length 3 never converts to an ARRAY of length 4"
      """
      CREATE JUNCTION wider_grid FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, wide_grid = <open>input.grid AS <i64_wide_grid><close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "BYTES conversions require bytes_from_utf8, bytes_to_utf8, base64 or hex functions"
      """
      CREATE JUNCTION bytes_to_text FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, texts = <open>input.chunks AS <text_vec><close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "BYTES conversions require bytes_from_utf8, bytes_to_utf8, base64 or hex functions"
      """
      CREATE JUNCTION numbers_to_bytes FROM list_sources UNBRANCHED
        TO converted_values SET id = input.id, payloads = <open>input.counts AS <bytes_vec><close>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | open      | close | i64_vec  | i64_pair      | i64_grid         | bytes_vec  | text_vec    | i64_rows      | i64_triple    | i64_wide_grid    |
      | 1            |           |       | VEC<I64> | ARRAY<I64, 2> | ARRAY<I64, 2, 3> | VEC<BYTES> | VEC<STRING> | VEC<VEC<I64>> | ARRAY<I64, 3> | ARRAY<I64, 2, 4> |
      | 3            |           |       | VEC<I64> | ARRAY<I64, 2> | ARRAY<I64, 2, 3> | VEC<BYTES> | VEC<STRING> | VEC<VEC<I64>> | ARRAY<I64, 3> | ARRAY<I64, 2, 4> |
      | 1            | TRY_CAST( | )     | VEC<I64> | ARRAY<I64, 2> | ARRAY<I64, 2, 3> | VEC<BYTES> | VEC<STRING> | VEC<VEC<I64>> | ARRAY<I64, 3> | ARRAY<I64, 2, 4> |
      | 3            | TRY_CAST( | )     | VEC<I64> | ARRAY<I64, 2> | ARRAY<I64, 2, 3> | VEC<BYTES> | VEC<STRING> | VEC<VEC<I64>> | ARRAY<I64, 3> | ARRAY<I64, 2, 4> |
