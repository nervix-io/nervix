Feature: Composed sequence values
  Scenario Outline: Interleaved generator samples become normalized inference matrices on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      """
    And client "model_uploader" is connected to the leader node
    When client "model_uploader" uploads resource "inference" from "{{onnx_model}}" with identity "sequence-model"
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (tenant STRING, sample_id I64, value F64);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE SCHEMA history (
        values <pair_type>,
        neighborhoods <window_type>,
        sample_count I64,
        total F64,
        average F64,
        deviation F64
      );
      CREATE SCHEMA analysis (
        values <pair_type>,
        neighborhoods <window_type>,
        sample_count I64,
        total F64,
        average F64,
        deviation F64,
        list_deviation F64 OPTIONAL,
        normalized <flat_type>,
        matrix <matrix_type>
      );
      CREATE SCHEMA inferred (transformed <matrix_type>);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY samples SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY unique_samples SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE RELAY matrices SCHEMA analysis BRANCHED BY by_tenant;
      CREATE RELAY scores SCHEMA inferred BRANCHED BY by_tenant;
      CREATE VHOST edge sequence-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR read_samples FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec TIMESTAMP NOW
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR sample_readings USING MATERIALIZED STATE readings EACH 100ms
        BRANCHED BY by_tenant TO samples
          SET tenant = relay_state.readings.tenant,
              sample_id = relay_state.readings.sample_id,
              value = relay_state.readings.value
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR sample_once FROM samples
        DEDUPLICATE ON input.sample_id MAX TIME 5m
        BRANCHED BY by_tenant TO unique_samples INHERIT ALL
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR collect_samples FROM unique_samples
        WIDTH 2 MESSAGES STEP 2 MESSAGES BRANCHED BY by_tenant
        TO histories
          SET values = COLLECT_ARRAY(input.value),
              neighborhoods = COLLECT_ARRAY([input.value - 2.0, input.value, input.value + 2.0]),
              sample_count = COUNT(input.value),
              total = SUM(input.value),
              average = AVG(input.value),
              deviation = STDDEV_POP(input.value)
          ON MESSAGE ERROR LOG;
      CREATE JUNCTION normalize_samples FROM histories BRANCHED BY by_tenant
        TO matrices INHERIT ALL
          SET list_deviation = list_stddev_pop(input.values),
              normalized = list_div_scalar(list_sub_scalar(list_flatten(input.neighborhoods), input.average),
                                           coalesce(output.list_deviation, 1.0)),
              matrix = list_reshape(output.normalized AS <cast_type>, 2, 3)
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE INFERENCER read_matrix FROM matrices
        USING RESOURCE inference VERSION 1 FILE 'models/matrix_identity.onnx'
        INPUTS { 'matrix' <tensor_type>[2, 3] = input.matrix }
        OUTPUT SCHEMA { 'transformed' <tensor_type>[2, 3] }
        BRANCHED BY by_tenant TO scores SET transformed = transformed
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION accepted TO unique_samples;
      CREATE SUBSCRIPTION normalized TO matrices;
      CREATE SUBSCRIPTION inferred_scores TO scores;
      START;
      """
    # The generator may read the same materialized row several times. Feed the window
    # once per sample ID and wait for both branches before publishing the second IDs.
    When http payload is posted to node "node-1" with host "sequence-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","sample_id":1,"value":2.0},{"tenant":"beta","sample_id":1,"value":18.0}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "tenant":"acme" | "sample_id":1, | "value":2.0}
      key={"tenant":"beta"} | "tenant":"beta" | "sample_id":1, | "value":18.0}
      """
    When http payload is posted to node "node-1" with host "sequence-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"beta","sample_id":2,"value":10.0},{"tenant":"acme","sample_id":2,"value":6.0}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "tenant":"acme" | "sample_id":2, | "value":6.0}
      key={"tenant":"beta"} | "tenant":"beta" | "sample_id":2, | "value":10.0}
      key={"tenant":"acme"} | "values":[2.0,6.0] | "neighborhoods":[[0.0,2.0,4.0],[4.0,6.0,8.0]] | "sample_count":2, | "total":8.0, | "average":4.0, | "deviation":2.0, | "list_deviation":2.0, | "normalized":[-2.0,-1.0,0.0,0.0,1.0,2.0] | "matrix":[[-2.0,-1.0,0.0],[0.0,1.0,2.0]]
      key={"tenant":"beta"} | "values":[18.0,10.0] | "neighborhoods":[[16.0,18.0,20.0],[8.0,10.0,12.0]] | "sample_count":2, | "total":28.0, | "average":14.0, | "deviation":4.0, | "list_deviation":4.0, | "normalized":[0.5,1.0,1.5,-1.5,-1.0,-0.5] | "matrix":[[0.5,1.0,1.5],[-1.5,-1.0,-0.5]]
      key={"tenant":"acme"} | "transformed":[[-2.0,-1.0,0.0],[0.0,1.0,2.0]]
      key={"tenant":"beta"} | "transformed":[[0.5,1.0,1.5],[-1.5,-1.0,-0.5]]
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | pair_type     | window_type      | flat_type     | cast_type     | matrix_type      | tensor_type       |
      | 1            | ARRAY<F64, 2> | ARRAY<F64, 2, 3> | ARRAY<F64, 6> | ARRAY<F32, 6> | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F64, 2> | ARRAY<F64, 2, 3> | ARRAY<F64, 6> | ARRAY<F32, 6> | ARRAY<F32, 2, 3> | DENSE TENSOR<F32> |
