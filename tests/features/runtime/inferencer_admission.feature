Feature: Inferencer admission
  Every inferencer runs its model once for each input batch, when the batch is admitted and before
  any route. Every route receives that one result, and a route's FLUSH decides only when the route
  dispatches what it holds.

  # Each tenant posts three one-row batches. The input row carries an 8B tenant string and an 8B
  # feature pair, 16B in all, while the output row carries only an 8B score pair. The later route's
  # 24B limit is therefore reached by three outputs of one branch. Mapping the features to both
  # model inputs makes a one-row run triple them; a run over several rows would add their mean.
  Scenario Outline: Routes with different FLUSH policies receive the result of one model run
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, features <pair_type> );
      CREATE SCHEMA scored ( scores <pair_type> );
      CREATE SCHEMA later_scored ( later_scores <pair_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, features array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored_now SCHEMA scored BRANCHED BY by_tenant;
      CREATE RELAY scored_later SCHEMA later_scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-one-run-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING features_codec
        TO features
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER one_run_score FROM features
        USING RESOURCE inference VERSION 1
        FILE 'models/batch_score.onnx'
        INPUTS { "features" <tensor_type>[BATCH, 2] = input.features, "mask" <tensor_type>[BATCH, 2] = input.features }
        OUTPUT SCHEMA { "scores" <tensor_type>[BATCH, 2] }
        BRANCHED BY by_tenant
        TO scored_now
          SET scores = scores
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO scored_later
          SET later_scores = scores
          FLUSH EACH 1h MAX BATCH SIZE 24B
          ON MESSAGE ERROR LOG;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_now_subscription TO scored_now;
      CREATE SUBSCRIPTION scored_later_subscription TO scored_later;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "one_run_score" is saved as placeholder "inferencer_owner"
    When http payload is posted to host "infer-one-run-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[1.0,10.0]}
      """
    And http payload is posted to host "infer-one-run-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[100.0,1000.0]}
      """
    And http payload is posted to host "infer-one-run-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[2.0,20.0]}
      """
    And http payload is posted to host "infer-one-run-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[200.0,2000.0]}
      """
    And http payload is posted to host "infer-one-run-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[3.0,30.0]}
      """
    And http payload is posted to host "infer-one-run-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "scores":[3.0,30.0]
      key={"tenant":"acme"} | "scores":[6.0,60.0]
      key={"tenant":"acme"} | "scores":[9.0,90.0]
      key={"tenant":"beta"} | "scores":[300.0,3000.0]
      key={"tenant":"beta"} | "scores":[600.0,6000.0]
      key={"tenant":"beta"} | "scores":[900.0,9000.0]
      key={"tenant":"acme"} | "later_scores":[3.0,30.0]
      key={"tenant":"acme"} | "later_scores":[6.0,60.0]
      key={"tenant":"acme"} | "later_scores":[9.0,90.0]
      key={"tenant":"beta"} | "later_scores":[300.0,3000.0]
      key={"tenant":"beta"} | "later_scores":[600.0,6000.0]
      key={"tenant":"beta"} | "later_scores":[900.0,9000.0]
      """
    And node "{{inferencer_owner}}" observability metric "nervix_batches_total" with labels eventually equals 2
      """
      target_kind="INFERENCER"
      target="one_run_score"
      direction="sent"
      relay="scored_later"
      """

    Examples:
      | cluster_size | pair_type     | tensor_type       |
      | 1            | ARRAY<F32, 2> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | DENSE TENSOR<F32> |

  # The fixture reshapes its one-element score to the size its first feature names, so a first
  # feature of 2 makes the model refuse that row. The state ports are bound as ordinary tensors.
  Scenario Outline: A row the model refuses fails alone while the rows around it are delivered
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, id STRING, vector <pair_type> );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE SCHEMA rejected ( failed_id STRING, message STRING );
      CREATE JAQ JSON CODEC features_codec TO SCHEMA features
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE RELAY rejected SCHEMA rejected BRANCHED BY by_tenant;
      CREATE VHOST edge infer-row-refusal-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER checked FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/checked_running_sum.onnx'
        INPUTS {
          'features' <tensor_type>[2] = input.vector,
          'state_in' <tensor_type>[2] = [0 AS F32, 0 AS F32]
        }
        OUTPUT SCHEMA { 'score' <tensor_type>[1], 'state_out' <tensor_type>[2] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO rejected
            SET failed_id = input.id, message = error.message;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
      CREATE SUBSCRIPTION rejected_subscription TO rejected;
      START;
      """
    When http payload is posted to host "infer-row-refusal-{{test_id}}.example.com" path "/features"
      """
      [{"tenant":"acme","id":"acme-first","vector":[1.0,10.0]},
       {"tenant":"acme","id":"acme-refused","vector":[2.0,100.0]},
       {"tenant":"acme","id":"acme-last","vector":[1.0,30.0]}]
      """
    And http payload is posted to host "infer-row-refusal-{{test_id}}.example.com" path "/features"
      """
      [{"tenant":"beta","id":"beta-first","vector":[1.0,1000.0]},
       {"tenant":"beta","id":"beta-refused","vector":[2.0,2.0]},
       {"tenant":"beta","id":"beta-last","vector":[1.0,3000.0]}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"acme"} | "score":[31.0]
      key={"tenant":"beta"} | "score":[1001.0]
      key={"tenant":"beta"} | "score":[3001.0]
      key={"tenant":"acme"} | "failed_id":"acme-refused" | the ONNX model refused this row
      key={"tenant":"beta"} | "failed_id":"beta-refused" | the ONNX model refused this row
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | pair_type     | score_type    | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  # A score of zero makes the route's reciprocal non-finite, which fails that row's construction.
  Scenario Outline: A route failure sends the original input message to the route's error relay
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, id STRING, vector <pair_type> );
      CREATE SCHEMA ratios ( ratio F32 );
      CREATE SCHEMA rejected ( failed_id STRING );
      CREATE JAQ JSON CODEC features_codec TO SCHEMA features
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY ratios SCHEMA ratios BRANCHED BY by_tenant;
      CREATE RELAY rejected SCHEMA rejected BRANCHED BY by_tenant;
      CREATE VHOST edge infer-route-failure-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER ratio_score FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/simple_score.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO ratios SET ratio = 1 AS F32 / dot(score, score) FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO rejected SET failed_id = input.id;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION ratios_subscription TO ratios;
      CREATE SUBSCRIPTION rejected_subscription TO rejected;
      START;
      """
    When http payload is posted to host "infer-route-failure-{{test_id}}.example.com" path "/features"
      """
      [{"tenant":"acme","id":"acme-half","vector":[0.5,0.0]},
       {"tenant":"acme","id":"acme-zero","vector":[0.5,1.0]}]
      """
    And http payload is posted to host "infer-route-failure-{{test_id}}.example.com" path "/features"
      """
      [{"tenant":"beta","id":"beta-zero","vector":[1.0,1.75]},
       {"tenant":"beta","id":"beta-quarter","vector":[-0.5,0.0]}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "ratio":4.0
      key={"tenant":"beta"} | "ratio":16.0
      key={"tenant":"acme"} | "failed_id":"acme-zero"
      key={"tenant":"beta"} | "failed_id":"beta-zero"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | pair_type     | tensor_type       |
      | 1            | ARRAY<F32, 2> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | DENSE TENSOR<F32> |

  # Each input row accounts 24B: 8B for the tenant string and 8B for each feature pair. Collection
  # releases a branch's batch at its third row, and the model's batch-wide mean shows that one call
  # covered the whole collected batch.
  Scenario Outline: A BATCH model runs over the batch COLLECT FOR releases
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, features <pair_type>, mask <pair_type> );
      CREATE SCHEMA scored ( scores <pair_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, features array, mask array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-collected-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING features_codec
        TO features
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER collected_score FROM features
        COLLECT FOR 1h MAX BATCH SIZE 72B
        USING RESOURCE inference VERSION 1
        FILE 'models/batch_score.onnx'
        INPUTS { "features" <tensor_type>[BATCH, 2] = input.features, "mask" <tensor_type>[BATCH, 2] = input.mask }
        OUTPUT SCHEMA { "scores" <tensor_type>[BATCH, 2] }
        BRANCHED BY by_tenant
        TO scored
          SET scores = scores
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "collected_score" is saved as placeholder "inferencer_owner"
    When http payload is posted to host "infer-collected-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[1.0,10.0],"mask":[100.0,1000.0]}
      """
    And http payload is posted to host "infer-collected-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[100.0,1000.0],"mask":[1.0,10.0]}
      """
    And http payload is posted to host "infer-collected-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[2.0,20.0],"mask":[200.0,2000.0]}
      """
    And http payload is posted to host "infer-collected-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[200.0,2000.0],"mask":[2.0,20.0]}
      """
    And http payload is posted to host "infer-collected-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","features":[3.0,30.0],"mask":[300.0,3000.0]}
      """
    And http payload is posted to host "infer-collected-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","features":[300.0,3000.0],"mask":[3.0,30.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "scores":[103.0,1030.0]
      key={"tenant":"acme"} | "scores":[204.0,2040.0]
      key={"tenant":"acme"} | "scores":[305.0,3050.0]
      key={"tenant":"beta"} | "scores":[301.0,3010.0]
      key={"tenant":"beta"} | "scores":[402.0,4020.0]
      key={"tenant":"beta"} | "scores":[503.0,5030.0]
      """
    And node "{{inferencer_owner}}" observability metric "nervix_batches_total" with labels eventually equals 2
      """
      target_kind="INFERENCER"
      target="collected_score"
      direction="sent"
      relay="scored"
      """

    Examples:
      | cluster_size | pair_type     | tensor_type       |
      | 1            | ARRAY<F32, 2> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | DENSE TENSOR<F32> |
