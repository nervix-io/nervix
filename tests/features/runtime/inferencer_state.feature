Feature: Stateful inferencer tensors

  @stateful_inferencer
  Scenario: Stateful model diagnostics keep private values out of the server log under trace logging
    Given a nervix-server process is started with trace filter "trace"
    And an ONNX fixture resource directory "onnx_model" exists
    And the server process is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector ARRAY<F32, 2> SENSITIVE );
      CREATE SCHEMA scored ( score ARRAY<F32, 1> SENSITIVE );
      CREATE CODEC features_codec FROM JSON TO SCHEMA features
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE VHOST edge infer-private-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR feature_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/checked_running_sum.onnx'
        INPUTS { 'features' DENSE TENSOR<F32>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores TO scored;
      START;
      """
    When the server process eventually accepts http payload with host "infer-private-{{test_id}}.example.com" path "/features"
      """
      {"vector":[1099511627776.0,10.0]}
      """
    Then the server process log eventually contains "native ONNX diagnostic native_level=Error"
    When the server process receives SIGTERM
    Then the server process exits with status 0
    And the server process log does not contain "1099511627776"

  @inferencer_sensitivity
  Scenario Outline: Inferencer accepts sensitive outputs and explicit mapped-input declassification
    Given a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <vector_type> SENSITIVE );
      CREATE SCHEMA scored ( score <score_type> <sensitivity> );
      CREATE CODEC features_codec FROM JSON TO SCHEMA features
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE VHOST edge infer-sensitive-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR feature_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE '<model_file>'
        INPUTS { 'features' <tensor_type>[2] = <mapping> }
        <state_clause>
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores TO scored;
      START;
      """
    When http payload is posted to host "infer-sensitive-{{test_id}}.example.com" path "/features"
      """
      {"vector":[4.0,2.0]}
      """
    Then within "30s" the relay subscription receives a payload
      """
      <expected>
      """

    Examples:
      | model_file               | state_clause                                                             | mapping                      | sensitivity | expected | vector_type   | score_type    | tensor_type       |
      | models/simple_score.onnx |                                                                          | input.vector                 | SENSITIVE   | score    | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | models/running_sum.onnx  | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } | input.vector                 | SENSITIVE   | score    | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | models/simple_score.onnx |                                                                          | leak_sensitive(input.vector) |             | 2.125    | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | models/running_sum.onnx  | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } | leak_sensitive(input.vector) |             | 6        | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
  @inferencer_sensitivity
  Scenario Outline: Inferencer generated results retain the sensitivity of their mapped inputs
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <vector_type> SENSITIVE );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      """
    When these Plexus commands fail with "would store sensitive data in a non-sensitive output field"
      """
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE '<model_file>'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        <state_clause>
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    @inferencer_sensitivity_stateless
    Examples: Stateless inference
      | cluster_size | model_file               | state_clause | vector_type   | score_type    | tensor_type       |
      | 1            | models/simple_score.onnx |              | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | models/simple_score.onnx |              | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

    @stateful_inferencer
    Examples: Stateful inference
      | cluster_size | model_file              | state_clause                                                             | vector_type   | score_type    | tensor_type       |
      | 1            | models/running_sum.onnx | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | models/running_sum.onnx | STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer
  Scenario Outline: Stateful inference carries one ordered sum per branch across independent routes
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE SCHEMA audited ( model_score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE CODEC features_codec FROM WIRE JSON SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE RELAY audited SCHEMA audited BRANCHED BY by_tenant;
      CREATE VHOST edge infer-state-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR feature_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        TO audited SET model_score = score
          FLUSH EACH 250ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scored_subscription TO scored;
      CREATE SUBSCRIPTION audited_subscription TO audited;
      START;
      """
    When http payload is posted to host "infer-state-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to host "infer-state-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to host "infer-state-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to host "infer-state-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    And http payload is posted to host "infer-state-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to host "infer-state-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"acme"} | "score":[66.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"beta"} | "score":[3300.0]
      key={"tenant":"beta"} | "score":[6600.0]
      key={"tenant":"acme"} | "model_score":[11.0]
      key={"tenant":"acme"} | "model_score":[33.0]
      key={"tenant":"acme"} | "model_score":[66.0]
      key={"tenant":"beta"} | "model_score":[1100.0]
      key={"tenant":"beta"} | "model_score":[3300.0]
      key={"tenant":"beta"} | "model_score":[6600.0]
      """

    Examples:
      | cluster_size | input_type    | output_type   | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer
  Scenario Outline: Unbranched inference skips filtered and failed inputs without advancing its constant initial state
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( id STRING, amount STRING, other STRING, wanted BOOL );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE SCHEMA rejected ( failed_id STRING, message STRING );
      CREATE CODEC features_codec FROM JSON TO SCHEMA features
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE RELAY rejected SCHEMA rejected UNBRANCHED;
      CREATE VHOST edge infer-inputs-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR feature_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features FILTER WHERE input.wanted
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = [input.amount AS F32, input.other AS F32] }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2]
          INITIAL [1 AS F32, 10 AS F32] }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED
        TO scored SET score = score FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR SEND TO rejected
            SET failed_id = input.id, message = error.message;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
      CREATE SUBSCRIPTION rejected_subscription TO rejected;
      START;
      """
    When http payload is posted to host "infer-inputs-{{test_id}}.example.com" path "/features"
      """
      [{"id":"first","amount":"1","other":"10","wanted":true},
       {"id":"bad","amount":"invalid","other":"100","wanted":true},
       {"id":"filtered","amount":"1000","other":"10000","wanted":false},
       {"id":"last","amount":"2","other":"20","wanted":true}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "score":[22.0]
      "score":[44.0]
      "failed_id":"bad"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | tensor_type       | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 1> |

  @stateful_inferencer
  Scenario Outline: Stateful inference refuses an invalid state declaration before activation
    Given a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <vector_type> );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      """
    When these Plexus commands fail with "<expected_error>"
      """
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[<input_dimensions>] = input.vector }
        STATE { '<state_input>' FROM '<state_output>' <tensor_type>[<state_dimensions>]
          INITIAL <initial> }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | input_dimensions | state_input | state_output | state_dimensions | initial                              | expected_error      | tensor_type       | vector_type   | score_type    |
      | 2                | missing     | state_out    | 2                | ZEROS                                | missing ONNX input  | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | missing      | 2                | ZEROS                                | missing ONNX output | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | features    | state_out    | 2                | ZEROS                                | declared twice      | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | BATCH, 2         | state_in    | state_out    | 2                | ZEROS                                | BATCH with STATE    | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | state_out    | DYNAMIC          | ZEROS                                | fixed axes          | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | state_out    | 2                | [1 AS F32]                           | INITIAL             | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | state_out    | 2                | [1.0, 2.0]                           | INITIAL             | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | state_out    | 2                | input.vector                         | INITIAL             | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | state_out    | 2                | [length(uuid_v4()) AS F32, 0 AS F32] | deterministic       | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 2                | state_in    | state_out    | 3                | ZEROS                                | incompatible shape  | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |

  @stateful_inferencer
  Scenario Outline: A model refusal preserves the preceding state and routes the original input
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( id STRING, vector <vector_type> );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE SCHEMA rejected ( failed_id STRING );
      CREATE CODEC features_codec FROM JSON TO SCHEMA features
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE RELAY rejected SCHEMA rejected UNBRANCHED;
      CREATE VHOST edge infer-refusal-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR feature_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/checked_running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED TO scored SET score = score FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR SEND TO rejected SET failed_id = input.id;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
      CREATE SUBSCRIPTION rejected_subscription TO rejected;
      START;
      """
    # A first feature of 2 asks the fixture to reshape a one-element score into two elements.
    # Its state successor has been computed when that model invocation refuses the row.
    When http payload is posted to host "infer-refusal-{{test_id}}.example.com" path "/features"
      """
      [{"id":"first","vector":[1.0,10.0]},
       {"id":"bad","vector":[2.0,100.0]},
       {"id":"last","vector":[1.0,30.0]}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "score":[11.0]
      "score":[42.0]
      "failed_id":"bad"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |

  @stateful_inferencer
  Scenario: Stateful model ports must all have bindings
    Given a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector ARRAY<F32, 2> );
      CREATE SCHEMA scored ( score ARRAY<F32, 1> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      """
    When these Plexus commands fail with "state_in"
      """
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' DENSE TENSOR<F32>[2] = input.vector }
        OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

  @stateful_inferencer
  Scenario: SHOW CREATE preserves a stateful inferencer declaration through recreation
    Given a 1 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector ARRAY<F32, 2> );
      CREATE SCHEMA scored ( score ARRAY<F32, 1> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' DENSE TENSOR<F32>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2]
          INITIAL [1 AS F32, 10 AS F32] }
        OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then inferencer "running" is recreated from SHOW CREATE as "running_copy" with its state declaration intact
