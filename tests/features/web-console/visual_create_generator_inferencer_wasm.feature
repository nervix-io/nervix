Feature: Web console visual generator, inferencer and WASM processor creation

  @visual_generator_branches
  Scenario Outline: A visually created generator emits each interleaved branch's materialized state on its domain cadence
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_generator_event (tenant STRING, amount I64);
      CREATE SCHEMA visual_generator_total (tenant STRING, total I64);
      CREATE SCHEMA visual_generator_key (tenant STRING);
      CREATE BRANCH visual_generator_tenant SCHEMA visual_generator_key TTL 5m;
      CREATE JAQ JSON CODEC visual_generator_codec TO SCHEMA visual_generator_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY visual_generator_state SCHEMA visual_generator_event BRANCHED BY visual_generator_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY visual_generator_plain SCHEMA visual_generator_event BRANCHED BY visual_generator_tenant;
      CREATE RELAY visual_generator_flat_state SCHEMA visual_generator_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY visual_generator_output SCHEMA visual_generator_total BRANCHED BY visual_generator_tenant;
      CREATE VHOST visual_generator_host generator-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_generator_endpoint ON visual_generator_host PATH '/state';
      CREATE HTTP ENDPOINT INGESTOR visual_generator_ingestor FROM visual_generator_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_generator_codec TIMESTAMP NOW
        TO visual_generator_state INHERIT ALL BRANCHED BY visual_generator_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='generator']" is clicked
    And selector ".create-name" is filled with "visual_generator_stage"
    # A generator reads no relay input: it has no input list, acknowledgement mode or node filter.
    Then selector ".create-processor-input" does not exist
    And selector ".create-processor-mode" does not exist
    And selector ".create-processor-filter" does not exist
    When selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_generator_tenant"
    Then selector ".create-processor-branch [data-value='visual_generator_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_generator_tenant']" is clicked
    # Only a materialized relay on exactly the generator's branch can feed it.
    And selector ".create-generator-relay .create-choice-search" is filled with "visual_generator_"
    Then selector ".create-generator-relay [data-value='visual_generator_state']" exists
    And selector ".create-generator-relay [data-value='visual_generator_plain']" does not exist
    And selector ".create-generator-relay [data-value='visual_generator_flat_state']" does not exist
    When selector ".create-generator-relay [data-value='visual_generator_state']" is clicked
    And selector ".create-generator-each" is filled with "100ms"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_generator_output"
    Then selector ".create-processor-route-relay [data-value='visual_generator_output']" exists
    # A generator route constructs its output with SET alone, from its state and branch.
    And selector ".create-processor-inherit" does not exist
    When selector ".create-processor-route-relay [data-value='visual_generator_output']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-output-fields [data-value='tenant']" exists
    When selector ".create-processor-output-fields [data-value='tenant']" is clicked
    And selector ".create-processor-branch-key-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-branch-key-fields [data-value='tenant']" exists
    When selector ".create-processor-branch-key-fields [data-value='tenant']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "total"
    Then selector ".create-processor-output-fields [data-value='total']" exists
    When selector ".create-processor-output-fields [data-value='total']" is clicked
    And selector ".create-generator-state-fields .create-choice-search" is filled with "amount"
    Then selector ".create-generator-state-fields [data-value='amount']" exists
    When selector ".create-generator-state-fields [data-value='amount']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE GENERATOR visual_generator_stage USING MATERIALIZED STATE visual_generator_state EACH 100ms BRANCHED BY visual_generator_tenant"
    And selector ".create-preview" contains "SET tenant = branch.tenant, total = relay_state.visual_generator_state.amount FLUSH IMMEDIATE ON MESSAGE ERROR LOG"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CREATE GENERATOR visual_generator_stage;
      """
    Then the last command output contains
      """
      USING MATERIALIZED STATE visual_generator_state
      """
    And the last command output contains
      """
      EACH 100ms
      """
    And the last command output contains
      """
      BRANCHED BY visual_generator_tenant
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_generator_subscription TO visual_generator_output;
      START;
      """
    # One input batch interleaves both branches; each branch generates from its own state.
    And http payload is posted to host "generator-{{test_id}}.example.com" path "/state"
      """
      [
        {"tenant":"a","amount":10},
        {"tenant":"b","amount":20}
      ]
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"a"} payload={"tenant":"a","total":10}
      key={"tenant":"b"} payload={"tenant":"b","total":20}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_inferencer_branches
  Scenario Outline: A visually created inferencer scores each interleaved branch with the model version it pinned
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has ONNX fixture resource directory "visual_onnx_model"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_infer_model;
      UPLOAD RESOURCE visual_infer_model VERSION '{{visual_onnx_model}}';
      CREATE SCHEMA visual_infer_feature (tenant STRING, vector <vector_type>);
      CREATE SCHEMA visual_infer_score (tenant STRING, score <score_type>);
      CREATE SCHEMA visual_infer_key (tenant STRING);
      CREATE BRANCH visual_infer_tenant SCHEMA visual_infer_key TTL 5m;
      CREATE WIRE JSON SCHEMA visual_infer_wire MODE STRICT (tenant string, vector array);
      CREATE WIRE JSON CODEC visual_infer_codec FROM WIRE SCHEMA visual_infer_wire TO SCHEMA visual_infer_feature;
      CREATE RELAY visual_infer_features SCHEMA visual_infer_feature BRANCHED BY visual_infer_tenant;
      CREATE RELAY visual_infer_scores SCHEMA visual_infer_score BRANCHED BY visual_infer_tenant;
      CREATE VHOST visual_infer_host infer-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_infer_endpoint ON visual_infer_host PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR visual_infer_ingestor FROM visual_infer_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_infer_codec
        TO visual_infer_features INHERIT ALL BRANCHED BY visual_infer_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='inferencer']" is clicked
    And selector ".create-name" is filled with "visual_infer_stage"
    And selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_infer_tenant"
    Then selector ".create-processor-branch [data-value='visual_infer_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_infer_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_infer_features"
    Then selector ".create-processor-input [data-value='visual_infer_features']" exists
    When selector ".create-processor-input [data-value='visual_infer_features']" is clicked
    And selector ".create-processor-resource .create-choice-search" is filled with "visual_infer_model"
    Then selector ".create-processor-resource [data-value='visual_infer_model']" exists
    When selector ".create-processor-resource [data-value='visual_infer_model']" is clicked
    Then selector ".create-processor-resource-version [data-value='1']" exists
    When selector ".create-processor-resource-version [data-value='1']" is clicked
    And selector ".create-processor-file" is filled with "models/simple_score.onnx"
    # Each INPUTS tensor declares its dense F32 shape and maps an expression over the live input.
    And selector ".create-inferencer-input-name" is filled with "features"
    And selector ".create-inferencer-input-shape .create-tensor-dimension-size" is filled with "2"
    And selector ".create-inferencer-input-fields .create-choice-search" is filled with "vector"
    Then selector ".create-inferencer-input-fields [data-value='vector']" exists
    When selector ".create-inferencer-input-fields [data-value='vector']" is clicked
    Then selector ".create-inferencer-input-expression" has value "input.vector"
    When selector ".create-inferencer-output-name" is filled with "score"
    And selector ".create-inferencer-output-shape .create-tensor-dimension-size" is filled with "1"
    Then selector ".create-inferencer-execution-mode" contains "per message"
    When selector ".create-processor-route-relay .create-choice-search" is filled with "visual_infer_scores"
    Then selector ".create-processor-route-relay [data-value='visual_infer_scores']" exists
    # An inferencer route reads its generated tensors and its branch, never the live input.
    And selector ".create-processor-inherit" does not exist
    When selector ".create-processor-route-relay [data-value='visual_infer_scores']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-output-fields [data-value='tenant']" exists
    When selector ".create-processor-output-fields [data-value='tenant']" is clicked
    And selector ".create-processor-branch-key-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-branch-key-fields [data-value='tenant']" exists
    When selector ".create-processor-branch-key-fields [data-value='tenant']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "score"
    Then selector ".create-processor-output-fields [data-value='score']" exists
    When selector ".create-processor-output-fields [data-value='score']" is clicked
    And selector ".create-inferencer-generated [data-tensor='score']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE INFERENCER visual_infer_stage FROM visual_infer_features ATTACHED USING RESOURCE visual_infer_model VERSION 1 FILE 'models/simple_score.onnx' INPUTS { 'features' <tensor_type>[2] = input.vector } OUTPUT SCHEMA { 'score' <tensor_type>[1] } BRANCHED BY visual_infer_tenant"
    And selector ".create-preview" contains "SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CREATE INFERENCER visual_infer_stage;
      """
    Then the last command output contains
      """
      USING RESOURCE visual_infer_model VERSION 1
      """
    And the last command output contains
      """
      'features' <tensor_type>[2] = input.vector
      """
    And the last command output contains
      """
      'score' <tensor_type>[1]
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_infer_subscription TO visual_infer_scores;
      START;
      """
    # The branches interleave; each scores its own features with the pinned model.
    And http payload is posted to host "infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"a","vector":[1.0,0.0]}
      """
    And http payload is posted to host "infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"b","vector":[0.0,1.0]}
      """
    And http payload is posted to host "infer-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"a","vector":[2.0,1.0]}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"a"} payload={"score":[0.875],"tenant":"a"}
      key={"tenant":"b"} payload={"score":[-0.375],"tenant":"b"}
      key={"tenant":"a"} payload={"score":[1.125],"tenant":"a"}
      """

    Examples:
      | cluster_size | vector_type   | score_type    | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @visual_wasm_branches
  Scenario Outline: A visually created WASM processor runs the uploaded guest in each interleaved branch
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has WASM processor fixture resource directory "visual_wasm_guest"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_wasm_bundle;
      UPLOAD RESOURCE visual_wasm_bundle VERSION '{{visual_wasm_guest}}';
      CREATE SCHEMA visual_wasm_event (value I32, tenant STRING);
      CREATE SCHEMA visual_wasm_tagged (value I32, tenant STRING, bucket STRING);
      CREATE SCHEMA visual_wasm_key (tenant STRING);
      CREATE BRANCH visual_wasm_tenant SCHEMA visual_wasm_key TTL 5m;
      CREATE WIRE JSON SCHEMA visual_wasm_wire MODE STRICT (value integer, tenant string);
      CREATE WIRE JSON CODEC visual_wasm_codec FROM WIRE SCHEMA visual_wasm_wire TO SCHEMA visual_wasm_event;
      CREATE RELAY visual_wasm_events SCHEMA visual_wasm_event BRANCHED BY visual_wasm_tenant;
      CREATE RELAY visual_wasm_tagged_events SCHEMA visual_wasm_tagged BRANCHED BY visual_wasm_tenant;
      CREATE VHOST visual_wasm_host wasm-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_wasm_endpoint ON visual_wasm_host PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR visual_wasm_ingestor FROM visual_wasm_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_wasm_codec
        TO visual_wasm_events INHERIT ALL BRANCHED BY visual_wasm_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='wasm-processor']" is clicked
    And selector ".create-name" is filled with "visual_wasm_stage"
    And selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_wasm_tenant"
    Then selector ".create-processor-branch [data-value='visual_wasm_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_wasm_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_wasm_events"
    Then selector ".create-processor-input [data-value='visual_wasm_events']" exists
    When selector ".create-processor-input [data-value='visual_wasm_events']" is clicked
    And selector ".create-processor-resource .create-choice-search" is filled with "visual_wasm_bundle"
    Then selector ".create-processor-resource [data-value='visual_wasm_bundle']" exists
    When selector ".create-processor-resource [data-value='visual_wasm_bundle']" is clicked
    # LATEST stays the requested version until the server applies the statement.
    Then selector ".create-processor-resource-version [data-value='LATEST']" exists
    When selector ".create-processor-resource-version [data-value='LATEST']" is clicked
    And selector ".create-processor-file" is filled with "processors/filter_even.wasm"
    And selector ".create-wasm-max-fuel" is filled with "1000000000"
    And selector ".create-wasm-max-memory" is filled with "64MiB"
    # A refused snapshot is preserved unless the processor opts in to one reset per refusal.
    Then selector ".create-wasm-rejected-state [data-policy='preserve'].active" exists
    And selector ".create-wasm-rejected-state-consequence" contains "keeps the snapshot"
    When selector ".create-wasm-rejected-state [data-policy='reset']" is clicked
    Then selector ".create-wasm-rejected-state-consequence" contains "initial state"
    When selector ".create-wasm-global-error [data-error='log']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_wasm_tagged_events"
    Then selector ".create-processor-route-relay [data-value='visual_wasm_tagged_events']" exists
    # The guest owns when a route emits, so a WASM route declares no flush and inherits nothing.
    And selector ".create-processor-flush" does not exist
    And selector ".create-processor-inherit" does not exist
    When selector ".create-processor-route-relay [data-value='visual_wasm_tagged_events']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "value"
    Then selector ".create-processor-output-fields [data-value='value']" exists
    When selector ".create-processor-output-fields [data-value='value']" is clicked
    And selector ".create-wasm-guest-fields .create-choice-search" is filled with "value"
    Then selector ".create-wasm-guest-fields [data-value='value']" exists
    When selector ".create-wasm-guest-fields [data-value='value']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-output-fields [data-value='tenant']" exists
    When selector ".create-processor-output-fields [data-value='tenant']" is clicked
    And selector ".create-wasm-guest-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-wasm-guest-fields [data-value='tenant']" exists
    When selector ".create-wasm-guest-fields [data-value='tenant']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "bucket"
    Then selector ".create-processor-output-fields [data-value='bucket']" exists
    When selector ".create-processor-output-fields [data-value='bucket']" is clicked
    And selector ".create-wasm-guest-fields .create-choice-search" is filled with "bucket"
    Then selector ".create-wasm-guest-fields [data-value='bucket']" exists
    When selector ".create-wasm-guest-fields [data-value='bucket']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "USING RESOURCE visual_wasm_bundle VERSION LATEST FILE 'processors/filter_even.wasm' MAX FUEL 1000000000 MAX MEMORY 64MiB BRANCHED BY visual_wasm_tenant"
    And selector ".create-preview" contains "SET value = value, tenant = tenant, bucket = bucket ON MESSAGE ERROR LOG ON REJECTED STATE RESET ON GLOBAL ERROR LOG"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CREATE WASM PROCESSOR visual_wasm_stage;
      """
    Then the last command output contains
      """
      USING RESOURCE visual_wasm_bundle VERSION 1
      """
    And the last command output contains
      """
      ON REJECTED STATE RESET
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_wasm_subscription TO visual_wasm_tagged_events;
      START;
      """
    # The guest keeps every second row of each branch, so each branch keeps its own second row.
    And http payload is posted to host "wasm-{{test_id}}.example.com" path "/events"
      """
      {"value":11,"tenant":"a"}
      """
    And http payload is posted to host "wasm-{{test_id}}.example.com" path "/events"
      """
      {"value":21,"tenant":"b"}
      """
    And http payload is posted to host "wasm-{{test_id}}.example.com" path "/events"
      """
      {"value":12,"tenant":"a"}
      """
    And http payload is posted to host "wasm-{{test_id}}.example.com" path "/events"
      """
      {"value":22,"tenant":"b"}
      """
    Then within "30s" the relay subscription receives payloads
      """
      key={"tenant":"a"} payload={"bucket":"EVEN","tenant":"a","value":12}
      key={"tenant":"b"} payload={"bucket":"EVEN","tenant":"b","value":22}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_generator_validation
  Scenario: A generator draft requires its branch, materialized relay, cadence and SET routes, and keeps itself through a refused input scope
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_check_event (amount I64);
      CREATE SCHEMA visual_check_total (total I64);
      CREATE RELAY visual_check_state SCHEMA visual_check_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY visual_check_totals SCHEMA visual_check_total UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='generator']" is clicked
    And selector ".create-name" is filled with "visual_check_generator"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "UNBRANCHED or a named branch"
    When selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "materialized relay"
    When selector ".create-generator-relay .create-choice-search" is filled with "visual_check_state"
    Then selector ".create-generator-relay [data-value='visual_check_state']" exists
    When selector ".create-generator-relay [data-value='visual_check_state']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "EACH"
    When selector ".create-generator-each" is filled with "soon"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "EACH"
    When selector ".create-generator-each" is filled with "250ms"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_check_totals"
    Then selector ".create-processor-route-relay [data-value='visual_check_totals']" exists
    When selector ".create-processor-route-relay [data-value='visual_check_totals']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "SET assignment"
    # A generator has no live input scope; validation refuses it and the draft survives the refusal.
    When selector ".create-processor-output-fields .create-choice-search" is filled with "total"
    Then selector ".create-processor-output-fields [data-value='total']" exists
    When selector ".create-processor-output-fields [data-value='total']" is clicked
    And selector ".create-processor-assignment-expression" is filled with "input.amount"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "visual_check_generator"
    And selector ".create-generator-each" has value "250ms"
    When selector ".create-processor-assignment-expression" is filled with ""
    And selector ".create-generator-state-fields .create-choice-search" is filled with "amount"
    Then selector ".create-generator-state-fields [data-value='amount']" exists
    When selector ".create-generator-state-fields [data-value='amount']" is clicked
    Then selector ".create-processor-assignment-expression" has value "relay_state.visual_check_state.amount"
    And selector ".create-preview" contains "USING MATERIALIZED STATE visual_check_state EACH 250ms UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CREATE GENERATOR visual_check_generator;
      """
    Then the last command output contains
      """
      EACH 250ms
      """
    And the last command output contains
      """
      SET total = relay_state.visual_check_state.amount
      """

  @visual_inferencer_state_validation
  Scenario: An inferencer draft checks tensor typing, keeps itself through an unreadable model file, and creates a stateful model
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has ONNX fixture resource directory "visual_onnx_state"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_state_model;
      UPLOAD RESOURCE visual_state_model VERSION '{{visual_onnx_state}}';
      CREATE SCHEMA visual_state_feature (vector ARRAY<F32, 2>);
      CREATE SCHEMA visual_state_score (score ARRAY<F32, 1>);
      CREATE RELAY visual_state_features SCHEMA visual_state_feature UNBRANCHED;
      CREATE RELAY visual_state_scores SCHEMA visual_state_score UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='inferencer']" is clicked
    And selector ".create-name" is filled with "visual_running_total"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_state_features"
    Then selector ".create-processor-input [data-value='visual_state_features']" exists
    When selector ".create-processor-input [data-value='visual_state_features']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Choose an existing resource"
    When selector ".create-processor-resource .create-choice-search" is filled with "visual_state_model"
    Then selector ".create-processor-resource [data-value='visual_state_model']" exists
    When selector ".create-processor-resource [data-value='visual_state_model']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "LATEST or a completed resource version"
    When selector ".create-processor-resource-version [data-value='1']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "FILE"
    When selector ".create-processor-file" is filled with "models/missing.onnx"
    And selector ".create-inferencer-input-name" is filled with "features"
    And selector ".create-inferencer-input-shape .create-tensor-dimension-size" is filled with "0"
    And selector ".create-inferencer-input-expression" is filled with "input.vector"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "positive"
    When selector ".create-inferencer-input-shape .create-tensor-dimension-size" is filled with "2"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "OUTPUT SCHEMA"
    When selector ".create-inferencer-output-name" is filled with "score"
    And selector ".create-inferencer-output-shape .create-tensor-dimension-size" is filled with "1"
    # A batched input beside a per-message output mixes the two execution modes.
    And selector ".create-inferencer-input-shape .create-tensor-add-dimension" is clicked
    And selector ".create-inferencer-input-shape .create-tensor-dimension:last-of-type [data-dimension-kind='batch']" is clicked
    Then selector ".create-inferencer-execution-mode" contains "mixes batched and per-message"
    When selector ".create-inferencer-input-shape .create-tensor-dimension:last-of-type .create-tensor-remove-dimension" is clicked
    Then selector ".create-inferencer-execution-mode" contains "per message"
    # State carries a fixed dense tensor from one row to the next of its branch.
    When selector ".create-inferencer-add-state" is clicked
    And selector ".create-inferencer-state-input" is filled with "state_in"
    And selector ".create-inferencer-state-output" is filled with "state_out"
    And selector ".create-inferencer-state-shape .create-tensor-dimension-size" is filled with "2"
    And selector ".create-inferencer-state-initial [data-initial='zeros']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_state_scores"
    Then selector ".create-processor-route-relay [data-value='visual_state_scores']" exists
    When selector ".create-processor-route-relay [data-value='visual_state_scores']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "score"
    Then selector ".create-processor-output-fields [data-value='score']" exists
    When selector ".create-processor-output-fields [data-value='score']" is clicked
    And selector ".create-inferencer-generated [data-tensor='score']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    # The model file is read when the inferencer is created; the refusal keeps the draft.
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "models/missing.onnx"
    And selector ".create-processor-file" has value "models/missing.onnx"
    And selector ".create-inferencer-state-input" has value "state_in"
    When selector ".create-processor-file" is filled with "models/running_sum.onnx"
    # Route construction cannot read the live input; validation refuses it and the draft stays.
    And selector ".create-processor-assignment-expression" is filled with "input.vector"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "visual_running_total"
    And selector ".create-processor-file" has value "models/running_sum.onnx"
    When selector ".create-processor-assignment-expression" is filled with ""
    And selector ".create-inferencer-generated [data-tensor='score']" is clicked
    Then selector ".create-preview" contains "INPUTS { 'features' DENSE TENSOR<F32>[2] = input.vector } STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS } OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] } UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CREATE INFERENCER visual_running_total;
      """
    Then the last command output contains
      """
      FILE 'models/running_sum.onnx'
      """
    And the last command output contains
      """
      'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS
      """

  @visual_wasm_validation
  Scenario: A WASM processor draft requires positive limits and its global error policy, and keeps itself through refused scopes and a missing guest
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has WASM processor fixture resource directory "visual_wasm_check"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE visual_check_bundle;
      UPLOAD RESOURCE visual_check_bundle VERSION '{{visual_wasm_check}}';
      CREATE SCHEMA visual_check_metric (value I32);
      CREATE RELAY visual_check_metrics SCHEMA visual_check_metric UNBRANCHED;
      CREATE RELAY visual_check_filtered SCHEMA visual_check_metric UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='wasm-processor']" is clicked
    And selector ".create-name" is filled with "visual_check_wasm"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_check_metrics"
    Then selector ".create-processor-input [data-value='visual_check_metrics']" exists
    When selector ".create-processor-input [data-value='visual_check_metrics']" is clicked
    And selector ".create-processor-resource .create-choice-search" is filled with "visual_check_bundle"
    Then selector ".create-processor-resource [data-value='visual_check_bundle']" exists
    When selector ".create-processor-resource [data-value='visual_check_bundle']" is clicked
    Then selector ".create-processor-resource-version [data-value='1']" exists
    When selector ".create-processor-resource-version [data-value='1']" is clicked
    And selector ".create-processor-file" is filled with "processors/missing.wasm"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MAX FUEL"
    When selector ".create-wasm-max-fuel" is filled with "0"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MAX FUEL"
    When selector ".create-wasm-max-fuel" is filled with "500000000"
    And selector ".create-wasm-max-memory" is filled with "0B"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MAX MEMORY"
    When selector ".create-wasm-max-memory" is filled with "32MiB"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_check_filtered"
    Then selector ".create-processor-route-relay [data-value='visual_check_filtered']" exists
    When selector ".create-processor-route-relay [data-value='visual_check_filtered']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "value"
    Then selector ".create-processor-output-fields [data-value='value']" exists
    When selector ".create-processor-output-fields [data-value='value']" is clicked
    And selector ".create-wasm-guest-fields .create-choice-search" is filled with "value"
    Then selector ".create-wasm-guest-fields [data-value='value']" exists
    When selector ".create-wasm-guest-fields [data-value='value']" is clicked
    Then selector ".create-processor-assignment-expression" has value "value"
    When selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "ON GLOBAL ERROR"
    When selector ".create-wasm-global-error [data-error='ignore']" is clicked
    And selector ".create-submit" is clicked
    # The guest module is compiled when the processor is created; the refusal keeps the draft.
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "processors/missing.wasm"
    And selector ".create-processor-file" has value "processors/missing.wasm"
    And selector ".create-wasm-max-memory" has value "32MiB"
    When selector ".create-processor-file" is filled with "processors/filter_even.wasm"
    # The live input is not a WASM route scope; validation refuses it and the draft stays.
    And selector ".create-processor-assignment-expression" is filled with "input.value"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "visual_check_wasm"
    And selector ".create-processor-file" has value "processors/filter_even.wasm"
    When selector ".create-processor-assignment-expression" is filled with "value"
    Then selector ".create-preview" contains "MAX FUEL 500000000 MAX MEMORY 32MiB UNBRANCHED"
    And selector ".create-preview" contains "ON REJECTED STATE PRESERVE ON GLOBAL ERROR IGNORE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed through the client on the leader node
      """
      SHOW CREATE WASM PROCESSOR visual_check_wasm;
      """
    Then the last command output contains
      """
      MAX MEMORY 32MiB
      """
    And the last command output contains
      """
      ON GLOBAL ERROR IGNORE
      """

  @visual_inferencer_stale_scope
  Scenario: An inferencer draft requires its input, resource and version again after its domain changes
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the active domain is saved as placeholder "home"
    And node "node-1" has ONNX fixture resource directory "visual_onnx_stale"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{home}};
      CREATE UNPACED DOMAIN {{home}}_other;
      CREATE RESOURCE visual_stale_model;
      UPLOAD RESOURCE visual_stale_model VERSION '{{visual_onnx_stale}}';
      CREATE SCHEMA visual_stale_feature (vector ARRAY<F32, 2>);
      CREATE SCHEMA visual_stale_score (score ARRAY<F32, 1>);
      CREATE RELAY visual_stale_features SCHEMA visual_stale_feature UNBRANCHED;
      CREATE RELAY visual_stale_scores SCHEMA visual_stale_score UNBRANCHED;
      """
    Given the active domain is "{{home}}_other"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE visual_stale_model;
      UPLOAD RESOURCE visual_stale_model VERSION '{{visual_onnx_stale}}';
      CREATE SCHEMA visual_stale_feature (vector ARRAY<F32, 2>);
      CREATE SCHEMA visual_stale_score (score ARRAY<F32, 1>);
      CREATE RELAY visual_stale_features SCHEMA visual_stale_feature UNBRANCHED;
      CREATE RELAY visual_stale_scores SCHEMA visual_stale_score UNBRANCHED;
      """
    Given the active domain is "{{home}}"
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{home}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='inferencer']" is clicked
    And selector ".create-name" is filled with "visual_stale_inferencer"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_stale_features"
    Then selector ".create-processor-input [data-value='visual_stale_features']" exists
    When selector ".create-processor-input [data-value='visual_stale_features']" is clicked
    And selector ".create-processor-resource .create-choice-search" is filled with "visual_stale_model"
    Then selector ".create-processor-resource [data-value='visual_stale_model']" exists
    When selector ".create-processor-resource [data-value='visual_stale_model']" is clicked
    Then selector ".create-processor-resource-version [data-value='1']" exists
    When selector ".create-processor-resource-version [data-value='1']" is clicked
    And selector ".create-processor-file" is filled with "models/simple_score.onnx"
    And selector ".create-inferencer-input-name" is filled with "features"
    And selector ".create-inferencer-input-shape .create-tensor-dimension-size" is filled with "2"
    And selector ".create-inferencer-input-expression" is filled with "input.vector"
    And selector ".create-inferencer-output-name" is filled with "score"
    And selector ".create-inferencer-output-shape .create-tensor-dimension-size" is filled with "1"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_stale_scores"
    Then selector ".create-processor-route-relay [data-value='visual_stale_scores']" exists
    When selector ".create-processor-route-relay [data-value='visual_stale_scores']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "score"
    Then selector ".create-processor-output-fields [data-value='score']" exists
    When selector ".create-processor-output-fields [data-value='score']" is clicked
    And selector ".create-inferencer-generated [data-tensor='score']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE INFERENCER visual_stale_inferencer"
    When selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{home}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='inferencer']" is clicked
    Then selector ".create-scope" contains "{{home}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{home}}_other"
    And selector ".create-name" has value "visual_stale_inferencer"
    And selector ".create-processor-file" has value "models/simple_score.onnx"
    # Every reference the draft selected belongs to the previous domain, so each must be chosen again.
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "Input 1: the relay belongs to a changed context"
    When selector ".create-processor-input .create-choice-search" is filled with "visual_stale_features"
    Then selector ".create-processor-input [data-value='visual_stale_features']" exists
    When selector ".create-processor-input [data-value='visual_stale_features']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "The selected resource belongs to a changed context"
    When selector ".create-processor-resource .create-choice-search" is filled with "visual_stale_model"
    Then selector ".create-processor-resource [data-value='visual_stale_model']" exists
    When selector ".create-processor-resource [data-value='visual_stale_model']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "select the version again"
    When selector ".create-processor-resource-version [data-value='1']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "output relay belongs to a changed context"
    And selector ".terminal" does not contain "visual_stale_inferencer"
