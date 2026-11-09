Feature: Stateful inference resource and declaration identity

  @stateful_inferencer_rebinding
  Scenario Outline: Rebinding resets both branches while uploads, unchanged versions and refused models preserve them
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
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
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 1h;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-rebind-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2]
          INITIAL [1 AS F32, 10 AS F32] }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores TO scored;
      START;
      """
    When http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[22.0]
      key={"tenant":"beta"} | "score":[1111.0]
      """
    When this Plexus command request is executed on the leader node
      """
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE INFERENCER running;
      """
    Then the last command output contains
      """
      USING RESOURCE inference VERSION 1
      """
    When http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[44.0]
      key={"tenant":"beta"} | "score":[3311.0]
      """
    When these Plexus commands are executed on the leader node
      """
      REBIND RESOURCE inference TO VERSION 1 FOR INFERENCER running;
      """
    Then the last command output contains
      """
      unchanged
      """
    When http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[6611.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores;
      STOP;
      START;
      CREATE SUBSCRIPTION scores TO scored;
      """
    Given node "node-1" has ONNX fixture resource directory "incompatible_model" with "models/running_sum.onnx" copied from fixture "simple_score.onnx"
    When this Plexus command request is executed on the leader node
      """
      UPLOAD RESOURCE inference VERSION '{{incompatible_model}}';
      """
    And these Plexus commands fail on the active session
      """
      REBIND RESOURCE inference TO VERSION 3 FOR INFERENCER running;
      """
    Then the last command error contains
      """
      binding validation failed
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INFERENCER running;
      """
    Then the last command output contains
      """
      USING RESOURCE inference VERSION 1
      """
    When http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[121.0]
      key={"tenant":"beta"} | "score":[11011.0]
      """
    When these Plexus commands are executed on the leader node
      """
      REBIND RESOURCE inference TO VERSION 2 FOR INFERENCER running;
      """
    Then the last command output contains
      """
      ENTITY_PAUSE
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INFERENCER running;
      """
    Then the last command output contains
      """
      USING RESOURCE inference VERSION 2
      """
    When http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[5.0,50.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[500.0,5000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[66.0]
      key={"tenant":"beta"} | "score":[5511.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores;
      """
    And the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_restarted TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[6.0,60.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[600.0,6000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[132.0]
      key={"tenant":"beta"} | "score":[12111.0]
      """
    When these Plexus commands are executed on the leader node
      """
      REBIND RESOURCE inference TO VERSION 1 FOR INFERENCER running;
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[7.0,70.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[700.0,7000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[88.0]
      key={"tenant":"beta"} | "score":[7711.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_restarted;
      DROP INFERENCER running;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2]
          INITIAL [10 AS F32, 100 AS F32] }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores_redeclared TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[7.0,70.0]}
      """
    And http payload is posted to node "node-1" with host "infer-rebind-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[700.0,7000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[187.0]
      key={"tenant":"beta"} | "score":[7810.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
