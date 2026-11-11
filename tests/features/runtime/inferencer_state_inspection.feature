Feature: Bounded typed inferencer state inspection

  @inferencer_state_inspection @deloxide_stress
  Scenario Outline: Stateful inference publishes typed tensor inspection pages
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, hint STRING <input_sensitivity>, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, hint string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING <branch_sensitivity> );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-lifecycle-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    Given client "reader" is connected to node "node-1"
    When client "reader" executes these Plexus commands
      """
      SHOW INFERENCER running STATE FORMAT JSON;
      """
    Then the last processor state counts 0 rows in 0 branches
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","hint":"confidential","vector":[1.0,10.0]}
      """
    Then within "30s" client "reader" observes a new publication of inferencer "running" for tenant "acme"
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","hint":"confidential","vector":[100.0,1000.0]}
      """
    Then within "30s" client "reader" observes a new publication of inferencer "running" for tenant "beta"
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","hint":"confidential","vector":[2.0,20.0]}
      """
    Then within "30s" client "reader" observes a new publication of inferencer "running" for tenant "acme"
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","hint":"confidential","vector":[200.0,2000.0]}
      """
    Then within "30s" client "reader" observes a new publication of inferencer "running" for tenant "beta"
    And within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"<first_branch>"} | "score":[11.0]
      key={"tenant":"<second_branch>"} | "score":[1100.0]
      key={"tenant":"<first_branch>"} | "score":[33.0]
      key={"tenant":"<second_branch>"} | "score":[3300.0]
      """
    When client "reader" executes these Plexus commands
      """
      SHOW INFERENCER running STATE FORMAT JSON;
      """
    Then the last processor state counts 2 rows in 2 branches
    And the last inference headers identify each returned tensor publication
    And the last processor state section "tensor_1" rows match
      """
      {"branch":{"tenant":"<first_branch>"},"fields":{"input":"state_in","output":"state_out","shape":[2],"values":<first_values>}}
      {"branch":{"tenant":"<second_branch>"},"fields":{"input":"state_in","output":"state_out","shape":[2],"values":<second_values>}}
      """
    And the last inspection JSON is the complete typed envelope

    And client "reader" reads inferencer "running" state one row per page with unchanged complete counts
    When inspection client "reader" connects away from the last relay owner when possible
    And client "reader" executes these Plexus commands
      """
      SHOW INFERENCER running STATE FOR BRANCH KEY { tenant = 'beta' } LIMIT 1 FORMAT JSON;
      """
    Then the last processor state counts 1 rows in 1 branches
    And the typed inspection used its unit owner and a remote page when the cluster permits it
    And the last processor state section "tensor_1" rows match
      """
      {"branch":{"tenant":"<second_branch>"},"fields":{"input":"state_in","output":"state_out","shape":[2],"values":<second_values>}}
      """
    When client "reader" saves a one row page of inferencer "running" state
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","hint":"confidential","vector":[3.0,30.0]}
      """
    Then within "30s" client "reader" observes a new publication of inferencer "running" for tenant "acme"
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "score":[66.0]
      """
    When client "reader" resumes the saved inferencer "running" state page
    Then the typed inspection refuses the read with "stale_continuation"
    When client "reader" submits this Plexus command request
      """
      SHOW INFERENCER absent STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "unknown_entity"

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       | input_sensitivity | branch_sensitivity | first_branch | second_branch | first_values | second_values  |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |                   |                    | acme         | beta          | [3.0,30.0]   | [300.0,3000.0] |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |                   |                    | acme         | beta          | [3.0,30.0]   | [300.0,3000.0] |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | SENSITIVE         |                    | acme         | beta          | "<masked>"   | "<masked>"     |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | SENSITIVE         |                    | acme         | beta          | "<masked>"   | "<masked>"     |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |                   | SENSITIVE          | <masked>     | <masked>      | "<masked>"   | "<masked>"     |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |                   | SENSITIVE          | <masked>     | <masked>      | "<masked>"   | "<masked>"     |
