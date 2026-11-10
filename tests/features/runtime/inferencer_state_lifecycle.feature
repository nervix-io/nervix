Feature: Stateful inference state lifetimes

  @stateful_inferencer_persistence @deloxide_stress_restore
  Scenario Outline: Stateful inference resumes both interleaved branch sums after STOP and START
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
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      STOP;
      START;
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[66.0]
      key={"tenant":"beta"} | "score":[6600.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer_persistence @deloxide_stress_restore
  Scenario Outline: Recreating an inferencer starts both branch sums from INITIAL
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
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      DROP INFERENCER running;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer_persistence @deloxide_stress_restore
  Scenario Outline: TTL eviction starts returning inferencer branches from INITIAL
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
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
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30s;
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
    When http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    Then within "60s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY features WHERE (tenant = 'acme');
      """
    And within "60s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY features WHERE (tenant = 'beta');
      """
    Then within "60s" inferencer "running" has no retained branch for tenant "acme"
    Then within "60s" inferencer "running" has no retained branch for tenant "beta"
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      STOP;
      START;
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lifecycle-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer_persistence @deloxide_stress_restore
  Scenario Outline: Capacity eviction cannot restore a previous inferencer lifetime after restart
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
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m MAX INSTANCES 2 EVICT LRU;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-capacity-{{test_id}}.example.com;
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
    When http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    When http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[1.0,1.0]}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "score":[3302.0]
      """
    When http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"gamma","vector":[7.0,70.0]}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"gamma"} | "score":[77.0]
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY features WHERE (tenant = 'acme');
      """
    Then within "60s" inferencer "running" has no retained branch for tenant "acme"
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      STOP;
      START;
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-capacity-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[33.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer_persistence @deloxide_stress_restore
  Scenario: A promoted inference replica continues both branches from at least their confirmed checkpoints
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, vector ARRAY<F32, 2> );
      CREATE SCHEMA scored ( tenant STRING, score ARRAY<F32, 1> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-persist-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' DENSE TENSOR<F32>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
        BRANCHED BY by_tenant
        TO scored SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    When http payload is posted to node "node-1" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "running" is saved as placeholder "infer_owner"
    And the first replica for scheduled "inferencer" "running" in the last cluster status is saved as placeholder "infer_replica"
    Given inferencer "running" has complete checkpoints for both branches on replica "{{infer_replica}}" of owner "{{infer_owner}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION intervening TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "node-1" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[7700.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION intervening;
      """
    And node "{{infer_owner}}" is stopped
    Then node "{{infer_replica}}" eventually observes a stable leader
    And within "60s" node "{{infer_replica}}" eventually reports scheduled "inferencer" "running" owner equals placeholder "infer_replica"
    When these Plexus commands are executed on node "{{infer_replica}}"
      """
      CREATE SUBSCRIPTION recovered TO scored
        WHERE (input.tenant = 'acme' AND first(input.score) >= (66.0 AS F32))
           OR (input.tenant = 'beta' AND first(input.score) >= (6600.0 AS F32));
      """
    And http payload is posted to node "{{infer_replica}}" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "{{infer_replica}}" with host "infer-persist-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":
      key={"tenant":"beta"} | "score":
      """

  @stateful_inferencer_bound
  Scenario Outline: An inferencer whose declared state exceeds a snapshot section is refused before storage
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      """
    When these Plexus commands fail with "exceeding one configured snapshot section (8388608 bytes)"
      """
      CREATE INFERENCER oversized FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2097152] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the last command error contains
      """
      INFERENCER 'oversized'
      """
    When these Plexus commands fail with "does not exist"
      """
      SHOW CREATE INFERENCER oversized;
      """

    Examples:
      | cluster_size | input_type    | output_type   | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
