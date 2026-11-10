Feature: Stateful inferencer state follows its branches across owners

  A planned handoff carries what each branch published last, a forced recovery stages the newest
  surviving checkpoint or reports the branch it had to reset, and a graceful stop writes the final
  generation. Every scenario interleaves two branches, so a sum that moved, reset or regressed is
  visible per branch.

  @stateful_inferencer_handoff @inferencer_handoff_capture @deloxide_stress_restore
  Scenario: Relocating a stateful inferencer moves both branch sums exactly
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these Plexus commands are executed on the leader node
      """
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
      CREATE VHOST edge infer-relocate-{{test_id}}.example.com;
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
      START;
      """
    And these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=inferencer name=running owner=node-1
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_before TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-relocate-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-relocate-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-relocate-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-relocate-{{test_id}}.example.com" path "/features"
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
      RELOCATE INFERENCER running ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 1 of 1 execution unit(s) onto node 'node-2'
      """
    And the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these Plexus commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=inferencer name=running owner=node-2 replicas=- transition_from=node-1 state_recovery=complete
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-relocate-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "node-1" with host "infer-relocate-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[7700.0]
      """

  @stateful_inferencer_handoff @inferencer_handoff_capture @deloxide_stress_restore
  Scenario: A drained owner hands both inferencer branch sums to its successor
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When leadership is transferred to node "node-1"
    And these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-1;
      CORDON NODE node-3;
      """
    And these Plexus commands are executed on the leader node
      """
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
      CREATE VHOST edge infer-drain-{{test_id}}.example.com;
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
      START;
      """
    And these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-3;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=inferencer name=running owner=node-2
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_before TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-drain-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-drain-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-drain-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-drain-{{test_id}}.example.com" path "/features"
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
    When node "node-2" is gracefully stopped
    Then the last shutdown of node "node-2" reports its drain-support phase "Completed"
    When these Plexus commands are executed on the active session
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "running" is saved as placeholder "drain_destination"
    And the last command output contains
      """
      - domain={{domain}} kind=inferencer name=running owner={{drain_destination}} replicas=- transition_from=node-2 state_recovery=complete
      """
    When http payload is posted to node "node-1" with host "infer-drain-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "node-1" with host "infer-drain-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[7700.0]
      """

  @stateful_inferencer_handoff
  Scenario Outline: An entity-paused alteration upstream of a stateful inferencer keeps both branch sums
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( tenant STRING, score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-quiesce-{{test_id}}.example.com;
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
        TO scored SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    And http payload is posted to node "node-1" with host "infer-quiesce-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-quiesce-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-quiesce-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-quiesce-{{test_id}}.example.com" path "/features"
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
      ALTER HTTP ENDPOINT INGESTOR feature_ingestor SET QUIESCE BUFFER MAX SIZE 2MiB;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "infer-quiesce-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "node-1" with host "infer-quiesce-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[7700.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @stateful_inferencer_handoff
  Scenario: Losing the owner without a replica restarts both branches from INITIAL and reports the lost lifecycle
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
      CREATE VHOST edge infer-lost-{{test_id}}.example.com;
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
    And http payload is posted to node "node-1" with host "infer-lost-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lost-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lost-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-lost-{{test_id}}.example.com" path "/features"
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
    Then the last cluster status owner for scheduled "inferencer" "running" is saved as placeholder "failed_owner"
    And a node other than placeholder "failed_owner" is saved as placeholder "surviving_node"
    When node "{{failed_owner}}" is stopped
    Then node "{{surviving_node}}" eventually observes a stable leader
    And within "60s" node "{{surviving_node}}" eventually reports scheduled "inferencer" "running" owner different from placeholder "failed_owner"
    When these Plexus commands are executed through the client on node "{{surviving_node}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      transition_from={{failed_owner}} state_recovery=reset resets=
      """
    And the last command output contains
      """
      branch_lifecycle:missing_checkpoint
      """
    And the last command output contains
      """
      inferencer:expired_branch_metadata
      """
    When these Plexus commands are executed on node "{{surviving_node}}"
      """
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "{{surviving_node}}" with host "infer-lost-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "{{surviving_node}}" with host "infer-lost-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[44.0]
      key={"tenant":"beta"} | "score":[4400.0]
      """

  @stateful_inferencer_handoff @inferencer_handoff_capture @deloxide_stress_restore
  Scenario: A returning former owner is fenced and receives its successor's sums when ownership moves back
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
      CREATE VHOST edge infer-fence-{{test_id}}.example.com;
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
    And http payload is posted to node "node-1" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-fence-{{test_id}}.example.com" path "/features"
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
    Then the last cluster status owner for scheduled "inferencer" "running" is saved as placeholder "former_owner"
    And the first replica for scheduled "inferencer" "running" in the last cluster status is saved as placeholder "successor"
    Given inferencer "running" has complete checkpoints for both branches on replica "{{successor}}" of owner "{{former_owner}}"
    When node "{{former_owner}}" is stopped
    Then node "{{successor}}" eventually observes a stable leader
    And within "60s" node "{{successor}}" eventually reports scheduled "inferencer" "running" owner equals placeholder "successor"
    When these Plexus commands are executed on node "{{successor}}"
      """
      CREATE SUBSCRIPTION promoted TO scored;
      """
    And http payload is posted to node "{{successor}}" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "{{successor}}" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[7700.0]
      """
    When node "{{former_owner}}" is started
    Then node "{{former_owner}}" eventually observes a stable leader
    When these Plexus commands are executed through the client on node "{{successor}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=inferencer name=running owner={{successor}}
      """
    When these Plexus commands are executed on node "{{successor}}"
      """
      CREATE SUBSCRIPTION fenced TO scored;
      """
    And http payload is posted to node "{{successor}}" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[8.0,80.0]}
      """
    And http payload is posted to node "{{successor}}" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[800.0,8000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[165.0]
      key={"tenant":"beta"} | "score":[16500.0]
      """
    When these Plexus commands are executed on node "{{successor}}"
      """
      RELOCATE INFERENCER running ONTO NODE {{former_owner}} IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 1 of 1 execution unit(s) onto node '{{former_owner}}'
      """
    When these Plexus commands are executed on node "{{successor}}"
      """
      CREATE SUBSCRIPTION returned TO scored;
      """
    And http payload is posted to node "{{successor}}" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[16.0,160.0]}
      """
    And http payload is posted to node "{{successor}}" with host "infer-fence-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[1600.0,16000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[341.0]
      key={"tenant":"beta"} | "score":[34100.0]
      """

  @stateful_inferencer_handoff
  Scenario Outline: A graceful cluster stop writes the final inference generation of both branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( tenant STRING, score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-stop-{{test_id}}.example.com;
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
        TO scored SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    And http payload is posted to node "node-1" with host "infer-stop-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-stop-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-stop-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-stop-{{test_id}}.example.com" path "/features"
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
      """
    And the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "node-1" with host "infer-stop-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      """
    And http payload is posted to node "node-1" with host "infer-stop-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[400.0,4000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[77.0]
      key={"tenant":"beta"} | "score":[7700.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
