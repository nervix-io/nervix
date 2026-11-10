@stateful_inferencer_acknowledgement
Feature: Stateful inferencer acknowledgements

  Scenario Outline: A stateful inferencer acknowledges every record of the batches it admits
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And Kafka topic "infer_ack_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "infer_ack_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( tenant STRING, score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE WIRE JSON SCHEMA scored_wire MODE STRICT ( tenant string, score array );
      CREATE WIRE JSON CODEC scored_codec FROM WIRE SCHEMA scored_wire TO SCHEMA scored;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT infer_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR feature_ingestor
        FROM infer_kafka TOPIC 'infer_ack_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'infer_ack_group_{{test_id}}' START AT EARLIEST
          MODE ACK PARALLEL MAX 32 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER scored_emitter
        FROM scored
        ATTACHED
        TO infer_kafka TOPIC 'infer_ack_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING scored_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Kafka messages are rapidly published to topic "infer_ack_in_{{test_id}}"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      {"tenant":"beta","vector":[100.0,1000.0]}
      {"tenant":"acme","vector":[2.0,20.0]}
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the observed broker receives payloads
      """
      "tenant":"acme","score":[33.0]
      "tenant":"beta","score":[3300.0]
      """
    And within "30s" Kafka consumer group "infer_ack_group_{{test_id}}" next offset for topic "infer_ack_in_{{test_id}}" partition 0 is "at least 4"

    Examples:
      | cluster_size | input_type    | output_type   | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @inferencer_promoted_acknowledgement @deloxide_stress_restore
  Scenario: Records a promoted inference replica emits are acknowledged to their source
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And Kafka topic "infer_ack_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "infer_ack_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector ARRAY<F32, 2> );
      CREATE SCHEMA scored ( tenant STRING, score ARRAY<F32, 1> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE WIRE JSON SCHEMA scored_wire MODE STRICT ( tenant string, score array );
      CREATE WIRE JSON CODEC scored_codec FROM WIRE SCHEMA scored_wire TO SCHEMA scored;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT infer_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR feature_ingestor
        FROM infer_kafka TOPIC 'infer_ack_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'infer_ack_group_{{test_id}}' START AT EARLIEST
          MODE ACK PARALLEL MAX 32 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' DENSE TENSOR<F32>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' DENSE TENSOR<F32>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' DENSE TENSOR<F32>[1] }
        BRANCHED BY by_tenant
        TO scored SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER scored_emitter
        FROM scored
        ATTACHED
        TO infer_kafka TOPIC 'infer_ack_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING scored_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR feature_ingestor ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY features, INFERENCER running, RELAY scored, EMITTER scored_emitter ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "inferencer" "running" is saved as placeholder "infer_owner"
    And the first replica for scheduled "inferencer" "running" in the last cluster status is saved as placeholder "infer_replica"
    And the last command output contains
      """
      - domain={{domain}} kind=ingestor name=feature_ingestor unit=instance:0 owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=inferencer name=running owner=node-3
      """
    When these Kafka messages are rapidly published to topic "infer_ack_in_{{test_id}}"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      {"tenant":"beta","vector":[100.0,1000.0]}
      {"tenant":"acme","vector":[2.0,20.0]}
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "30s" the observed broker receives payloads
      """
      "tenant":"acme","score":[33.0]
      "tenant":"beta","score":[3300.0]
      """
    And within "30s" Kafka consumer group "infer_ack_group_{{test_id}}" next offset for topic "infer_ack_in_{{test_id}}" partition 0 is "at least 4"
    Given inferencer "running" has complete checkpoints for both branches on replica "{{infer_replica}}" of owner "{{infer_owner}}"
    When node "{{infer_owner}}" is stopped
    Then within "60s" node "node-1" eventually reports scheduled "inferencer" "running" owner equals placeholder "infer_replica"
    When these Kafka messages are rapidly published to topic "infer_ack_in_{{test_id}}"
      """
      {"tenant":"acme","vector":[4.0,40.0]}
      {"tenant":"beta","vector":[400.0,4000.0]}
      {"tenant":"acme","vector":[8.0,80.0]}
      {"tenant":"beta","vector":[800.0,8000.0]}
      """
    Then within "30s" the observed broker receives payloads
      """
      "tenant":"acme","score":[165.0]
      "tenant":"beta","score":[16500.0]
      """
    And within "30s" Kafka consumer group "infer_ack_group_{{test_id}}" next offset for topic "infer_ack_in_{{test_id}}" partition 0 is "at least 8"

  Scenario Outline: A stateful inferencer's source stays unacknowledged until its emitter's broker takes the results
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And Kafka topic "infer_hold_in_{{test_id}}" exists with 1 partitions
    And a mock Kafka broker serves topic "infer_hold_out_{{test_id}}"
    And the mock Kafka broker refuses every write
    And mock Kafka topic "infer_hold_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( tenant STRING, score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE WIRE JSON SCHEMA scored_wire MODE STRICT ( tenant string, score array );
      CREATE WIRE JSON CODEC scored_codec FROM WIRE SCHEMA scored_wire TO SCHEMA scored;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT infer_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA CLIENT infer_sink CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR feature_ingestor
        FROM infer_kafka TOPIC 'infer_hold_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'infer_hold_group_{{test_id}}' START AT EARLIEST
          <ingestor_mode> ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET tenant = branch.tenant, score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER scored_emitter
        FROM scored
        ATTACHED
        TO infer_sink TOPIC 'infer_hold_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 5s
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING scored_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Kafka messages are rapidly published to topic "infer_hold_in_{{test_id}}"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      {"tenant":"beta","vector":[100.0,1000.0]}
      {"tenant":"acme","vector":[2.0,20.0]}
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "5s" Kafka consumer group "infer_hold_group_{{test_id}}" next offset for topic "infer_hold_in_{{test_id}}" partition 0 is "below 1"
    When the mock Kafka broker accepts writes again
    Then within "30s" the observed broker receives payloads
      """
      "tenant":"acme","score":[33.0]
      "tenant":"beta","score":[3300.0]
      """
    And within "30s" Kafka consumer group "infer_hold_group_{{test_id}}" next offset for topic "infer_hold_in_{{test_id}}" partition 0 is "at least 4"

    Examples:
      | cluster_size | ingestor_mode                                | input_type    | output_type   | tensor_type       |
      | 1            | MODE ACK SEQUENTIAL                          | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 1            | MODE ACK PARALLEL MAX 32 BATCH TIMEOUT 100ms | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | MODE ACK PARALLEL MAX 32 BATCH TIMEOUT 100ms | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
