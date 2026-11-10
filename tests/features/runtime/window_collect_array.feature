Feature: Fixed arrays of retained window values
  Scenario Outline: Sliding fixed collection keeps each branch's values in FIRST to LAST order
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (tenant STRING, value I64);
      CREATE SCHEMA history (tenant STRING, values <vector_type>, first_value I64, last_value I64, head I64 OPTIONAL, mean_value F64 OPTIONAL);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge collect-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR history_window FROM readings
        COLLECT FOR 100ms MAX BATCH SIZE 1MiB
        WIDTH 3 MESSAGES STEP 1 MESSAGES BRANCHED BY by_tenant
        TO histories SET tenant = FIRST(input.tenant), values = COLLECT_ARRAY(input.value),
          first_value = FIRST(input.value), last_value = LAST(input.value),
          head = nth(COLLECT_ARRAY(input.value), 0), mean_value = mean(COLLECT_ARRAY(input.value))
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION history_subscription TO histories;
      START;
      """
    When http payload is posted to node "node-1" with host "collect-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","value":1},{"tenant":"beta","value":10},
       {"tenant":"acme","value":2},{"tenant":"beta","value":20},
       {"tenant":"acme","value":3},{"tenant":"beta","value":30},
       {"tenant":"acme","value":4},{"tenant":"beta","value":40}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "values":[1,2,3] | "first_value":1 | "last_value":3 | "head":1 | "mean_value":2.0
      key={"tenant":"beta"} | "values":[10,20,30] | "first_value":10 | "last_value":30 | "head":10 | "mean_value":20.0
      key={"tenant":"acme"} | "values":[2,3,4] | "first_value":2 | "last_value":4 | "head":2 | "mean_value":3.0
      key={"tenant":"beta"} | "values":[20,30,40] | "first_value":20 | "last_value":40 | "head":20 | "mean_value":30.0
      """

    Examples:
      | cluster_size | vector_type   |
      | 1            | ARRAY<I64, 3> |
      | 3            | ARRAY<I64, 3> |

  Scenario Outline: Tumbling fixed collection nests arrays and preserves watermark order and sensitivity
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1m SKEW 1m;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (tenant STRING, value I64, secret STRING SENSITIVE, occurred_at DATETIME);
      CREATE SCHEMA history (tenant STRING, pairs <pairs_type>, secrets <secrets_type> SENSITIVE, visible <secrets_type>, first_value I64, last_value I64);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge collect-order-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec TIMESTAMP AT occurred_at
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR history_window FROM readings
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_tenant
        TO histories SET tenant = FIRST(input.tenant), pairs = COLLECT_ARRAY([input.value, input.value + 1]),
          secrets = COLLECT_ARRAY(input.secret), visible = leak_sensitive(COLLECT_ARRAY(input.secret)),
          first_value = FIRST(input.value), last_value = LAST(input.value)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION history_subscription TO histories;
      START AT '2000-01-01T00:01:00Z' TIME RATE 1e-300;
      """
    When http payload is posted to node "node-1" with host "collect-order-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","value":10,"secret":"a-last","occurred_at":"2000-01-01T00:00:03Z"},
       {"tenant":"beta","value":300,"secret":"b-last","occurred_at":"2000-01-01T00:00:03Z"},
       {"tenant":"acme","value":20,"secret":"a-first","occurred_at":"2000-01-01T00:00:01Z"},
       {"tenant":"beta","value":100,"secret":"b-first","occurred_at":"2000-01-01T00:00:01Z"},
       {"tenant":"acme","value":30,"secret":"a-tied","occurred_at":"2000-01-01T00:00:01Z"},
       {"tenant":"beta","value":200,"secret":"b-middle","occurred_at":"2000-01-01T00:00:02Z"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "pairs":[[20,21],[30,31],[10,11]] | "secrets":"<masked_value>" | "visible":["a-first","a-tied","a-last"] | "first_value":20 | "last_value":10
      key={"tenant":"beta"} | "pairs":[[100,101],[200,201],[300,301]] | "secrets":"<masked_value>" | "visible":["b-first","b-middle","b-last"] | "first_value":100 | "last_value":300
      """

    Examples:
      | cluster_size | pairs_type       | secrets_type     | masked_value |
      | 1            | ARRAY<I64, 3, 2> | ARRAY<STRING, 3> | <masked>     |
      | 3            | ARRAY<I64, 3, 2> | ARRAY<STRING, 3> | <masked>     |

  Scenario Outline: Fixed collection contracts are refused when the processor is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading (value <input_type>);
      CREATE SCHEMA history (values <output_type>);
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY histories SCHEMA history UNBRANCHED;
      """
    When these Plexus commands fail with "<error>"
      """
      CREATE WINDOW PROCESSOR invalid_collection FROM readings
        WIDTH <width> STEP <step> UNBRANCHED
        TO histories SET values = COLLECT_ARRAY(input.value) ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | input_type   | output_type   | width                  | step        | error                                             |
      | 1            | I64          | ARRAY<I64, 3> | 1s DURATION            | 1s DURATION | COLLECT_ARRAY requires WIDTH n MESSAGES           |
      | 3            | I64          | ARRAY<I64, 3> | 1s DURATION            | 1s DURATION | COLLECT_ARRAY requires WIDTH n MESSAGES           |
      | 1            | I64          | ARRAY<I64, 3> | 3 MESSAGES 1s DURATION | 1 MESSAGES  | COLLECT_ARRAY requires WIDTH n MESSAGES           |
      | 3            | I64          | ARRAY<I64, 3> | 3 MESSAGES 1s DURATION | 1 MESSAGES  | COLLECT_ARRAY requires WIDTH n MESSAGES           |
      | 1            | I64          | ARRAY<I64, 2> | 3 MESSAGES             | 1 MESSAGES  | has expression type                               |
      | 3            | I64          | ARRAY<I64, 2> | 3 MESSAGES             | 1 MESSAGES  | has expression type                               |
      | 1            | I64 OPTIONAL | ARRAY<I64, 3> | 3 MESSAGES             | 1 MESSAGES  | COLLECT_ARRAY requires a required argument        |
      | 3            | I64 OPTIONAL | ARRAY<I64, 3> | 3 MESSAGES             | 1 MESSAGES  | COLLECT_ARRAY requires a required argument        |
      | 1            | I64          | ARRAY<I64, 3> | 2147483648 MESSAGES    | 1 MESSAGES  | COLLECT_ARRAY cannot represent the declared width |
      | 3            | I64          | ARRAY<I64, 3> | 2147483648 MESSAGES    | 1 MESSAGES  | COLLECT_ARRAY cannot represent the declared width |

  Scenario Outline: Fixed collection restores branch-local arrays after cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );
        CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64,
        collected <collected_type>
      );
        CREATE WIRE JSON SCHEMA metric_wire MODE STRICT (
        tenant string,
        latency integer
      );
        CREATE WIRE JSON CODEC metric_codec
        FROM WIRE SCHEMA metric_wire
        TO SCHEMA metric;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_metric_ingestor SCHEMA tenant_branch TTL 5m;
        CREATE RELAY metrics SCHEMA metric BRANCHED BY by_metric_ingestor;
        CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_metric_ingestor;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE HTTP ENDPOINT ingress ON edge PATH '/metrics';
        CREATE HTTP ENDPOINT INGESTOR metric_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics
        INHERIT ALL
        BRANCHED BY by_metric_ingestor
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE WINDOW PROCESSOR restart_latency FROM metrics
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        BRANCHED BY by_metric_ingestor
        TO metric_summaries
        SET tenant = FIRST(input.tenant), sample_count = COUNT(input.latency), total_latency = SUM(input.latency), collected = COLLECT_ARRAY(input.latency)
        ON MESSAGE ERROR LOG;
        START;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":100}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":20}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":200}
      """
    Then within "5s" DESCRIBE DOMAIN section "processed" metric "messages_total" "received" relay "metrics" across physical nodes totals 4
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION metric_summaries_subscription TO metric_summaries;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":300}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "sample_count":3 | "total_latency":60 | "collected":[10,20,30]
      key={"tenant":"beta"} | "tenant":"beta" | "sample_count":3 | "total_latency":600 | "collected":[100,200,300]
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count | collected_type |
      | 1            | 0             | ARRAY<I64, 3>  |
      | 3            | 0             | ARRAY<I64, 3>  |
      | 3            | 1             | ARRAY<I64, 3>  |

  Scenario Outline: A generator's sampled values reach an inferencer as fixed arrays per branch
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
      CREATE SCHEMA reading (tenant STRING, value F32);
      CREATE SCHEMA sample (tenant STRING, value F32, sampled_at DATETIME);
      CREATE SCHEMA history (series <series_type>, sampled_at <times_type>);
      CREATE SCHEMA scored (score <score_type>);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY samples SCHEMA sample BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE RELAY scores SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge collect-sample-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE GENERATOR sampler USING MATERIALIZED STATE readings EACH 100ms
        BRANCHED BY by_tenant TO samples
        SET tenant = relay_state.readings.tenant, value = relay_state.readings.value,
          sampled_at = now()
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR sequence_window FROM samples
        WIDTH 2 MESSAGES STEP 2 MESSAGES BRANCHED BY by_tenant
        TO histories SET series = COLLECT_ARRAY(input.value),
          sampled_at = COLLECT_ARRAY(input.sampled_at) ON MESSAGE ERROR LOG;
      CREATE INFERENCER score_model FROM histories
        USING RESOURCE inference VERSION 1 FILE 'models/simple_score.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.series }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant TO scores SET score = score
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION history_subscription TO histories;
      START;
      """
    When http payload is posted to node "node-1" with host "collect-sample-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","value":4.0},{"tenant":"beta","value":8.0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "series":[4.0,4.0] | "sampled_at":[
      key={"tenant":"beta"} | "series":[8.0,8.0] | "sampled_at":[
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_subscription TO scores;
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "score":[1.125]
      key={"tenant":"beta"} | "score":[2.125]
      """

    Examples:
      | cluster_size | series_type   | times_type         | score_type    | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<DATETIME, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<DATETIME, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |


  Scenario: Relocating a window owner preserves incomplete branch-local fixed arrays
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA window_input (tenant STRING, value I64);
      CREATE SCHEMA window_output (tenant STRING, first_value I64, samples I64, collected ARRAY<I64, 3>);
      CREATE WIRE JSON SCHEMA window_wire MODE STRICT (tenant string, value integer);
      CREATE WIRE JSON CODEC window_codec FROM WIRE SCHEMA window_wire TO SCHEMA window_input;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m MAX INSTANCES 2 EVICT LRU;
      CREATE RELAY window_inputs SCHEMA window_input BRANCHED BY by_tenant;
      CREATE RELAY window_outputs SCHEMA window_output BRANCHED BY by_tenant;
      CREATE VHOST edge window-move-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR window_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING window_codec
        TO window_inputs INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR moving_window FROM window_inputs
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_tenant
        TO window_outputs
          SET tenant = FIRST(input.tenant),
              first_value = FIRST(input.value),
              samples = COUNT(input.value), collected = COLLECT_ARRAY(input.value)
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION window_subscription TO window_outputs;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "window_processor" "moving_window" is saved as placeholder "window_owner"
    And the first replica for scheduled "window_processor" "moving_window" in the last cluster status is saved as placeholder "window_destination"
    When http payload is posted to node "node-1" with host "window-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":10}
      """
    And http payload is posted to node "node-1" with host "window-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":100}
      """
    When these Plexus commands are executed on the active session
      """
      RELOCATE WINDOW PROCESSOR moving_window ONTO NODE {{window_destination}} IGNORE PREFERENCES;
      """
    Then within "60s" node "{{window_destination}}" eventually reports scheduled "window_processor" "moving_window" owner equals placeholder "window_destination"
    When http payload is posted to node "node-1" with host "window-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":20}
      """
    And http payload is posted to node "node-1" with host "window-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":200}
      """
    And http payload is posted to node "node-1" with host "window-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":30}
      """
    And http payload is posted to node "node-1" with host "window-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":300}
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "first_value":10 | "samples":3 | "collected":[10,20,30]
      key={"tenant":"beta"} | "first_value":100 | "samples":3 | "collected":[100,200,300]
      """
