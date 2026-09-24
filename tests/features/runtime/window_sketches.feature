Feature: Bounded window sketches
  Scenario Outline: Sliding time windows merge distinct, quantile and top-k panes per branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA sketch_input (tenant STRING, value I64 OPTIONAL);
      CREATE SCHEMA sketch_output (
        tenant STRING,
        distinct_values I64,
        median_value F64 OPTIONAL,
        frequent_values <value_vec_type>
      );
      CREATE WIRE JSON SCHEMA sketch_wire MODE STRICT (
        tenant string,
        value integer OPTIONAL
      );
      CREATE CODEC sketch_codec FROM WIRE JSON SCHEMA sketch_wire TO SCHEMA sketch_input;
      CREATE SCHEMA sketch_branch_schema (tenant STRING);
      CREATE BRANCH sketch_branch SCHEMA sketch_branch_schema TTL 5m MAX INSTANCES 4 EVICT LRU;
      CREATE RELAY sketch_inputs SCHEMA sketch_input BRANCHED BY sketch_branch;
      CREATE RELAY sketch_outputs SCHEMA sketch_output BRANCHED BY sketch_branch;
      CREATE VHOST edge sketches-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/sketches' TYPE HTTP;
      CREATE INGESTOR sketch_ingestor
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sketch_codec
        TO sketch_inputs
        INHERIT ALL
        BRANCHED BY sketch_branch
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR sketch_window FROM sketch_inputs
        WIDTH 10s DURATION
        STEP 5s DURATION
        MAX STATE SIZE 1MiB
        BRANCHED BY sketch_branch
        TO sketch_outputs
          SET tenant = FIRST(input.tenant),
              distinct_values = APPROX_COUNT_DISTINCT(input.value, 10),
              median_value = APPROX_QUANTILE(input.value, 50, 128),
              frequent_values = APPROX_TOP_K(input.value, 2, 16)
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION sketch_subscription TO sketch_outputs;
      START;
      """
    When http payload is posted to node "node-1" with host "sketches-{{test_id}}.example.com" path "/sketches"
      """
      {"tenant":"acme","value":10}
      """
    And http payload is posted to node "node-1" with host "sketches-{{test_id}}.example.com" path "/sketches"
      """
      {"tenant":"beta","value":100}
      """
    And http payload is posted to node "node-1" with host "sketches-{{test_id}}.example.com" path "/sketches"
      """
      {"tenant":"acme","value":10}
      """
    And http payload is posted to node "node-1" with host "sketches-{{test_id}}.example.com" path "/sketches"
      """
      {"tenant":"acme","value":20}
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "distinct_values":2 | "median_value":10.0 | "frequent_values":[10,20]
      key={"tenant":"beta"} | "distinct_values":1 | "median_value":100.0 | "frequent_values":[100]
      """

    Examples:
      | cluster_size | replica_count | value_vec_type |
      | 1            | 0             | VEC<I64>       |
      | 3            | 1             | VEC<I64>       |
