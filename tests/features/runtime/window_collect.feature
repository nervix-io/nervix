Feature: Window collection of retained values
  Scenario Outline: Sliding collection keeps each branch's values in FIRST to LAST order
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
        WIDTH 3 MESSAGES STEP 1 MESSAGES BRANCHED BY by_tenant
        TO histories SET tenant = FIRST(input.tenant), values = COLLECT_VEC(input.value),
          first_value = FIRST(input.value), last_value = LAST(input.value),
          head = nth(COLLECT_VEC(input.value), 0), mean_value = mean(COLLECT_VEC(input.value))
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
      | cluster_size | vector_type |
      | 1            | VEC<I64>    |
      | 3            | VEC<I64>    |

  Scenario Outline: Tumbling collection nests arrays and preserves watermark order and sensitivity
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
        TO histories SET tenant = FIRST(input.tenant), pairs = COLLECT_VEC([input.value, input.value + 1]),
          secrets = COLLECT_VEC(input.secret), visible = leak_sensitive(COLLECT_VEC(input.secret)),
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
      | cluster_size | pairs_type         | secrets_type | masked_value |
      | 1            | VEC<ARRAY<I64, 2>> | VEC<STRING>  | <masked>     |
      | 3            | VEC<ARRAY<I64, 2>> | VEC<STRING>  | <masked>     |

  Scenario Outline: Bounded duration windows collect differing list lengths and explicit null defaults
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (tenant STRING, value I64 OPTIONAL);
      CREATE SCHEMA history (tenant STRING, values <vector_type>);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge collect-duration-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR history_window FROM readings
        WIDTH 1s DURATION STEP 1s DURATION MAX STATE SIZE 1MiB BRANCHED BY by_tenant
        TO histories SET tenant = FIRST(input.tenant), values = COLLECT_VEC(coalesce(input.value, 0))
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION history_subscription TO histories;
      START;
      """
    When http payload is posted to node "node-1" with host "collect-duration-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"acme","value":1},{"tenant":"beta","value":10},{"tenant":"acme"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "values":[1,0]
      key={"tenant":"beta"} | "values":[10]
      """

    Examples:
      | cluster_size | vector_type |
      | 1            | VEC<I64>    |
      | 3            | VEC<I64>    |

  Scenario Outline: Collection shares its retained argument with FIRST and LAST in inspection
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (tenant STRING, value I64);
      CREATE SCHEMA history (values <vector_type>, first_value I64, last_value I64);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge collect-inspect-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR history_window FROM readings
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_tenant
        TO histories SET values = COLLECT_VEC(input.value),
          first_value = FIRST(input.value), last_value = LAST(input.value)
        ON MESSAGE ERROR LOG;
      START;
      DESCRIBE WINDOW PROCESSOR history_window;
      """
    Then the last command output contains
      """
      aggregate structures: 1
      """
    And the last command output contains
      """
      structure 0:
        functions: COLLECT_VEC, FIRST, LAST
        storage: sequence
        references: 3
        input: input.value
      """
    When http payload is posted to node "node-1" with host "collect-inspect-{{test_id}}.example.com" path "/readings"
      """
      [{"tenant":"east","value":10},{"tenant":"west","value":100},
       {"tenant":"east","value":20},{"tenant":"west","value":200}]
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees window "history_window" hold 4 rows
    And the last processor state counts 4 rows in 2 branches
    And the last processor state section "rows" rows match
      """
      {"branch":{"tenant":"east"},"fields":{"tenant":"east","value":10,"sequence":0,"argument_0":10}}
      {"branch":{"tenant":"east"},"fields":{"tenant":"east","value":20,"sequence":1,"argument_0":20}}
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":100,"sequence":0,"argument_0":100}}
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":200,"sequence":1,"argument_0":200}}
      """
    And the last processor state section "accumulators" rows match
      """
      {"branch":{"tenant":"east"},"fields":{"structure":0,"kind":"sequence","count":2}}
      {"branch":{"tenant":"west"},"fields":{"structure":0,"kind":"sequence","count":2}}
      """
    And the last inspection JSON is the complete typed envelope

    Examples:
      | cluster_size | vector_type |
      | 1            | VEC<I64>    |
      | 3            | VEC<I64>    |

  Scenario Outline: Collection retention refuses an oversized row and keeps accepted branch values
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (tenant STRING, value STRING);
      CREATE SCHEMA history (values <vector_type>);
      CREATE JAQ JSON CODEC reading_codec TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '{tenant:.tenant,value:("x" * .size)}';
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge collect-budget-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR history_window FROM readings
        WIDTH 2 MESSAGES STEP 2 MESSAGES MAX STATE SIZE 32KiB BRANCHED BY by_tenant
        TO histories SET values = COLLECT_VEC(input.value) ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION history_subscription TO histories;
      START;
      """
    When http payload is posted to node "node-1" with host "collect-budget-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"east","size":1}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees window "history_window" hold 1 rows
    When http payload is posted to node "node-1" with host "collect-budget-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"east","size":65536}
      """
    Then within "30s" the active session observes a server error containing
      """
      exceeding MAX STATE SIZE 32768B
      """
    And within "30s" client "reader" sees window "history_window" hold 1 rows
    When http payload is posted to node "node-1" with host "collect-budget-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"west","size":2}
      """
    And http payload is posted to node "node-1" with host "collect-budget-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"east","size":3}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"east"} | "values":["x","xxx"]
      """
    And within "30s" client "reader" sees window "history_window" hold 1 rows
    And within "30s" client "reader" sees window "history_window" section "rows" rows matching
      """
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":"xx","sequence":0,"argument_0":"xx"}}
      """
    And the last processor state section "rows" rows match
      """
      {"branch":{"tenant":"west"},"fields":{"tenant":"west","value":"xx","sequence":0,"argument_0":"xx"}}
      """

    Examples:
      | cluster_size | vector_type |
      | 1            | VEC<STRING> |
      | 3            | VEC<STRING> |

  Scenario Outline: Collection contracts are refused when the processor is applied
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
        TO histories SET values = <expression> ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | input_type   | output_type | width       | step        | expression                    | error                                                             |
      | 1            | I64 OPTIONAL | VEC<I64>    | 3 MESSAGES  | 1 MESSAGES  | COLLECT_VEC(input.value)      | COLLECT_VEC requires a required argument; use coalesce            |
      | 3            | I64 OPTIONAL | VEC<I64>    | 3 MESSAGES  | 1 MESSAGES  | COLLECT_VEC(input.value)      | COLLECT_VEC requires a required argument; use coalesce            |
      | 1            | BYTES        | VEC<BYTES>  | 3 MESSAGES  | 1 MESSAGES  | COLLECT_VEC(input.value)      | cannot retain BYTES                                               |
      | 3            | BYTES        | VEC<BYTES>  | 3 MESSAGES  | 1 MESSAGES  | COLLECT_VEC(input.value)      | cannot retain BYTES                                               |
      | 1            | I64          | VEC<I64>    | 1s DURATION | 1s DURATION | COLLECT_VEC(input.value)      | COLLECT_VEC requires a message WIDTH or MAX STATE SIZE            |
      | 3            | I64          | VEC<I64>    | 1s DURATION | 1s DURATION | COLLECT_VEC(input.value)      | COLLECT_VEC requires a message WIDTH or MAX STATE SIZE            |
      | 1            | I64          | VEC<I64>    | 3 MESSAGES  | 1 MESSAGES  | COLLECT_VEC(SUM(input.value)) | aggregate functions must not be nested inside aggregate arguments |
      | 3            | I64          | VEC<I64>    | 3 MESSAGES  | 1 MESSAGES  | COLLECT_VEC(SUM(input.value)) | aggregate functions must not be nested inside aggregate arguments |
