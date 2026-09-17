Feature: Window statistics
  Scenario Outline: Sliding windows compute exact statistics per branch and retract stepped rows
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        tenant STRING,
        sensor STRING,
        value I64,
        load F64,
        healthy BOOL
      );
      CREATE SCHEMA reading_statistics (
        tenant STRING,
        healthy_samples I64,
        all_healthy BOOL,
        any_healthy BOOL,
        mean_value F64,
        value_var_samp F64 OPTIONAL,
        value_var_pop F64,
        value_stddev_samp F64 OPTIONAL,
        value_stddev_pop F64,
        load_covar_samp F64 OPTIONAL,
        load_covar_pop F64,
        load_corr F64 OPTIONAL,
        defined_corr F64,
        lowest_sensor STRING,
        highest_sensor STRING
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        tenant string,
        sensor string,
        value integer,
        load number,
        healthy boolean
      );
      CREATE CODEC reading_codec
        FROM WIRE JSON SCHEMA reading_wire
        TO SCHEMA reading;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_reading_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_reading_tenant;
      CREATE RELAY reading_statistics SCHEMA reading_statistics BRANCHED BY by_reading_tenant;
      CREATE VHOST edge statistics-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_ingestor
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
        INHERIT ALL
        BRANCHED BY by_reading_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR sliding_statistics FROM readings
        WIDTH 3 MESSAGES
        STEP 1 MESSAGES
        BRANCHED BY by_reading_tenant
        TO reading_statistics
          SET tenant = FIRST(input.tenant),
              healthy_samples = COUNT_IF(input.healthy),
              all_healthy = BOOL_AND(input.healthy),
              any_healthy = BOOL_OR(input.healthy),
              mean_value = AVG(input.value),
              value_var_samp = VAR_SAMP(input.value),
              value_var_pop = VAR_POP(input.value),
              value_stddev_samp = STDDEV_SAMP(input.value),
              value_stddev_pop = STDDEV_POP(input.value),
              load_covar_samp = COVAR_SAMP(input.value, input.load),
              load_covar_pop = COVAR_POP(input.value, input.load),
              load_corr = CORR(input.value, input.load),
              defined_corr = COALESCE(CORR(input.value, input.load), -2.0),
              lowest_sensor = ARG_MIN(input.sensor, input.value),
              highest_sensor = ARG_MAX(input.sensor, input.value)
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION reading_statistics_subscription TO reading_statistics;
      START;
      """
    When http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"north","value":2,"load":10.0,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","sensor":"solo","value":100,"load":1.0,"healthy":false}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"south","value":8,"load":40.0,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","sensor":"pair","value":100,"load":2.0,"healthy":false}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"east","value":5,"load":25.0,"healthy":false}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","sensor":"trio","value":100,"load":3.0,"healthy":false}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"west","value":8,"load":40.0,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"center","value":5,"load":25.0,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"top","value":2,"load":10.0,"healthy":true}
      """
    Then within "15s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "healthy_samples":2 | "all_healthy":false | "any_healthy":true | "mean_value":5.0 | "value_var_samp":9.0 | "value_var_pop":6.0 | "value_stddev_samp":3.0 | "value_stddev_pop":2.449489742783178 | "load_covar_samp":45.0 | "load_covar_pop":30.0 | "load_corr":1.0 | "defined_corr":1.0 | "lowest_sensor":"north" | "highest_sensor":"south"
      key={"tenant":"acme"} | "healthy_samples":2 | "all_healthy":false | "mean_value":7.0 | "value_var_samp":3.0 | "value_var_pop":2.0 | "value_stddev_samp":1.7320508075688772 | "value_stddev_pop":1.4142135623730951 | "load_covar_samp":15.0 | "load_covar_pop":10.0 | "load_corr":1.0 | "lowest_sensor":"east" | "highest_sensor":"south"
      key={"tenant":"acme"} | "healthy_samples":2 | "all_healthy":false | "mean_value":6.0 | "value_var_samp":3.0 | "value_var_pop":2.0 | "load_covar_pop":10.0 | "lowest_sensor":"east" | "highest_sensor":"west"
      key={"tenant":"acme"} | "healthy_samples":3 | "all_healthy":true | "any_healthy":true | "mean_value":5.0 | "value_var_samp":9.0 | "value_var_pop":6.0 | "load_covar_pop":30.0 | "lowest_sensor":"top" | "highest_sensor":"west"
      key={"tenant":"beta"} | "healthy_samples":0 | "all_healthy":false | "any_healthy":false | "mean_value":100.0 | "value_var_samp":0.0 | "value_var_pop":0.0 | "value_stddev_samp":0.0 | "value_stddev_pop":0.0 | "load_covar_samp":0.0 | "load_covar_pop":0.0 | "defined_corr":-2.0 | "lowest_sensor":"solo" | "highest_sensor":"solo"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Tumbling windows skip null arguments and emit typed nulls
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA sparse_reading (
        tenant STRING,
        value I64 OPTIONAL,
        healthy BOOL OPTIONAL,
        label STRING OPTIONAL
      );
      CREATE SCHEMA sparse_summary (
        tenant STRING,
        samples I64,
        total I64 OPTIONAL,
        mean_value F64 OPTIONAL,
        lowest I64 OPTIONAL,
        first_value I64 OPTIONAL,
        value_var_samp F64 OPTIONAL,
        healthy_samples I64,
        all_healthy BOOL OPTIONAL,
        lowest_label STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA sparse_reading_wire MODE STRICT (
        tenant string,
        value integer OPTIONAL,
        healthy boolean OPTIONAL,
        label string OPTIONAL
      );
      CREATE CODEC sparse_reading_codec
        FROM WIRE JSON SCHEMA sparse_reading_wire
        TO SCHEMA sparse_reading;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_sparse_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY sparse_readings SCHEMA sparse_reading BRANCHED BY by_sparse_tenant;
      CREATE RELAY sparse_summaries SCHEMA sparse_summary BRANCHED BY by_sparse_tenant;
      CREATE VHOST edge sparse-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/sparse' TYPE HTTP;
      CREATE INGESTOR sparse_ingestor
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sparse_reading_codec
        TO sparse_readings
        INHERIT ALL
        BRANCHED BY by_sparse_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR sparse_window FROM sparse_readings
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        BRANCHED BY by_sparse_tenant
        TO sparse_summaries
          SET tenant = FIRST(input.tenant),
              samples = COUNT(input.value),
              total = SUM(input.value),
              mean_value = AVG(input.value),
              lowest = MIN(input.value),
              first_value = FIRST(input.value),
              value_var_samp = VAR_SAMP(input.value),
              healthy_samples = COUNT_IF(input.healthy),
              all_healthy = BOOL_AND(input.healthy),
              lowest_label = ARG_MIN(input.label, input.value)
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION sparse_summaries_subscription TO sparse_summaries;
      START;
      """
    When http payload is posted to node "node-1" with host "sparse-{{test_id}}.example.com" path "/sparse"
      """
      {"tenant":"acme","label":"missing"}
      """
    And http payload is posted to node "node-1" with host "sparse-{{test_id}}.example.com" path "/sparse"
      """
      {"tenant":"beta"}
      """
    And http payload is posted to node "node-1" with host "sparse-{{test_id}}.example.com" path "/sparse"
      """
      {"tenant":"acme","value":4,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "sparse-{{test_id}}.example.com" path "/sparse"
      """
      {"tenant":"beta","healthy":null}
      """
    And http payload is posted to node "node-1" with host "sparse-{{test_id}}.example.com" path "/sparse"
      """
      {"tenant":"beta","value":null,"label":"orphan"}
      """
    Then within "15s" the relay subscription receives a payload
      """
      "samples":3
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'
    And the last relay subscription payload contains
      """
      "healthy_samples":0
      """
    And the last relay subscription payload does not contain "total\""
    And the last relay subscription payload does not contain "mean_value\""
    And the last relay subscription payload does not contain "lowest\""
    And the last relay subscription payload does not contain "first_value\""
    And the last relay subscription payload does not contain "value_var_samp\""
    And the last relay subscription payload does not contain "all_healthy\""
    And the last relay subscription payload does not contain "lowest_label\""
    When http payload is posted to node "node-1" with host "sparse-{{test_id}}.example.com" path "/sparse"
      """
      {"tenant":"acme","value":10,"healthy":false,"label":"ten"}
      """
    Then within "15s" the relay subscription receives a payload
      """
      "total":14
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload contains
      """
      "samples":3
      "mean_value":7.0
      "lowest":4
      "first_value":4
      "value_var_samp":18.0
      "healthy_samples":1
      "all_healthy":false
      "lowest_label":"ten"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: An outlier that leaves a sliding window leaves no residue in later statistics
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA measurement (
        tenant STRING,
        reading F64
      );
      CREATE SCHEMA measurement_summary (
        tenant STRING,
        total F64,
        mean_reading F64,
        reading_var_pop F64,
        highest F64
      );
      CREATE WIRE JSON SCHEMA measurement_wire MODE STRICT (
        tenant string,
        reading number
      );
      CREATE CODEC measurement_codec
        FROM WIRE JSON SCHEMA measurement_wire
        TO SCHEMA measurement;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_measurement_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY measurements SCHEMA measurement BRANCHED BY by_measurement_tenant;
      CREATE RELAY measurement_summaries SCHEMA measurement_summary BRANCHED BY by_measurement_tenant;
      CREATE VHOST edge outlier-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/measurements' TYPE HTTP;
      CREATE INGESTOR measurement_ingestor
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING measurement_codec
        TO measurements
        INHERIT ALL
        BRANCHED BY by_measurement_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR outlier_window FROM measurements
        WIDTH 2 MESSAGES
        STEP 1 MESSAGES
        BRANCHED BY by_measurement_tenant
        TO measurement_summaries
          SET tenant = FIRST(input.tenant),
              total = SUM(input.reading),
              mean_reading = AVG(input.reading),
              reading_var_pop = VAR_POP(input.reading),
              highest = MAX(input.reading)
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION measurement_summaries_subscription TO measurement_summaries;
      START;
      """
    When http payload is posted to node "node-1" with host "outlier-{{test_id}}.example.com" path "/measurements"
      """
      {"tenant":"acme","reading":1e16}
      """
    And http payload is posted to node "node-1" with host "outlier-{{test_id}}.example.com" path "/measurements"
      """
      {"tenant":"beta","reading":5.0}
      """
    And http payload is posted to node "node-1" with host "outlier-{{test_id}}.example.com" path "/measurements"
      """
      {"tenant":"acme","reading":1.0}
      """
    And http payload is posted to node "node-1" with host "outlier-{{test_id}}.example.com" path "/measurements"
      """
      {"tenant":"beta","reading":7.0}
      """
    And http payload is posted to node "node-1" with host "outlier-{{test_id}}.example.com" path "/measurements"
      """
      {"tenant":"acme","reading":3.0}
      """
    Then within "15s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "total":4.0 | "mean_reading":2.0 | "reading_var_pop":1.0 | "highest":3.0
      key={"tenant":"beta"} | "total":12.0 | "mean_reading":6.0 | "reading_var_pop":1.0 | "highest":7.0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Window statistic contracts are enforced when the processor is applied
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA tagged_reading (
        tenant STRING,
        value I64,
        load F64,
        healthy BOOL,
        tags <tags_type>
      );
      CREATE SCHEMA contract_summary (
        spread F64,
        mean_value F64 OPTIONAL,
        healthy_samples I64 OPTIONAL,
        lowest_tenant STRING OPTIONAL,
        correlation F64 OPTIONAL
      );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_contract_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY tagged_readings SCHEMA tagged_reading BRANCHED BY by_contract_tenant;
      CREATE RELAY contract_summaries SCHEMA contract_summary BRANCHED BY by_contract_tenant;
      """
    When these NSPL commands fail with "<error>"
      """
      CREATE WINDOW PROCESSOR invalid_statistics FROM tagged_readings
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        BRANCHED BY by_contract_tenant
        TO contract_summaries
          SET <assignment>
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | tags_type   | assignment                                                                       | error                                                           |
      | 1            | VEC<STRING> | spread = VAR_SAMP(input.value)                                                   | SET field 'spread' may be null but the output field is required |
      | 3            | VEC<STRING> | spread = VAR_SAMP(input.value)                                                   | SET field 'spread' may be null but the output field is required |
      | 1            | VEC<STRING> | spread = VAR_POP(input.value), mean_value = AVG(input.tenant)                    | function 'AVG' requires a numeric argument                      |
      | 3            | VEC<STRING> | spread = VAR_POP(input.value), mean_value = AVG(input.tenant)                    | function 'AVG' requires a numeric argument                      |
      | 1            | VEC<STRING> | spread = VAR_POP(input.value), healthy_samples = COUNT_IF(input.value)           | function 'COUNT_IF' requires a BOOL argument                    |
      | 3            | VEC<STRING> | spread = VAR_POP(input.value), healthy_samples = COUNT_IF(input.value)           | function 'COUNT_IF' requires a BOOL argument                    |
      | 1            | VEC<STRING> | spread = VAR_POP(input.value), lowest_tenant = ARG_MIN(input.tenant, input.tags) | function 'ARG_MIN' requires an orderable key                    |
      | 3            | VEC<STRING> | spread = VAR_POP(input.value), lowest_tenant = ARG_MIN(input.tenant, input.tags) | function 'ARG_MIN' requires an orderable key                    |
      | 1            | VEC<STRING> | spread = VAR_POP(input.value), correlation = CORR(input.value)                   | CORR expects 2 argument(s), found 1                             |
      | 3            | VEC<STRING> | spread = VAR_POP(input.value), correlation = CORR(input.value)                   | CORR expects 2 argument(s), found 1                             |

  Scenario Outline: Window statistics resume from retained rows after cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        tenant STRING,
        value I64,
        healthy BOOL
      );
      CREATE SCHEMA reading_summary (
        tenant STRING,
        samples I64,
        mean_value F64,
        value_stddev_samp F64 OPTIONAL,
        highest_value I64,
        all_healthy BOOL
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        tenant string,
        value integer,
        healthy boolean
      );
      CREATE CODEC reading_codec
        FROM WIRE JSON SCHEMA reading_wire
        TO SCHEMA reading;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_restart_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_restart_tenant;
      CREATE RELAY reading_summaries SCHEMA reading_summary BRANCHED BY by_restart_tenant;
      CREATE VHOST edge restart-statistics-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_ingestor
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
        INHERIT ALL
        BRANCHED BY by_restart_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR restart_statistics FROM readings
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        BRANCHED BY by_restart_tenant
        TO reading_summaries
          SET tenant = FIRST(input.tenant),
              samples = COUNT(input.value),
              mean_value = AVG(input.value),
              value_stddev_samp = STDDEV_SAMP(input.value),
              highest_value = MAX(input.value),
              all_healthy = BOOL_AND(input.healthy)
          ON MESSAGE ERROR LOG;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "restart-statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":2,"healthy":true}
      """
    When http payload is posted to node "node-1" with host "restart-statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":10,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "restart-statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":4,"healthy":false}
      """
    And http payload is posted to node "node-1" with host "restart-statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":20,"healthy":true}
      """
    Then within "5s" DESCRIBE DOMAIN section "processed" metric "messages_total" "received" relay "readings" across physical nodes totals 4
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION reading_summaries_subscription TO reading_summaries;
      """
    And http payload is posted to node "node-1" with host "restart-statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","value":30,"healthy":true}
      """
    And http payload is posted to node "node-1" with host "restart-statistics-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","value":9,"healthy":true}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "samples":3 | "mean_value":5.0 | "value_stddev_samp":3.605551275463989 | "highest_value":9 | "all_healthy":false
      key={"tenant":"beta"} | "samples":3 | "mean_value":20.0 | "value_stddev_samp":10.0 | "highest_value":30 | "all_healthy":true
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Describe window processor reports shared statistic structures
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        tenant STRING,
        sensor STRING,
        value I64,
        load F64,
        healthy BOOL
      );
      CREATE SCHEMA described_statistics (
        tenant STRING,
        mean_value F64,
        value_var_samp F64 OPTIONAL,
        value_stddev_pop F64,
        load_covar_pop F64,
        load_corr F64 OPTIONAL,
        healthy_samples I64,
        all_healthy BOOL,
        any_healthy BOOL,
        lowest_sensor STRING,
        highest_sensor STRING,
        lowest_value I64
      );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_described_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_described_tenant;
      CREATE RELAY described_statistics SCHEMA described_statistics BRANCHED BY by_described_tenant;
      CREATE WINDOW PROCESSOR described_statistics FROM readings
        WIDTH 3 MESSAGES
        STEP 1 MESSAGES
        BRANCHED BY by_described_tenant
        TO described_statistics
          SET tenant = FIRST(input.tenant),
              mean_value = AVG(input.value),
              value_var_samp = VAR_SAMP(input.value),
              value_stddev_pop = STDDEV_POP(input.value),
              load_covar_pop = COVAR_POP(input.value, input.load),
              load_corr = CORR(input.value, input.load),
              healthy_samples = COUNT_IF(input.healthy),
              all_healthy = BOOL_AND(input.healthy),
              any_healthy = BOOL_OR(input.healthy),
              lowest_sensor = ARG_MIN(input.sensor, input.value),
              highest_sensor = ARG_MAX(input.sensor, input.value),
              lowest_value = MIN(input.value)
          ON MESSAGE ERROR LOG;
      DESCRIBE WINDOW PROCESSOR described_statistics;
      """
    Then the last command output contains
      """
      aggregate structures: 6
      """
    And the last command output contains
      """
      structure 1:
        functions: AVG, STDDEV_POP, VAR_SAMP
        storage: moments
        references: 3
        input: input.value
      structure 2:
        functions: CORR, COVAR_POP
        storage: co_moments
        references: 2
        inputs: input.value, input.load
      structure 3:
        functions: BOOL_AND, BOOL_OR, COUNT_IF
        storage: truth_counter
        references: 3
        input: input.healthy
      structure 4:
        functions: ARG_MAX, ARG_MIN
        storage: arg_extremes
        references: 2
        inputs: input.sensor, input.value
      structure 5:
        functions: MIN
        storage: extremes
        references: 1
        input: input.value
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
