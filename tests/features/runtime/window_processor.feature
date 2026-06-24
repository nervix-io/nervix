Feature: Window processor runtime behavior
  Scenario Outline: Tumbling message windows emit non-overlapping aggregates per branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        first_latency I64,
        last_latency I64,
        total_latency I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR tumbling_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency),
          metric_summaries.first_latency = FIRST(metrics.latency),
          metric_summaries.last_latency = LAST(metrics.latency),
          metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":20}
      """
    Then the relay subscription receives a payload
      """
      "total_latency":30
      """
    And the last relay subscription payload contains
      """
      "sample_count":2
      "first_latency":10
      "last_latency":20
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":40}
      """
    Then the relay subscription receives a payload
      """
      "total_latency":70
      """
    And the last relay subscription payload contains
      """
      "first_latency":30
      "last_latency":40
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Sliding message windows delete stepped entries from online aggregate state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        first_latency I64,
        last_latency I64,
        min_latency I64,
        max_latency I64,
        total_latency I64,
        latency_p50 F64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR sliding_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 3 MESSAGES
        STEP 1 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency),
          metric_summaries.first_latency = FIRST(metrics.latency),
          metric_summaries.last_latency = LAST(metrics.latency),
          metric_summaries.min_latency = MIN(metrics.latency),
          metric_summaries.max_latency = MAX(metrics.latency),
          metric_summaries.total_latency = SUM(metrics.latency),
          metric_summaries.latency_p50 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 50, 10, 0, 100, '0ms') ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":20}
      """
    Then the relay subscription receives a payload
      """
      "total_latency":60
      """
    And the last relay subscription payload contains
      """
      "first_latency":30
      "last_latency":20
      "min_latency":10
      "max_latency":30
      "latency_p50":25.0
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":40}
      """
    Then the relay subscription receives a payload
      """
      "total_latency":70
      """
    And the last relay subscription payload contains
      """
      "first_latency":10
      "last_latency":40
      "min_latency":10
      "max_latency":40
      "latency_p50":25.0
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Window processor restores branch-local state after cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR restart_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency),
          metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG;

      START;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":20}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30}
      """
    Then the relay subscription receives a payload
      """
      "sample_count":3
      """
    And the last relay subscription payload contains
      """
      "total_latency":60
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Linear histogram percentiles are configured declaratively in aggregate expressions
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        latency_p50 F64,
        latency_p90 F64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR histogram_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.latency_p50 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 50, 10, 0, 100, '2s'),
          metric_summaries.latency_p90 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 10, 0, 100, '2s') ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":20}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30}
      """
    Then the relay subscription receives a payload
      """
      "latency_p50":25.0
      """
    And the last relay subscription payload contains
      """
      "latency_p90":35.0
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Linear histogram invalid configuration is rejected before runtime starts
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        latency_p90 F64
      );

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;
      """
    When these NSPL commands fail with "invalid PERCENTILE_LINEAR_HISTOGRAM delay duration"
      """
      CREATE WINDOW PROCESSOR invalid_histogram_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.latency_p90 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 10, 0, 100, 'not-a-duration') ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Describe window processor reports deduplicated aggregate structures
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        first_latency I64,
        last_latency I64,
        min_latency I64,
        max_latency I64,
        latency_p50 F64,
        latency_p90 F64,
        sample_count I64,
        total_latency I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR described_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 3 MESSAGES
        STEP 3 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.first_latency = FIRST(metrics.latency),
          metric_summaries.last_latency = LAST(metrics.latency),
          metric_summaries.min_latency = MIN(metrics.latency),
          metric_summaries.max_latency = MAX(metrics.latency),
          metric_summaries.latency_p50 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 50, 10, 0, 100, '2s'),
          metric_summaries.latency_p90 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 90, 10, 0, 100, '2s'),
          metric_summaries.sample_count = COUNT(metrics.latency),
          metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG;

      DESCRIBE WINDOW PROCESSOR described_latency;
      """
    Then the last command output contains
      """
      window processor: described_latency
      kind: WINDOW PROCESSOR
      """
    And the last command output contains
      """
      owner: node-
      """
    And the last command output contains
      """
      replicas:
      """
    And the last command output contains
      """
      aggregate structures: 8
      """
    And the last command output contains
      """
      structure 5:
        function: PERCENTILE_LINEAR_HISTOGRAM
        storage: linear_histogram
        references: 2
        input: metrics.latency
        buckets: 10
        min: 0.0
        max: 100.0
        delay: 2s
      """
    And the last command output contains
      """
      function: COUNT
      """
    And the last command output contains
      """
      function: SUM
      """
    And the last command output does not contain
      """
      structure 8:
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Linear histogram delay retains removed step buckets for later windows
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        latency_p0 F64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR delayed_histogram_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES
        STEP 1 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.latency_p0 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 0, 10, 0, 100, '2s') ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":90}
      """
    Then the relay subscription receives a payload
      """
      "latency_p0":15.0
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":90}
      """
    Then the relay subscription receives a payload
      """
      "latency_p0":15.0
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Linear histogram delay expires on a scheduled timeout without new records
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        latency_p0 F64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR delayed_histogram_timeout
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES 5s DURATION
        STEP 1 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.latency_p0 = PERCENTILE_LINEAR_HISTOGRAM(metrics.latency, 0, 10, 0, 100, '1s') ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":90}
      """
    Then the relay subscription receives a payload
      """
      "latency_p0":15.0
      """
    And the relay subscription does not receive a payload within "500ms"
    Then the relay subscription receives a payload
      """
      "latency_p0":95.0
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @parameterized_deadline_cache
  Scenario Outline: Duration-only windows flush on a scheduled timeout
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR duration_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 300ms DURATION
        STEP 300ms DURATION
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency),
          metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    Then the relay subscription does not receive a payload within "100ms"
    Then the relay subscription receives a payload
      """
      "total_latency":10
      """
    And the last relay subscription payload contains
      """
      "sample_count":1
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Combined width conditions flush when either messages or duration expires
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR combined_latency
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 3 MESSAGES 3s DURATION
        STEP 3 MESSAGES 3s DURATION
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency),
          metric_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO metric_summaries WHERE metric_summaries.tenant = 'acme';
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":2}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":3}
      """
    Then the relay subscription receives a payload
      """
      "sample_count":3
      """
    And the last relay subscription payload contains
      """
      "total_latency":6
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10}
      """
    Then the relay subscription does not receive a payload within "300ms"
    Then the relay subscription receives a payload
      """
      "sample_count":1
      """
    And the last relay subscription payload contains
      """
      "total_latency":10
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Parameterized window chains preserve concrete branch keys through output routes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64
      );

      CREATE SCHEMA chain_summary (
        tenant STRING,
        high_window_count I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY high_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY low_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY chain_summaries SCHEMA chain_summary PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/metrics' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR metric_ingestor
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR first_window
        FROM metrics
        TO high_summaries WHERE high_summaries.total_latency >= 100
        TO low_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        AGGREGATE
          high_summaries.tenant = FIRST(metrics.tenant),
          high_summaries.sample_count = COUNT(metrics.latency),
          high_summaries.total_latency = SUM(metrics.latency) ON MESSAGE ERROR LOG;

      CREATE WINDOW PROCESSOR second_window
        FROM high_summaries
        TO chain_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        AGGREGATE
          chain_summaries.tenant = FIRST(high_summaries.tenant),
          chain_summaries.high_window_count = COUNT(high_summaries.total_latency) ON MESSAGE ERROR LOG;

      START;
      SUBSCRIBE SESSION TO chain_summaries WHERE chain_summaries.tenant = 'acme';
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":60}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":70}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":80}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":90}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":50}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":60}
      """
    Then the relay subscription receives a payload
      """
      "high_window_count":2
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL replays when an attached window branch output fails
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE JSON WIRE SCHEMA metric_summary_wire (
        tenant string,
        sample_count integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE CODEC metric_summary_codec
        FROM WIRE JSON SCHEMA metric_summary_wire
        TO SCHEMA metric_summary;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092',
        'auto.offset.reset' = 'earliest'
      };

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR kafka_metrics
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC metrics_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR attached_window
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency) ON MESSAGE ERROR LOG;

      CREATE EMITTER kafka_forward
        FROM metric_summaries
        ENCODE USING metric_summary_codec
        TO KAFKA kafka_main TOPIC metric_summaries_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO metrics;
      START;
      """
    When emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "metrics_{{test_id}}"
      """
      {"tenant":"acme","latency":10}
      """
    And Kafka message is published to topic "metrics_{{test_id}}"
      """
      {"tenant":"acme","latency":20}
      """
    Then within "8s" the relay subscription receives payloads
      """
      "latency":10
      "latency":20
      "latency":10
      "latency":20
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL does not replay when a detached window branch output fails
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64
      );

      CREATE JSON WIRE SCHEMA metric_wire (
        tenant string,
        latency integer
      );

      CREATE JSON WIRE SCHEMA metric_summary_wire (
        tenant string,
        sample_count integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE CODEC metric_summary_codec
        FROM WIRE JSON SCHEMA metric_summary_wire
        TO SCHEMA metric_summary;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metrics SCHEMA metric PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY metric_summaries SCHEMA metric_summary PARAMETERIZED BY tenant_branch;

      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '127.0.0.1:9092',
        'auto.offset.reset' = 'earliest'
      };

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR kafka_metrics
        TO metrics
        DECODE USING metric_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = metrics.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC metrics_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DETACHED WINDOW PROCESSOR detached_window
        FROM metrics
        TO metric_summaries PARAMETERIZED BY tenant_branch
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        AGGREGATE
          metric_summaries.tenant = FIRST(metrics.tenant),
          metric_summaries.sample_count = COUNT(metrics.latency) ON MESSAGE ERROR LOG;

      CREATE EMITTER kafka_forward
        FROM metric_summaries
        ENCODE USING metric_summary_codec
        TO KAFKA kafka_main TOPIC metric_summaries_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO metrics;
      START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "metrics_{{test_id}}"
      """
      {"tenant":"acme","latency":10}
      """
    And Kafka message is published to topic "metrics_{{test_id}}"
      """
      {"tenant":"acme","latency":20}
      """
    Then within "2s" the relay subscription receives payloads
      """
      "latency":10
      "latency":20
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
