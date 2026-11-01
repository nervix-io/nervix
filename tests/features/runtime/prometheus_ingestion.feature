Feature: Prometheus ingestion
  Scenario Outline: Prometheus ingestor delivers queried samples to a subscribed relay
    Given Prometheus is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Prometheus endpoint is published under fixture DNS
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE CODEC sample_codec
        FROM JSON
        TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '{source: .metric.source, value, timestamp}';
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE PROMETHEUS CLIENT prom_main
        CONFIG {
          'addr' = '{{prometheus_dns_addr}}',
          'timeout_ms' = 5000
        };
        CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'label_replace(vector(42.5), "source", "local", "", "")' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples
        INHERIT ALL
        BRANCHED BY by_prom_samples
        SET source = message.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION samples_subscription TO samples;
        START;
      """
    Then the relay subscription receives a payload
      """
      "source":"local"
      """
    And the last relay subscription payload contains key fragment '{"source":"local"}'
    And the DNS fixture eventually receives a question for "prometheus.nervix.test"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Prometheus ingestor reports transient source failures and recovers
    Given Prometheus is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "prom_samples" enters fault mode
    And these Plexus commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE CODEC sample_codec
        FROM JSON
        TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '{source: .metric.source, value, timestamp}';
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE PROMETHEUS CLIENT prom_main
        CONFIG {
          'addr' = '{{prometheus_addr}}',
          'timeout_ms' = 5000
        };
        CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'label_replace(vector(43.5), "source", "recover", "", "")' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples
        INHERIT ALL
        BRANCHED BY by_prom_samples
        SET source = message.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION samples_subscription TO samples;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "prom_samples" leaves fault mode
    Then the relay subscription receives a payload
      """
      "source":"recover"
      """
    And within "5s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Prometheus ingestor follows paced domain logical time and cadence
    Given Prometheus is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 30s SKEW 100000h;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE CODEC sample_codec
        FROM JSON
        TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '{source: .metric.source, value, timestamp}';
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE PROMETHEUS CLIENT prom_main
        CONFIG {
          'addr' = '{{prometheus_addr}}',
          'timeout_ms' = 5000
        };
        CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'label_replace(vector(time()), "source", "local", "", "")' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TIMESTAMP NOW
        TO samples
        INHERIT ALL
        BRANCHED BY by_prom_samples
        SET source = message.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION samples_subscription TO samples;
      """
    When these Plexus commands are executed on the leader node
      """
      START AT '2026-04-07T00:00:00Z' TIME RATE 4.0;
      """
    Then within "5s" the relay subscription receives payloads
      """
      "source":"local","timestamp":"2026-04-07T00:00:
      "source":"local","timestamp":"2026-04-07T00:00:
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @prometheus_client_config
  Scenario Outline: A Prometheus client refuses a Prometheus CONFIG that cannot configure a query source
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "invalid Prometheus client config key 'addr': the URL has a query string"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090/?stale=true',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "invalid Prometheus client config key 'addr': the URL has a fragment"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090/#graph',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "invalid client config key 'addr': expected URL"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'prometheus.nervix.test:9090',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "missing required client config key 'timeout_ms'"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090'
      };
      """
    When these Plexus statements fail with "invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090',
        'timeout_ms' = 0
      };
      """
    When these Plexus statements fail with "unknown client config key 'timeout'"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090',
        'timeout_ms' = 5000,
        'timeout' = 5000
      };
      """
    When these Plexus statements fail with "client config key 'tls_cert_file' requires 'tls_key_file'"
      """
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'https://prometheus.nervix.test:9090',
        'timeout_ms' = 5000,
        'tls_cert_file' = '/etc/nervix/tls/client.pem'
      };
      """
    Given the leader node is configured with these Plexus commands
      """
      CREATE RESOURCE trust_files;
      """
    And node "node-1" has resource directory "prometheus_tls_fixture" containing
      """
      {"ca.pem": "CA material is parsed on transport initialization\n", "client.pem": "certificate material is parsed on transport initialization\n", "client-key.pem": "key material is parsed on transport initialization\n"}
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE trust_files VERSION "{{prometheus_tls_fixture}}";
      """
    And these Plexus statements are executed on the leader node
      """
      CREATE PROMETHEUS CLIENT prom_main MOUNT trust_files VERSION 1 CONFIG {
        'addr' = 'https://prometheus.nervix.test:9090/monitoring/',
        'timeout_ms' = 5000,
        'tls_ca_file' = '{{ trust_files }}/ca.pem',
        'tls_cert_file' = '{{ trust_files }}/client.pem',
        'tls_key_file' = '{{ trust_files }}/client-key.pem'
      };
      SHOW CREATE CLIENT prom_main;
      """
    Then the last command output contains
      """
      'addr' = 'https://prometheus.nervix.test:9090/monitoring/'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_query_statement
  Scenario Outline: A Prometheus query ingestor refuses a statement its entity does not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA sample ( source STRING, value F64, timestamp STRING );
      CREATE CODEC sample_codec FROM JSON TO SCHEMA sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '{source: .metric.source, value, timestamp}';
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "expected NO ACK"
      """
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'up' EVERY 1s
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "found BATCH"
      """
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'up' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "PROMETHEUS QUERY ingestors do not support read_header or read_headers"
      """
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'up' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL
        SET source = read_header('source')
        UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'up' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR prom_samples;
      """
    Then the last command output contains
      """
      MODE NO ACK
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @prometheus_body_bound
  Scenario Outline: A Prometheus response above the ingestor's MAX BODY SIZE fails the poll
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA bulk_sample ( value F64, timestamp STRING );
      CREATE WIRE JSON SCHEMA bulk_sample_wire MODE LOOSE ( value number, timestamp string );
      CREATE CODEC bulk_sample_codec FROM WIRE JSON SCHEMA bulk_sample_wire TO SCHEMA bulk_sample;
      CREATE RELAY samples SCHEMA bulk_sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{mock_http_addr}}/prometheus-vector/{{test_id}}/100/always',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "MAX BODY SIZE must be greater than zero"
      """
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'bulk' MAX BODY SIZE 0B EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING bulk_sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'bulk' MAX BODY SIZE 1KiB EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING bulk_sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR prom_samples;
      """
    Then the last command output contains
      """
      QUERY 'bulk' MAX BODY SIZE 1KiB EVERY 1s
      """
    When these Plexus statements are executed on the leader node
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: Prometheus query response exceeds MAX BODY SIZE 1KiB
      """
    And the last command output contains
      """
      options: QUERY 'bulk' MAX BODY SIZE 1KiB EVERY 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # MAX BATCH SIZE 1B flushes every route input on its own, so each relay batch the ingestor sends is
  # exactly one ingest group: the count of batches is the count of intake batches.
  @prometheus_intake_batches
  Scenario Outline: A Prometheus vector arrives in intake batches of at most 1024 samples, each sample once
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA bulk_sample ( value F64, timestamp STRING );
      CREATE WIRE JSON SCHEMA bulk_sample_wire MODE LOOSE ( value number, timestamp string );
      CREATE CODEC bulk_sample_codec FROM WIRE JSON SCHEMA bulk_sample_wire TO SCHEMA bulk_sample;
      CREATE RELAY samples SCHEMA bulk_sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = '{{mock_http_addr}}/prometheus-vector/{{test_id}}/50000/once',
        'timeout_ms' = 5000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'bulk' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING bulk_sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH EACH 1h MAX BATCH SIZE 1B ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "samples" across physical nodes totals 50000
    And within "30s" DESCRIBE DOMAIN section "input_output" metric "batches_total" "sent" relay "samples" across physical nodes totals 49
    And within "30s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      options: QUERY 'bulk' MAX BODY SIZE 16MiB EVERY 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
