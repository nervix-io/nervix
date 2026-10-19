Feature: Prometheus ingestion
  Scenario Outline: Prometheus ingestor delivers queried samples to a subscribed relay
    Given Prometheus is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the Prometheus endpoint is published under fixture DNS
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE WIRE JSON SCHEMA sample_wire MODE STRICT (
        source string,
        value number,
        timestamp string
      );
        CREATE CODEC sample_codec
        FROM WIRE JSON SCHEMA sample_wire
        TO SCHEMA sample;
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE CLIENT prom_main
        TYPE PROMETHEUS
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "prom_samples" enters fault mode
    And these NSPL commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE WIRE JSON SCHEMA sample_wire MODE STRICT (
        source string,
        value number,
        timestamp string
      );
        CREATE CODEC sample_codec
        FROM WIRE JSON SCHEMA sample_wire
        TO SCHEMA sample;
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE CLIENT prom_main
        TYPE PROMETHEUS
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
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 30s SKEW 100000h;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA sample (
        source STRING,
        value F64,
        timestamp STRING
      );
        CREATE WIRE JSON SCHEMA sample_wire MODE STRICT (
        source string,
        value number,
        timestamp string
      );
        CREATE CODEC sample_codec
        FROM WIRE JSON SCHEMA sample_wire
        TO SCHEMA sample;
        CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING );
        CREATE IF NOT EXISTS BRANCH by_prom_samples SCHEMA source_branch TTL 5m;
        CREATE RELAY samples SCHEMA sample BRANCHED BY by_prom_samples;
        CREATE CLIENT prom_main
        TYPE PROMETHEUS
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
    When these NSPL commands are executed on the leader node
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
  Scenario Outline: CREATE CLIENT refuses a Prometheus CONFIG that cannot configure a query source
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'addr' is not an API root: the URL has a query string"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090/?stale=true',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'addr' is not an API root: the URL has a fragment"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090/#graph',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'addr' is not an API root: the URL's scheme is not http or https"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'prometheus.nervix.test:9090',
        'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'timeout_ms' is required"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090'
      };
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'timeout_ms' is not a usable request timeout: not a positive whole number of milliseconds"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090',
        'timeout_ms' = 0
      };
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'timeout' is not a key this client reads"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'http://prometheus.nervix.test:9090',
        'timeout_ms' = 5000,
        'timeout' = 5000
      };
      """
    When these Plexus statements fail with "PROMETHEUS client 'prom_main' in domain '{{domain}}' cannot use its CONFIG: 'tls_cert_file' and 'tls_key_file' must be set together"
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'https://prometheus.nervix.test:9090',
        'timeout_ms' = 5000,
        'tls_cert_file' = '/etc/nervix/tls/client.pem'
      };
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
        'addr' = 'https://prometheus.nervix.test:9090/monitoring/',
        'timeout_ms' = 5000,
        'tls_ca_file' = '/etc/nervix/tls/ca.pem',
        'tls_cert_file' = '/etc/nervix/tls/client.pem',
        'tls_key_file' = '/etc/nervix/tls/client-key.pem'
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
      CREATE WIRE JSON SCHEMA sample_wire MODE STRICT ( source string, value number, timestamp string );
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE CLIENT prom_main TYPE PROMETHEUS CONFIG {
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
