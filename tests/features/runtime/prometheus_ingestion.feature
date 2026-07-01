Feature: Prometheus ingestion
  Scenario Outline: Prometheus ingestor delivers queried samples to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
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
        CREATE STRICT WIRE JSON SCHEMA sample_wire (
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
          'addr' = 'http://127.0.0.1:9090',
          'timeout_ms' = 5000
        };
        CREATE INGESTOR prom_samples
        TO samples
        DECODE USING sample_codec
        BRANCHED BY by_prom_samples VALUES { source = samples.source }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM PROMETHEUS prom_main
        QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
        EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO samples;
        START;
      """
    Then the relay subscription receives a payload
      """
      "source":"local"
      """
    And the last relay subscription payload contains key fragment '{"source":"local"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Prometheus ingestor reports transient source failures and recovers
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
        CREATE STRICT WIRE JSON SCHEMA sample_wire (
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
          'addr' = 'http://127.0.0.1:9090',
          'timeout_ms' = 5000
        };
        CREATE INGESTOR prom_samples
        TO samples
        DECODE USING sample_codec
        BRANCHED BY by_prom_samples VALUES { source = samples.source }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM PROMETHEUS prom_main
        QUERY 'label_replace(vector(43.5), "source", "recover", "", "")'
        EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO samples;
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
        CREATE STRICT WIRE JSON SCHEMA sample_wire (
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
          'addr' = 'http://127.0.0.1:9090',
          'timeout_ms' = 5000
        };
        CREATE INGESTOR prom_samples
        TO samples
        DECODE USING sample_codec
        BRANCHED BY by_prom_samples VALUES { source = samples.source }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM PROMETHEUS prom_main
        QUERY 'label_replace(vector(time()), "source", "local", "", "")'
        EVERY 1s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO samples;
      """
    When these NSPL commands are executed on the leader node
      """
      START AT '2026-04-07T00:00:00Z' TIME RATE 4.0;
      """
    Then within "2s" the relay subscription receives payloads
      """
      "timestamp":"2026-04-07T00:00:00.
      "timestamp":"2026-04-07T00:00:01.
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
