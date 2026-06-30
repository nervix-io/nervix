Feature: Prometheus TLS resource mounts
  Scenario Outline: Prometheus ingestor queries over native TLS with a mounted resource directory
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
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

      CREATE RELAY samples SCHEMA sample;

      CREATE CLIENT prom_tls
        TYPE PROMETHEUS
        MOUNT dev_tls
        CONFIG {
          'addr' = 'https://127.0.0.1:9443',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };

      CREATE IF NOT EXISTS SCHEMA source_branch ( source STRING ); CREATE IF NOT EXISTS BRANCH by_prom_samples PARAMETERIZED BY source_branch VALUES { source = samples.source } TTL 5m; CREATE INGESTOR prom_samples
        TO samples
        DECODE USING sample_codec
        BRANCHED BY by_prom_samples
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM PROMETHEUS prom_tls
        QUERY 'label_replace(vector(42.5), "source", "prometheus", "", "")'
        EVERY 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO samples;
      START;
      """
    Then the relay subscription receives a payload
      """
      "source":"prometheus"
      """
    And the last relay subscription payload contains key fragment '{"source":"prometheus"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
