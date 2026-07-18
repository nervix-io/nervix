Feature: Deduplicator state replication
  Scenario Outline: Deduplicator suppression survives a cluster restart from persisted snapshots
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I64
      );
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
        CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
        CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
        CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA transaction_id_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE RELAY ss2 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;
        CREATE INGESTOR source_txns
        TO ss1 FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING transaction_codec
        BRANCHED BY by_source_txns VALUES { transaction_id = ss1.transaction_id }

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG BRANCHED BY by_source_txns
        DEDUPLICATE ON ss1.transaction_id
        MAX TIME 10m;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE EMITTER kafka_notifications
        FROM ss2
        ENCODE USING transaction_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    Then within "10s" repeatedly posting http payload to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup" yields an observed broker payload
      """
      {"transaction_id":"warmup","amount":1}
      """
    And within "10s" repeatedly posting http payload to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup" yields an observed broker payload
      """
      {"transaction_id":"txn-1","amount":10}
      """
    And the observed broker does not receive a payload within "300ms"
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    Then the observed broker does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
