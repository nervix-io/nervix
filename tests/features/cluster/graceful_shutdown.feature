Feature: Graceful shutdown

  Scenario: Graceful shutdown skips drain when no replacement node exists
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "5s"
    And a 1 node nervix cluster is started
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "2s"
    When node "node-1" is started
    Then node "node-1" eventually observes a stable leader

  Scenario: Graceful shutdown drain timeout bounds full cluster termination
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "1ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};

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
      CREATE RELAY inbound SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY deduped SCHEMA transaction PARAMETERIZED BY transaction_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;

      CREATE IF NOT EXISTS BRANCH by_source_txns PARAMETERIZED BY transaction_id_branch VALUES { transaction_id = inbound.transaction_id } TTL 5m; CREATE INGESTOR source_txns
        TO inbound
        DECODE USING transaction_codec
        BRANCHED BY by_source_txns
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns
        FROM inbound TO deduped BRANCHED BY by_source_txns
        DEDUPLICATE ON inbound.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """
    When all nodes are gracefully stopped
    Then the last cluster operation completes within "5s"
