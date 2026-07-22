Feature: Drain node

  Scenario: Draining a primary promotes a live replica
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
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

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );

      CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA transaction_id_branch TTL 5m;

      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_source_txns;

      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_source_txns;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;

      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_source_txns
        TO deduped
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;

      DESCRIBE DEDUPLICATOR dedup_txns;
      """
    Then the last command output owner is saved as placeholder "drained_primary_node"
    And the first replica in the last command output is saved as placeholder "expected_promoted_replica"
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE {{drained_primary_node}};
      DESCRIBE DEDUPLICATOR dedup_txns;
      """
    Then the last command output owner equals placeholder "expected_promoted_replica"

  Scenario: Draining a node cordons it and moves scheduled graph nodes away
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE UNPACED DOMAIN {{domain}};

      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE INGESTOR kafka_a
        FROM KAFKA kafka_main TOPIC notifications_a_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_a_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR kafka_b
        FROM KAFKA kafka_main TOPIC notifications_b_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_b_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR kafka_c
        FROM KAFKA kafka_main TOPIC notifications_c_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_c_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      DRAIN NODE node-2;
      """
    Then the last command output contains
      """
      drained node 'node-2' (moved
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      raft.cordoned_nodes: node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR kafka_a;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR kafka_b;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR kafka_c;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """
