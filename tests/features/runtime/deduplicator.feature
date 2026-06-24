Feature: Relay deduplication
  Scenario Outline: Deduplicator forwards the first message and suppresses duplicates
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I64
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss1 SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss2 SCHEMA transaction PARAMETERIZED BY transaction_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING ); CREATE INGESTOR source_txns
        TO ss1
        DECODE USING transaction_codec
        PARAMETERIZED BY transaction_id_branch VALUES { transaction_id = ss1.transaction_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 PARAMETERIZED BY transaction_id_branch
        DEDUPLICATE ON ss1.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss2;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "transaction_id":"txn-1"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Deduplicator accepts the same key again after MAX TIME expires
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I64
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss1 SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss2 SCHEMA transaction PARAMETERIZED BY transaction_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/dedup-expire' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING ); CREATE INGESTOR source_txns
        TO ss1
        DECODE USING transaction_codec
        PARAMETERIZED BY transaction_id_branch VALUES { transaction_id = ss1.transaction_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 PARAMETERIZED BY transaction_id_branch
        DEDUPLICATE ON ss1.transaction_id
        MAX TIME 300ms
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss2;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup-expire"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup-expire"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "transaction_id":"txn-1"
      """
    And the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup-expire"
      """
      {"transaction_id":"txn-1","amount":10}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "transaction_id":"txn-1"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Deduplicator evaluates DEDUPLICATE ON function calls through the VM
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        tenant STRING,
        transaction_id STRING,
        amount I32,
        payload STRING
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        tenant string,
        transaction_id string,
        amount integer,
        payload string
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss1 SCHEMA transaction PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss2 SCHEMA transaction PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup-functions'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR source_txns
        TO ss1
        DECODE USING transaction_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = ss1.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 PARAMETERIZED BY tenant_branch
        DEDUPLICATE ON lower(trim(ss1.transaction_id)), abs(ss1.amount)
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss2 WHERE ss2.tenant = 'acme';

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup-functions"
      """
      {"tenant":"acme","transaction_id":" Txn-1 ","amount":-10,"payload":"first"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup-functions"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10,"payload":"duplicate"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup-functions"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":11,"payload":"second-key"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "payload":"first"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    Then the relay subscription receives a payload
      """
      "payload":"second-key"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Describe deduplicator reports branch-local persistent state structure
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        tenant STRING,
        transaction_id STRING,
        amount I64
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        tenant string,
        transaction_id string,
        amount integer
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss1 SCHEMA transaction PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss2 SCHEMA transaction PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/dedup-describe' TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR source_txns
        TO ss1
        DECODE USING transaction_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = ss1.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 PARAMETERIZED BY tenant_branch
        DEDUPLICATE ON ss1.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      DESCRIBE DEDUPLICATOR dedup_txns;
      """
    Then the last command output contains
      """
      deduplicator: dedup_txns
      """
    And the last command output contains
      """
      branch-local: true
      """
    And the last command output contains
      """
      persistent state: true
      """
    And the last command output contains
      """
      replicated state: true
      """
    And the last command output contains
      """
      state structures: 1
      """
    And the last command output contains
      """
      structure 0:
        function: DEDUPLICATE_ON
        storage: recent_key_set
        key expressions: ss1.transaction_id
        max time: 10m
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Deduplicator filter-map reads materialized relay state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        transaction_id STRING,
        amount I32,
        source STRING
      );

      CREATE JSON WIRE SCHEMA transaction_wire (
        transaction_id string,
        amount integer,
        source string
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE RELAY state_txns
        SCHEMA transaction
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss1 SCHEMA transaction PARAMETERIZED BY transaction_id_branch;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss2 SCHEMA transaction PARAMETERIZED BY transaction_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup'
        TYPE HTTP;

      CREATE INGESTOR state_txns_ingestor
        TO state_txns
        DECODE USING transaction_codec
        UNPARAMETERIZED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE INGESTOR source_txns
        TO ss1
        DECODE USING transaction_codec
        PARAMETERIZED BY transaction_id_branch VALUES { transaction_id = ss1.transaction_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 SET ss2.source = state_txns.source PARAMETERIZED BY transaction_id_branch
        DEDUPLICATE ON ss1.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss2;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"transaction_id":"txn-1","amount":10,"source":"state"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "state_txns" containing
      """
      key=none payload={"amount":10,"source":"state","transaction_id":"txn-1"}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"transaction_id":"txn-1","amount":10,"source":"input"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"amount":10,"source":"state","transaction_id":"txn-1"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Deduplicator creation rejects unknown deduplication fields
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "DEDUPLICATE ON compile failed"
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING
      );

      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss1 SCHEMA notification PARAMETERIZED BY transaction_id_branch;
      CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
      CREATE RELAY ss2 SCHEMA notification PARAMETERIZED BY transaction_id_branch;

      CREATE DEDUPLICATOR dedup_txns
        FROM ss1 TO ss2 PARAMETERIZED BY transaction_id_branch
        DEDUPLICATE ON ss1.transaction_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
