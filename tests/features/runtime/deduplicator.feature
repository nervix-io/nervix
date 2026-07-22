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
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
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
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_txns FROM ss1
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_source_txns
        TO ss2
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss2_subscription TO ss2;
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
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        transaction_id string,
        amount integer
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
        CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
        CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA transaction_id_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE RELAY ss2 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress ON edge PATH '/dedup-expire' TYPE HTTP;
        CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_txns FROM ss1
        DEDUPLICATE ON input.transaction_id
        MAX TIME 300ms
        BRANCHED BY by_source_txns
        TO ss2
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss2_subscription TO ss2;
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
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        tenant string,
        transaction_id string,
        amount integer,
        payload string
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA tenant_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE RELAY ss2 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/dedup-functions'
        TYPE HTTP;
        CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_txns FROM ss1
        DEDUPLICATE ON lower(trim(input.transaction_id)), abs(input.amount)
        MAX TIME 10m
        BRANCHED BY by_source_txns
        TO ss2
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss2_subscription TO ss2 WHERE tenant = 'acme';
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
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        tenant string,
        transaction_id string,
        amount integer
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA tenant_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE RELAY ss2 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress ON edge PATH '/dedup-describe' TYPE HTTP;
        CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_txns FROM ss1
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_source_txns
        TO ss2
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
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
        key expressions: input.transaction_id
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
        CREATE STRICT WIRE JSON SCHEMA transaction_wire (
        transaction_id string,
        amount integer,
        source string
      );
        CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
        CREATE IF NOT EXISTS SCHEMA transaction_id_branch ( transaction_id STRING );
        CREATE IF NOT EXISTS BRANCH by_source_txns SCHEMA transaction_id_branch TTL 5m;
        CREATE RELAY state_txns
        SCHEMA transaction BRANCHED BY by_source_txns
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE RELAY ss1 SCHEMA transaction BRANCHED BY by_source_txns;
        CREATE RELAY ss2 SCHEMA transaction BRANCHED BY by_source_txns;
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
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO state_txns
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING transaction_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_txns
        SET transaction_id = message.transaction_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR dedup_txns FROM ss1
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_source_txns
        USING MATERIALIZED STATE state_txns REQUIRED WAIT
        TO ss2
        INHERIT ALL
        SET source = relay_state.state_txns.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss2_subscription TO ss2;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"transaction_id":"txn-1","amount":10,"source":"state"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "state_txns" containing
      """
      key={"transaction_id":"txn-1"} payload={"amount":10,"source":"state","transaction_id":"txn-1"}
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
        CREATE IF NOT EXISTS BRANCH by_dedup_txns SCHEMA transaction_id_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA notification BRANCHED BY by_dedup_txns;
        CREATE RELAY ss2 SCHEMA notification BRANCHED BY by_dedup_txns;
        CREATE DEDUPLICATOR dedup_txns FROM ss1
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_dedup_txns
        TO ss2
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
