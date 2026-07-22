Feature: Branched branch behavior
  @json_branch_key
  Scenario Outline: Branch keys are rendered as JSON instead of delimiter encoded text
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id U32,
        body STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer,
        body string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_user_branch (
        tenant STRING,
        user_id U32
      );

      CREATE IF NOT EXISTS BRANCH by_source_notifications SCHEMA tenant_user_branch TTL 5m;

      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_source_notifications;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/notifications'
        TYPE HTTP; CREATE INGESTOR source_notifications
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_source_notifications
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/notifications"
      """
      {"tenant":"acme|west=1","user_id":42,"body":"hello"}
      """
    Then the relay subscription receives a payload
      """
      "body":"hello"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme|west=1","user_id":42}'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Deduplicator suppresses duplicates only within the same concrete relay
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
        CREATE SUBSCRIPTION ss2_subscription TO ss2;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    Then the relay subscription receives a payload
      """
      "amount":10
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    Then the relay subscription receives a payload
      """
      "amount":20
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/dedup"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":30}
      """
    Then the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Window processor aggregates only within each concrete branch
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
        latency F64
      );
        CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64
      );
        CREATE STRICT WIRE JSON SCHEMA metric_wire (
        tenant string,
        latency number
      );
        CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_metric_ingestor SCHEMA tenant_branch TTL 5m;
        CREATE RELAY metrics SCHEMA metric BRANCHED BY by_metric_ingestor;
        CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_metric_ingestor;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/metrics'
        TYPE HTTP;
        CREATE INGESTOR metric_ingestor
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING metric_codec
        TO metrics
        INHERIT ALL
        BRANCHED BY by_metric_ingestor
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE WINDOW PROCESSOR latency_window FROM metrics
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        BRANCHED BY by_metric_ingestor
        TO metric_summaries
        SET tenant = FIRST(input.tenant), sample_count = COUNT(input.latency)
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION metric_summaries_subscription TO metric_summaries WHERE tenant = 'acme';
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":10.0}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":20.0}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"acme","latency":30.0}
      """
    Then the relay subscription receives a payload
      """
      "sample_count":2
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Junction preserves aligned branched relays without mixing them
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        source STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        source string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_source_one SCHEMA tenant_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA notification BRANCHED BY by_source_one;
        CREATE RELAY ss2 SCHEMA notification BRANCHED BY by_source_one;
        CREATE RELAY ss10 SCHEMA notification BRANCHED BY by_source_one;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress_one
        ON edge
        PATH '/ingest-a'
        TYPE HTTP;
        CREATE ENDPOINT ingress_two
        ON edge
        PATH '/ingest-b'
        TYPE HTTP;
        CREATE INGESTOR source_one
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_one
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR source_two
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss2
        INHERIT ALL
        BRANCHED BY by_source_one
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE JUNCTION join_streams FROM ss1, ss2
        BRANCHED BY by_source_one
        TO ss10
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss10_subscription TO ss10 WHERE tenant = 'acme';
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest-a"
      """
      {"tenant":"beta","source":"left"}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest-b"
      """
      {"tenant":"acme","source":"right"}
      """
    Then the relay subscription receives a payload
      """
      "source":"right"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Reingestor is the node that changes branch branching
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA tenant_user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_tenant_partition SCHEMA tenant_branch TTL 5m;
        CREATE RELAY tenant_notifications SCHEMA notification BRANCHED BY by_tenant_partition;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE REINGESTOR tenant_partition FROM notifications
        TO tenant_notifications
        INHERIT ALL
        BRANCHED BY by_tenant_partition
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION tenant_notifications_subscription TO tenant_notifications WHERE tenant = 'acme';
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":1}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":11}
      """
    Then the relay subscription receives a payload
      """
      {"tenant":"acme","user_id":11}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":22}
      """
    Then the relay subscription receives a payload
      """
      {"tenant":"acme","user_id":22}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
