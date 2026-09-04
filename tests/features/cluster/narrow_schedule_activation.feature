Feature: Narrow schedule activation on ownership change

  Scenario: Deduplicator state on an unaffected runtime node survives a drain
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        tenant STRING,
        transaction_id STRING,
        amount I64
      );

      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string,
        amount integer
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_tenant SCHEMA tenant_branch TTL 5m;

      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;

      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;

      CREATE RELAY spare_input SCHEMA transaction BRANCHED BY by_tenant;

      CREATE RELAY spare_output SCHEMA transaction BRANCHED BY by_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/narrow'
        TYPE HTTP;

      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      CORDON NODE node-1;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION spare_route FROM spare_input
        BRANCHED BY by_tenant
        TO spare_output
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=dedup_txns owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=spare_route owner=node-2
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION deduped_subscription TO deduped;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"acme","transaction_id":"txn-warmup","amount":1}
      """
    And within "20s" the relay subscription receives a payload
      """
      "transaction_id":"txn-warmup"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"amount":10,"tenant":"acme","transaction_id":"txn-1"}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And within "10s" the relay subscription receives a payload
      """
      {"amount":20,"tenant":"beta","transaction_id":"txn-1"}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'
    And the relay subscription does not receive a payload within "2s"
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE node-2;
      SHOW CLUSTER STATUS;
      """
    Then the last command output does not contain
      """
      - domain={{domain}} kind=junction name=spare_route owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=dedup_txns owner=node-1
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION deduped_after_drain TO deduped;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    Then the relay subscription does not receive a payload within "5s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/narrow"
      """
      {"tenant":"acme","transaction_id":"txn-2","amount":11}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"amount":11,"tenant":"acme","transaction_id":"txn-2"}
      """

  Scenario: Live branch gauges stay continuous for an unaffected runtime node
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        tenant STRING,
        transaction_id STRING
      );

      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_gauge_tenant SCHEMA tenant_branch TTL 30m;

      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_gauge_tenant;

      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_gauge_tenant;

      CREATE RELAY spare_input SCHEMA transaction BRANCHED BY by_gauge_tenant;

      CREATE RELAY spare_output SCHEMA transaction BRANCHED BY by_gauge_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/gauge'
        TYPE HTTP;

      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_gauge_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_gauge_tenant
        TO deduped
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      CORDON NODE node-1;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION spare_route FROM spare_input
        BRANCHED BY by_gauge_tenant
        TO spare_output
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION deduped_subscription TO deduped;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/gauge"
      """
      {"tenant":"acme","transaction_id":"txn-warmup"}
      """
    And within "20s" the relay subscription receives a payload
      """
      "transaction_id":"txn-warmup"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/gauge"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "transaction_id":"txn-1"
      """
    And node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 1
      """
      domain="{{domain}}"
      branch="by_gauge_tenant"
      physical_node_id="node-1"
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE node-2;
      SHOW CLUSTER STATUS;
      """
    Then the last command output does not contain
      """
      - domain={{domain}} kind=junction name=spare_route owner=node-2
      """
    And node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 1
      """
      domain="{{domain}}"
      branch="by_gauge_tenant"
      physical_node_id="node-1"
      """

  Scenario: REQUIRED WAIT retention on an unaffected reader survives another runtime node's move
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64,
        source STRING
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer,
        source string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_wait_tenant SCHEMA tenant_branch TTL 30m;

      CREATE RELAY tenant_state SCHEMA notification
        BRANCHED BY by_wait_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_wait_tenant;

      CREATE RELAY enriched SCHEMA notification BRANCHED BY by_wait_tenant;

      CREATE RELAY spare_input SCHEMA notification BRANCHED BY by_wait_tenant;

      CREATE RELAY spare_output SCHEMA notification BRANCHED BY by_wait_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT state_ingress
        ON edge
        PATH '/wait-state'
        TYPE HTTP;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/wait-ingest'
        TYPE HTTP;

      CREATE INGESTOR state_source
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO tenant_state
        INHERIT ALL
        BRANCHED BY by_wait_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR event_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming
        INHERIT ALL
        BRANCHED BY by_wait_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE JUNCTION enrich_events FROM incoming
        BRANCHED BY by_wait_tenant
        USING MATERIALIZED STATE tenant_state REQUIRED WAIT
        TO enriched
        INHERIT ALL
        SET source = relay_state.tenant_state.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      CORDON NODE node-1;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION spare_route FROM spare_input
        BRANCHED BY by_wait_tenant
        TO spare_output
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=enrich_events owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=spare_route owner=node-2
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION enriched_subscription TO enriched;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/wait-state"
      """
      {"tenant":"warmup","user_id":1,"source":"warmup-state"}
      """
    And node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/wait-ingest"
      """
      {"tenant":"warmup","user_id":2,"source":"input"}
      """
    And within "20s" the relay subscription receives a payload
      """
      {"source":"warmup-state","tenant":"warmup","user_id":2}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/wait-ingest"
      """
      {"tenant":"acme","user_id":10,"source":"input"}
      """
    Then the relay subscription does not receive a payload within "2s"
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE node-2;
      SHOW CLUSTER STATUS;
      """
    Then the last command output does not contain
      """
      - domain={{domain}} kind=junction name=spare_route owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=enrich_events owner=node-1
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION enriched_after_drain TO enriched;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/wait-state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-state"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/wait-state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-state"}
      """
    Then within "15s" the relay subscription receives a payload
      """
      {"source":"acme-state","tenant":"acme","user_id":10}
      """

  Scenario: A partial window on an unaffected window processor survives a failover elsewhere
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA metric (
        tenant STRING,
        latency I64
      );

      CREATE SCHEMA metric_summary (
        tenant STRING,
        sample_count I64,
        total_latency I64
      );

      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT (
        tenant string,
        latency integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_window_tenant SCHEMA tenant_branch TTL 30m;

      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_window_tenant;

      CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_window_tenant;

      CREATE RELAY spare_input SCHEMA metric BRANCHED BY by_window_tenant;

      CREATE RELAY spare_output SCHEMA metric BRANCHED BY by_window_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/window'
        TYPE HTTP;

      CREATE INGESTOR metric_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics
        INHERIT ALL
        BRANCHED BY by_window_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE WINDOW PROCESSOR latency_window FROM metrics
        WIDTH 4 MESSAGES
        STEP 4 MESSAGES
        BRANCHED BY by_window_tenant
        TO metric_summaries
        SET tenant = FIRST(input.tenant), sample_count = COUNT(input.latency), total_latency = SUM(input.latency)
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-3;
      CORDON NODE node-1;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION spare_route FROM spare_input
        BRANCHED BY by_window_tenant
        TO spare_output
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-2;
      """
    And these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=window_processor name=latency_window owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=spare_route owner=node-3
      """
    And the last cluster status owner for scheduled "junction" "spare_route" is saved as placeholder "stopped_spare_owner"
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION metric_summaries_subscription TO metric_summaries;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/window"
      """
      {"tenant":"acme","latency":10}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/window"
      """
      {"tenant":"acme","latency":20}
      """
    Then the relay subscription does not receive a payload within "2s"
    When node "node-3" is stopped
    Then node "node-1" eventually observes a stable leader
    And within "30s" node "node-1" eventually reports scheduled "junction" "spare_route" owner different from placeholder "stopped_spare_owner"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/window"
      """
      {"tenant":"acme","latency":30}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/window"
      """
      {"tenant":"acme","latency":40}
      """
    Then within "20s" the relay subscription receives a payload
      """
      {"sample_count":4,"tenant":"acme","total_latency":100}
      """

  Scenario: Moving a REQUIRE COLOCATION group restarts only its members
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction (
        tenant STRING,
        transaction_id STRING
      );

      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string
      );

      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_group_tenant SCHEMA tenant_branch TTL 30m;

      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_group_tenant;

      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_group_tenant;

      CREATE RELAY corridor_input SCHEMA transaction BRANCHED BY by_group_tenant;

      CREATE RELAY corridor_stage SCHEMA transaction BRANCHED BY by_group_tenant;

      CREATE RELAY corridor_output SCHEMA transaction BRANCHED BY by_group_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/group'
        TYPE HTTP;

      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound
        INHERIT ALL
        BRANCHED BY by_group_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_group_tenant
        TO deduped
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      CORDON NODE node-1;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION corridor_source FROM corridor_input
        BRANCHED BY by_group_tenant
        TO corridor_stage
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-3;
      CORDON NODE node-2;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION corridor_sink FROM corridor_stage
        BRANCHED BY by_group_tenant
        TO corridor_output
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;

      CREATE PLACEMENT keep_corridor_local
        FROM corridor_source TO corridor_sink PREFER COLOCATION RANK 1;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-2;
      """
    And these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=dedup_txns owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=corridor_source owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=corridor_sink owner=node-3
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION deduped_subscription TO deduped;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/group"
      """
      {"tenant":"acme","transaction_id":"txn-warmup"}
      """
    And within "20s" the relay subscription receives a payload
      """
      "transaction_id":"txn-warmup"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/group"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "transaction_id":"txn-1"
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER PLACEMENT keep_corridor_local SET POLICY REQUIRE COLOCATION;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "corridor_source" is saved as placeholder "consolidated_owner"
    And within "10s" node "node-1" eventually reports scheduled "junction" "corridor_sink" owner equals placeholder "consolidated_owner"
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=dedup_txns owner=node-1
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION deduped_after_group_move TO deduped;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/group"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    Then the relay subscription does not receive a payload within "5s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/group"
      """
      {"tenant":"acme","transaction_id":"txn-2"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "transaction_id":"txn-2"
      """

  Scenario: A reader keeps running when its materialized relay moves
    Given runtime replication is configured with replica count 1 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-1;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64,
        source STRING
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer,
        source string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_move_tenant SCHEMA tenant_branch TTL 30m;

      CREATE RELAY tenant_state SCHEMA notification
        BRANCHED BY by_move_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_move_tenant;

      CREATE RELAY enriched SCHEMA notification BRANCHED BY by_move_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT state_ingress
        ON edge
        PATH '/move-state'
        TYPE HTTP;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/move-ingest'
        TYPE HTTP;

      CREATE INGESTOR state_source
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO tenant_state
        INHERIT ALL
        BRANCHED BY by_move_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR event_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming
        INHERIT ALL
        BRANCHED BY by_move_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION enrich_events FROM incoming
        BRANCHED BY by_move_tenant
        USING MATERIALIZED STATE tenant_state REQUIRED WAIT
        TO enriched
        INHERIT ALL
        SET source = relay_state.tenant_state.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=enrich_events owner=node-1
      """
    And the last cluster status owner for scheduled "relay" "tenant_state" is saved as placeholder "relay_owner"
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION enriched_subscription TO enriched;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/move-state"
      """
      {"tenant":"warmup","user_id":1,"source":"warmup-state"}
      """
    And node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/move-ingest"
      """
      {"tenant":"warmup","user_id":2,"source":"input"}
      """
    And within "20s" the relay subscription receives a payload
      """
      {"source":"warmup-state","tenant":"warmup","user_id":2}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/move-ingest"
      """
      {"tenant":"acme","user_id":10,"source":"input"}
      """
    Then the relay subscription does not receive a payload within "2s"
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE {{relay_owner}};
      """
    Then within "15s" node "node-1" eventually reports scheduled "relay" "tenant_state" owner different from placeholder "relay_owner"
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=enrich_events owner=node-1
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION enriched_after_move TO enriched;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/move-state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-state"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/move-state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-state"}
      """
    Then within "15s" the relay subscription receives a payload
      """
      {"source":"acme-state","tenant":"acme","user_id":10}
      """
