Feature: Relay branch presence
  Scenario Outline: Relay branch presence follows interleaved branches through LRU eviction and recreation
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
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

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m MAX INSTANCES 2 EVICT LRU;

      CREATE RELAY tenant_state
        SCHEMA notification
        BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY incoming_notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE RELAY enriched_notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR state_notifications
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO tenant_state
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR http_notifications
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming_notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR enrich_notifications
        FROM incoming_notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_tenant
        USING MATERIALIZED STATE tenant_state REQUIRED WAIT
        TO enriched_notifications
          INHERIT ALL
          SET source = relay_state.tenant_state.source
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION enriched_notifications_subscription TO enriched_notifications;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-first"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"beta","user_id":2,"source":"beta-first"}
      """
    Then within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'acme');
      """
    And within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'beta');
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'gamma');
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":10,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"source":"acme-first","tenant":"acme","user_id":10}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":20,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"source":"beta-first","tenant":"beta","user_id":20}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"gamma","user_id":3,"source":"gamma-first"}
      """
    Then within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'gamma');
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'acme');
      """
    And within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'beta');
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"gamma"} payload={"source":"gamma-first","tenant":"gamma","user_id":3}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","user_id":4,"source":"acme-second"}
      """
    Then within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'acme');
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'beta');
      """
    And within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY tenant_state WHERE (tenant = 'gamma');
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"acme"} payload={"source":"acme-second","tenant":"acme","user_id":4}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":30,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"source":"acme-second","tenant":"acme","user_id":30}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"gamma","user_id":31,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"source":"gamma-first","tenant":"gamma","user_id":31}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":32,"source":"input"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"source":"acme-second","tenant":"acme","user_id":32}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
