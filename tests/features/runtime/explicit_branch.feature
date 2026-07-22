Feature: Explicit branches
  Scenario Outline: Explicit branch LRU eviction removes the least recently used concrete branch
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
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
        message STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        message string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m MAX INSTANCES 1 EVICT LRU;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

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
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","message":"first"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      key={"tenant":"acme"} payload={"message":"first","tenant":"acme"}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","message":"second"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      key={"tenant":"beta"} payload={"message":"second","tenant":"beta"}
      """
    And within "5s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (tenant = 'acme');
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
