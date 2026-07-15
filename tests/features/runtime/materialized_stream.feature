Feature: Materialized relay state
  Scenario Outline: Materialized relay state is resolved from the current concrete branch
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer,
        source string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_state_notifications SCHEMA tenant_branch TTL 5m;
        CREATE RELAY tenant_state
        SCHEMA notification
        BRANCHED BY by_state_notifications
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE RELAY incoming_notifications SCHEMA notification BRANCHED BY by_state_notifications;
        CREATE RELAY enriched_notifications SCHEMA notification BRANCHED BY by_state_notifications;
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
        TO tenant_state
        DECODE USING notification_codec
        BRANCHED BY by_state_notifications VALUES { tenant = tenant_state.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE INGESTOR http_notifications
        TO incoming_notifications
        DECODE USING notification_codec
        BRANCHED BY by_state_notifications VALUES { tenant = incoming_notifications.tenant }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR enrich_notifications
        FROM incoming_notifications
        TO enriched_notifications
          SET enriched_notifications.source = tenant_state.source
        BRANCHED BY by_state_notifications
        DEDUPLICATE ON incoming_notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION enriched_notifications_subscription TO enriched_notifications;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-state"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"beta","user_id":2,"source":"beta-state"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"acme"} payload={"source":"acme-state","tenant":"acme","user_id":1}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"beta"} payload={"source":"beta-state","tenant":"beta","user_id":2}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":10,"source":"input"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"source":"acme-state","tenant":"acme","user_id":10}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":20,"source":"input"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"source":"beta-state","tenant":"beta","user_id":20}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Materialized relays keep the latest value by message watermark
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
        user_id I64,
        amount I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        amount integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications
        SCHEMA notification BRANCHED BY by_http_notifications
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":1}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      key={"user_id":42} payload={"amount":1,"user_id":42}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      materialized relay: notifications
      kind: MATERIALIZER
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      owner: node-
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      replicas:
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":2}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      key={"user_id":42} payload={"amount":2,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: START resets materialized relay state
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
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications
        SCHEMA notification BRANCHED BY by_http_notifications
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      key={"user_id":42} payload={"user_id":42}
      """
    When these NSPL commands are executed on the leader node
      """
      STOP;
      START;
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      relay 'notifications' materialized state is empty
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Expiration deletes materialized relay state
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
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 500ms;
        CREATE RELAY notifications
        SCHEMA notification BRANCHED BY by_http_notifications
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      key={"user_id":42} payload={"user_id":42}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "notifications" containing
      """
      relay 'notifications' materialized state is empty
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
