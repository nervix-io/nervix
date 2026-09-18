Feature: Materialized relay state
  Scenario Outline: Unbranched materialized reports render the root branch locally and remotely
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA root_event (
        value STRING
      );

      CREATE WIRE JSON SCHEMA root_event_wire MODE STRICT (
        value string
      );

      CREATE CODEC root_event_codec
        FROM WIRE JSON SCHEMA root_event_wire
        TO SCHEMA root_event;

      CREATE RELAY root_state
        SCHEMA root_event
        UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE VHOST edge root-state-{{test_id}}.example.com;
      CREATE ENDPOINT root_state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;

      CREATE INGESTOR root_state_source
        FROM ENDPOINT root_state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING root_event_codec
        TO root_state
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    When http payload is posted to node "node-1" with host "root-state-{{test_id}}.example.com" path "/state"
      """
      {"value":"root-value"}
      """
    Then within "5s" node "<report_node>" eventually reports materialized state for relay "root_state" containing
      """
      key=(root) payload={"value":"root-value"}
      """

    Examples:
      | cluster_size | report_node |
      | 1            | node-1      |
      | 3            | node-1      |
      | 3            | node-2      |
      | 3            | node-3      |

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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer,
        source string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
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
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO tenant_state
        INHERIT ALL
        BRANCHED BY by_state_notifications
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO incoming_notifications
        INHERIT ALL
        BRANCHED BY by_state_notifications
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR enrich_notifications FROM incoming_notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_state_notifications
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
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":30,"source":"input"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"source":"acme-state","tenant":"acme","user_id":30}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario: Materialized dependencies resolve in written order after REQUIRED WAIT wakes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA dependency_event (
        tenant STRING,
        value STRING,
        first_value STRING OPTIONAL,
        second_value STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA dependency_event_wire MODE STRICT (
        tenant string,
        value string,
        first_value string OPTIONAL,
        second_value string OPTIONAL
      );
      CREATE CODEC dependency_event_codec
        FROM WIRE JSON SCHEMA dependency_event_wire
        TO SCHEMA dependency_event;
      CREATE SCHEMA dependency_tenant ( tenant STRING );
      CREATE BRANCH by_dependency_tenant SCHEMA dependency_tenant TTL 5m;
      CREATE RELAY first_dependency_state
        SCHEMA dependency_event
        BRANCHED BY by_dependency_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY second_dependency_state
        SCHEMA dependency_event
        BRANCHED BY by_dependency_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY dependency_input
        SCHEMA dependency_event
        BRANCHED BY by_dependency_tenant;
      CREATE RELAY dependency_output
        SCHEMA dependency_event
        BRANCHED BY by_dependency_tenant;
      CREATE VHOST edge http-materialized-order-{{test_id}}.example.com;
      CREATE ENDPOINT first_state_ingress ON edge PATH '/first-state' TYPE HTTP;
      CREATE ENDPOINT second_state_ingress ON edge PATH '/second-state' TYPE HTTP;
      CREATE ENDPOINT dependency_ingress ON edge PATH '/dependency-input' TYPE HTTP;
      CREATE INGESTOR first_state_source
        FROM ENDPOINT first_state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING dependency_event_codec
        TO first_dependency_state
          INHERIT ALL
          BRANCHED BY by_dependency_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR second_state_source
        FROM ENDPOINT second_state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING dependency_event_codec
        TO second_dependency_state
          INHERIT ALL
          BRANCHED BY by_dependency_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR dependency_source
        FROM ENDPOINT dependency_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING dependency_event_codec
        TO dependency_input
          INHERIT ALL
          BRANCHED BY by_dependency_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION resolve_dependencies
        FROM dependency_input
        BRANCHED BY by_dependency_tenant
        USING MATERIALIZED STATE first_dependency_state REQUIRED WAIT
        USING MATERIALIZED STATE second_dependency_state REQUIRED SKIP
        TO dependency_output
          INHERIT ALL
          SET first_value = relay_state.first_dependency_state.value,
              second_value = relay_state.second_dependency_state.value
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION dependency_output_subscription TO dependency_output;
      START;
      """
    When http payload is posted to node "node-1" with host "http-materialized-order-{{test_id}}.example.com" path "/dependency-input"
      """
      {"tenant":"acme","value":"input-acme"}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When http payload is posted to node "node-1" with host "http-materialized-order-{{test_id}}.example.com" path "/second-state"
      """
      {"tenant":"acme","value":"second-acme"}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When http payload is posted to node "node-1" with host "http-materialized-order-{{test_id}}.example.com" path "/first-state"
      """
      {"tenant":"acme","value":"first-acme"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"first_value":"first-acme","second_value":"second-acme","tenant":"acme","value":"input-acme"}
      """
    When http payload is posted to node "node-1" with host "http-materialized-order-{{test_id}}.example.com" path "/dependency-input"
      """
      {"tenant":"beta","value":"input-beta"}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When http payload is posted to node "node-1" with host "http-materialized-order-{{test_id}}.example.com" path "/first-state"
      """
      {"tenant":"beta","value":"first-beta"}
      """
    Then the relay subscription does not receive a payload within "300ms"
    When http payload is posted to node "node-1" with host "http-materialized-order-{{test_id}}.example.com" path "/second-state"
      """
      {"tenant":"beta","value":"second-beta"}
      """
    Then the relay subscription does not receive a payload within "1s"

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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
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
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
      kind: RELAY
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
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
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
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
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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

  Scenario Outline: Concurrent branch updates recover from one consistent columnar snapshot
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And a repeated text placeholder "bulk_blob" of 1500000 bytes is prepared
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
        source STRING,
        zblob STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        user_id integer,
        source string,
        zblob string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_tenant_notifications SCHEMA tenant_branch TTL 5m;
        CREATE RELAY tenant_state
        SCHEMA notification
        BRANCHED BY by_tenant_notifications
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;
        CREATE INGESTOR state_notifications
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 32MiB DECODE USING notification_codec
        TO tenant_state
        INHERIT ALL
        BRANCHED BY by_tenant_notifications
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","user_id":1,"source":"acme-first","zblob":"x"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"globex","user_id":2,"source":"globex-first","zblob":"x"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","user_id":3,"source":"acme-second","zblob":"{{bulk_blob}}"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"globex","user_id":4,"source":"globex-second","zblob":"y"}
      """
    Then within "10s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"acme"} payload={"source":"acme-second","tenant":"acme","user_id":3,"zblob":"aaaaaaaaaa
      """
    And within "10s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"globex"} payload={"source":"globex-second","tenant":"globex","user_id":4,"zblob":"y"}
      """
    When the cluster is restarted
    Then within "30s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"acme"} payload={"source":"acme-second","tenant":"acme","user_id":3,"zblob":"aaaaaaaaaa
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "tenant_state" containing
      """
      key={"tenant":"globex"} payload={"source":"globex-second","tenant":"globex","user_id":4,"zblob":"y"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
