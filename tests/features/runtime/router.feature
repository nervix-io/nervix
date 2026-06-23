Feature: Relay routing
  Scenario Outline: Router MATCH ALL forwards to all matching branches and defaults when none match
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        id STRING,
        active BOOL,
        level STRING,
        urgent BOOL
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        id string,
        active boolean,
        level string,
        urgent boolean
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY incoming_logs SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY errors_ss SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY warnings_ss SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY info_ss SCHEMA notification PARAMETERIZED BY id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/route'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING ); CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        PARAMETERIZED BY id_branch VALUES { id = incoming_logs.id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE ROUTER log_router
        FROM incoming_logs
        WHERE incoming_logs.active
        TO errors_ss WHERE incoming_logs.level = "error"
        TO warnings_ss WHERE incoming_logs.urgent
        MATCH ALL DEFAULT TO info_ss PARAMETERIZED BY id_branch
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO errors_ss;
      SUBSCRIBE SESSION TO warnings_ss;
      SUBSCRIBE SESSION TO info_ss;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route"
      """
      {"id":"err-1","active":true,"level":"error","urgent":true}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route"
      """
      {"id":"info-1","active":true,"level":"info","urgent":false}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route"
      """
      {"id":"ignored","active":false,"level":"error","urgent":true}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "id":"err-1"
      "id":"err-1"
      "id":"info-1"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Router default route preserves parameterization and applies filter-map rewrites
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification_in (
        tenant STRING,
        user_id I64,
        active BOOL,
        amount I64,
        raw STRING
      );

      CREATE SCHEMA notification_out (
        tenant STRING,
        user_id I64,
        amount I64,
        normalized STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer,
        active boolean,
        amount integer,
        raw string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE RELAY notifications SCHEMA notification_in PARAMETERIZED BY tenant_user_id_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE RELAY projected_notifications SCHEMA notification_out PARAMETERIZED BY tenant_user_id_branch;

      CREATE VHOST edge http-default-router-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/default-route'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE ROUTER project_notifications
        FROM notifications
        SET notifications.normalized = lower(trim(notifications.raw)), notifications.amount = notifications.amount + 1
        UNSET notifications.raw, notifications.active
        WHERE notifications.active
        DEFAULT TO projected_notifications PARAMETERIZED BY tenant_user_id_branch
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO projected_notifications;
      START;
      """
    When http payload is posted to node "node-1" with host "http-default-router-{{test_id}}.example.com" path "/default-route"
      """
      {"tenant":"acme","user_id":42,"active":true,"amount":9,"raw":"  HELLO  "}
      """
    Then the relay subscription receives a payload
      """
      "normalized":"hello"
      """
    And the last relay subscription payload contains
      """
      "tenant":"acme"
      "user_id":42
      "amount":10
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme","user_id":42}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload does not contain "active\""
    When http payload is posted to node "node-1" with host "http-default-router-{{test_id}}.example.com" path "/default-route"
      """
      {"tenant":"acme","user_id":42,"active":false,"amount":99,"raw":"should drop"}
      """
    Then the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Router MATCH FIRST forwards only to the first matching branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        id STRING,
        level STRING,
        urgent BOOL
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        id string,
        level string,
        urgent boolean
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY incoming_logs SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY errors_ss SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY warnings_ss SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY info_ss SCHEMA notification PARAMETERIZED BY id_branch;

      CREATE VHOST edge http-first-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/route-first'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING ); CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        PARAMETERIZED BY id_branch VALUES { id = incoming_logs.id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE ROUTER log_router
        FROM incoming_logs
        TO errors_ss WHERE incoming_logs.level = "error"
        TO warnings_ss WHERE incoming_logs.urgent
        MATCH FIRST DEFAULT TO info_ss PARAMETERIZED BY id_branch
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO errors_ss;
      SUBSCRIBE SESSION TO warnings_ss;
      SUBSCRIBE SESSION TO info_ss;

      START;
      """
    When http payload is posted to node "node-1" with host "http-first-{{test_id}}.example.com" path "/route-first"
      """
      {"id":"err-1","level":"error","urgent":true}
      """
    And http payload is posted to node "node-1" with host "http-first-{{test_id}}.example.com" path "/route-first"
      """
      {"id":"info-1","level":"info","urgent":false}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "id":"err-1"
      "id":"info-1"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Router branch predicates evaluate against rewritten output records
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification_in (
        id STRING,
        active BOOL,
        level STRING,
        legacy STRING
      );

      CREATE SCHEMA notification_out (
        id STRING,
        active BOOL,
        severity STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        id string,
        active boolean,
        level string,
        legacy string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY incoming_logs SCHEMA notification_in PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY errors_ss SCHEMA notification_out PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY warnings_ss SCHEMA notification_out PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
      CREATE RELAY info_ss SCHEMA notification_out PARAMETERIZED BY id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/rewrite-route'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING ); CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        PARAMETERIZED BY id_branch VALUES { id = incoming_logs.id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE ROUTER rewrite_router
        FROM incoming_logs
        SET incoming_logs.severity = lower(incoming_logs.level) UNSET incoming_logs.level, incoming_logs.legacy WHERE incoming_logs.active
        TO errors_ss WHERE incoming_logs.severity = "error"
        TO warnings_ss WHERE incoming_logs.severity = "warn"
        DEFAULT TO info_ss PARAMETERIZED BY id_branch
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO errors_ss;
      SUBSCRIBE SESSION TO warnings_ss;
      SUBSCRIBE SESSION TO info_ss;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/rewrite-route"
      """
      {"id":"err-2","active":true,"level":"ERROR","legacy":"old-error"}
      """
    Then the relay subscription receives a payload
      """
      "severity":"error"
      """
    And the last relay subscription payload does not contain "level\""
    And the last relay subscription payload does not contain "legacy\""
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/rewrite-route"
      """
      {"id":"warn-2","active":true,"level":"WARN","legacy":"old-warn"}
      """
    Then the relay subscription receives a payload
      """
      "severity":"warn"
      """
    And the last relay subscription payload does not contain "level\""
    And the last relay subscription payload does not contain "legacy\""
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/rewrite-route"
      """
      {"id":"info-2","active":true,"level":"INFO","legacy":"old-info"}
      """
    Then the relay subscription receives a payload
      """
      "severity":"info"
      """
    And the last relay subscription payload does not contain "level\""
    And the last relay subscription payload does not contain "legacy\""
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
