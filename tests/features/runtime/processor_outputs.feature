Feature: Processor output routing
  Scenario Outline: Processor output routes fan out to conditional and unconditional destinations
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

      CREATE DEDUPLICATOR log_splitter
        FROM incoming_logs
        FILTER WHERE incoming_logs.active
        TO errors_ss WHERE incoming_logs.level = "error"
        TO warnings_ss WHERE incoming_logs.urgent
        TO info_ss
        PARAMETERIZED BY id_branch
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m
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
      "id":"err-1"
      "id":"info-1"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Single processor output route preserves parameterization and applies destination rewrites
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

      CREATE VHOST edge http-output-route-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/output-route'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR project_notifications
        FROM notifications
        FILTER WHERE notifications.active
        TO projected_notifications
          SET projected_notifications.normalized = lower(trim(notifications.raw)), projected_notifications.amount = notifications.amount + 1
          UNSET notifications.raw, notifications.active
        PARAMETERIZED BY tenant_user_id_branch
        DEDUPLICATE ON notifications.tenant, notifications.user_id
        MAX TIME 10m
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO projected_notifications;
      START;
      """
    When http payload is posted to node "node-1" with host "http-output-route-{{test_id}}.example.com" path "/output-route"
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
    When http payload is posted to node "node-1" with host "http-output-route-{{test_id}}.example.com" path "/output-route"
      """
      {"tenant":"acme","user_id":43,"active":false,"amount":99,"raw":"should drop"}
      """
    Then the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Processor output routes project each destination independently
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

      CREATE VHOST edge http-project-output-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/project-output'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING ); CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        PARAMETERIZED BY id_branch VALUES { id = incoming_logs.id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR project_by_destination
        FROM incoming_logs
        FILTER WHERE incoming_logs.active
        TO errors_ss
          SET errors_ss.severity = lower(incoming_logs.level)
          UNSET incoming_logs.level, incoming_logs.legacy
          WHERE incoming_logs.level = "ERROR"
        TO warnings_ss
          SET warnings_ss.severity = lower(incoming_logs.level)
          UNSET incoming_logs.level, incoming_logs.legacy
          WHERE incoming_logs.level = "WARN"
        TO info_ss
          SET info_ss.severity = lower(incoming_logs.level)
          UNSET incoming_logs.level, incoming_logs.legacy
        PARAMETERIZED BY id_branch
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO errors_ss;
      SUBSCRIBE SESSION TO warnings_ss;
      SUBSCRIBE SESSION TO info_ss;

      START;
      """
    When http payload is posted to node "node-1" with host "http-project-output-{{test_id}}.example.com" path "/project-output"
      """
      {"id":"err-2","active":true,"level":"ERROR","legacy":"old-error"}
      """
    Then the relay subscription receives a payload
      """
      "severity":"error"
      """
    And the last relay subscription payload does not contain "level\""
    And the last relay subscription payload does not contain "legacy\""
    When http payload is posted to node "node-1" with host "http-project-output-{{test_id}}.example.com" path "/project-output"
      """
      {"id":"warn-2","active":true,"level":"WARN","legacy":"old-warn"}
      """
    Then the relay subscription receives a payload
      """
      "severity":"warn"
      """
    And the last relay subscription payload does not contain "level\""
    And the last relay subscription payload does not contain "legacy\""
    When http payload is posted to node "node-1" with host "http-project-output-{{test_id}}.example.com" path "/project-output"
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
