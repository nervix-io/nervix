Feature: Processor output routing
  Scenario Outline: Ingestor output routes filter input before fan-out
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event_in (
        id STRING,
        active BOOL,
        level STRING,
        raw STRING
      );

      CREATE SCHEMA event_projection (
      id STRING,
      route STRING,
      normalized STRING
      );

      CREATE STRICT WIRE JSON SCHEMA event_wire (
        id string,
        active boolean,
        level string,
        raw string
      );

      CREATE CODEC event_codec
        FROM WIRE JSON SCHEMA event_wire
        TO SCHEMA event_in;

      CREATE RELAY error_events SCHEMA event_projection UNBRANCHED;
      CREATE RELAY audit_events SCHEMA event_projection UNBRANCHED;

      CREATE VHOST edge http-ingestor-output-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/ingestor-output'
        TYPE HTTP;

      CREATE INGESTOR event_source
        FILTER WHERE message.active
        TO error_events
          SET error_events.route = "error",
              error_events.normalized = lower(trim(message.raw))
          UNSET error_events.active, error_events.level, error_events.raw
          WHERE message.level = "ERROR"
        TO audit_events
          SET audit_events.route = "audit",
              audit_events.normalized = lower(trim(message.raw))
          UNSET audit_events.active, audit_events.level, audit_events.raw
        DECODE USING event_codec
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION error_events_subscription TO error_events;
      CREATE SUBSCRIPTION audit_events_subscription TO audit_events;

      START;
      """
    When http payload is posted to node "node-1" with host "http-ingestor-output-{{test_id}}.example.com" path "/ingestor-output"
      """
      {"id":"err-1","active":true,"level":"ERROR","raw":"  FAIL  "}
      """
    And http payload is posted to node "node-1" with host "http-ingestor-output-{{test_id}}.example.com" path "/ingestor-output"
      """
      {"id":"info-1","active":true,"level":"INFO","raw":"  OK  "}
      """
    And http payload is posted to node "node-1" with host "http-ingestor-output-{{test_id}}.example.com" path "/ingestor-output"
      """
      {"id":"drop-1","active":false,"level":"ERROR","raw":"  DROP  "}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "id":"err-1","normalized":"fail","route":"error"
      "id":"err-1","normalized":"fail","route":"audit"
      "id":"info-1","normalized":"ok","route":"audit"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        id string,
        active boolean,
        level string,
        urgent boolean
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS BRANCH by_source_logs_a SCHEMA id_branch TTL 5m;
        CREATE RELAY incoming_logs_a SCHEMA notification BRANCHED BY by_source_logs_a;
        CREATE RELAY incoming_logs_b SCHEMA notification BRANCHED BY by_source_logs_a;
        CREATE RELAY errors_ss SCHEMA notification BRANCHED BY by_source_logs_a;
        CREATE RELAY warnings_ss SCHEMA notification BRANCHED BY by_source_logs_a;
        CREATE RELAY info_ss SCHEMA notification BRANCHED BY by_source_logs_a;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress_a
        ON edge
        PATH '/route-a'
        TYPE HTTP;
        CREATE ENDPOINT ingress_b
        ON edge
        PATH '/route-b'
        TYPE HTTP;
        CREATE INGESTOR source_logs_a
        TO incoming_logs_a
        DECODE USING notification_codec
        BRANCHED BY by_source_logs_a VALUES { id = incoming_logs_a.id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress_a MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE INGESTOR source_logs_b
        TO incoming_logs_b
        DECODE USING notification_codec
        BRANCHED BY by_source_logs_a VALUES { id = incoming_logs_b.id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR log_splitter
        FROM incoming_logs_a WHERE incoming_logs_a.level != "skip",
             incoming_logs_b WHERE incoming_logs_b.level != "hold"
        FILTER WHERE incoming_logs_a.active
        TO errors_ss WHERE incoming_logs_a.level = "error"
        TO warnings_ss WHERE incoming_logs_a.urgent
        TO info_ss
        BRANCHED BY by_source_logs_a
        DEDUPLICATE ON incoming_logs_a.id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION errors_ss_subscription TO errors_ss;
        CREATE SUBSCRIPTION warnings_ss_subscription TO warnings_ss;
        CREATE SUBSCRIPTION info_ss_subscription TO info_ss;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route-a"
      """
      {"id":"err-1","active":true,"level":"error","urgent":true}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route-a"
      """
      {"id":"info-1","active":true,"level":"info","urgent":false}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route-a"
      """
      {"id":"source-drop","active":true,"level":"skip","urgent":true}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route-a"
      """
      {"id":"ignored","active":false,"level":"error","urgent":true}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route-b"
      """
      {"id":"warn-b","active":true,"level":"warn","urgent":true}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/route-b"
      """
      {"id":"source-drop-b","active":true,"level":"hold","urgent":true}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "id":"err-1"
      "id":"err-1"
      "id":"err-1"
      "id":"info-1"
      "id":"warn-b"
      "id":"warn-b"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Single processor output route preserves branching and applies destination rewrites
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
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
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA tenant_user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification_in BRANCHED BY by_http_notifications;
        CREATE RELAY projected_notifications SCHEMA notification_out BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-output-route-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/output-route'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = notifications.tenant, user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR project_notifications
        FROM notifications
        FILTER WHERE notifications.active
        TO projected_notifications
          SET projected_notifications.normalized = lower(trim(notifications.raw)), projected_notifications.amount = notifications.amount + 1
          UNSET notifications.raw, notifications.active
        BRANCHED BY by_http_notifications
        DEDUPLICATE ON notifications.tenant, notifications.user_id
        MAX TIME 10m
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION projected_notifications_subscription TO projected_notifications;
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
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        id string,
        active boolean,
        level string,
        legacy string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS SCHEMA id_branch ( id STRING );
        CREATE IF NOT EXISTS BRANCH by_source_logs SCHEMA id_branch TTL 5m;
        CREATE RELAY incoming_logs SCHEMA notification_in BRANCHED BY by_source_logs;
        CREATE RELAY errors_ss SCHEMA notification_out BRANCHED BY by_source_logs;
        CREATE RELAY warnings_ss SCHEMA notification_out BRANCHED BY by_source_logs;
        CREATE RELAY info_ss SCHEMA notification_out BRANCHED BY by_source_logs;
        CREATE VHOST edge http-project-output-{{test_id}}.example.com;
        CREATE ENDPOINT ingress
        ON edge
        PATH '/project-output'
        TYPE HTTP;
        CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        BRANCHED BY by_source_logs VALUES { id = incoming_logs.id }
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
        BRANCHED BY by_source_logs
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION errors_ss_subscription TO errors_ss;
        CREATE SUBSCRIPTION warnings_ss_subscription TO warnings_ss;
        CREATE SUBSCRIPTION info_ss_subscription TO info_ss;
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
