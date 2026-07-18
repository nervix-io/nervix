Feature: Processor output routing
  Scenario Outline: Each output route owns its message error policy
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA routed_event (
        id STRING,
        divisor I64,
        result I64 OPTIONAL
      );
      CREATE SCHEMA error_record (
        error_message STRING,
        failed_record STRING
      );
      CREATE STRICT WIRE JSON SCHEMA routed_event_wire (
        id string,
        divisor integer,
        result integer OPTIONAL
      );
      CREATE CODEC routed_event_codec
        FROM WIRE JSON SCHEMA routed_event_wire
        TO SCHEMA routed_event;
      CREATE RELAY source_events SCHEMA routed_event UNBRANCHED;
      CREATE RELAY successful_events SCHEMA routed_event UNBRANCHED;
      CREATE RELAY failing_events SCHEMA routed_event UNBRANCHED;
      CREATE RELAY route_errors SCHEMA error_record UNBRANCHED;
      CREATE VHOST edge output-error-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        TO source_events FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        DECODE USING routed_event_codec
        UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON GENERAL ERROR LOG;
      CREATE JUNCTION project_events
        FROM source_events
        TO successful_events FLUSH IMMEDIATE
          SET successful_events.result = 10
          ON MESSAGE ERROR LOG
        TO failing_events FLUSH IMMEDIATE
          SET failing_events.result = 10 / source_events.divisor
          ON MESSAGE ERROR DLQ route_errors
            SET error_message = message_error.message,
                failed_record = message_error.record
        UNBRANCHED;
      CREATE SUBSCRIPTION successful_events_subscription TO successful_events;
      CREATE SUBSCRIPTION failing_events_subscription TO failing_events;
      CREATE SUBSCRIPTION route_errors_subscription TO route_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "output-error-{{test_id}}.example.com" path "/events"
      """
      {"id":"event-1","divisor":0,"result":0}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "divisor":0,"id":"event-1","result":10
      "failed_record":"{\"divisor\":0,\"id\":\"event-1\",\"result\":0}"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Each processor output owns its flush policy
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA routed_event (
        id STRING,
        route STRING OPTIONAL
      );
      CREATE STRICT WIRE JSON SCHEMA routed_event_wire (
        id string,
        route string OPTIONAL
      );
      CREATE CODEC routed_event_codec
        FROM WIRE JSON SCHEMA routed_event_wire
        TO SCHEMA routed_event;
      CREATE RELAY incoming_events SCHEMA routed_event UNBRANCHED;
      CREATE RELAY immediate_events SCHEMA routed_event UNBRANCHED;
      CREATE RELAY delayed_events SCHEMA routed_event UNBRANCHED;
      CREATE VHOST edge output-flush-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        TO incoming_events FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        DECODE USING routed_event_codec
        UNBRANCHED
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events
        FROM incoming_events
        TO immediate_events FLUSH IMMEDIATE
          SET immediate_events.route = "immediate" ON MESSAGE ERROR LOG
        TO delayed_events FLUSH EACH 2s MAX BATCH SIZE 1MiB
          SET delayed_events.route = "delayed" ON MESSAGE ERROR LOG
        UNBRANCHED;
      CREATE SUBSCRIPTION immediate_events_subscription TO immediate_events;
      CREATE SUBSCRIPTION delayed_events_subscription TO delayed_events;
      START;
      """
    And http payload is posted to node "node-1" with host "output-flush-{{test_id}}.example.com" path "/events"
      """
      {"id":"event-1"}
      """
    Then within "1s" the relay subscription receives a payload
      """
      "id":"event-1","route":"immediate"
      """
    And the relay subscription does not receive a payload within "500ms"
    And within "3s" the relay subscription receives a payload
      """
      "id":"event-1","route":"delayed"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

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
        TO error_events FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          SET error_events.route = "error",
              error_events.normalized = lower(trim(message.raw))
          UNSET error_events.active, error_events.level, error_events.raw
          WHERE message.level = "ERROR"
          ON MESSAGE ERROR LOG
        TO audit_events FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          SET audit_events.route = "audit",
              audit_events.normalized = lower(trim(message.raw))
          UNSET audit_events.active, audit_events.level, audit_events.raw ON MESSAGE ERROR LOG
        DECODE USING event_codec
        UNBRANCHED

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;

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

  Scenario Outline: SET assignments observe earlier assignments to the same destination field
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA source_event (
        id STRING
      );
      CREATE SCHEMA projected_event (
        id STRING,
        amount I64
      );
      CREATE STRICT WIRE JSON SCHEMA source_event_wire (
        id string
      );
      CREATE CODEC source_event_codec
        FROM WIRE JSON SCHEMA source_event_wire
        TO SCHEMA source_event;
      CREATE RELAY projected_events SCHEMA projected_event UNBRANCHED;
      CREATE VHOST edge sequential-set-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source_events
        TO projected_events FLUSH IMMEDIATE
          SET projected_events.amount = 1,
              projected_events.amount = projected_events.amount + 1 ON MESSAGE ERROR LOG
        DECODE USING source_event_codec
        UNBRANCHED

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION projected_events_subscription TO projected_events;
      START;
      """
    When http payload is posted to host "sequential-set-{{test_id}}.example.com" path "/events"
      """
      {"id":"event-1"}
      """
    Then the relay subscription receives a payload
      """
      "amount":2,"id":"event-1"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Validation rejects required output fields that are neither inherited nor assigned
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "required output field 'amount' remains uninitialized"
      """
      CREATE SCHEMA source_event (
        note STRING OPTIONAL
      );
      CREATE SCHEMA projected_event (
        amount I64,
        note STRING OPTIONAL
      );
      CREATE RELAY source_events
        SCHEMA source_event
        UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY projected_events SCHEMA projected_event UNBRANCHED;
      CREATE GENERATOR project_events
        TO projected_events
        UNBRANCHED
        EACH 100ms
        FLUSH IMMEDIATE
        SET projected_events.note = source_events.note
        ON MESSAGE ERROR LOG;
      """

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
        TO incoming_logs_a FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_source_logs_a VALUES { id = incoming_logs_a.id }

        FROM ENDPOINT ingress_a MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE INGESTOR source_logs_b
        TO incoming_logs_b FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_source_logs_a VALUES { id = incoming_logs_b.id }

        FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR log_splitter
        FROM incoming_logs_a WHERE incoming_logs_a.level != "skip",
             incoming_logs_b WHERE incoming_logs_b.level != "hold"
        FILTER WHERE incoming_logs_a.active
        TO errors_ss FLUSH EACH 100ms MAX BATCH SIZE 1MiB WHERE incoming_logs_a.level = "error" ON MESSAGE ERROR LOG
        TO warnings_ss FLUSH EACH 100ms MAX BATCH SIZE 1MiB WHERE incoming_logs_a.urgent ON MESSAGE ERROR LOG
        TO info_ss FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        BRANCHED BY by_source_logs_a
        DEDUPLICATE ON incoming_logs_a.id
        MAX TIME 10m;
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
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { tenant = notifications.tenant, user_id = notifications.user_id }

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR project_notifications
        FROM notifications
        FILTER WHERE notifications.active
        TO projected_notifications FLUSH IMMEDIATE
          SET projected_notifications.normalized = lower(trim(notifications.raw)), projected_notifications.amount = notifications.amount + 1
          UNSET notifications.raw, notifications.active ON MESSAGE ERROR LOG
        BRANCHED BY by_http_notifications
        DEDUPLICATE ON notifications.tenant, notifications.user_id
        MAX TIME 10m;
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
        TO incoming_logs FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY by_source_logs VALUES { id = incoming_logs.id }

        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR project_by_destination
        FROM incoming_logs
        FILTER WHERE incoming_logs.active
        TO errors_ss FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          SET errors_ss.severity = lower(incoming_logs.level)
          UNSET incoming_logs.level, incoming_logs.legacy
          WHERE incoming_logs.level = "ERROR" ON MESSAGE ERROR LOG
        TO warnings_ss FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          SET warnings_ss.severity = lower(incoming_logs.level)
          UNSET incoming_logs.level, incoming_logs.legacy
          WHERE incoming_logs.level = "WARN" ON MESSAGE ERROR LOG
        TO info_ss FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          SET info_ss.severity = lower(incoming_logs.level)
          UNSET incoming_logs.level, incoming_logs.legacy ON MESSAGE ERROR LOG
        BRANCHED BY by_source_logs
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m;
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
