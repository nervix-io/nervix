Feature: HTTP ingestor domain pacing
  Scenario Outline: Unpaced HTTP ingestors accept payloads without an explicit timestamp field
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
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
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Paced HTTP ingestors reject payloads until the domain clock starts
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 200ms SKEW 1s;
      """
    When these NSPL commands are executed
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
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest" and fails
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then within "5s" repeatedly posting http payload to host "http-{{test_id}}.example.com" path "/ingest" yields a relay subscription payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Unpaced HTTP ingestors accept explicit timestamp fields without a domain clock
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        occurred_at DATETIME
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        occurred_at string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification
        ENCODE occurred_at AS RFC3339;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP AT occurred_at
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    Then the relay subscription receives a payload
      """
      "occurred_at":"2026-04-07T00:00:00+00:00"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Paced HTTP ingestors require a running domain clock even with explicit timestamps
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 200ms SKEW 100000h;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        occurred_at DATETIME
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        occurred_at string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification
        ENCODE occurred_at AS RFC3339;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP AT occurred_at
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest" and fails
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then within "5s" repeatedly posting http payload to host "http-{{test_id}}.example.com" path "/ingest" yields a relay subscription payload
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
