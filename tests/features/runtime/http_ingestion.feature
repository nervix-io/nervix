Feature: HTTP endpoint ingestion
  Scenario Outline: HTTP endpoint ingestor delivers a single JSON payload to a subscribed relay
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
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: HTTP endpoint ingestor delivers each posted payload once to the relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA metric (
        value I32
      );

      CREATE STRICT WIRE JSON SCHEMA metric_wire (
        value integer
      );

      CREATE CODEC metric_codec
        FROM WIRE JSON SCHEMA metric_wire
        TO SCHEMA metric;

      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT raw_metrics_endpoint
        ON edge
        PATH '/metrics'
        TYPE HTTP;

      CREATE INGESTOR raw_metrics_source
        TO raw_metrics
        DECODE USING metric_codec
        UNBRANCHED
        FLUSH IMMEDIATE
        FROM ENDPOINT raw_metrics_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION raw_metrics_subscription TO raw_metrics;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":3}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":4}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"value":1}
      {"value":2}
      {"value":3}
      {"value":4}
      """
    And the relay subscription does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: HTTPS endpoint ingestor delivers a single JSON payload to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "tls_bundle" for hosts "http-{{test_id}}.example.com"
    When these NSPL commands are executed
      """
      CREATE RESOURCE tls_bundle;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE tls_bundle VERSION "{{tls_bundle}}";
      """
    And these NSPL commands are executed
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
      CREATE VHOST edge http-{{test_id}}.example.com WITH TLS tls_bundle;
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
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And https payload is posted to host "http-{{test_id}}.example.com" path "/ingest" using CA from resource directory "tls_bundle"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
