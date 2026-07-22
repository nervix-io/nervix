Feature: Relay metrics

  Scenario Outline: DESCRIBE RELAY reports buffer metrics while Prometheus reports relay traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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
        CREATE IF NOT EXISTS BRANCH by_relay_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_relay_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT relay_metrics_ingress ON edge PATH '/relay-metrics' TYPE HTTP;
        CREATE INGESTOR relay_metrics_source
        FROM ENDPOINT relay_metrics_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP AT occurred_at
        TO notifications
        INHERIT ALL
        BRANCHED BY by_relay_metrics_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/relay-metrics"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/relay-metrics"
      """
      {"user_id":43,"occurred_at":"2026-04-07T00:00:02Z"}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "user_id":42
      "user_id":43
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE RELAY notifications;
      """
    Then the last command output contains
      """
      relay: notifications
      """
    And the last command output contains
      """
      branch fields: user_id
      """
    And the last command output contains
      """
      branch-local describe: use WHERE bindings
      """
    And the last command output does not contain
      """
      requires WHERE bindings
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    Then the last command output contains
      """
      exists
      """
    And the last command output contains
      """
      metrics:
      """
    And the last command output contains
      """
      relay_buffers:
      """
    And the last command output contains
      """
      relay_buffer_len concrete relay=notifications
      """
    And the last command output metric "relay_buffer_len" "concrete" relay "notifications" on any physical node has numeric values
      """
      capacity>=1
      p50_1m>=0
      p90_1m>=0
      p99_1m>=0
      """
    And the last command output does not contain
      """
      messages_total received relay=notifications
      """
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains "nervix_messages_total"
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target_kind="RELAY"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target="notifications"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'direction="received"'
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="RELAY"
      target="notifications"
      direction="received"
      relay="notifications"
      """
    And node "node-1" observability metric "nervix_batches_total" with labels eventually equals 2
      """
      target_kind="RELAY"
      target="notifications"
      direction="received"
      relay="notifications"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @relay_buffer_statistics
  Scenario Outline: DESCRIBE RELAY reports runtime buffer occupancy percentiles
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
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

      CREATE IF NOT EXISTS BRANCH by_relay_buffer_source SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_relay_buffer_source CAPACITY 3;
      CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_relay_buffer_source;

      CREATE VHOST edge http-{{test_id}}-buffer.example.com;
      CREATE ENDPOINT relay_buffer_ingress ON edge PATH '/relay-buffer' TYPE HTTP; CREATE INGESTOR relay_buffer_source
        FROM ENDPOINT relay_buffer_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_relay_buffer_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR relay_buffer_forwarder FROM notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_relay_buffer_source
        TO forwarded_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications WHERE user_id = 42;
      START;
      """
    And http payload is posted to host "http-{{test_id}}-buffer.example.com" path "/relay-buffer"
      """
      {"user_id":42}
      """
    Then within "5s" node "node-1" eventually reports describe relay as "relay_buffer_len concrete relay=notifications"
      """
      DESCRIBE RELAY notifications;
      """
    And the last command output metric "relay_buffer_len" "concrete" relay "notifications" on any physical node has numeric values
      """
      capacity=3
      p50_1m>=0
      p90_1m>=0
      p99_1m>=0
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Relay edge metrics are preserved when the owning node is drained
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on node "node-1"
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
        CREATE IF NOT EXISTS BRANCH by_relay_metrics_drain_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_relay_metrics_drain_source;
        CREATE VHOST edge http-{{test_id}}-drain.example.com;
        CREATE ENDPOINT relay_metrics_drain_ingress ON edge PATH '/relay-metrics-drain' TYPE HTTP;
        CREATE INGESTOR relay_metrics_drain_source
        FROM ENDPOINT relay_metrics_drain_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP AT occurred_at
        TO notifications
        INHERIT ALL
        BRANCHED BY by_relay_metrics_drain_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-drain.example.com" path "/relay-metrics-drain"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "relay_metrics_drain_source" as "messages_total sent relay=notifications physical_node=node-1 total=1"
    When http payload is posted to node "node-1" with host "http-{{test_id}}-drain.example.com" path "/relay-metrics-drain"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:02Z"}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "relay_metrics_drain_source" as "messages_total sent relay=notifications physical_node=node-1 total=2"
    And the last command output metric "messages_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      domain_rate_per_sec=1
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DRAIN NODE node-1;
      DESCRIBE INGESTOR relay_metrics_drain_source;
      """
    Then the last command output contains
      """
      metrics:
      """
    And the last command output metric "messages_total" "sent" relay "notifications" on any physical node has values
      """
      total=2
      domain_rate_per_sec=1
      """
    And the last command output metric "batches_total" "sent" relay "notifications" on any physical node has values
      """
      total=2
      domain_rate_per_sec=1
      """

  Scenario Outline: Relay edge metrics are restored when a running domain process restarts
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on node "node-1"
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
        CREATE IF NOT EXISTS BRANCH by_relay_metrics_restart_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_relay_metrics_restart_source;
        CREATE VHOST edge http-{{test_id}}-restart.example.com;
        CREATE ENDPOINT relay_metrics_restart_ingress ON edge PATH '/relay-metrics-restart' TYPE HTTP;
        CREATE INGESTOR relay_metrics_restart_source
        FROM ENDPOINT relay_metrics_restart_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP AT occurred_at
        TO notifications
        INHERIT ALL
        BRANCHED BY by_relay_metrics_restart_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-restart.example.com" path "/relay-metrics-restart"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:00Z"}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "relay_metrics_restart_source" as "messages_total sent relay=notifications physical_node=node-1 total=1"
    When http payload is posted to node "node-1" with host "http-{{test_id}}-restart.example.com" path "/relay-metrics-restart"
      """
      {"user_id":42,"occurred_at":"2026-04-07T00:00:02Z"}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "relay_metrics_restart_source" as "messages_total sent relay=notifications physical_node=node-1 total=2"
    When the cluster is restarted
    Then within "10s" node "node-1" eventually reports describe ingestor "relay_metrics_restart_source" as "messages_total sent relay=notifications physical_node=node-1 total=2"
    And the last command output metric "batches_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    When these NSPL commands fail with "already running"
      """
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE INGESTOR relay_metrics_restart_source;
      """
    Then the last command output metric "messages_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
