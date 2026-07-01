Feature: Emitter metrics

  Scenario Outline: DESCRIBE EMITTER and Prometheus report emitter traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_emitter_metrics_source BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_emitter_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT emitter_metrics_ingress ON edge PATH '/emitter-metrics' TYPE HTTP;
        CREATE INGESTOR emitter_metrics_source
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_emitter_metrics_source VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT emitter_metrics_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE EMITTER emitter_metrics_node
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_main ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/emitter-metrics"
      """
      {"user_id":42}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/emitter-metrics"
      """
      {"user_id":43}
      """
    Then the observed broker receives a payload
      """
      "user_id":
      """
    When these NSPL commands are executed
      """
      DESCRIBE EMITTER emitter_metrics_node;
      """
    Then the last command output contains
      """
      emitter: emitter_metrics_node
      """
    And the last command output contains
      """
      from: notifications
      """
    And the last command output contains
      """
      sink: ZEROMQ client=zeromq_main
      """
    And the last command output contains
      """
      metrics:
      """
    And the last command output contains
      """
      messages_total received relay=notifications physical_node=node-1 total=2
      """
    And the last command output contains
      """
      messages_total sent relay=notifications physical_node=node-1 total=2
      """
    And the last command output metric "messages_total" "received" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "messages_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "received" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=1
      """
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target_kind="EMITTER"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target="emitter_metrics_node"'
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="EMITTER"
      target="emitter_metrics_node"
      direction="received"
      relay="notifications"
      """
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="EMITTER"
      target="emitter_metrics_node"
      direction="sent"
      relay="notifications"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
