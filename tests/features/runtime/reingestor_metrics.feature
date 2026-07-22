Feature: Reingestor metrics

  Scenario Outline: DESCRIBE REINGESTOR and Prometheus report reingestor traffic metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_reingestor_metrics_source SCHEMA tenant_user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_reingestor_metrics_source;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
        CREATE IF NOT EXISTS BRANCH by_reingestor_metrics_node SCHEMA tenant_branch TTL 5m;
        CREATE RELAY tenant_notifications SCHEMA notification BRANCHED BY by_reingestor_metrics_node;
        CREATE IF NOT EXISTS BRANCH by_audit_reingestor_metrics_node SCHEMA tenant_branch TTL 5m;
        CREATE RELAY audit_notifications SCHEMA notification BRANCHED BY by_audit_reingestor_metrics_node;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT reingestor_metrics_ingress ON edge PATH '/reingestor-metrics' TYPE HTTP;
        CREATE INGESTOR reingestor_metrics_source
        FROM ENDPOINT reingestor_metrics_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_reingestor_metrics_source
        SET tenant = message.tenant, user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE REINGESTOR reingestor_metrics_node FROM notifications
        TO tenant_notifications
        INHERIT ALL
        BRANCHED BY by_reingestor_metrics_node
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE REINGESTOR audit_reingestor_metrics_node FROM notifications
        TO audit_notifications
        INHERIT ALL
        BRANCHED BY by_audit_reingestor_metrics_node
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION tenant_notifications_subscription TO tenant_notifications;
        CREATE SUBSCRIPTION audit_notifications_subscription TO audit_notifications;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/reingestor-metrics"
      """
      {"tenant":"acme","user_id":42}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/reingestor-metrics"
      """
      {"tenant":"acme","user_id":43}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "tenant":"acme","user_id":42
      "tenant":"acme","user_id":43
      """
    When these NSPL commands are executed
      """
      DESCRIBE REINGESTOR reingestor_metrics_node;
      """
    Then the last command output contains
      """
      reingestor: reingestor_metrics_node
      """
    And the last command output contains
      """
      from: notifications
      """
    And the last command output contains
      """
      output 0: into=tenant_notifications construction=present branch=by_reingestor_metrics_node flush=100ms max-batch-size=1MiB
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
      messages_total sent relay=tenant_notifications physical_node=node-1 total=2
      """
    And the last command output metric "messages_total" "received" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "messages_total" "sent" relay "tenant_notifications" physical node "node-1" has values
      """
      total=2
      """
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target_kind="REINGESTOR"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target="reingestor_metrics_node"'
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="REINGESTOR"
      target="reingestor_metrics_node"
      direction="received"
      relay="notifications"
      """
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="REINGESTOR"
      target="reingestor_metrics_node"
      direction="sent"
      relay="tenant_notifications"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
