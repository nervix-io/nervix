Feature: Branch lifecycle metrics

  Scenario Outline: Prometheus reports live, LRU-evicted, and TTL-expired branch instances
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA user_id_branch (
        user_id I64
      );

      CREATE BRANCH by_metric_users
        SCHEMA user_id_branch TTL 500ms MAX INSTANCES 1 EVICT LRU;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_metric_users;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT branch_metrics_ingress
        ON edge
        PATH '/branch-metrics'
        TYPE HTTP;

      CREATE INGESTOR branch_metrics_source
        FROM ENDPOINT branch_metrics_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_metric_users
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE SUBSCRIPTION notifications_subscription TO notifications;

      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/branch-metrics"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 1
      """
      domain="{{domain}}"
      branch="by_metric_users"
      physical_node_id="node-1"
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/branch-metrics"
      """
      {"user_id":7}
      """
    Then the relay subscription receives a payload
      """
      "user_id":7
      """
    And node "node-1" observability metric "nervix_branch_evictions_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      branch="by_metric_users"
      physical_node_id="node-1"
      reason="lru"
      """
    And node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 1
      """
      domain="{{domain}}"
      branch="by_metric_users"
      physical_node_id="node-1"
      """
    And node "node-1" observability metric "nervix_branch_evictions_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      branch="by_metric_users"
      physical_node_id="node-1"
      reason="ttl"
      """
    And node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 0
      """
      domain="{{domain}}"
      branch="by_metric_users"
      physical_node_id="node-1"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
