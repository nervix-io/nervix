Feature: Ingestor metrics

  Scenario Outline: DESCRIBE INGESTOR and Prometheus report ingestor output metrics
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
        CREATE IF NOT EXISTS BRANCH by_ingestor_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ingestor_metrics_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingestor_metrics_ingress ON edge PATH '/ingestor-metrics' TYPE HTTP;
        CREATE INGESTOR ingestor_metrics_source
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ingestor_metrics_source VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingestor_metrics_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingestor-metrics"
      """
      {"user_id":42}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingestor-metrics"
      """
      {"user_id":43}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"user_id":42}
      {"user_id":43}
      """
    When these NSPL commands are executed
      """
      DESCRIBE INGESTOR ingestor_metrics_source;
      """
    Then the last command output contains
      """
      ingestor: ingestor_metrics_source
      """
    And the last command output contains
      """
      metrics:
      """
    And the last command output contains
      """
      incoming_edges:
      """
    And the last command output contains
      """
      outgoing_edges:
      """
    And the last command output contains
      """
      messages_total received relay=- physical_node=node-1 total=2
      """
    And the last command output contains
      """
      bytes_total received relay=- physical_node=node-1
      """
    And the last command output does not contain
      """
      batches_total received relay=-
      """
    And the last command output contains
      """
      messages_total sent relay=notifications physical_node=node-1 total=2
      """
    And the last command output contains
      """
      batches_total sent relay=notifications physical_node=node-1 total=2
      """
    And the last command output metric "messages_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "batches_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      """
    And the last command output metric "messages_per_batch" "sent" relay "notifications" physical node "node-1" has values
      """
      p90_1m=1.0
      """
    When these NSPL commands are executed
      """
      DESCRIBE RELAY notifications;
      """
    Then the last command output contains
      """
      relay: notifications
      """
    And the last command output contains
      """
      relay_buffers:
      """
    And the last command output does not contain
      """
      messages_total received relay=notifications
      """
    And the last command output does not contain
      """
      messages_per_batch received relay=notifications
      """
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target_kind="INGESTOR"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains 'target="ingestor_metrics_source"'
    And node "node-1" observability path "/metrics" eventually responds with 200 and contains "nervix_messages_per_batch_bucket"
    And node "node-1" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="INGESTOR"
      target="ingestor_metrics_source"
      direction="sent"
      relay="notifications"
      """
    And node "node-1" observability metric "nervix_messages_per_batch_count" with labels eventually equals 2
      """
      target_kind="INGESTOR"
      target="ingestor_metrics_source"
      direction="sent"
      relay="notifications"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: DESCRIBE INGESTOR reports flush-sized batches for rapid same-branch <source_kind> input
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
        CREATE IF NOT EXISTS BRANCH by_ingestor_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ingestor_metrics_source;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingestor-metrics-{{test_id}}'
        };
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE INGESTOR ingestor_metrics_source
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ingestor_metrics_source VALUES { user_id = notifications.user_id }
        FLUSH EACH 1s MAX BATCH SIZE 1MiB
        FROM <source_clause>
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "ingestor_metrics_source" as "status: running"
    When 10 JSON messages with user id 42 are rapidly published to "<source_kind>" input "<input>"
    Then within "10s" node "node-1" eventually reports describe ingestor "ingestor_metrics_source" as "messages_total sent relay=notifications physical_node=node-1 total=10"
    And the last command output metric "messages_total" "sent" relay "notifications" on any physical node has numeric values
      """
      total=10
      """
    And the last command output metric "batches_total" "sent" relay "notifications" on any physical node has numeric values
      """
      total=1
      """
    And the last command output metric "messages_per_batch" "sent" relay "notifications" on any physical node has numeric values
      """
      p50_1m>=9
      p50_1m<=10
      p90_1m>=9
      p90_1m<=10
      p99_1m>=9
      p99_1m<=10
      p50_15m>=9
      p50_15m<=10
      p90_15m>=9
      p90_15m<=10
      p99_15m>=9
      p99_15m<=10
      """

    Examples:
      | cluster_size | source_kind | source_clause                                                                    | input                     |
      | 1            | REDIS       | REDIS PUBSUB redis_main CHANNEL notifications_{{test_id}} MODE NO_ACK SEQUENTIAL | notifications_{{test_id}} |
      | 1            | MQTT        | MQTT mqtt_main TOPIC notifications_{{test_id}} MODE NO_ACK SEQUENTIAL            | notifications_{{test_id}} |

  Scenario Outline: DESCRIBE INGESTOR restores metrics for a running domain after process restart
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
        CREATE IF NOT EXISTS BRANCH by_ingestor_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ingestor_metrics_source;
        CREATE VHOST edge http-{{test_id}}-ingestor-restart.example.com;
        CREATE ENDPOINT ingestor_metrics_restart_ingress ON edge PATH '/ingestor-metrics-restart' TYPE HTTP;
        CREATE INGESTOR ingestor_metrics_source
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ingestor_metrics_source VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingestor_metrics_restart_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-ingestor-restart.example.com" path "/ingestor-metrics-restart"
      """
      {"user_id":42}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-ingestor-restart.example.com" path "/ingestor-metrics-restart"
      """
      {"user_id":43}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "ingestor_metrics_source" as "messages_total sent relay=notifications physical_node=node-1 total=2"
    When the cluster is restarted
    Then within "10s" node "node-1" eventually reports describe ingestor "ingestor_metrics_source" as "messages_total sent relay=notifications physical_node=node-1 total=2"
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
      DESCRIBE INGESTOR ingestor_metrics_source;
      """
    Then the last command output metric "messages_total" "sent" relay "notifications" physical node "node-1" has values
      """
      total=2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: DESCRIBE INGESTOR from a non-owner node reports owner metrics after restart
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on node "node-1"
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
        CREATE IF NOT EXISTS BRANCH by_remote_owner_metrics_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_remote_owner_metrics_source;
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE INGESTOR remote_owner_metrics_source
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_remote_owner_metrics_source VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM REDIS PUBSUB redis_main CHANNEL remote_owner_notifications_{{test_id}} MODE NO_ACK SEQUENTIAL
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
        DRAIN NODE node-1;
        SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "remote_owner_metrics_source" is saved as placeholder "ingestor_owner"
    And Redis channel "remote_owner_notifications_{{test_id}}" eventually has 1 subscribers
    When 2 JSON messages with user id 42 are rapidly published to "REDIS" input "remote_owner_notifications_{{test_id}}"
    Then within "10s" node "node-1" eventually reports describe ingestor "remote_owner_metrics_source" as "messages_total sent relay=notifications physical_node={{ingestor_owner}} total=2"
    When the cluster is restarted
    Then within "10s" node "node-1" eventually reports describe ingestor "remote_owner_metrics_source" as "messages_total sent relay=notifications physical_node={{ingestor_owner}} total=2"
