Feature: Domain lifecycle
  Scenario Outline: Semicolon-separated command batches execute as one request
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When this NSPL command batch is executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE SCHEMA notification (
        user_id I64
      )
      """
    Then the last command output contains
      """
      stored model 'notifications'
      """
    And the last command output contains
      """
      stored model 'notification'
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE RELAY notifications
      """
    Then the last command output contains
      """
      CREATE RELAY notifications SCHEMA notification UNBRANCHED CAPACITY 1;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA notification
      """
    Then the last command output contains
      """
      CREATE SCHEMA notification (user_id I64);
      """
    When this NSPL command batch is executed on the leader node
      """
      CREATE SCHEMA zip_code (
        zip_code U64,
        latitude F64,
        longitude F64,
        city STRING,
        state STRING,
        county STRING
      );
      CREATE SCHEMA zip_code2 (
        zip_code U64,
        latitude F64,
        longitude F64,
        city STRING,
        state STRING,
        county STRING
      )
      """
    Then the last command output contains
      """
      stored model 'zip_code'
      """
    And the last command output contains
      """
      stored model 'zip_code2'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Domain commands isolate session context and drive a replicated clock
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 200ms SKEW 1s;
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      SHOW CREATE SCHEMA notification;
      """
    Then the last command output contains
      """
      CREATE SCHEMA notification (user_id I64);
      """
    When these NSPL commands are executed on the leader node
      """
      START AT '2026-04-07T00:00:00Z' TIME RATE 4.0;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Running"
    And node "node-1" eventually reports status containing "{{domain}} status=Running pace=PACED period=200ms skew=1s"
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=PACED"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Unpaced domains ingest without a domain clock
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
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
        CREATE IF NOT EXISTS BRANCH by_http_notifications BY user_id_branch TTL 5m;
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
        SUBSCRIBE SESSION TO notifications;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest" and fails
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "1s"
    And node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
    When these NSPL commands are executed
      """
      START;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Running pace=UNPACED"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
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

  Scenario Outline: Stopped unpaced domains do not start implicitly from Kafka traffic
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "1s"
    And node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Stopped unpaced domains do not start implicitly from websocket traffic
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_ws_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE VHOST edge ws-{{test_id}}.example.com;
        CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
        CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_ws_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
    When websocket message is published to host "ws-{{test_id}}.example.com" path "/ws" and fails
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "1s"
    And node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
