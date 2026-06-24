Feature: Cluster scheduling
  Scenario Outline: All nodes can be terminated with an active session
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    And all nodes are stopped

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Followers reject non-subscription NSPL commands
    Given a 2 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands fail on a follower node with "not-a-leader"
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      """

  Scenario: Client forwards follower commands to the leader
    Given a 2 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on a follower node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      current_leader:
      """

  Scenario: Scheduled deduplicators receive relay traffic across nodes
    Given a 2 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY forwarded_notifications SCHEMA notification PARAMETERIZED BY user_id_branch;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR passthrough
        FROM notifications
        TO forwarded_notifications PARAMETERIZED BY user_id_branch
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      START;
      """
    And these NSPL commands are executed on node "node-2"
      """
      SUBSCRIBE SESSION TO forwarded_notifications;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

  Scenario: Describe relay forwards to the scheduled owner node
    Given a 2 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then within "15s" node "node-2" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then within "15s" node "node-2" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """

  Scenario: Describe deduplicator on the leader reports scheduled owner metrics
    Given a 2 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    Then the current leader node is saved as placeholder "leader"
    When these NSPL commands are executed on the leader node
      """
      CORDON NODE {{leader}};

      CREATE SCHEMA notification (
        id I64,
        level STRING
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        id integer,
        level string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id I64 );
      CREATE RELAY incoming_logs SCHEMA notification PARAMETERIZED BY id_branch;
      CREATE IF NOT EXISTS SCHEMA id_branch ( id I64 );
      CREATE RELAY routed_logs SCHEMA notification PARAMETERIZED BY id_branch;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        PARAMETERIZED BY id_branch VALUES { id = incoming_logs.id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC deduplicator_describe_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_deduplicator_describe_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR remote_deduplicator
        FROM incoming_logs
        TO routed_logs WHERE incoming_logs.level = "error"
        TO routed_logs
        PARAMETERIZED BY id_branch
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "remote_deduplicator" is saved as placeholder "deduplicator_owner"
    And within "5s" node "{{leader}}" eventually reports scheduled "deduplicator" "remote_deduplicator" owner different from placeholder "leader"
    When these NSPL commands are executed on node "node-2"
      """
      SUBSCRIBE SESSION TO routed_logs;
      """
    And Kafka message is published to topic "deduplicator_describe_{{test_id}}"
      """
      {"id":42,"level":"error"}
      """
    Then the relay subscription receives a payload
      """
      "id":42
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE DEDUPLICATOR remote_deduplicator;
      """
    Then the last command output contains
      """
      owner: {{deduplicator_owner}}
      """
    And the last command output contains
      """
      messages_total received relay=incoming_logs physical_node={{deduplicator_owner}} total=1
      """

  Scenario: All nodes report describe relay for a parameterized HTTP relay
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually reports interconnect to "node-1" as "connected"
    And node "node-3" eventually reports interconnect to "node-2" as "connected"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then within "15s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    And within "15s" node "node-2" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    And within "15s" node "node-3" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then within "15s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    And within "15s" node "node-2" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    And within "15s" node "node-3" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """

  Scenario: Attached ACK propagates back across nodes
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker does not receive a payload within "1s"
    When emitter "kafka_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      "user_id":42
      """

  Scenario: Attached ACK stays alive across explicitly placed remote nodes
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=kafka_notifications owner=node-1
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      DRAIN NODE node-1;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=kafka_notifications owner=node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      CORDON NODE node-2;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE EMITTER kafka_forward
        FROM notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main
        TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=emitter name=kafka_forward owner=node-1
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-3;
      DRAIN NODE node-1;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=kafka_notifications owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=kafka_forward owner=node-3
      """
    When these NSPL commands are executed on the leader node
      """
      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the relay subscription does not receive a payload within "1s"
    And the observed broker does not receive a payload within "1s"
    When emitter "kafka_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      "user_id":42
      """
