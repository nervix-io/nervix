Feature: Cluster scheduling
  Scenario: Relays are scheduled and only materialized state has replicas
    Given runtime replication is configured with replica count 1 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE RELAY transient_events SCHEMA event UNBRANCHED;
      CREATE RELAY latest_events SCHEMA event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "relay" "transient_events" is saved as placeholder "transient_relay_owner"
    And the last command output contains
      """
      kind=relay name=transient_events owner={{transient_relay_owner}} replicas=-
      """
    And the last cluster status owner for scheduled "relay" "latest_events" is saved as placeholder "materialized_relay_owner"
    And the first replica for scheduled "relay" "latest_events" in the last cluster status is saved as placeholder "materialized_relay_replica"

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

  @random_scheduler
  Scenario: The default three-node test scheduler executes a graph across node boundaries
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA event_branch ( id I64 );
      CREATE BRANCH by_event SCHEMA event_branch TTL 5m;
      CREATE RELAY stage_0 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_1 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_2 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_3 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_4 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_5 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_6 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_7 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_8 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_9 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_10 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_11 SCHEMA event BRANCHED BY by_event;
      CREATE RELAY stage_12 SCHEMA event BRANCHED BY by_event;
      CREATE VHOST edge random-scheduler-{{test_id}}.example.com;
      CREATE ENDPOINT event_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_ingestor FROM ENDPOINT event_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec TO stage_0 INHERIT ALL BRANCHED BY by_event SET id = message.id FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR hop_1 FROM stage_0 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_1 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_2 FROM stage_1 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_2 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_3 FROM stage_2 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_3 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_4 FROM stage_3 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_4 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_5 FROM stage_4 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_5 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_6 FROM stage_5 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_6 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_7 FROM stage_6 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_7 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_8 FROM stage_7 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_8 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_9 FROM stage_8 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_9 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_10 FROM stage_9 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_10 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_11 FROM stage_10 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_11 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR hop_12 FROM stage_11 DEDUPLICATE ON input.id MAX TIME 10m BRANCHED BY by_event TO stage_12 INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION final_stage TO stage_12;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least 2 distinct owners
    When http payload is posted to node "node-1" with host "random-scheduler-{{test_id}}.example.com" path "/events"
      """
      {"id":42}
      """
    Then the relay subscription receives a payload
      """
      "id":42
      """

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
    Given Kafka is running
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
      CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_kafka_notifications;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        }; CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR passthrough FROM notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_kafka_notifications
        TO forwarded_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;

      START;
      """
    And these NSPL commands are executed on node "node-2"
      """
      CREATE SUBSCRIPTION forwarded_notifications_subscription TO forwarded_notifications;
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given Kafka is running
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

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        id integer,
        level string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id I64 );

      CREATE IF NOT EXISTS SCHEMA id_branch ( id I64 );

      CREATE IF NOT EXISTS BRANCH by_source_logs SCHEMA id_branch TTL 5m;

      CREATE RELAY incoming_logs SCHEMA notification BRANCHED BY by_source_logs;

      CREATE RELAY routed_logs SCHEMA notification BRANCHED BY by_source_logs;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE INGESTOR source_logs
        FROM KAFKA kafka_main TOPIC deduplicator_describe_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_deduplicator_describe_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO incoming_logs
        INHERIT ALL
        BRANCHED BY by_source_logs
        SET id = message.id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR remote_deduplicator FROM incoming_logs
        DEDUPLICATE ON input.id
        MAX TIME 10m
        BRANCHED BY by_source_logs
        TO routed_logs
        INHERIT ALL
        WHERE level = "error"
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        TO routed_logs
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;

      START;

      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "remote_deduplicator" is saved as placeholder "deduplicator_owner"
    And within "5s" node "{{leader}}" eventually reports scheduled "deduplicator" "remote_deduplicator" owner different from placeholder "leader"
    When these NSPL commands are executed on node "node-2"
      """
      CREATE SUBSCRIPTION routed_logs_subscription TO routed_logs;
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

  Scenario: Deduplicator executes on its scheduled owner separate from its upstream ingestor
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
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
        id I64,
        level STRING
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        id integer,
        level string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id I64 );

      CREATE IF NOT EXISTS BRANCH by_source_logs SCHEMA id_branch TTL 5m;

      CREATE RELAY incoming_logs SCHEMA notification BRANCHED BY by_source_logs;

      CREATE RELAY routed_logs SCHEMA notification BRANCHED BY by_source_logs;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE INGESTOR source_logs
        FROM KAFKA kafka_main TOPIC scheduled_dedup_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_scheduled_dedup_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO incoming_logs
        INHERIT ALL
        BRANCHED BY by_source_logs
        SET id = message.id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      DRAIN NODE node-1;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=source_logs owner=node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      CORDON NODE node-2;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DEDUPLICATOR remote_deduplicator FROM incoming_logs
        DEDUPLICATE ON input.id
        MAX TIME 10m
        BRANCHED BY by_source_logs
        TO routed_logs
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=source_logs owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=remote_deduplicator owner=node-1
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed on node "node-3"
      """
      CREATE SUBSCRIPTION routed_logs_subscription TO routed_logs;
      """
    And Kafka message is published to topic "scheduled_dedup_{{test_id}}"
      """
      {"id":42,"level":"error"}
      """
    Then the relay subscription receives a payload
      """
      "id":42
      """
    When Kafka message is published to topic "scheduled_dedup_{{test_id}}"
      """
      {"id":7,"level":"info"}
      """
    Then the relay subscription receives a payload
      """
      "id":7
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE DEDUPLICATOR remote_deduplicator;
      """
    Then the last command output contains
      """
      owner: node-1
      """
    And the last command output contains
      """
      messages_total received relay=incoming_logs physical_node=node-1 total=2
      """
    And the last command output does not contain
      """
      physical_node=node-2
      """

  Scenario: All nodes report describe relay for a branched HTTP relay
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    Given Kafka is running
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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

  @planned-ingestor-handoff
  Scenario: A planned Kafka ingestor handoff drains attached ACKs from two branches
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
      CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      status: running
      """
    When emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":84}
      """
    Then within "10s" the relay subscription receives payloads
      """
      "user_id":42
      "user_id":84
      """
    And the observed broker does not receive a payload within "1s"
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these NSPL commands begin executing in the background
      """
      DRAIN NODE node-2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When emitter "kafka_forward" leaves fault mode
    Then within "10s" the observed broker receives payloads
      """
      "user_id":42
      "user_id":84
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background NSPL execution succeeds
    And the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      - kind=ingestor name=kafka_notifications from=node-2 to=
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output does not contain
      """
      - domain={{domain}} kind=ingestor name=kafka_notifications owner=node-2
      """
    And the relay subscription does not receive a payload within "1s"
    And the observed broker does not receive a payload within "1s"

  Scenario: Deduplicator schedule movement resumes processing on the new owner
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
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
        id I64,
        level STRING
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        id integer,
        level string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA id_branch ( id I64 );

      CREATE IF NOT EXISTS BRANCH by_source_logs SCHEMA id_branch TTL 5m;

      CREATE RELAY incoming_logs SCHEMA notification BRANCHED BY by_source_logs;

      CREATE RELAY routed_logs SCHEMA notification BRANCHED BY by_source_logs;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE INGESTOR source_logs
        FROM KAFKA kafka_main TOPIC moved_dedup_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_moved_dedup_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO incoming_logs
        INHERIT ALL
        BRANCHED BY by_source_logs
        SET id = message.id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      DRAIN NODE node-1;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=source_logs owner=node-2
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      CORDON NODE node-2;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DEDUPLICATOR remote_deduplicator FROM incoming_logs
        DEDUPLICATE ON input.id
        MAX TIME 10m
        BRANCHED BY by_source_logs
        TO routed_logs
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=remote_deduplicator owner=node-1
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    And these NSPL commands are executed on node "node-3"
      """
      CREATE SUBSCRIPTION routed_logs_subscription TO routed_logs;
      """
    And Kafka message is published to topic "moved_dedup_{{test_id}}"
      """
      {"id":42,"level":"error"}
      """
    Then the relay subscription receives a payload
      """
      "id":42
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-3;
      DRAIN NODE node-1;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=source_logs owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=remote_deduplicator owner=node-3
      """
    When these NSPL commands are executed on node "node-3"
      """
      CREATE SUBSCRIPTION routed_logs_after_move TO routed_logs;
      """
    And Kafka message is published to topic "moved_dedup_{{test_id}}"
      """
      {"id":99,"level":"info"}
      """
    Then the relay subscription receives a payload
      """
      "id":99
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE DEDUPLICATOR remote_deduplicator;
      """
    Then the last command output contains
      """
      owner: node-3
      """
    And the last command output contains
      """
      messages_total received relay=incoming_logs physical_node=node-3 total=1
      """
