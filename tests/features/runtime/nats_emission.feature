Feature: NATS emission
  Scenario Outline: NATS emitter drains a wide columnar batch in order without duplicates
    Given NATS is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "columnar_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA columnar_event (
        sequence I64,
        tenant STRING,
        secret STRING SENSITIVE,
        metric_01 I64,
        metric_02 I64,
        metric_03 I64,
        metric_04 I64,
        metric_05 I64,
        metric_06 I64,
        metric_07 I64,
        metric_08 I64,
        metric_09 I64
      );

      CREATE WIRE JSON SCHEMA columnar_event_wire MODE STRICT (
        sequence integer,
        tenant string,
        secret string,
        metric_01 integer,
        metric_02 integer,
        metric_03 integer,
        metric_04 integer,
        metric_05 integer,
        metric_06 integer,
        metric_07 integer,
        metric_08 integer,
        metric_09 integer
      );

      CREATE CODEC columnar_event_codec
        FROM WIRE JSON SCHEMA columnar_event_wire
        TO SCHEMA columnar_event;

      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY columnar_events SCHEMA columnar_event BRANCHED BY by_tenant CAPACITY 64;

      CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = '{{nats_addr}}'
        };

      CREATE INGESTOR columnar_events_in
        FROM NATS nats_main SUBJECT columnar_in_{{test_id}}
        QUEUE GROUP columnar_in_group_{{test_id}} INSTANCES 1 MODE NO_ACK SEQUENTIAL
        DECODE USING columnar_event_codec
        TO columnar_events
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 2s MAX BATCH SIZE 16MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE ATTACHED EMITTER columnar_events_out
        FROM columnar_events
        TO NATS nats_main SUBJECT columnar_out_{{test_id}}
          MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING columnar_event_codec
        INHERIT ALL EXCEPT secret
        SET secret = leak_sensitive(input.secret)
        INVOKE write_header("route", "primary"),
               write_header("route", "columnar")
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "columnar_events_in" is saved as placeholder "columnar_ingestor_owner"
    When emitter "columnar_events_out" enters stall mode
    And <message_count> sequential NATS messages are published to subject "columnar_in_{{test_id}}"
      """
      {"sequence":{{sequence}},"tenant":"acme","secret":"visible-by-explicit-leak","metric_01":1,"metric_02":2,"metric_03":3,"metric_04":4,"metric_05":5,"metric_06":6,"metric_07":7,"metric_08":8,"metric_09":9}
      """
    Then node "{{columnar_ingestor_owner}}" observability metric "nervix_messages_total" with labels eventually equals <message_count>
      """
      target_kind="INGESTOR"
      target="columnar_events_in"
      direction="sent"
      relay="columnar_events"
      """
    When emitter "columnar_events_out" leaves stall mode
    Then within "5s" the observed broker receives <message_count> messages in sequence by field "sequence" with headers
      """
      route=primary
      route=columnar
      """

    Examples:
      | cluster_size | message_count |
      | 1            | 32768         |
      | 3            | 32768         |

  Scenario Outline: NATS emitter publishes JSON payloads from a relay
    Given MQTT is running
    And NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "notifications_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE EMITTER nats_notifications FROM notifications TO NATS nats_main SUBJECT notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "nats_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "10s" DESCRIBE EMITTER "nats_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "nats_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: NATS emitter honors its flush deadline during sustained input collection
    # The message count stays below Core NATS's bounded subscription queue. The emitter's input
    # and output size limits are deliberately above this narrow batch's Arrow footprint, so
    # their independent timers must make progress.
    Given NATS is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "narrow_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA narrow_event (
        sequence I64,
        tenant STRING,
        service STRING
      );

      CREATE WIRE JSON SCHEMA narrow_event_wire MODE LOOSE (
        sequence integer,
        tenant string,
        service string
      );

      CREATE CODEC narrow_event_codec
        FROM WIRE JSON SCHEMA narrow_event_wire
        TO SCHEMA narrow_event;

      CREATE RELAY narrow_events SCHEMA narrow_event UNBRANCHED CAPACITY 64;

      CREATE CLIENT nats_main
        TYPE NATS
        CONFIG {
          'addr' = '{{nats_addr}}'
        };

      CREATE INGESTOR narrow_events_in
        FROM NATS nats_main SUBJECT narrow_in_{{test_id}}
        QUEUE GROUP narrow_in_group_{{test_id}} INSTANCES 1 MODE NO_ACK SEQUENTIAL
        DECODE USING narrow_event_codec
        TO narrow_events
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 1s MAX BATCH SIZE 64MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE ATTACHED EMITTER narrow_events_out
        FROM narrow_events
        COLLECT FOR 1ms MAX BATCH SIZE 64MiB
        TO NATS nats_main SUBJECT narrow_out_{{test_id}}
          MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING narrow_event_codec
        INHERIT ALL
        INVOKE write_header("route", "narrow")
        FLUSH EACH 100ms MAX BATCH SIZE 64MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "emitter" "narrow_events_out" is saved as placeholder "narrow_emitter_owner"
    When <message_count> sequential NATS messages are published to subject "narrow_in_{{test_id}}"
      """
      {"sequence":{{sequence}},"tenant":"acme","service":"checkout","dropped_by_loose":"x","also_dropped":1}
      """
    # Assert on the emitter's own counter first. If this passes but the broker assertion
    # below fails, the loss is downstream of Nervix (NATS or the observer); if it fails,
    # the emitter itself never published the messages.
    Then within "90s" node "{{narrow_emitter_owner}}" observability metric "nervix_messages_total" with labels eventually equals <message_count>
      """
      target_kind="EMITTER"
      target="narrow_events_out"
      direction="sent"
      relay="narrow_events"
      """
    Then within "90s" the observed broker receives <message_count> messages in sequence by field "sequence" with headers
      """
      route=narrow
      """

    Examples:
      | cluster_size | message_count |
      | 1            | 32768         |
      | 3            | 32768         |

  @nats_jetstream_missing_stream
  Scenario Outline: NATS JetStream emitter retries a missing stream until it is provisioned
    Given Kafka is running
    And NATS is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "jetstream_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE SCHEMA user_id_branch ( user_id I64 );
      CREATE BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC jetstream_in_{{test_id}}
          OFFSET BY CONSUMER GROUP jetstream_boundary_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          BRANCHED BY by_kafka_notifications
          SET user_id = message.user_id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT nats_main TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE EMITTER nats_jetstream FROM notifications
        TO NATS nats_main SUBJECT jetstream_out_{{test_id}}
          MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 1s
            RETRY POLICY BACKOFF 100ms MAX 500ms
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "jetstream_boundary_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "jetstream_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "2s" Kafka consumer group "jetstream_boundary_group_{{test_id}}" next offset for topic "jetstream_in_{{test_id}}" partition 0 is "below 1"
    And within "10s" DESCRIBE EMITTER "nats_jetstream" on the leader node contains
      """
      no stream found for given subject
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    When NATS JetStream stream "notifications_{{test_id}}" is provisioned for subject "jetstream_out_{{test_id}}"
    Then NATS JetStream stream "notifications_{{test_id}}" eventually contains a payload on subject "jetstream_out_{{test_id}}"
      """
      {"user_id":42}
      """
    And within "5s" Kafka consumer group "jetstream_boundary_group_{{test_id}}" next offset for topic "jetstream_in_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
