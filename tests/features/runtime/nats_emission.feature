Feature: NATS emission
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
        CREATE EMITTER nats_notifications FROM notifications TO NATS nats_main SUBJECT notifications_out_{{test_id}} ENCODE USING notification_codec
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

  Scenario Outline: NATS emitter drains a narrow LOOSE-decoded relay under a size-bounded flush
    # Guards the emitter flush accounting for schemas whose Arrow footprint is small.
    # `estimated_bytes` is Arrow-memory based, so a narrow schema accumulates very
    # differently from a wide one against the same MAX BATCH SIZE bound. The sibling
    # columnar scenario uses FLUSH IMMEDIATE and therefore never exercises that path.
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
        TO NATS nats_main SUBJECT narrow_out_{{test_id}} ENCODE USING narrow_event_codec
        INHERIT ALL
        INVOKE write_header("route", "narrow")
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And <message_count> sequential NATS messages are published to subject "narrow_in_{{test_id}}"
      """
      {"sequence":{{sequence}},"tenant":"acme","service":"checkout","dropped_by_loose":"x","also_dropped":1}
      """
    Then within "90s" the observed broker receives <message_count> messages in sequence by field "sequence" with headers
      """
      route=narrow
      """

    Examples:
      | cluster_size | message_count |
      | 1            | 200000        |
      | 3            | 200000        |
