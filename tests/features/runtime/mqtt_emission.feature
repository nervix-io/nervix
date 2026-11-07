Feature: MQTT emission
  Scenario Outline: MQTT TOPIC emitter publishes JSON payloads from a relay
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "notifications_out_{{test_id}}" is observed
    When these Plexus commands are executed
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
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE MQTT CLIENT mqtt_main
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-emitter-{{test_id}}'
        };
        CREATE MQTT TOPIC EMITTER mqtt_notifications_out FROM notifications TO mqtt_main TOPIC 'notifications_out_{{test_id}}' MODE QOS 1 ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "mqtt_notifications_out" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "mqtt_notifications_out" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "mqtt_notifications_out" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @mqtt_emitter_qos1_boundary
  Scenario Outline: MQTT QoS 1 holds the input offset until PUBACK
    Given Kafka is running
    And MQTT is running
    And a stallable MQTT endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "mqtt_boundary_in_{{test_id}}" exists with 1 partitions
    And MQTT topic "mqtt_boundary_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE SCHEMA emitter_error (
        error_code STRING,
        source_user_id I64
      );
      CREATE SCHEMA user_id_branch ( user_id I64 );
      CREATE BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
      CREATE RELAY emitter_errors SCHEMA emitter_error BRANCHED BY by_kafka_notifications;
      CREATE KAFKA CLIENT kafka_ingress CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_ingress TOPIC 'mqtt_boundary_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'mqtt_boundary_group_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          BRANCHED BY by_kafka_notifications
          SET user_id = message.user_id
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT CLIENT mqtt_sink CONFIG {
        'addr' = '{{mqtt_stallable_addr}}',
        'client_id' = 'nervix-mqtt-boundary-{{test_id}}'
      };
      CREATE ATTACHED MQTT TOPIC EMITTER mqtt_boundary FROM notifications
        TO mqtt_sink TOPIC 'mqtt_boundary_out_{{test_id}}'
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 300ms
            RETRY POLICY BACKOFF 100ms MAX 200ms
          ENCODE USING notification_codec
        INHERIT ALL
        <batch>
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO emitter_errors
          SET error_code = error.code,
              source_user_id = input.user_id
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    Then Kafka consumer group "mqtt_boundary_group_{{test_id}}" eventually has 1 consumers
    When the stallable endpoint "mqtt" is paused
    And Kafka message is published to topic "mqtt_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "2s" Kafka consumer group "mqtt_boundary_group_{{test_id}}" next offset for topic "mqtt_boundary_in_{{test_id}}" partition 0 is "below 1"
    And the relay subscription does not receive a payload within "500ms"
    When the stallable endpoint "mqtt" is resumed
    Then the observed broker receives a payload
      """
      <payload>
      """
    And within "5s" Kafka consumer group "mqtt_boundary_group_{{test_id}}" next offset for topic "mqtt_boundary_in_{{test_id}}" partition 0 is "at least 1"

    # A batch payload is one PUBLISH packet, so its PUBACK is the completion point.
    Examples:
      | cluster_size | batch                              | payload          |
      | 1            |                                    | {"user_id":42}   |
      | 3            |                                    | {"user_id":42}   |
      | 1            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |
      | 3            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |

  # One MQTT message carries four records, which the ingest codec splits into one batch, so the
  # emitter hands all four to its client in one publish. The endpoint drops the first record's
  # PUBLISH: the broker confirms the other three and never sees or answers the first. When the
  # first record's answer misses ACK TIMEOUT, the confirmation window settles the answers the
  # broker gave, so the emitter sends again only the first record.
  @mqtt_emitter_confirmation_window
  Scenario Outline: An MQTT <mode> emitter sends again only the record its broker never answered
    Given MQTT is running
    And an MQTT endpoint that drops chosen publishes is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "window_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_batch_codec FROM JSON TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-window-ingress-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'window_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_batch_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT CLIENT mqtt_sink CONFIG {
        'addr' = '{{mqtt_filtered_addr}}',
        'client_id' = 'nervix-window-emitter-{{test_id}}'
      };
      CREATE MQTT TOPIC EMITTER window_out FROM notifications
        TO mqtt_sink TOPIC 'window_out_{{test_id}}'
          MODE <mode> ACK PARALLEL MAX 8 ACK TIMEOUT 10s
            RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And the MQTT endpoint drops every publish carrying
      """
      {"user_id":1}
      """
    And MQTT message is published to topic "window_in_{{test_id}}"
      """
      [{"user_id":1},{"user_id":2},{"user_id":3},{"user_id":4}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"user_id":2}
      {"user_id":3}
      {"user_id":4}
      """
    And within "60s" DESCRIBE EMITTER "window_out" on the leader node contains
      """
      transient error: the broker did not answer within ACK TIMEOUT 10s
      """
    When the MQTT endpoint forwards every publish
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"user_id":1}
      """
    And within "60s" DESCRIBE EMITTER "window_out" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | mode  |
      | 1            | QOS 1 |
      | 3            | QOS 1 |
      | 1            | QOS 2 |
      | 3            | QOS 2 |

  # A QOS 0 emitter delivers a record once its client takes it. The endpoint holds the client's
  # CONNECT, so the client still holds the record it took when STOP finishes the emitter: the finish
  # waits for the client to send it, which it can only do once the endpoint resumes.
  @mqtt_finish_at_stop @emitter_finish_at_stop @deloxide_stress
  Scenario Outline: STOP waits for the publish a QOS 0 MQTT TOPIC emitter's client holds
    Given MQTT is running
    And a stallable MQTT endpoint is configured
    And drain timeout is configured as "60s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "held_out_{{test_id}}" is observed
    When the stallable endpoint "mqtt" is paused
    And these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA held_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA held_wire MODE STRICT ( sequence integer );
      CREATE CODEC held_codec FROM WIRE JSON SCHEMA held_wire TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE VHOST edge mqtt-held-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR held_source
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_codec
        TIMESTAMP NOW
        TO held_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT CLIENT mqtt_held CONFIG {
        'addr' = '{{mqtt_stallable_addr}}',
        'client_id' = 'nervix-cucumber-held-{{test_id}}'
      };
      CREATE MQTT TOPIC EMITTER held_output FROM held_events
        TO mqtt_held TOPIC 'held_out_{{test_id}}'
          MODE QOS 0 RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING held_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "mqtt-held-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    Then within "30s" DESCRIBE EMITTER "held_output" on the leader node contains
      """
      messages_total sent relay=held_events
      """
    And the last command output owner is saved as placeholder "held_owner"
    And within "30s" DESCRIBE EMITTER "held_output" on the leader node contains
      """
      messages_total sent relay=held_events physical_node={{held_owner}} total=1
      """
    When STOP runs while the stallable endpoint "mqtt" resumes after "2s"
    Then STOP completed after the stallable endpoint resumed
    And within "30s" the observed broker receives exactly these payloads
      """
      {"sequence":1}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
