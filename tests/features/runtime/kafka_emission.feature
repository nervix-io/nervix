Feature: Kafka emission
  @kafka_producer_queue_backpressure
  Scenario Outline: A confirmed Kafka emitter delivers its whole window once through a smaller producer queue
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "small_queue_{{test_id}}" exists with 1 partitions
    And Kafka topic "small_queue_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA queue_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA queue_wire MODE STRICT ( sequence integer );
      CREATE CODEC queue_codec FROM WIRE JSON SCHEMA queue_wire TO SCHEMA queue_event;
      CREATE CODEC queue_ingest_codec FROM JSON TO SCHEMA queue_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY queue_events SCHEMA queue_event UNBRANCHED;
      CREATE VHOST edge small-queue-{{test_id}}.example.com;
      CREATE ENDPOINT queue_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR queue_source
        FROM queue_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING queue_ingest_codec
        TO queue_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT small_queue CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'queue.buffering.max.messages' = '2',
        'linger.ms' = '200'
      };
      CREATE KAFKA TOPIC EMITTER queue_output FROM queue_events
        TO small_queue TOPIC 'small_queue_{{test_id}}'
          MODE ACK PARALLEL MAX 8 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING queue_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to node "node-1" with host "small-queue-{{test_id}}.example.com" path "/events"
      """
      [{"sequence":1},{"sequence":2},{"sequence":3},{"sequence":4},{"sequence":5},{"sequence":6},{"sequence":7},{"sequence":8}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"sequence":1}
      {"sequence":2}
      {"sequence":3}
      {"sequence":4}
      {"sequence":5}
      {"sequence":6}
      {"sequence":7}
      {"sequence":8}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @json_columnar_emission
  Scenario Outline: Kafka JSON emission preserves exact bytes for escaped and clean columns
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "columnar_json_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA incoming_event (
        clean STRING,
        escaped STRING,
        secret STRING SENSITIVE,
        note STRING OPTIONAL,
        occurred_at DATETIME,
        blob BYTES
      );
      CREATE WIRE JSON SCHEMA incoming_wire MODE STRICT (
        clean string,
        escaped string,
        secret string,
        note string OPTIONAL,
        occurred_at string,
        blob BYTES
      );
      CREATE CODEC incoming_codec FROM WIRE JSON SCHEMA incoming_wire TO SCHEMA incoming_event
        ENCODE occurred_at AS RFC3339;
      CREATE SCHEMA outgoing_event (
        clean STRING,
        escaped STRING,
        note STRING OPTIONAL,
        occurred_at DATETIME,
        blob BYTES
      );
      CREATE WIRE JSON SCHEMA outgoing_wire MODE STRICT (
        clean string,
        escaped string,
        note string OPTIONAL,
        occurred_at string,
        blob BYTES
      );
      CREATE CODEC outgoing_codec FROM WIRE JSON SCHEMA outgoing_wire TO SCHEMA outgoing_event
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY events SCHEMA incoming_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING incoming_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER kafka_events FROM events
        TO kafka_main TOPIC 'columnar_json_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING outgoing_codec
        INHERIT ALL EXCEPT secret
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"clean":"plain café","escaped":"quote \" slash \\ newline \n","secret":"hidden","note":null,"occurred_at":"2023-11-14T22:13:20.123456789+00:00","blob":"AP8="}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      {"clean":"second","escaped":"no escapes","secret":"also hidden","note":"present","occurred_at":"2023-11-14T22:13:20+00:00","blob":""}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"clean":"plain café","escaped":"quote \" slash \\ newline \n","occurred_at":"2023-11-14T22:13:20.123456789+00:00","blob":"AP8="}
      {"clean":"second","escaped":"no escapes","note":"present","occurred_at":"2023-11-14T22:13:20+00:00","blob":""}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Kafka emitter filter-map publishes message fields and headers
    Given Kafka is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_headers_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        amount I64,
        raw STRING,
        active BOOL
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        amount integer,
        raw string,
        active boolean
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA emitted_notification (
        tenant STRING,
        amount I64,
        normalized STRING
      );

      CREATE WIRE JSON SCHEMA emitted_notification_wire MODE STRICT (
        tenant string,
        amount integer,
        normalized string
      );

      CREATE CODEC emitted_notification_codec
        FROM WIRE JSON SCHEMA emitted_notification_wire
        TO SCHEMA emitted_notification;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );

      CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA tenant_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;

      CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress
        TOPIC 'notifications_headers_in_{{test_id}}'
        MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          BRANCHED BY by_mqtt_notifications
          SET tenant = message.tenant
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE KAFKA TOPIC EMITTER kafka_notifications
        FROM notifications
        TO kafka_main TOPIC 'notifications_headers_out_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING emitted_notification_codec
        INHERIT ALL EXCEPT raw, active
        SET amount = amount + 1,
            normalized = lower(input.raw)
        WHERE input.active
        INVOKE write_header(lower("TENANT"), output.tenant),
               write_header(lower("ROUTE"), "primary"),
               write_header(lower("ROUTE"), output.normalized)
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "notifications_headers_in_{{test_id}}"
      """
      {"tenant":"acme","amount":42,"raw":"FAST-LANE","active":true}
      """
    Then the observed broker receives a payload
      """
      {"tenant":"acme","amount":43,"normalized":"fast-lane"}
      """
    And the last observed broker message has headers
      """
      tenant=acme
      route=primary
      route=fast-lane
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Kafka emitter publishes JSON payloads from a relay
    Given Kafka is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
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
        FROM mqtt_ingress TOPIC 'notifications_in_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC EMITTER kafka_notifications FROM notifications TO kafka_main TOPIC 'notifications_out_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 2s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "kafka_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker does not receive a payload within "500ms"
    And within "5s" DESCRIBE EMITTER "kafka_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "kafka_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @kafka_emitter_ack_boundary
  Scenario Outline: Kafka emitter mode controls when the input offset is committed
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "mode_boundary_in_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE SCHEMA user_id_branch ( user_id I64 );
      CREATE BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
      CREATE KAFKA CLIENT kafka_ingress CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA CLIENT unavailable_kafka_sink CONFIG {
        'bootstrap.servers' = '127.0.0.1:1',
        'delivery.timeout.ms' = '100',
        'socket.timeout.ms' = '100'
      };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_ingress TOPIC 'mode_boundary_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'mode_boundary_group_{{test_id}}'
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
      CREATE ATTACHED KAFKA TOPIC EMITTER kafka_mode_boundary FROM notifications
        TO unavailable_kafka_sink TOPIC 'unavailable_mode_boundary'
          MODE <publishing_mode>
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "mode_boundary_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "mode_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "3s" Kafka consumer group "mode_boundary_group_{{test_id}}" next offset for topic "mode_boundary_in_{{test_id}}" partition 0 is "<offset_condition>"

    Examples:
      | cluster_size | publishing_mode                                                       | offset_condition |
      | 1            | NO ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |
      | 3            | NO ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |

  @detached_confirming_mode
  Scenario Outline: A DETACHED confirming emitter commits upstream while retaining publish retries
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "detached_confirming_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "detached_confirming_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "detached_confirming_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
      FROM kafka_main TOPIC 'detached_confirming_in_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'detached_confirming_group_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE DETACHED KAFKA TOPIC EMITTER kafka_detached_confirming
      FROM notifications
      TO kafka_main TOPIC 'detached_confirming_out_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "detached_confirming_group_{{test_id}}" eventually has 1 consumers
    When emitter "kafka_detached_confirming" enters stall mode
    And Kafka message is published to topic "detached_confirming_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "10s" Kafka consumer group "detached_confirming_group_{{test_id}}" next offset for topic "detached_confirming_in_{{test_id}}" partition 0 is "at least 1"
    And the observed broker does not receive a payload within "500ms"
    And within "5s" DESCRIBE EMITTER "kafka_detached_confirming" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And emitter "kafka_detached_confirming" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_emitter_stale_copy_recovery
  Scenario Outline: A Kafka emitter whose broker refused its records leaves no stale copy, so recovery delivers each record once
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "stale_copy_in_{{test_id}}" exists with 1 partitions
    And a mock Kafka broker serves topic "stale_copy_out_{{test_id}}"
    And the mock Kafka broker refuses every write
    And mock Kafka topic "stale_copy_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_ingress CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA CLIENT kafka_sink CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}',
        'queue.buffering.max.messages' = '1'
      };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
      FROM kafka_ingress TOPIC 'stale_copy_in_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'stale_copy_group_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE DETACHED KAFKA TOPIC EMITTER kafka_stale_copy
      FROM notifications
      TO kafka_sink TOPIC 'stale_copy_out_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 10s MAX 10s
        ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stale_copy_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stale_copy_in_{{test_id}}"
      """
      {"user_id":1}
      """
    And Kafka message is published to topic "stale_copy_in_{{test_id}}"
      """
      {"user_id":2}
      """
    # The broker refuses the first record, which fills the producer queue of one record, until the
    # emitter stops waiting for it at ACK TIMEOUT. The producer gives that copy up by then, at its
    # next timeout scan a second later at most, so it holds no copy for the broker's recovery to
    # deliver beside the one the emitter's next attempt sends.
    Then within "60s" DESCRIBE EMITTER "kafka_stale_copy" on the leader node reports a transient error with its next attempt at most "8s" away
    When the mock Kafka broker accepts writes again
    Then within "60s" DESCRIBE EMITTER "kafka_stale_copy" on the leader node contains
      """
      transient error: -
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      {"user_id":1}
      {"user_id":2}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A NO ACK emitter delivers a record once its producer takes it, so a record can still be queued in
  # the producer when the domain stops. The client lingers for two minutes, so the producer holds the
  # records the emitter already delivered until STOP finishes the emitter and its producer sends them.
  @emitter_finish_at_stop @deloxide_stress
  Scenario Outline: STOP sends the records a NO ACK Kafka emitter's producer still holds
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "held_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "held_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA held_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT ( sequence integer );
      CREATE CODEC held_event_codec FROM WIRE JSON SCHEMA held_event_wire TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE VHOST edge held-stop-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR held_source
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_event_codec
        TIMESTAMP NOW
        TO held_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT lingering_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'linger.ms' = '120000'
      };
      CREATE KAFKA TOPIC EMITTER held_output FROM held_events
        TO lingering_kafka TOPIC 'held_out_{{test_id}}' MODE NO ACK
          RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING held_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "held-stop-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    And http payload is posted to node "node-1" with host "held-stop-{{test_id}}.example.com" path "/events"
      """
      {"sequence":2}
      """
    And http payload is posted to node "node-1" with host "held-stop-{{test_id}}.example.com" path "/events"
      """
      {"sequence":3}
      """
    Then within "30s" DESCRIBE EMITTER "held_output" on the leader node contains
      """
      messages_total sent relay=held_events
      """
    And the last command output owner is saved as placeholder "held_owner"
    And within "30s" DESCRIBE EMITTER "held_output" on the leader node contains
      """
      messages_total sent relay=held_events physical_node={{held_owner}} total=3
      """
    # The producer holds the three delivered records for the rest of its two-minute linger.
    And the observed broker does not receive a payload within "2s"
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"sequence":1}
      {"sequence":2}
      {"sequence":3}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A NO ACK emitter delivers a record once its producer takes it. The broker refuses every write
  # with an answer the producer retries, so the producer still holds the three records when STOP
  # tears the domain down. The finish counts the records the producer holds without a served
  # delivery report, never the events librdkafka queues beside them.
  @emitter_unsent_at_stop
  Scenario Outline: STOP reports exactly how many records a NO ACK Kafka emitter's producer still holds
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And schema change drain timeout is configured as "5s"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And a mock Kafka broker serves topic "unsent_out_{{test_id}}"
    And the mock Kafka broker refuses every write
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA unsent_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA unsent_event_wire MODE STRICT ( sequence integer );
      CREATE CODEC unsent_event_codec FROM WIRE JSON SCHEMA unsent_event_wire TO SCHEMA unsent_event;
      CREATE RELAY unsent_events SCHEMA unsent_event UNBRANCHED;
      CREATE VHOST edge unsent-stop-{{test_id}}.example.com;
      CREATE ENDPOINT unsent_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR unsent_source
        FROM unsent_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING unsent_event_codec
        TIMESTAMP NOW
        TO unsent_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT refusing_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}'
      };
      CREATE KAFKA TOPIC EMITTER unsent_output FROM unsent_events
        TO refusing_kafka TOPIC 'unsent_out_{{test_id}}' MODE NO ACK
          RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING unsent_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "unsent-stop-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    And http payload is posted to node "node-1" with host "unsent-stop-{{test_id}}.example.com" path "/events"
      """
      {"sequence":2}
      """
    And http payload is posted to node "node-1" with host "unsent-stop-{{test_id}}.example.com" path "/events"
      """
      {"sequence":3}
      """
    Then within "30s" DESCRIBE EMITTER "unsent_output" on the leader node contains
      """
      messages_total sent relay=unsent_events
      """
    And the last command output owner is saved as placeholder "unsent_owner"
    And within "30s" DESCRIBE EMITTER "unsent_output" on the leader node contains
      """
      messages_total sent relay=unsent_events physical_node={{unsent_owner}} total=3
      """
    # The broker refuses every write, so the producer still holds all three records when STOP gives
    # the emitter's finish the five-second domain drain timeout. STOP runs on the session that is
    # already open, which therefore observes the error the teardown reports while STOP runs.
    When these Plexus statements are executed on the active session
      """
      STOP;
      """
    Then within "30s" the active session observes a server error containing
      """
      kafka emitter's transport client still held 3 records when its finish deadline passed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Replacing the emitter stops its task without tearing the domain down. The failed swap must
  # carry the finish's count through the transaction failure on both cluster sizes.
  @emitter_unsent_at_swap
  Scenario Outline: A Kafka emitter swap reports the records its producer still holds
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And schema change drain timeout is configured as "5s"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And a mock Kafka broker serves topic "swap_out_{{test_id}}"
    And the mock Kafka broker refuses every write
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA swap_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA swap_wire MODE STRICT ( sequence integer );
      CREATE CODEC swap_codec FROM WIRE JSON SCHEMA swap_wire TO SCHEMA swap_event;
      CREATE RELAY swap_events SCHEMA swap_event UNBRANCHED;
      CREATE VHOST edge swap-{{test_id}}.example.com;
      CREATE ENDPOINT swap_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR swap_source
        FROM swap_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING swap_codec
        TIMESTAMP NOW
        TO swap_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT refusing_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}'
      };
      CREATE KAFKA CLIENT replacement_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}'
      };
      CREATE KAFKA TOPIC EMITTER swap_output FROM swap_events
        TO refusing_kafka TOPIC 'swap_out_{{test_id}}' MODE NO ACK
          RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING swap_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "swap_leader"
    When these Plexus statements are executed on the leader node
      """
      RELOCATE EMITTER swap_output ONTO NODE {{swap_leader}} IGNORE PREFERENCES;
      """
    And http payload is posted to node "node-1" with host "swap-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    And http payload is posted to node "node-1" with host "swap-{{test_id}}.example.com" path "/events"
      """
      {"sequence":2}
      """
    And http payload is posted to node "node-1" with host "swap-{{test_id}}.example.com" path "/events"
      """
      {"sequence":3}
      """
    Then within "30s" DESCRIBE EMITTER "swap_output" on the leader node contains
      """
      messages_total sent relay=swap_events
      """
    And within "30s" DESCRIBE EMITTER "swap_output" on the leader node contains
      """
      messages_total sent relay=swap_events physical_node={{swap_leader}} total=3
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these Plexus statements
      """
      BEGIN;
      ALTER KAFKA TOPIC EMITTER swap_output
        SET CLIENT replacement_kafka, SET DETACHED;
      """
    Then client "owner" transaction id is saved as placeholder "swap_transaction"
    When client "owner" attempts to commit its transaction
    Then the last command error contains
      """
      kafka emitter's transport client still held 3 records when its finish deadline passed
      """
    And transaction "{{swap_transaction}}" eventually has state "FAILED"
    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The producer delivered its one record under ACK, so it holds nothing when its emitter is dropped
  # and the finish returns at once. Destroying it while the broker holds every answer still runs as
  # one job on a connector worker of the emitter's owner, which no Kafka ingestor shares here,
  # while an HTTP ingestor goes on delivering.
  @kafka_producer_retirement
  Scenario Outline: A Kafka emitter dropped on <cluster_size> nodes retires its producer on a connector worker while another ingestor delivers
    Given a mock Kafka broker serves topic "retired_out_{{test_id}}"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA retired_event ( seq I64 );
      CREATE WIRE JSON SCHEMA retired_event_wire MODE STRICT ( seq integer );
      CREATE CODEC retired_event_codec
        FROM WIRE JSON SCHEMA retired_event_wire TO SCHEMA retired_event;
      CREATE RELAY retired_events SCHEMA retired_event UNBRANCHED;
      CREATE VHOST edge retire-out-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR healthy_source
        FROM intake MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING retired_event_codec
        TO retired_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT retiring_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}'
      };
      CREATE KAFKA TOPIC EMITTER retiring_output FROM retired_events
        TO retiring_kafka TOPIC 'retired_out_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING retired_event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION retirement_subscription TO retired_events;
      START;
      """
    And http payload is posted to host "retire-out-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":1
      """
    And within "30s" DESCRIBE EMITTER "retiring_output" on the leader node contains
      """
      messages_total sent relay=retired_events
      """
    And the last command output owner is saved as placeholder "kafka_owner"
    And within "30s" DESCRIBE EMITTER "retiring_output" on the leader node contains
      """
      messages_total sent relay=retired_events physical_node={{kafka_owner}} total=1
      """
    And within "30s" node "{{kafka_owner}}" observability metric "nervix_execution_jobs_total" with labels eventually equals 0
      """
      class="connector_storage"
      """
    Given the mock Kafka broker holds every answer
    When these Plexus statements are executed on the leader node
      """
      DROP EMITTER retiring_output;
      """
    And http payload is posted to host "retire-out-{{test_id}}.example.com" path "/events"
      """
      {"seq":42}
      """
    Then within "120s" the relay subscription receives a payload
      """
      "seq":42
      """
    And within "120s" node "{{kafka_owner}}" observability metric "nervix_execution_jobs_completed_total" with labels eventually reaches at least 1
      """
      class="connector_storage"
      """
    When the mock Kafka broker releases its answers
    Then within "60s" node "{{kafka_owner}}" observability metric "nervix_execution_jobs_pending" with labels eventually equals 0
      """
      class="connector_storage"
      """
    And within "60s" node "{{kafka_owner}}" observability metric "nervix_execution_jobs_running" with labels eventually equals 0
      """
      class="connector_storage"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
