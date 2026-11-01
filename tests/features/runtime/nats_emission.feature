Feature: NATS emission
  @nats_core_connection_loss @deloxide_stress
  Scenario Outline: A core NATS write retries every record through connection loss
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "core_loss_out_{{test_id}}" is observed
    Given the leader node is configured with these Plexus statements
      """
      CREATE SCHEMA core_loss_input ( sequence I64 );
      CREATE SCHEMA core_loss_event ( sequence I64, padding STRING );
      CREATE WIRE JSON SCHEMA core_loss_wire MODE STRICT ( sequence integer, padding string );
      CREATE CODEC core_loss_input_codec FROM JSON TO SCHEMA core_loss_input
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC core_loss_codec FROM WIRE JSON SCHEMA core_loss_wire TO SCHEMA core_loss_event;
      CREATE RELAY core_loss_events SCHEMA core_loss_event UNBRANCHED;
      CREATE NATS CLIENT core_loss_in CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT INGESTOR core_loss_source
        FROM core_loss_in SUBJECT 'core_loss_in_{{test_id}}'
        QUEUE GROUP 'core_loss_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING core_loss_input_codec
        TO core_loss_events
          INHERIT ALL
          SET padding = repeat('x', 1000)
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS CLIENT core_loss_out CONFIG {
        'addr' = '{{nats_controlled_addr}}',
        'timeout_ms' = '60000'
      };
      CREATE NATS SUBJECT EMITTER core_loss_output FROM core_loss_events
        TO core_loss_out SUBJECT 'core_loss_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING core_loss_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "core_loss_source" on the leader node contains
      """
      ready: true
      """
    When the controlled NATS endpoint holds what each client sends after 0 publishes
    And a NATS message carrying records 1 to 12000 by field "sequence" is published to subject "core_loss_in_{{test_id}}"
    Then within "60s" the controlled NATS endpoint has stopped reading a publish
    When the controlled NATS connection is lost during publication
    Then within "180s" the observed broker receives every record from 1 to 12000 by field "sequence"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @nats_core_connection_loss @nats_core_stop_connection_loss @deloxide_stress
  Scenario Outline: Nervix reports a core NATS connection change during STOP
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "core_loss_out_{{test_id}}" is observed
    Given the leader node is configured with these Plexus statements
      """
      CREATE SCHEMA core_loss_input ( sequence I64 );
      CREATE SCHEMA core_loss_event ( sequence I64, padding STRING );
      CREATE WIRE JSON SCHEMA core_loss_wire MODE STRICT ( sequence integer, padding string );
      CREATE CODEC core_loss_input_codec FROM JSON TO SCHEMA core_loss_input
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC core_loss_codec FROM WIRE JSON SCHEMA core_loss_wire TO SCHEMA core_loss_event;
      CREATE RELAY core_loss_events SCHEMA core_loss_event UNBRANCHED;
      CREATE NATS CLIENT core_loss_in CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT INGESTOR core_loss_source
        FROM core_loss_in SUBJECT 'core_loss_in_{{test_id}}'
        QUEUE GROUP 'core_loss_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING core_loss_input_codec
        TO core_loss_events
          INHERIT ALL
          SET padding = repeat('x', 1000)
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS CLIENT core_loss_out CONFIG {
        'addr' = '{{nats_controlled_addr}}',
        'timeout_ms' = '60000'
      };
      CREATE NATS SUBJECT EMITTER core_loss_output FROM core_loss_events
        TO core_loss_out SUBJECT 'core_loss_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING core_loss_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "core_loss_source" on the leader node contains
      """
      ready: true
      """
    When the controlled NATS endpoint holds what each client sends after 0 publishes
    And a NATS message carrying records 1 to 12000 by field "sequence" is published to subject "core_loss_in_{{test_id}}"
    Then within "60s" the controlled NATS endpoint has stopped reading a publish
    When the controlled NATS connection is lost during STOP
    Then within "30s" the active session observes a server error containing
      """
      connection changed before
      """
    And the last server error contains
      """
      pending records were flushed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: NATS SUBJECT emitter drains a wide columnar batch in order without duplicates
    Given NATS is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "columnar_out_{{test_id}}" is observed
    When these Plexus commands are executed
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

      CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };

      CREATE NATS SUBJECT INGESTOR columnar_events_in
        FROM nats_main SUBJECT 'columnar_in_{{test_id}}'
        QUEUE GROUP 'columnar_in_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING columnar_event_codec
        TO columnar_events
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 2s MAX BATCH SIZE 16MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE ATTACHED NATS SUBJECT EMITTER columnar_events_out
        FROM columnar_events
        TO nats_main SUBJECT 'columnar_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
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
    # Exercise both remote hops in the three-node example, before any record is published.
    And these Plexus commands are executed
      """
      RELOCATE INGESTOR columnar_events_in ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY columnar_events ONTO NODE <relay_node> IGNORE PREFERENCES;
      RELOCATE EMITTER columnar_events_out ONTO NODE node-1 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=ingestor name=columnar_events_in owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=columnar_events owner=<relay_node>
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=columnar_events_out owner=node-1
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
    # The independent sequence observation drains the complete burst while the runner polls
    # other scenarios, retaining the original deadline and every sequence/header assertion.
    Then within "90s" the observed broker receives <message_count> messages in sequence by field "sequence" with headers
      """
      route=primary
      route=columnar
      """

    Examples:
      | cluster_size | relay_node | message_count |
      | 1            | node-1     | 32768         |
      | 3            | node-2     | 32768         |

  Scenario Outline: NATS SUBJECT emitter publishes JSON payloads from a relay
    Given MQTT is running
    And NATS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "notifications_out_{{test_id}}" is observed
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
        CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };
        CREATE NATS SUBJECT EMITTER nats_notifications FROM notifications TO nats_main SUBJECT 'notifications_out_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
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

  Scenario Outline: NATS SUBJECT emitter honors its flush deadline during sustained input collection
    # The message count stays below Core NATS's bounded subscription queue. The emitter's input
    # and output size limits are deliberately above this narrow batch's Arrow footprint, so
    # their independent timers must make progress.
    Given NATS is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "narrow_out_{{test_id}}" is observed
    When these Plexus commands are executed
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

      CREATE NATS CLIENT nats_main
        CONFIG {
          'addr' = '{{nats_addr}}'
        };

      CREATE NATS SUBJECT INGESTOR narrow_events_in
        FROM nats_main SUBJECT 'narrow_in_{{test_id}}'
        QUEUE GROUP 'narrow_in_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING narrow_event_codec
        TO narrow_events
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 1s MAX BATCH SIZE 64MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE ATTACHED NATS SUBJECT EMITTER narrow_events_out
        FROM narrow_events
        COLLECT FOR 1ms MAX BATCH SIZE 64MiB
        TO nats_main SUBJECT 'narrow_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING narrow_event_codec
        INHERIT ALL
        INVOKE write_header("route", "narrow")
        FLUSH EACH 100ms MAX BATCH SIZE 64MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed
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
    # Verify every sequence and header on the observation's runtime task, so a loaded scenario
    # runner cannot spend the delivery budget between polls of an already buffered message.
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
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "jetstream_in_{{test_id}}" exists with 1 partitions
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
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_ingress TOPIC 'jetstream_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'jetstream_boundary_group_{{test_id}}'
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
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT EMITTER nats_jetstream FROM notifications
        TO nats_main SUBJECT 'jetstream_out_{{test_id}}'
          MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 1s
            RETRY POLICY BACKOFF 100ms MAX 500ms
          ENCODE USING notification_codec
        INHERIT ALL
        <batch>
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
      <payload>
      """
    And within "5s" Kafka consumer group "jetstream_boundary_group_{{test_id}}" next offset for topic "jetstream_in_{{test_id}}" partition 0 is "at least 1"

    # A batch payload is one stream message, so its PubAck is the completion point.
    Examples:
      | cluster_size | batch                              | payload          |
      | 1            |                                    | {"user_id":42}   |
      | 3            |                                    | {"user_id":42}   |
      | 1            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |
      | 3            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |

  # One NATS message carries every record, which the ingest codec splits into one batch, so the
  # emitter hands all of them to JetStream in one publish. The emitter publishes through an endpoint
  # the scenario controls, which forwards its first 16 publishes and then stops reading from it, as
  # a server that stopped reading would. JetStream answers those 16, while the records behind them
  # back up in the client until it cannot take another one within ACK TIMEOUT. The confirmation
  # window settles the 16 answers JetStream already gave before the publish fails, so the retry
  # after the endpoint forwards again never sends those records a second time. A record the client
  # had taken without an answer reaches the stream again, as at-least-once delivery allows.
  @nats_jetstream_confirmation_window
  Scenario Outline: A NATS JetStream emitter never resends the records JetStream confirmed before its client stopped taking records
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS JetStream stream "window_{{test_id}}" exists for subject "window_out_{{test_id}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA window_input ( sequence I64 );
      CREATE SCHEMA window_record ( sequence I64, padding STRING );
      CREATE WIRE JSON SCHEMA window_record_wire MODE STRICT ( sequence integer, padding string );
      CREATE CODEC window_input_codec FROM JSON TO SCHEMA window_input
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC window_record_codec
        FROM WIRE JSON SCHEMA window_record_wire
        TO SCHEMA window_record;
      CREATE RELAY window_records SCHEMA window_record UNBRANCHED;
      CREATE NATS CLIENT nats_in CONFIG { 'addr' = '{{nats_addr}}' };
      CREATE NATS SUBJECT INGESTOR window_in
        FROM nats_in SUBJECT 'window_in_{{test_id}}' QUEUE GROUP 'window_in_group_{{test_id}}' INSTANCES 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING window_input_codec
        TO window_records
          INHERIT ALL
          SET padding = repeat('x', 1000)
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS CLIENT nats_out CONFIG { 'addr' = '{{nats_controlled_addr}}' };
      CREATE NATS SUBJECT EMITTER window_out FROM window_records
        TO nats_out SUBJECT 'window_out_{{test_id}}'
          MODE JETSTREAM ACK PARALLEL MAX 16384 ACK TIMEOUT 3s
            RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING window_record_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "window_in" on the leader node contains
      """
      ready: true
      """
    When the controlled NATS endpoint holds what each client sends after 16 publishes
    And a NATS message carrying records 1 to 12000 by field "sequence" is published to subject "window_in_{{test_id}}"
    Then within "60s" DESCRIBE EMITTER "window_out" on the leader node contains
      """
      timed out: didn't receive ack in time
      """
    When the controlled NATS endpoint forwards everything
    Then within "180s" NATS JetStream stream "window_{{test_id}}" holds every record from 1 to 12000 by field "sequence", and records 1 to 16 exactly once

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A core NATS write delivers its records once the client flushed them to its socket. The emitter
  # publishes through an endpoint the scenario controls, which goes down, so the client holds the
  # three records one write hands it and the write runs out of the client's timeout_ms with its
  # records unresolved. Every retry first waits for the client to flush what it holds and hands it
  # nothing more, so the client holds one copy of each record. STOP ends the emitter either by
  # draining it, whose retry waits for the client to send what it holds and then sends its own copy
  # of each record, or by finishing it, which flushes what the client holds. Either way STOP returns
  # only once the endpoint forwards again, and every held record reaches the subscriber, a retried
  # one possibly twice.
  @nats_finish_at_stop @emitter_finish_at_stop @deloxide_stress
  Scenario Outline: STOP sends the records a core NATS write left with its client
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    And drain timeout is configured as "60s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "held_out_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA held_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT ( sequence integer );
      CREATE CODEC held_events_codec FROM JSON TO SCHEMA held_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC held_event_codec FROM WIRE JSON SCHEMA held_event_wire TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE VHOST edge held-nats-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR held_source
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_events_codec
        TIMESTAMP NOW
        TO held_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS CLIENT held_nats CONFIG {
        'addr' = '{{nats_controlled_addr}}',
        'timeout_ms' = '1000'
      };
      CREATE NATS SUBJECT EMITTER held_output FROM held_events
        TO held_nats SUBJECT 'held_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING held_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "held-nats-{{test_id}}.example.com" path "/events"
      """
      [{"sequence":0}]
      """
    Then the observed broker receives a payload
      """
      {"sequence":0}
      """
    When the controlled NATS endpoint goes down
    Then the controlled NATS endpoint refuses a client that reconnects
    When http payload is posted to node "node-1" with host "held-nats-{{test_id}}.example.com" path "/events"
      """
      [{"sequence":1},{"sequence":2},{"sequence":3}]
      """
    Then within "30s" DESCRIBE EMITTER "held_output" on the leader node contains
      """
      did not finish within timeout_ms 1s
      """
    When STOP runs while the controlled NATS endpoint forwards everything after "2s"
    Then STOP completed after the controlled NATS endpoint forwarded everything
    And within "30s" the observed broker receives payloads
      """
      {"sequence":1}
      {"sequence":2}
      {"sequence":3}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
