Feature: Pulsar emission
  Scenario Outline: Pulsar emitter publishes JSON payloads from a relay
    Given Pulsar is running
    Then Pulsar serves its default namespace
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "notifications_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_pulsar_ingress SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_ingress;
        CREATE PULSAR CLIENT pulsar_main
        CONFIG {
          'addr' = '{{pulsar_addr}}'
        };
        CREATE PULSAR TOPIC INGESTOR pulsar_ingress
        FROM pulsar_main TOPIC 'notifications_in_{{test_id}}' SUBSCRIPTION 'nervix_pulsar_emission_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST INSTANCES 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_ingress
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE PULSAR TOPIC EMITTER pulsar_notifications FROM notifications TO pulsar_main TOPIC 'notifications_out_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "pulsar_notifications" enters stall mode
    And Pulsar message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "pulsar_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "pulsar_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @pulsar_emitter_ack_boundary
  Scenario Outline: Pulsar mode controls when the input offset is committed
    Given Kafka is running
    And Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "pulsar_boundary_in_{{test_id}}" exists with 1 partitions
    And Pulsar topic "pulsar_boundary_out_{{test_id}}" is observed
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
        FROM kafka_ingress TOPIC 'pulsar_boundary_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'pulsar_boundary_group_{{test_id}}'
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
      CREATE PULSAR CLIENT pulsar_sink CONFIG {
        'addr' = '{{pulsar_stallable_addr}}'
      };
      CREATE ATTACHED PULSAR TOPIC EMITTER pulsar_boundary FROM notifications
        TO pulsar_sink TOPIC 'pulsar_boundary_out_{{test_id}}'
          MODE <publishing_mode>
          ENCODE USING notification_codec
        INHERIT ALL
        <batch>
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "pulsar_boundary_group_{{test_id}}" eventually has 1 consumers
    When the stallable endpoint "pulsar" is paused
    And Kafka message is published to topic "pulsar_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "2s" Kafka consumer group "pulsar_boundary_group_{{test_id}}" next offset for topic "pulsar_boundary_in_{{test_id}}" partition 0 is "<offset_condition>"
    When the stallable endpoint "pulsar" is resumed
    Then the observed broker receives a payload
      """
      <payload>
      """
    And within "5s" Kafka consumer group "pulsar_boundary_group_{{test_id}}" next offset for topic "pulsar_boundary_in_{{test_id}}" partition 0 is "at least 1"

    # A batch payload is one publish, so batching keeps each mode's completion point: producer
    # acceptance for NO ACK and the broker receipt of the one message for ACK.
    Examples:
      | cluster_size | publishing_mode                                                       | offset_condition | batch                              | payload          |
      | 1            | NO ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |                                    | {"user_id":42}   |
      | 3            | NO ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       |                                    | {"user_id":42}   |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |                                    | {"user_id":42}   |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          |                                    | {"user_id":42}   |
      | 3            | NO ACK RETRY POLICY BACKOFF 100ms MAX 200ms                           | at least 1       | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 300ms RETRY POLICY BACKOFF 100ms MAX 200ms | below 1          | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |

  # A record's TIMESTAMP is the event time its Pulsar message carries. Pulsar counts whole
  # milliseconds, so a finer DATETIME is written as the millisecond that contains it. Pulsar reads a
  # zero event time as none, and a message carries no time before the Unix epoch, so a TIMESTAMP
  # whose millisecond is not after the epoch rejects its record and no other.
  @pulsar_emitter_envelope
  Scenario Outline: A Pulsar emitter writes each message's TIMESTAMP as its event time
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "event_time_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, event_at DATETIME );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, event_at string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event
        ENCODE event_at AS RFC3339;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge event-time-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER timestamped FROM events
        TO pulsar_main TOPIC 'event_time_{{test_id}}'
          MODE <mode>
          ENCODE USING event_codec
          TIMESTAMP input.event_at
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "event-time-{{test_id}}.example.com" path "/events"
      """
      {"seq":1,"event_at":"2001-02-03T04:05:06.789+00:00"}
      """
    And http payload is posted to host "event-time-{{test_id}}.example.com" path "/events"
      """
      {"seq":2,"event_at":"2001-02-03T04:05:06.789999999+00:00"}
      """
    And http payload is posted to host "event-time-{{test_id}}.example.com" path "/events"
      """
      {"seq":3,"event_at":"1970-01-01T00:00:00.001+00:00"}
      """
    And http payload is posted to host "event-time-{{test_id}}.example.com" path "/events"
      """
      {"seq":4,"event_at":"1970-01-01T00:00:00.000999999+00:00"}
      """
    And http payload is posted to host "event-time-{{test_id}}.example.com" path "/events"
      """
      {"seq":5,"event_at":"1969-12-31T23:59:59.999+00:00"}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":4 | "error_code":"external" | "operation":"publish" | "error_message":"PULSAR rejected record: message TIMESTAMP 1970-01-01T00:00:00.000
      "seq":5 | "error_code":"external" | "operation":"publish" | "error_message":"PULSAR rejected record: message TIMESTAMP 1969-12-31T23:59:59.999
      """
    And within "30s" the observed broker receives exactly these Pulsar messages
      | payload                                                    | event time               |
      | {"seq":1,"event_at":"2001-02-03T04:05:06.789+00:00"}       | 2001-02-03T04:05:06.789Z |
      | {"seq":2,"event_at":"2001-02-03T04:05:06.789999999+00:00"} | 2001-02-03T04:05:06.789Z |
      | {"seq":3,"event_at":"1970-01-01T00:00:00.001+00:00"}       | 1970-01-01T00:00:00.001Z |
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER timestamped;
      """
    Then the last command output contains
      """
      envelope: TIMESTAMP input.event_at
      """
    And SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter     | clause                   |
      | timestamped | ENCODE USING event_codec |
      | timestamped | TIMESTAMP input.event_at |

    Examples:
      | cluster_size | mode                                                                |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s     |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s     |
      | 1            | ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s |
      | 3            | ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s |
      | 1            | NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                             |
      | 3            | NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                             |

  # DELIVER AFTER is measured from the batch's execution time, which TIMESTAMP now() writes beside
  # it. A broker holds a delayed message back from a shared subscription and may hand it over as
  # much as its delayedDeliveryTickTimeMillis early, one second by default, so the message arrives
  # at least that much inside the delay.
  @pulsar_emitter_envelope
  Scenario Outline: A Pulsar emitter delivers each message DELIVER AFTER its batch's execution time
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "delayed_{{test_id}}" is observed through a shared subscription
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge delayed-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER delayed FROM events
        TO pulsar_main TOPIC 'delayed_{{test_id}}'
          MODE <mode>
          ENCODE USING event_codec
          TIMESTAMP now()
          DELIVER AFTER 4s
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "delayed-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"seq":1}
      """
    And the last observed Pulsar message is due "4s" after its event time
    And the last observed Pulsar message arrived at least "3s" after its event time
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER delayed;
      """
    Then the last command output contains
      """
      envelope: TIMESTAMP now() DELIVER AFTER 4s
      """
    And SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause           |
      | delayed | TIMESTAMP now()  |
      | delayed | DELIVER AFTER 4s |

    Examples:
      | cluster_size | mode                                                                |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s     |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s     |
      | 1            | ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s |
      | 3            | ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s |
      | 1            | NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                             |
      | 3            | NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                             |

  # The endpoint holds the first attempt past its ACK TIMEOUT, so its outcome is unknown and the
  # emitter sends the message again. The host keeps the envelope and execution time of the batch it
  # admitted, so the copy a later attempt sends carries the event time and delivery time of the
  # first, however much later it is sent.
  @pulsar_emitter_envelope
  Scenario Outline: A Pulsar message keeps its event time and delivery time when the emitter sends it again
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "resent_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge resent-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_sink CONFIG { 'addr' = '{{pulsar_stallable_addr}}' };
      CREATE PULSAR TOPIC EMITTER resent FROM events
        TO pulsar_sink TOPIC 'resent_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 2s MAX 2s
          ENCODE USING event_codec
          TIMESTAMP now()
          DELIVER AFTER 5s
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And the Pulsar endpoint pauses after its producer is ready within "30s"
    And http payload is posted to host "resent-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "60s" DESCRIBE EMITTER "resent" on the leader node reports a transient error with its next attempt at most "2s" away
    When the stallable endpoint "pulsar" is resumed
    Then within "60s" the observed broker receives this Pulsar message sent again at least "1s" after its event time
      """
      {"seq":1}
      """
    And the last observed Pulsar message is due "5s" after its event time

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # ALTER … SET TO replaces the whole destination body, so a body that writes the clauses starts
  # them for later messages and a body without them returns those messages to no event time and
  # immediate delivery.
  @pulsar_emitter_envelope
  Scenario Outline: ALTER SET TO starts and stops a Pulsar emitter's TIMESTAMP and DELIVER AFTER
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "altered_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge altered-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER altered FROM events
        TO pulsar_main TOPIC 'altered_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "altered-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "30s" the observed broker receives exactly these Pulsar messages
      | payload   | event time |
      | {"seq":1} |            |
    And the last observed Pulsar message carries no delivery time
    When these Plexus commands are executed on the leader node
      """
      ALTER PULSAR TOPIC EMITTER altered SET TO pulsar_main TOPIC 'altered_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING event_codec
        TIMESTAMP now()
        DELIVER AFTER 2s;
      DESCRIBE EMITTER altered;
      """
    Then the last command output contains
      """
      envelope: TIMESTAMP now() DELIVER AFTER 2s
      """
    When http payload is posted to host "altered-{{test_id}}.example.com" path "/events"
      """
      {"seq":2}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"seq":2}
      """
    And the last observed Pulsar message is due "2s" after its event time
    When these Plexus commands are executed on the leader node
      """
      ALTER PULSAR TOPIC EMITTER altered SET TO pulsar_main TOPIC 'altered_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING event_codec;
      DESCRIBE EMITTER altered;
      """
    Then the last command output contains
      """
      envelope: none
      """
    When http payload is posted to host "altered-{{test_id}}.example.com" path "/events"
      """
      {"seq":3}
      """
    Then within "30s" the observed broker receives exactly these Pulsar messages
      | payload   | event time |
      | {"seq":3} |            |
    And the last observed Pulsar message carries no delivery time

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A topic's delivery policy may bound how far ahead a message asks to be delivered. The broker
  # refuses a message past that bound for good, so it follows ON MESSAGE ERROR instead of being sent
  # again, and a message within the bound is delivered.
  @pulsar_emitter_envelope
  Scenario Outline: A delay past the topic's maximum delivery delay rejects its record
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "delay_limit_{{test_id}}" is observed
    And Pulsar topic "delay_limit_{{test_id}}" allows a delivery delay of at most "1h"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge delay-limit-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER delayed FROM events
        TO pulsar_main TOPIC 'delay_limit_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
          DELIVER AFTER 2h
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "delay-limit-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "error_code":"external" | "operation":"publish" | "error_message":"PULSAR rejected record: the Pulsar broker does not allow the message
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A paced domain's execution time can stand so late that adding the delay leaves the signed
  # Unix-nanosecond range, which no Nervix timestamp holds, so that record is rejected. No delay a
  # statement can write reaches past that range from an unpaced domain's actual time. A SKEW of at
  # least the PERIOD admits every delivery, so the record reaches its emitter whenever it arrives.
  @pulsar_emitter_envelope
  Scenario Outline: A delivery time past the last Nervix timestamp rejects its record
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And Pulsar topic "last_time_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge last-time-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP NOW
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER delayed FROM events
        TO pulsar_main TOPIC 'last_time_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
          DELIVER AFTER 876000h
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START AT '2200-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And http payload is posted to host "last-time-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "error_code":"external" | "operation":"publish" | "error_message":"PULSAR rejected record: DELIVER AFTER 3153600000s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
