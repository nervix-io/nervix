Feature: RabbitMQ emission
  Scenario Outline: RabbitMQ emitter publishes JSON payloads from a relay
    Given RabbitMQ is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_out_{{test_id}}" is observed
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
        CREATE RABBITMQ CLIENT rabbit_main
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
        CREATE RABBITMQ QUEUE EMITTER rabbit_notifications FROM notifications TO rabbit_main QUEUE 'notifications_out_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "rabbit_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "rabbit_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "rabbit_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @rabbitmq_emitter_ack_boundary
  Scenario Outline: RabbitMQ publisher confirms hold the input offset until confirmation
    Given Kafka is running
    And RabbitMQ is running
    And a stallable RabbitMQ endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rabbitmq_boundary_in_{{test_id}}" exists with 1 partitions
    And RabbitMQ queue "rabbitmq_boundary_out_{{test_id}}" is observed
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
        FROM kafka_ingress TOPIC 'rabbitmq_boundary_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'rabbitmq_boundary_group_{{test_id}}'
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
      CREATE RABBITMQ CLIENT rabbitmq_sink CONFIG {
        'addr' = '{{rabbitmq_stallable_addr}}'
      };
      CREATE ATTACHED RABBITMQ QUEUE EMITTER rabbitmq_boundary FROM notifications
        TO rabbitmq_sink QUEUE 'rabbitmq_boundary_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 300ms
            RETRY POLICY BACKOFF 100ms MAX 200ms
          ENCODE USING notification_codec
        INHERIT ALL
        <batch>
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "rabbitmq_boundary_group_{{test_id}}" eventually has 1 consumers
    When the stallable endpoint "rabbitmq" is paused
    And Kafka message is published to topic "rabbitmq_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "2s" Kafka consumer group "rabbitmq_boundary_group_{{test_id}}" next offset for topic "rabbitmq_boundary_in_{{test_id}}" partition 0 is "below 1"
    When the stallable endpoint "rabbitmq" is resumed
    Then the observed broker receives a payload
      """
      <payload>
      """
    And within "5s" Kafka consumer group "rabbitmq_boundary_group_{{test_id}}" next offset for topic "rabbitmq_boundary_in_{{test_id}}" partition 0 is "at least 1"

    # A batch payload is one message, so its publisher confirm is the completion point.
    Examples:
      | cluster_size | batch                              | payload          |
      | 1            |                                    | {"user_id":42}   |
      | 3            |                                    | {"user_id":42}   |
      | 1            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |
      | 3            | BATCH MAX MESSAGES 2 MAX SIZE 1KiB | [{"user_id":42}] |

  # Lapin logs each message a broker returns at warn with the whole message, its body included. The
  # node is one process with one log, so a single server process shows what an operator's filter
  # lets into it. The first record proves the emitter checked its queue at open; the second is
  # returned because the queue was deleted after that check, and the emitter's own report of that
  # return names the reply without the record.
  Scenario Outline: A message RabbitMQ returns stays out of the node log under the trace filter "<filter>"
    Given RabbitMQ is running
    And RabbitMQ queue "returned_{{test_id}}" exists
    And a nervix-server process is started with trace filter "<filter>"
    And the server process is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the server process is configured with these Plexus commands
      """
      CREATE SCHEMA returned_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA returned_event_wire MODE STRICT ( sequence integer );
      CREATE CODEC returned_event_codec
        FROM WIRE JSON SCHEMA returned_event_wire
        TO SCHEMA returned_event;
      CREATE RELAY returned_events SCHEMA returned_event UNBRANCHED;
      CREATE VHOST edge returned-{{test_id}}.example.com;
      CREATE ENDPOINT returned_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR returned_source
        FROM returned_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING returned_event_codec
        TIMESTAMP NOW
        TO returned_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT rabbit_main
      CONFIG {
        'addr' = '{{rabbitmq_addr}}'
      };
      CREATE RABBITMQ QUEUE EMITTER returned_output FROM returned_events
        TO rabbit_main QUEUE 'returned_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 30s MAX 30s
        ENCODE USING returned_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When the server process eventually accepts http payload with host "returned-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    Then RabbitMQ queue "returned_{{test_id}}" eventually has 1 outstanding deliveries in total
    When RabbitMQ queue "returned_{{test_id}}" is deleted
    And http payload is posted to the server process with host "returned-{{test_id}}.example.com" path "/events"
      """
      {"sequence":2}
      """
    Then within "60s" DESCRIBE EMITTER "returned_output" on the server process contains
      """
      rabbitmq returned message with reply code 312: NO_ROUTE
      """
    When the server process receives SIGKILL
    Then the server process is terminated by SIGKILL within "30s" of the last signal
    And the server process log contains "rabbitmq returned message with reply code 312: NO_ROUTE"
    And the server process log does not contain "Server returned us a message"

    Examples:
      | filter                             |
      | info                               |
      | info,lapin::returned_messages=warn |
