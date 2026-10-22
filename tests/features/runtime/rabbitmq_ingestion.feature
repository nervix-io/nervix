Feature: RabbitMQ ingestion
  Scenario Outline: RabbitMQ ingestor delivers JSON payloads to a subscribed relay
    Given RabbitMQ is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_{{test_id}}" exists
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
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'notifications_{{test_id}}' INSTANCES <instances> MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then RabbitMQ queue "notifications_{{test_id}}" eventually has <instances> consumers
    When RabbitMQ message is published to queue "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 1         | 0             |
      | 3            | 2         | 0             |
      | 3            | 1         | 1             |
      | 3            | 2         | 1             |

  Scenario Outline: RabbitMQ ingestor reports transient source failures and recovers
    Given RabbitMQ is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_reconnect_{{test_id}}" exists
    When ingestor "rabbit_notifications" enters fault mode
    And these NSPL commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'notifications_reconnect_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "rabbit_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "rabbit_notifications" leaves fault mode
    And RabbitMQ message is published to queue "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "rabbit_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @rabbitmq_ingestor_redelivery
  Scenario Outline: RabbitMQ ACK SEQUENTIAL redelivers a payload whose acknowledgement failed
    Given RabbitMQ is running
    And MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_redelivery_{{test_id}}" exists
    And MQTT topic "notifications_redelivery_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
        CREATE CLIENT mqtt_out
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-rabbit-redelivery-out-{{test_id}}'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'notifications_redelivery_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER mqtt_forward FROM notifications TO MQTT mqtt_out TOPIC notifications_redelivery_out_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then RabbitMQ queue "notifications_redelivery_{{test_id}}" eventually has 1 consumers
    When emitter "mqtt_forward" enters fault mode
    And RabbitMQ message is published to queue "notifications_redelivery_{{test_id}}"
      """
      {"user_id":44}
      """
    Then the relay subscription receives a payload
      """
      "user_id":44
      """
    And within "2s" the relay subscription receives payloads
      """
      "user_id":44
      """
    When emitter "mqtt_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":44}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # The ingestor reads through a stallable endpoint, so the connection RabbitMQ delivered on can be
  # dropped without restarting the broker the other scenarios share. A stalled emitter keeps the
  # delivery unacknowledged until the connection is gone, and its fault then makes the ingestor
  # reject the delivery on that lost connection.
  Scenario Outline: RabbitMQ ACK SEQUENTIAL redelivers once a delivery it rejects after its connection was lost
    Given RabbitMQ is running
    And MQTT is running
    And a stallable RabbitMQ endpoint is configured
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_lost_reject_{{test_id}}" exists
    And MQTT topic "notifications_lost_reject_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = '{{rabbitmq_stallable_addr}}'
        };
        CREATE CLIENT mqtt_out
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-rabbit-lost-reject-out-{{test_id}}'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'notifications_lost_reject_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER mqtt_forward FROM notifications TO MQTT mqtt_out TOPIC notifications_lost_reject_out_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then RabbitMQ queue "notifications_lost_reject_{{test_id}}" eventually has 1 consumers
    When emitter "mqtt_forward" enters stall mode
    And RabbitMQ message is published to queue "notifications_lost_reject_{{test_id}}"
      """
      {"user_id":45}
      """
    Then the relay subscription receives a payload
      """
      "user_id":45
      """
    When the stallable endpoint "rabbitmq" resets its connections
    Then RabbitMQ queue "notifications_lost_reject_{{test_id}}" eventually has 0 consumers
    When emitter "mqtt_forward" enters fault mode
    Then within "30s" DESCRIBE INGESTOR "rabbit_notifications" on the leader node contains
      """
      nervix_ingestor_transport_redeliveries_total: 1
      """
    And the relay subscription receives a payload
      """
      "user_id":45
      """
    When emitter "mqtt_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":45}
      """
    When RabbitMQ message is published to queue "notifications_lost_reject_{{test_id}}"
      """
      {"user_id":46}
      """
    Then the observed broker receives a payload
      """
      {"user_id":46}
      """
    And within "5s" DESCRIBE INGESTOR "rabbit_notifications" on the leader node contains
      """
      nervix_ingestor_transport_redeliveries_total: 1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  # The same lost connection, but the stalled emitter recovers instead: the graph acknowledges the
  # delivery after its connection is gone, so RabbitMQ never receives that acknowledgement and
  # delivers the message again.
  Scenario Outline: RabbitMQ ACK SEQUENTIAL delivers again a delivery acknowledged after its connection was lost
    Given RabbitMQ is running
    And MQTT is running
    And a stallable RabbitMQ endpoint is configured
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "notifications_lost_ack_{{test_id}}" exists
    And MQTT topic "notifications_lost_ack_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = '{{rabbitmq_stallable_addr}}'
        };
        CREATE CLIENT mqtt_out
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-rabbit-lost-ack-out-{{test_id}}'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'notifications_lost_ack_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER mqtt_forward FROM notifications TO MQTT mqtt_out TOPIC notifications_lost_ack_out_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then RabbitMQ queue "notifications_lost_ack_{{test_id}}" eventually has 1 consumers
    When emitter "mqtt_forward" enters stall mode
    And RabbitMQ message is published to queue "notifications_lost_ack_{{test_id}}"
      """
      {"user_id":47}
      """
    Then the relay subscription receives a payload
      """
      "user_id":47
      """
    When the stallable endpoint "rabbitmq" resets its connections
    Then RabbitMQ queue "notifications_lost_ack_{{test_id}}" eventually has 0 consumers
    When emitter "mqtt_forward" leaves stall mode
    Then the observed broker receives a payload
      """
      {"user_id":47}
      """
    And within "30s" DESCRIBE INGESTOR "rabbit_notifications" on the leader node contains
      """
      nervix_ingestor_transport_redeliveries_total: 1
      """
    And the observed broker receives a payload
      """
      {"user_id":47}
      """
    When RabbitMQ message is published to queue "notifications_lost_ack_{{test_id}}"
      """
      {"user_id":48}
      """
    Then the observed broker receives a payload
      """
      {"user_id":48}
      """
    And within "5s" DESCRIBE INGESTOR "rabbit_notifications" on the leader node contains
      """
      nervix_ingestor_transport_redeliveries_total: 1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @decode_rejection
  Scenario Outline: RabbitMQ ACK SEQUENTIAL acknowledges a payload its codec refuses, reports its record and delivers the next one
    Given RabbitMQ is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "poison_{{test_id}}" exists
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
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT rabbit_main
        TYPE RABBITMQ
        CONFIG {
          'addr' = '{{rabbitmq_addr}}'
        };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_poison
        FROM rabbit_main QUEUE 'poison_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then RabbitMQ queue "poison_{{test_id}}" eventually has 1 consumers
    When RabbitMQ message is published to queue "poison_{{test_id}}"
      """
      {"user_id":"one"}
      """
    And RabbitMQ message is published to queue "poison_{{test_id}}"
      """
      {"user_id":2}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":2
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'rabbit_poison' general error in domain '{{domain}}': decode rejection
      """
    And the last server error contains
      """
      codec 'notification_codec' refused the payload: codec 'notification_codec' failed to parse field 'user_id'
      """
    And within "30s" DESCRIBE INGESTOR "rabbit_poison" on the leader node contains
      """
      nervix_ingestor_decode_rejections_total: 1
      """
    # A delivery the ingestor still held would return to the queue once its consumer leaves.
    When these NSPL commands are executed
      """
      STOP;
      """
    Then RabbitMQ queue "poison_{{test_id}}" eventually has 0 consumers
    And RabbitMQ queue "poison_{{test_id}}" holds 0 messages

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
