Feature: RabbitMQ delivery metadata
  A RABBITMQ QUEUE ingestor exposes each delivery's envelope and its AMQP basic properties as typed
  metadata.<field> values: exchange, routing_key, delivery_tag and redelivered from the delivery, and
  content_type, content_encoding, message_id, correlation_id, reply_to, type, app_id, user_id,
  priority, expiration and timestamp from its properties. A property the publisher did not set reads
  as null. TIMESTAMP AT metadata.timestamp admits a message at the instant its producer wrote, and a
  timestamp no DATETIME holds refuses the message wherever a program reads it.

  @rabbitmq_metadata
  Scenario Outline: A RabbitMQ ingestor reads the delivery envelope and every AMQP property as metadata
    Given RabbitMQ is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ exchange "orders_{{test_id}}" of type direct exists
    And RabbitMQ queue "orders_{{test_id}}" exists
    And RabbitMQ queue "orders_{{test_id}}" is bound to exchange "orders_{{test_id}}" with routing key "orders.created"
    When these Plexus commands are executed
      """
      CREATE SCHEMA order_event ( user_id I64 );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT ( user_id integer );
      CREATE CODEC order_codec FROM WIRE JSON SCHEMA order_wire TO SCHEMA order_event;
      CREATE SCHEMA delivered_order (
        user_id I64,
        exchange STRING OPTIONAL,
        routing_key STRING OPTIONAL,
        delivery_tag U64 OPTIONAL,
        redelivered BOOL OPTIONAL,
        content_type STRING OPTIONAL,
        content_encoding STRING OPTIONAL,
        message_id STRING OPTIONAL,
        correlation_id STRING OPTIONAL,
        reply_to STRING OPTIONAL,
        message_type STRING OPTIONAL,
        app_id STRING OPTIONAL,
        publisher STRING OPTIONAL,
        priority U8 OPTIONAL,
        expiration STRING OPTIONAL,
        published_at DATETIME OPTIONAL,
        no_properties BOOL
      );
      CREATE RELAY orders SCHEMA delivered_order UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = '{{rabbitmq_addr}}'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_orders
        FROM rabbit_main QUEUE 'orders_{{test_id}}' MODE <mode>
        ON QUIESCE SUSPEND DECODE USING order_codec
        TO orders INHERIT ALL
        SET exchange = metadata.exchange,
          routing_key = metadata.routing_key,
          delivery_tag = metadata.delivery_tag,
          redelivered = metadata.redelivered,
          content_type = metadata.content_type,
          content_encoding = metadata.content_encoding,
          message_id = metadata.message_id,
          correlation_id = metadata.correlation_id,
          reply_to = metadata.reply_to,
          message_type = metadata.type,
          app_id = metadata.app_id,
          publisher = metadata.user_id,
          priority = metadata.priority,
          expiration = metadata.expiration,
          published_at = metadata.timestamp,
          no_properties = is_null(metadata.content_type) AND is_null(metadata.content_encoding)
            AND is_null(metadata.message_id) AND is_null(metadata.correlation_id)
            AND is_null(metadata.reply_to) AND is_null(metadata.type) AND is_null(metadata.app_id)
            AND is_null(metadata.user_id) AND is_null(metadata.priority)
            AND is_null(metadata.expiration) AND is_null(metadata.timestamp)
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION orders_subscription TO orders;
      START;
      """
    Then RabbitMQ queue "orders_{{test_id}}" eventually has 1 consumers
    When a RabbitMQ message is published to exchange "orders_{{test_id}}" with routing key "orders.created" and these AMQP properties
      | payload          | {"user_id":42}   |
      | content_type     | application/json |
      | content_encoding | identity         |
      | message_id       | order-42         |
      | correlation_id   | request-7        |
      | reply_to         | order-replies    |
      | type             | order.created    |
      | app_id           | billing          |
      | user_id          | guest            |
      | priority         | 5                |
      | expiration       | 600000           |
      | timestamp        | 946684800        |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":42 | "exchange":"orders_{{test_id}}" | "routing_key":"orders.created" | "delivery_tag":1 | "redelivered":false | "content_type":"application/json" | "content_encoding":"identity" | "message_id":"order-42" | "correlation_id":"request-7" | "reply_to":"order-replies" | "message_type":"order.created" | "app_id":"billing" | "publisher":"guest" | "priority":5 | "expiration":"600000" | "published_at":"2000-01-01T00:00:00+00:00" | "no_properties":false
      """
    When RabbitMQ message is published to queue "orders_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":43 | "exchange":"" | "routing_key":"orders_{{test_id}}" | "redelivered":false | "no_properties":true
      """

    Examples:
      | cluster_size | mode                                                             |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |
      | 1            | NO ACK                                                           |
      | 3            | NO ACK                                                           |

  @rabbitmq_metadata
  Scenario Outline: A RabbitMQ delivery the ingestor rejected reads metadata.redelivered as true when the broker delivers it again
    Given RabbitMQ is running
    And MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "redelivered_{{test_id}}" exists
    And MQTT topic "redelivered_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA delivered_notification ( user_id I64, redelivered BOOL OPTIONAL );
      CREATE RELAY notifications SCHEMA delivered_notification UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = '{{rabbitmq_addr}}'
      };
      CREATE MQTT CLIENT mqtt_out CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-rabbit-redelivered-out-{{test_id}}'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_main QUEUE 'redelivered_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL SET redelivered = metadata.redelivered UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC EMITTER mqtt_forward FROM notifications TO mqtt_out
        TOPIC 'redelivered_out_{{test_id}}' MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec
        INHERIT ALL EXCEPT redelivered
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then RabbitMQ queue "redelivered_{{test_id}}" eventually has 1 consumers
    When emitter "mqtt_forward" enters fault mode
    And RabbitMQ message is published to queue "redelivered_{{test_id}}"
      """
      {"user_id":44}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "redelivered":false | "user_id":44
      "redelivered":true | "user_id":44
      """
    When emitter "mqtt_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":44}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @rabbitmq_metadata
  Scenario Outline: TIMESTAMP AT metadata.timestamp windows RabbitMQ messages at the instant their producer wrote
    Given RabbitMQ is running
    And a <cluster_size> node nervix cluster is started
    And RabbitMQ queue "readings_{{test_id}}" exists
    And the leader node is configured with these Plexus statements
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 1s;
      CREATE SCHEMA reading ( value I64 );
      CREATE SCHEMA timed_reading ( value I64, published_at DATETIME OPTIONAL );
      CREATE SCHEMA summary ( total I64, samples I64, first_published_at DATETIME OPTIONAL );
      CREATE CODEC reading_codec FROM JSON TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY readings SCHEMA timed_reading UNBRANCHED;
      CREATE RELAY summaries SCHEMA summary UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = '{{rabbitmq_addr}}'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_readings
        FROM rabbit_main QUEUE 'readings_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING reading_codec
        TIMESTAMP AT metadata.timestamp
        TO readings INHERIT ALL SET published_at = metadata.timestamp UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR totals FROM readings WIDTH 30ms DURATION STEP 30ms DURATION
        UNBRANCHED
        TO summaries SET total = SUM(input.value), samples = COUNT(input.value),
          first_published_at = FIRST(input.published_at)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observations TO summaries;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.01;
      """
    Then RabbitMQ queue "readings_{{test_id}}" eventually has 1 consumers
    # One delivery carries both readings, so they share one AMQP timestamp and reach the window in
    # one decoded group however long the steps between publications take.
    When a RabbitMQ message is published to exchange "" with routing key "readings_{{test_id}}" and these AMQP properties
      | payload   | [{"value":10},{"value":20}] |
      | timestamp | 946684800                   |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "first_published_at":"2000-01-01T00:00:00+00:00" | "samples":2 | "total":30
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @rabbitmq_metadata
  Scenario Outline: A RabbitMQ timestamp no DATETIME holds refuses its message only where metadata.timestamp is read
    Given RabbitMQ is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ exchange "stamps_{{test_id}}" of type fanout exists
    And RabbitMQ queue "stamped_{{test_id}}" exists
    And RabbitMQ queue "unstamped_{{test_id}}" exists
    And RabbitMQ queue "stamped_{{test_id}}" is bound to exchange "stamps_{{test_id}}" with routing key ""
    And RabbitMQ queue "unstamped_{{test_id}}" is bound to exchange "stamps_{{test_id}}" with routing key ""
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading ( value I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( value integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA observed_reading ( value I64, source STRING, published_at DATETIME OPTIONAL );
      CREATE SCHEMA refused_reading ( value I64, code STRING, operation STRING, message STRING );
      CREATE RELAY readings SCHEMA observed_reading UNBRANCHED;
      CREATE RELAY refusals SCHEMA refused_reading UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = '{{rabbitmq_addr}}'
      };
      CREATE RABBITMQ QUEUE INGESTOR stamped
        FROM rabbit_main QUEUE 'stamped_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING reading_codec
        TO readings INHERIT ALL SET source = 'stamped', published_at = metadata.timestamp UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO refusals
          SET value = input.value, code = error.code, operation = error.operation,
            message = error.message
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ QUEUE INGESTOR unstamped
        FROM rabbit_main QUEUE 'unstamped_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING reading_codec
        TO readings INHERIT ALL SET source = 'unstamped' UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION reading_observations TO readings;
      CREATE SUBSCRIPTION refusal_observations TO refusals;
      START;
      """
    Then RabbitMQ queue "stamped_{{test_id}}" eventually has 1 consumers
    And RabbitMQ queue "unstamped_{{test_id}}" eventually has 1 consumers
    # Milliseconds written where AMQP specifies seconds: an instant in the year 31969.
    When a RabbitMQ message is published to exchange "stamps_{{test_id}}" with routing key "" and these AMQP properties
      | payload   | {"value":7}  |
      | timestamp | 946684800000 |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "source":"unstamped" | "value":7
      "code":"validation" | "operation":"admit" | metadata.timestamp | 946684800000 | "value":7
      """
    When a RabbitMQ message is published to exchange "stamps_{{test_id}}" with routing key "" and these AMQP properties
      | payload   | {"value":8} |
      | timestamp | 946684800   |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "published_at":"2000-01-01T00:00:00+00:00" | "source":"stamped" | "value":8
      "source":"unstamped" | "value":8
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
