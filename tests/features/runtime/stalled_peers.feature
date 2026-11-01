Feature: Stalled peers

  A connector entity whose waits a client deadline key bounds reports a peer that stops answering
  as its transient status within that bound, and keeps retrying on its policy. A silent endpoint
  accepts every connection and never answers on it, so a client completes its TCP handshake and then
  waits on its own deadline.

  Scenario Outline: A Redis subscription whose server never answers is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "redis_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = 'redis://{{redis_peer}}',
        'connect_timeout_ms' = '1000',
        'subscription_buffer_messages' = '1024',
        'idle_timeout_ms' = '30000'
      };
      CREATE REDIS PUBSUB INGESTOR redis_in
        FROM redis_main CHANNEL 'stalled_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "redis_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "redis_in" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Redis emitter whose server never answers is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "redis_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge stalled-redis-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://{{redis_peer}}',
        'connect_timeout_ms' = '1000',
        'subscription_buffer_messages' = '1024',
        'idle_timeout_ms' = '30000'
      };
      CREATE REDIS PUBSUB EMITTER redis_out FROM notifications
        TO redis_main CHANNEL 'stalled_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-redis-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42}
      """
    Then the silent endpoint "redis_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "redis_out" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A RabbitMQ ingestor whose broker never answers its TLS handshake is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "rabbitmq_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = 'amqps://{{rabbitmq_peer}}/%2f',
        'connect_timeout_ms' = '1000'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_in
        FROM rabbit_main QUEUE 'stalled_{{test_id}}' INSTANCES 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "rabbitmq_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "rabbit_in" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A RabbitMQ emitter whose broker never answers its TLS handshake is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "rabbitmq_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = 'amqps://{{rabbitmq_peer}}/%2f',
        'connect_timeout_ms' = '1000'
      };
      CREATE RABBITMQ <entity> EMITTER rabbit_out FROM notifications
        TO rabbit_main <destination>
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "rabbitmq_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "rabbit_out" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size | entity   | destination                                          |
      | 1            | QUEUE    | QUEUE 'stalled_{{test_id}}'                          |
      | 3            | QUEUE    | QUEUE 'stalled_{{test_id}}'                          |
      | 1            | EXCHANGE | EXCHANGE 'stalled_{{test_id}}' ROUTING KEY 'stalled' |
      | 3            | EXCHANGE | EXCHANGE 'stalled_{{test_id}}' ROUTING KEY 'stalled' |

  Scenario Outline: A RabbitMQ ingestor whose broker never answers its AMQP handshake is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "rabbitmq_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = 'amqp://{{rabbitmq_peer}}/%2f',
        'connect_timeout_ms' = '1000'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_in
        FROM rabbit_main QUEUE 'stalled_{{test_id}}' INSTANCES 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "rabbitmq_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "rabbit_in" on the leader node contains
      """
      the AMQP handshake with the RabbitMQ broker did not finish within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A RabbitMQ emitter whose broker never answers its AMQP handshake is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "rabbitmq_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = 'amqp://{{rabbitmq_peer}}/%2f',
        'connect_timeout_ms' = '1000'
      };
      CREATE RABBITMQ <entity> EMITTER rabbit_out FROM notifications
        TO rabbit_main <destination>
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "rabbitmq_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "rabbit_out" on the leader node contains
      """
      the AMQP handshake with the RabbitMQ broker did not finish within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size | entity   | destination                                          |
      | 1            | QUEUE    | QUEUE 'stalled_{{test_id}}'                          |
      | 3            | QUEUE    | QUEUE 'stalled_{{test_id}}'                          |
      | 1            | EXCHANGE | EXCHANGE 'stalled_{{test_id}}' ROUTING KEY 'stalled' |
      | 3            | EXCHANGE | EXCHANGE 'stalled_{{test_id}}' ROUTING KEY 'stalled' |

  Scenario Outline: A RabbitMQ NO ACK emitter whose broker stops answering its round trip is reported within timeout_ms
    Given RabbitMQ is running
    And a stallable RabbitMQ endpoint is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    # The queue is also bound under the exchange's key, so both entities reach the observed queue.
    And RabbitMQ exchange "stalled_{{test_id}}" of type direct exists
    And RabbitMQ queue "stalled_{{test_id}}" is bound to exchange "stalled_{{test_id}}" with routing key "stalled"
    And RabbitMQ queue "stalled_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge stalled-rabbitmq-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = '{{rabbitmq_stallable_addr}}',
        'timeout_ms' = '1000'
      };
      CREATE RABBITMQ <entity> EMITTER rabbit_out FROM notifications
        TO rabbit_main <destination>
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-rabbitmq-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1}
      """
    Then the observed broker receives a payload
      """
      {"user_id":1}
      """
    # The broker routes a message before its answer to the write's round trip reaches the emitter,
    # so the emitter's own count shows that the first write ended before the endpoint stalls.
    And within "20s" DESCRIBE EMITTER "rabbit_out" on the leader node contains
      """
      owner:
      """
    And the last command output owner is saved as placeholder "rabbit_owner"
    And within "20s" DESCRIBE EMITTER "rabbit_out" on the leader node contains
      """
      messages_total sent relay=notifications physical_node={{rabbit_owner}} total=1
      """
    When the stallable endpoint "rabbitmq" is paused
    And http payload is posted to host "stalled-rabbitmq-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2}
      """
    Then within "20s" DESCRIBE EMITTER "rabbit_out" on the leader node contains
      """
      the round trip that ends a RabbitMQ NO ACK write did not finish within timeout_ms 1s
      """
    When the stallable endpoint "rabbitmq" is resumed
    Then the observed broker receives a payload
      """
      {"user_id":2}
      """

    Examples:
      | cluster_size | entity   | destination                                          |
      | 1            | QUEUE    | QUEUE 'stalled_{{test_id}}'                          |
      | 3            | QUEUE    | QUEUE 'stalled_{{test_id}}'                          |
      | 1            | EXCHANGE | EXCHANGE 'stalled_{{test_id}}' ROUTING KEY 'stalled' |
      | 3            | EXCHANGE | EXCHANGE 'stalled_{{test_id}}' ROUTING KEY 'stalled' |

  Scenario Outline: A Syslog collector whose server never answers its TLS handshake is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "syslog_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_forwarder CONFIG {
        'protocol' = 'tls',
        'addr' = '{{syslog_peer}}',
        'connect_timeout_ms' = '1000'
      };
      CREATE SYSLOG COLLECTOR EMITTER syslog_forward FROM events
        TO syslog_forwarder MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING syslog_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "syslog_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "syslog_forward" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Syslog collector whose server stops reading is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "syslog_peer" is listening
    # One frame larger than both TCP buffers between the collector and a server that reads
    # nothing keeps the collector's write waiting.
    And a repeated text placeholder "bulk_message" of 8000000 bytes is prepared
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE WIRE JSON SCHEMA syslog_event_wire MODE STRICT (
        facility integer, severity integer, message string
      );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA syslog_event_wire TO SCHEMA syslog_event;
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE VHOST edge stalled-syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 32MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG CLIENT syslog_forwarder CONFIG {
        'protocol' = 'tcp',
        'addr' = '{{syslog_peer}}',
        'timeout_ms' = '1000'
      };
      CREATE SYSLOG COLLECTOR EMITTER syslog_forward FROM events
        TO syslog_forwarder MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING syslog_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-syslog-{{test_id}}.example.com" path "/events"
      """
      {"facility":1,"severity":5,"message":"{{bulk_message}}"}
      """
    Then within "30s" DESCRIBE EMITTER "syslog_forward" on the leader node contains
      """
      within timeout_ms 1s
      """
    And the silent endpoint "syslog_peer" eventually accepts 2 connections

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WebSocket connection whose server never answers its upgrade is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "websocket_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE WEBSOCKETS CLIENT ws_main CONFIG {
        'endpoint' = 'ws://{{websocket_peer}}/stalled',
        'connect_timeout_ms' = '1000'
      };
      CREATE WEBSOCKETS CONNECTION INGESTOR ws_in
        FROM ws_main MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "websocket_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "ws_in" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An HTTP polling ingestor whose server never answers is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "http_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE HTTP CLIENT poll_source CONFIG {
        'endpoint' = 'http://{{http_peer}}/notifications',
        'timeout_ms' = 1000
      };
      CREATE HTTP REQUEST INGESTOR poll_reader
        FROM poll_source METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "http_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "poll_reader" on the leader node contains
      """
      transient error: failed to read from http ingestor: failed to send HTTP ingestor request
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Prometheus ingestor whose server never answers is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "prometheus_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA sample (value F64);
      CREATE WIRE JSON SCHEMA sample_wire MODE STRICT (value number);
      CREATE CODEC sample_codec FROM WIRE JSON SCHEMA sample_wire TO SCHEMA sample;
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE PROMETHEUS CLIENT prom_main CONFIG {
        'addr' = 'http://{{prometheus_peer}}',
        'timeout_ms' = 1000
      };
      CREATE PROMETHEUS QUERY INGESTOR prom_samples
        FROM prom_main QUERY 'vector(1)' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING sample_codec
        TO samples INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "prometheus_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "prom_samples" on the leader node contains
      """
      transient error: failed to read from prometheus ingestor: failed to send Prometheus query
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Sentry emitter whose server never answers is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "sentry_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA error_event (message STRING, level STRING);
      CREATE WIRE JSON SCHEMA error_event_wire MODE STRICT (message string, level string);
      CREATE CODEC error_event_codec FROM WIRE JSON SCHEMA error_event_wire TO SCHEMA error_event;
      CREATE RELAY errors SCHEMA error_event UNBRANCHED;
      CREATE VHOST edge stalled-sentry-{{test_id}}.example.com;
      CREATE ENDPOINT error_ingress ON edge PATH '/errors' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR error_source
        FROM error_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING error_event_codec
        TO errors INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SENTRY CLIENT sentry_main CONFIG {
        'dsn' = 'http://public@{{sentry_peer}}/1',
        'timeout_ms' = 1000
      };
      CREATE SENTRY EVENT EMITTER sentry_errors FROM errors
        TO sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING error_event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-sentry-{{test_id}}.example.com" path "/errors"
      """
      {"message":"database unavailable","level":"error"}
      """
    Then the silent endpoint "sentry_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "sentry_errors" on the leader node contains
      """
      transient error: Sentry envelope request failed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An OTEL <signal> emitter whose collector never answers over <protocol> is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "otel_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64, body STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer, body string);
      CREATE CODEC ingest_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge stalled-otel-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE OTEL CLIENT otel_main CONFIG {
        'endpoint' = 'http://{{otel_peer}}',
        'protocol' = '<protocol>',
        'timeout_ms' = 1000
      };
      CREATE OTEL <signal> EMITTER stalled_export FROM events
        TO otel_main <signal_options>
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          <signal_body>
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-otel-{{test_id}}.example.com" path "/events"
      """
      {"seq":1,"body":"stalled-{{test_id}}"}
      """
    Then the silent endpoint "otel_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "stalled_export" on the leader node contains
      """
      transient error: <reported>
      """

    Examples:
      | cluster_size | signal  | protocol      | signal_options                                                                         | signal_body                                                                                                                                                                                                                                                                        | reported                                                   |
      | 1            | LOGS    | grpc          |                                                                                        | VALUES { 'time' = now(), 'body' = input.body } RESOURCE { 'service.name' = 'nervix-cucumber' } SCOPE 'stalled' VERSION '1.0'                                                                                                                                                       | OTEL gRPC export ended without an answer from the receiver |
      | 3            | LOGS    | http/protobuf |                                                                                        | VALUES { 'time' = now(), 'body' = input.body } RESOURCE { 'service.name' = 'nervix-cucumber' } SCOPE 'stalled' VERSION '1.0'                                                                                                                                                       | OTEL HTTP export request failed                            |
      | 1            | TRACES  | http/protobuf |                                                                                        | VALUES { 'trace_id' = '00112233445566778899aabbccddeeff', 'span_id' = '0011223344556677', 'name' = input.body, 'kind' = 'INTERNAL', 'start_time' = now(), 'end_time' = now(), 'status_code' = 'OK' } RESOURCE { 'service.name' = 'nervix-cucumber' } SCOPE 'stalled' VERSION '1.0' | OTEL HTTP export request failed                            |
      | 3            | TRACES  | grpc          |                                                                                        | VALUES { 'trace_id' = '00112233445566778899aabbccddeeff', 'span_id' = '0011223344556677', 'name' = input.body, 'kind' = 'INTERNAL', 'start_time' = now(), 'end_time' = now(), 'status_code' = 'OK' } RESOURCE { 'service.name' = 'nervix-cucumber' } SCOPE 'stalled' VERSION '1.0' | OTEL gRPC export ended without an answer from the receiver |
      | 1            | METRICS | grpc          | METRIC 'nervix.test.stalled' UNIT '1' DESCRIPTION 'Stalled export' SUM MONOTONIC DELTA | VALUES { 'time' = now(), 'start_time' = now(), 'value' = input.seq } RESOURCE { 'service.name' = 'nervix-cucumber' } SCOPE 'stalled' VERSION '1.0'                                                                                                                                 | OTEL gRPC export ended without an answer from the receiver |
      | 3            | METRICS | http/protobuf | METRIC 'nervix.test.stalled' UNIT '1' DESCRIPTION 'Stalled export' SUM MONOTONIC DELTA | VALUES { 'time' = now(), 'start_time' = now(), 'value' = input.seq } RESOURCE { 'service.name' = 'nervix-cucumber' } SCOPE 'stalled' VERSION '1.0'                                                                                                                                 | OTEL HTTP export request failed                            |

  Scenario Outline: A ClickHouse emitter whose server never answers is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "clickhouse_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge stalled-clickhouse-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLICKHOUSE CLIENT clickhouse_main CONFIG {
        'addr' = 'http://{{clickhouse_peer}}',
        'timeout_ms' = 1000
      };
      CREATE CLICKHOUSE TABLE EMITTER to_clickhouse FROM notifications
        TO clickhouse_main TABLE 'stalled_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } BATCH MAX MESSAGES 1 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-clickhouse-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42}
      """
    Then the silent endpoint "clickhouse_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "to_clickhouse" on the leader node contains
      """
      transient error: ClickHouse insert request failed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An SQS QUEUE emitter whose service stops answering is reported within timeout_ms
    Given SQS is running
    And a <cluster_size> node nervix cluster is started
    And a stallable endpoint "sqs_peer" stands in front of "sqs_endpoint"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "stalled_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge stalled-sqs-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_peer}}',
        'region' = 'us-east-1',
        'timeout_ms' = 1000,
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_out FROM notifications
        TO sqs_main QUEUE 'stalled_{{test_id}}'
        MODE SINGLE RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-sqs-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1}
      """
    Then the observed broker receives a payload
      """
      {"user_id":1}
      """
    When the stallable endpoint "sqs_peer" is paused
    And http payload is posted to host "stalled-sqs-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2}
      """
    Then within "20s" DESCRIBE EMITTER "sqs_out" on the leader node contains
      """
      did not finish within timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @sqs_ingestor_receive_deadline @deloxide_stress
  Scenario Outline: An SQS QUEUE ingestor whose service stops answering is reported within timeout_ms
    Given SQS is running
    And a <cluster_size> node nervix cluster is started
    And a stallable endpoint "sqs_peer" stands in front of "sqs_endpoint"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "stalled_in_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_peer}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret',
        'timeout_ms' = 1000
      };
      CREATE SQS QUEUE INGESTOR sqs_in
        FROM sqs_main QUEUE 'stalled_in_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And SQS message is published to queue "stalled_in_{{test_id}}"
      """
      {"user_id":1}
      """
    Then the relay subscription receives a payload
      """
      "user_id":1
      """
    When the stallable endpoint "sqs_peer" is paused
    Then within "20s" DESCRIBE INGESTOR "sqs_in" on the leader node contains
      """
      did not finish within timeout_ms 1s
      """
    When the stallable endpoint "sqs_peer" is resumed
    And SQS message is published to queue "stalled_in_{{test_id}}"
      """
      {"user_id":2}
      """
    Then the relay subscription receives a payload
      """
      "user_id":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A ZeroMQ emitter whose peer never answers its handshake is reported within connect_timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "zeromq_peer" is listening
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE ZEROMQ CLIENT zeromq_main CONFIG {
        'addr' = 'tcp://{{zeromq_peer}}',
        'connect_timeout_ms' = '1000'
      };
      CREATE ZEROMQ PUSH EMITTER zeromq_out FROM notifications
        TO zeromq_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "zeromq_peer" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "zeromq_out" on the leader node contains
      """
      within connect_timeout_ms 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A ZeroMQ emitter whose peer stops reading is reported within timeout_ms
    Given a <cluster_size> node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And a stallable endpoint "zeromq_peer" stands in front of the ZeroMQ emission endpoint
    # One message larger than the TCP buffers between the emitter and a peer that reads nothing
    # keeps the emitter's send waiting.
    And a repeated text placeholder "bulk_message" of 8000000 bytes is prepared
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64, message STRING);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer, message string);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge stalled-zeromq-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 32MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT zeromq_main CONFIG {
        'addr' = '{{zeromq_peer}}',
        'timeout_ms' = '1000'
      };
      CREATE ZEROMQ PUSH EMITTER zeromq_out FROM notifications
        TO zeromq_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 2s
        ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-zeromq-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1,"message":"ready"}
      """
    Then the observed broker receives a payload
      """
      {"message":"ready","user_id":1}
      """
    When the stallable endpoint "zeromq_peer" is paused
    And http payload is posted to host "stalled-zeromq-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2,"message":"{{bulk_message}}"}
      """
    Then within "30s" DESCRIBE EMITTER "zeromq_out" on the leader node contains
      """
      sending a message to the ZeroMQ peer did not finish within timeout_ms 1s
      """
    # The emitter opens a new connection after the failed send and sends the message again once
    # the peer reads, and the connection the failed send left part of the message on ends unread.
    When the stallable endpoint "zeromq_peer" is resumed
    Then the observed broker receives a payload
      """
      "user_id":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A core NATS write hands its records to the client and flushes them to the client's socket. The
  # emitter publishes through an endpoint the scenario controls, which goes down: it ends the
  # client's connection and refuses every reconnection, so the client holds what the emitter hands
  # it and the write's flush cannot finish until the client is back.
  Scenario Outline: A NATS emitter whose server goes down is reported within timeout_ms
    Given NATS is running
    And a NATS endpoint the scenario controls is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And NATS subject "stalled_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge stalled-nats-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = '{{nats_controlled_addr}}',
        'timeout_ms' = '1000'
      };
      CREATE NATS SUBJECT EMITTER nats_out FROM notifications
        TO nats_main SUBJECT 'stalled_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "stalled-nats-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":1}
      """
    Then the observed broker receives a payload
      """
      {"user_id":1}
      """
    When the controlled NATS endpoint goes down
    Then the controlled NATS endpoint refuses a client that reconnects
    When http payload is posted to host "stalled-nats-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":2}
      """
    Then within "20s" DESCRIBE EMITTER "nats_out" on the leader node contains
      """
      did not finish within timeout_ms 1s
      """
    When the controlled NATS endpoint forwards everything
    Then the observed broker receives a payload
      """
      {"user_id":2}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
