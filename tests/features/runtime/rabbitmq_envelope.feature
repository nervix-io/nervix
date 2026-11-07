Feature: RabbitMQ emitter properties follow the declared envelope

  Scenario Outline: Both RabbitMQ emitter entities write every AMQP property and retain it across a retry
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ exchange "envelope_{{test_id}}" of type direct exists
    And RabbitMQ queue "envelope_{{test_id}}" is bound to exchange "envelope_{{test_id}}" with routing key "route"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge envelope-{{test_id}}.example.com;
      CREATE ENDPOINT incoming ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR incoming FROM incoming MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ <entity> EMITTER outgoing FROM events
        TO broker <destination> MODE <mode> RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING event_codec PERSISTENT MANDATORY
        TIMESTAMP from_unix('second', 1700000000)
        PROPERTIES {
          'content_type' = 'application/json',
          'content_encoding' = 'utf-8',
          'message_id' = concat('message-', (input.seq AS STRING)),
          'correlation_id' = 'correlation', 'reply_to' = 'replies',
          'expiration' = '600000', 'priority' = (7 AS U8), 'type' = 'event',
          'app_id' = 'nervix', 'user_id' = 'guest'
        }
        INHERIT ALL
        INVOKE write_header(CASE WHEN input.seq > 0 THEN 'content_type' ELSE 'other' END, 'first'),
               write_header(CASE WHEN input.seq > 0 THEN 'content_type' ELSE 'other' END, 'separate-header')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And emitter "outgoing" enters stall mode
    And http payload is posted to host "envelope-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "30s" DESCRIBE EMITTER "outgoing" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    When emitter "outgoing" leaves fault mode
    Then RabbitMQ queue "envelope_{{test_id}}" yields this envelope message
      """
      {"payload":{"seq":1},"properties":{"content_type":"application/json","content_encoding":"utf-8","message_id":"message-1","correlation_id":"correlation","reply_to":"replies","expiration":"600000","priority":7,"type":"event","app_id":"nervix","user_id":"guest","timestamp":1700000000,"delivery_mode":2,"headers":{"content_type":"separate-header"}}}
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      PERSISTENT
      """
    And the last command output contains
      """
      PROPERTIES
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER outgoing;
      """
    Then the last command output contains
      """
      envelope: PERSISTENT MANDATORY TIMESTAMP
      """
    And the last command output contains
      """
      'message_id' = concat
      """

    When these Plexus commands are executed on the leader node
      """
      ALTER RABBITMQ <entity> EMITTER outgoing DROP PERSISTENT, DROP TIMESTAMP, DROP PROPERTIES;
      """
    And http payload is posted to host "envelope-{{test_id}}.example.com" path "/events"
      """
      {"seq":2}
      """
    Then RabbitMQ queue "envelope_{{test_id}}" yields this envelope message
      """
      {"payload":{"seq":2},"properties":{"content_type":null,"content_encoding":null,"message_id":null,"correlation_id":null,"reply_to":null,"expiration":null,"priority":null,"type":null,"app_id":null,"user_id":null,"timestamp":null,"delivery_mode":null,"headers":{"content_type":"separate-header"}}}
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER RABBITMQ <entity> EMITTER outgoing SET PERSISTENT, SET TIMESTAMP from_unix('second', 0), SET PROPERTIES { 'priority' = 9 AS U8 };
      """
    And http payload is posted to host "envelope-{{test_id}}.example.com" path "/events"
      """
      {"seq":3}
      """
    Then RabbitMQ queue "envelope_{{test_id}}" yields this envelope message
      """
      {"payload":{"seq":3},"properties":{"content_type":null,"content_encoding":null,"message_id":null,"correlation_id":null,"reply_to":null,"expiration":null,"priority":9,"type":null,"app_id":null,"user_id":null,"timestamp":0,"delivery_mode":2,"headers":{"content_type":"separate-header"}}}
      """

    Examples:
      | cluster_size | entity   | destination                                         | mode                               |
      | 1            | QUEUE    | QUEUE 'envelope_{{test_id}}'                        | NO ACK                             |
      | 3            | QUEUE    | QUEUE 'envelope_{{test_id}}'                        | ACK SEQUENTIAL ACK TIMEOUT 30s     |
      | 1            | EXCHANGE | EXCHANGE 'envelope_{{test_id}}' ROUTING KEY 'route' | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |
      | 3            | EXCHANGE | EXCHANGE 'envelope_{{test_id}}' ROUTING KEY 'route' | NO ACK                             |

  Scenario Outline: A persistent RabbitMQ message survives its broker restart after Nervix stops
    Given a scenario-owned RabbitMQ broker is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And durable RabbitMQ queue "persistent_{{test_id}}" exists
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge persistent-{{test_id}}.example.com;
      CREATE ENDPOINT incoming ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR incoming FROM incoming MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{envelope_broker_addr}}' };
      CREATE RABBITMQ QUEUE EMITTER outgoing FROM events
        TO broker QUEUE 'persistent_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING event_codec PERSISTENT INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "persistent-{{test_id}}.example.com" path "/events"
      """
      {"seq":42}
      """
    Then RabbitMQ envelope queue "persistent_{{test_id}}" holds 1 ready messages
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    And the scenario-owned RabbitMQ broker is restarted
    Then RabbitMQ queue "persistent_{{test_id}}" yields this envelope message
      """
      {"payload":{"seq":42},"properties":{"content_type":null,"content_encoding":null,"message_id":null,"correlation_id":null,"reply_to":null,"expiration":null,"priority":null,"type":null,"app_id":null,"user_id":null,"timestamp":null,"delivery_mode":2,"headers":{}}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: RabbitMQ property types are refused before the emitter is created
    Given RabbitMQ is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      """
    When these Plexus commands fail with "requires an exact, non-null, non-sensitive <type> value"
      """
      CREATE RABBITMQ QUEUE EMITTER refused FROM events TO broker QUEUE 'unused'
        MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s ENCODE USING event_codec
        PROPERTIES { '<property>' = <value> }
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | property     | value     | type   |
      | 1            | priority     | 7         | U8     |
      | 3            | priority     | 7         | U8     |
      | 1            | content_type | input.seq | STRING |
      | 3            | content_type | input.seq | STRING |

  Scenario Outline: RabbitMQ refuses unrepresentable timestamps per record and publishes its neighbours
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And RabbitMQ queue "time_{{test_id}}" exists
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA rejected ( seq I64, error_code STRING, operation STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected UNBRANCHED;
      CREATE VHOST edge time-{{test_id}}.example.com;
      CREATE ENDPOINT incoming ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR incoming FROM incoming MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ QUEUE EMITTER outgoing FROM events TO broker QUEUE 'time_{{test_id}}'
        MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING event_codec TIMESTAMP from_unix('nanosecond', input.seq)
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq, error_code = error.code, operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_subscription TO rejected_events;
      START;
      """
    When http payload is posted to host "time-{{test_id}}.example.com" path "/events"
      """
      {"seq":-1}
      """
    And http payload is posted to host "time-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    And http payload is posted to host "time-{{test_id}}.example.com" path "/events"
      """
      {"seq":1000000000}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":-1 | "error_code":"external" | "operation":"publish"
      "seq":1 | "error_code":"external" | "operation":"publish"
      """
    And RabbitMQ queue "time_{{test_id}}" yields this envelope message
      """
      {"payload":{"seq":1000000000},"properties":{"content_type":null,"content_encoding":null,"message_id":null,"correlation_id":null,"reply_to":null,"expiration":null,"priority":null,"type":null,"app_id":null,"user_id":null,"timestamp":1,"delivery_mode":null,"headers":{}}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
