Feature: Batching across broker and message emitters
  Kafka, Pulsar, RabbitMQ, Redis Pub/Sub, MQTT, NATS, ZeroMQ and SQS emitters publish every batch
  payload as one external message through their own driver, in every publishing mode, and without
  the BATCH clause each record stays its own message. A batch message carries the key, headers and
  FIFO group its members share. An SQS request answers for each batch message it carries. A payload
  that fits MAX SIZE but not a limit the destination declares is rejected, with every member it
  carries, before it is written.

  @emitter_batch_brokers
  Scenario Outline: Every broker publishing mode carries one batch in one external message
    Given the "<target>" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<target>" emission target "<destination>" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge brokers-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER unbatched FROM events
        TO <sink>
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER batched FROM events
        TO <sink>
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 3 MAX SIZE 80B
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
    # One post decodes into one Arrow carrier that both emitters receive. The emitter without the
    # clause publishes every record as its own message. The batching emitter fills its first
    # message to MAX MESSAGES 3, seals the second at exactly MAX SIZE 80B because a third member
    # would pass it, keeps the one-element array shape for the member left over and for the record
    # after the one that alone exceeds MAX SIZE, and rejects that record.
    And http payload is posted to host "brokers-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"},{"seq":4,"note":"dddddddddddddddddddd"},{"seq":5,"note":"eeeeeeeeeeeeeeeeeee"},{"seq":6,"note":"f"},{"seq":7,"note":"gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"},{"seq":8,"note":"h"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"seq":1,"note":"a"}
      {"seq":2,"note":"b"}
      {"seq":3,"note":"c"}
      {"seq":4,"note":"dddddddddddddddddddd"}
      {"seq":5,"note":"eeeeeeeeeeeeeeeeeee"}
      {"seq":6,"note":"f"}
      {"seq":7,"note":"gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"}
      {"seq":8,"note":"h"}
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"}]
      [{"seq":4,"note":"dddddddddddddddddddd"},{"seq":5,"note":"eeeeeeeeeeeeeeeeeee"}]
      [{"seq":6,"note":"f"}]
      [{"seq":8,"note":"h"}]
      """
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      "seq":7 | "error_code":"validation" | "operation":"encode" | exceeds MAX SIZE 80B
      """

    Examples:
      | cluster_size | target         | destination          | client                                                                                          | sink                                                                                                                     |
      | 1            | Kafka          | batched_{{test_id}}  | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                | KAFKA sink TOPIC batched_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                        |
      | 3            | Kafka          | batched_{{test_id}}  | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                | KAFKA sink TOPIC batched_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                |
      | 1            | Kafka          | batched_{{test_id}}  | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                | KAFKA sink TOPIC batched_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s            |
      | 3            | Pulsar         | batched_{{test_id}}  | CREATE CLIENT sink TYPE PULSAR CONFIG { 'addr' = '{{pulsar_addr}}' };                           | PULSAR sink TOPIC batched_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                       |
      | 1            | Pulsar         | batched_{{test_id}}  | CREATE CLIENT sink TYPE PULSAR CONFIG { 'addr' = '{{pulsar_addr}}' };                           | PULSAR sink TOPIC batched_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s               |
      | 3            | Pulsar         | batched_{{test_id}}  | CREATE CLIENT sink TYPE PULSAR CONFIG { 'addr' = '{{pulsar_addr}}' };                           | PULSAR sink TOPIC batched_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s           |
      | 1            | RabbitMQ       | batched_{{test_id}}  | CREATE CLIENT sink TYPE RABBITMQ CONFIG { 'addr' = '{{rabbitmq_addr}}' };                       | RABBITMQ sink QUEUE batched_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                     |
      | 3            | RabbitMQ       | batched_{{test_id}}  | CREATE CLIENT sink TYPE RABBITMQ CONFIG { 'addr' = '{{rabbitmq_addr}}' };                       | RABBITMQ sink QUEUE batched_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s             |
      | 1            | RabbitMQ       | batched_{{test_id}}  | CREATE CLIENT sink TYPE RABBITMQ CONFIG { 'addr' = '{{rabbitmq_addr}}' };                       | RABBITMQ sink QUEUE batched_{{test_id}} MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s         |
      | 1            | Redis          | batched_{{test_id}}  | CREATE CLIENT sink TYPE REDIS POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{redis_addr}}' };       | REDIS PUBSUB sink CHANNEL batched_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                               |
      | 3            | Redis          | batched_{{test_id}}  | CREATE CLIENT sink TYPE REDIS POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{redis_addr}}' };       | REDIS PUBSUB sink CHANNEL batched_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                               |
      | 3            | MQTT           | batched_{{test_id}}  | CREATE CLIENT sink TYPE MQTT CONFIG { 'addr' = '{{mqtt_addr}}' };                               | MQTT sink TOPIC batched_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 50ms MAX 1s                                          |
      | 1            | MQTT           | batched_{{test_id}}  | CREATE CLIENT sink TYPE MQTT CONFIG { 'addr' = '{{mqtt_addr}}' };                               | MQTT sink TOPIC batched_{{test_id}} MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s           |
      | 3            | MQTT           | batched_{{test_id}}  | CREATE CLIENT sink TYPE MQTT CONFIG { 'addr' = '{{mqtt_addr}}' };                               | MQTT sink TOPIC batched_{{test_id}} MODE QOS 2 ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s       |
      | 1            | NATS           | batched_{{test_id}}  | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                               | NATS sink SUBJECT batched_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                       |
      | 3            | NATS JetStream | batched_{{test_id}}  | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                               | NATS sink SUBJECT batched_{{test_id}} MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s     |
      | 1            | NATS JetStream | batched_{{test_id}}  | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                               | NATS sink SUBJECT batched_{{test_id}} MODE JETSTREAM ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s |
      | 1            | ZeroMQ         | {{zeromq_emit_addr}} | CREATE CLIENT sink TYPE ZEROMQ CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = 'false' };    | ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                                 |
      | 3            | ZeroMQ         | {{zeromq_emit_addr}} | CREATE CLIENT sink TYPE ZEROMQ CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = 'false' };    | ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                                 |
      | 3            | SQS            | batched_{{test_id}}  | CREATE CLIENT sink TYPE SQS CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1' }; | SQS sink QUEUE batched_{{test_id}} MODE SINGLE RETRY POLICY BACKOFF 50ms MAX 1s                                          |
      | 1            | SQS            | batched_{{test_id}}  | CREATE CLIENT sink TYPE SQS CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1' }; | SQS sink QUEUE batched_{{test_id}} MODE BATCH RETRY POLICY BACKOFF 50ms MAX 1s                                           |

  @emitter_batch_brokers
  Scenario Outline: A batch message carries the key, headers and FIFO group its members share
    Given the "<target>" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<target>" emission target "<destination>" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING, source STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        seq integer, tenant string, source string
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge keyed-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
          INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER keyed FROM events
        TO <sink>
          ENCODE USING event_codec
        INHERIT ALL
        INVOKE write_header('source', output.source)
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # The two tenants arrive interleaved and leave in separate branch carriers. Within a tenant a
    # different source header seals the open message, so no message carries a header, key or group
    # that is untrue for one of its members, and no record is reordered around the boundary.
    And http payload is posted to host "keyed-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tenant":"acme","source":"web"},{"seq":2,"tenant":"beta","source":"web"},{"seq":3,"tenant":"acme","source":"web"},{"seq":4,"tenant":"beta","source":"web"},{"seq":5,"tenant":"acme","source":"app"},{"seq":6,"tenant":"acme","source":"web"}]
      """
    Then within "30s" the observed broker receives exactly these messages
      | payload                                                                             | key        | headers    | group        |
      | [{"seq":1,"tenant":"acme","source":"web"},{"seq":3,"tenant":"acme","source":"web"}] | <acme_key> | source=web | <acme_group> |
      | [{"seq":5,"tenant":"acme","source":"app"}]                                          | <acme_key> | source=app | <acme_group> |
      | [{"seq":6,"tenant":"acme","source":"web"}]                                          | <acme_key> | source=web | <acme_group> |
      | [{"seq":2,"tenant":"beta","source":"web"},{"seq":4,"tenant":"beta","source":"web"}] | <beta_key> | source=web | <beta_group> |

    Examples:
      | cluster_size | target   | destination            | client                                                                                          | sink                                                                                                       | acme_key          | beta_key          | acme_group        | beta_group        |
      | 1            | Kafka    | keyed_{{test_id}}      | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                | KAFKA sink TOPIC keyed_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s    | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   |
      | 3            | Kafka    | keyed_{{test_id}}      | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                | KAFKA sink TOPIC keyed_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                            | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   |
      | 1            | Pulsar   | keyed_{{test_id}}      | CREATE CLIENT sink TYPE PULSAR CONFIG { 'addr' = '{{pulsar_addr}}' };                           | PULSAR sink TOPIC keyed_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                           | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   |
      | 3            | Pulsar   | keyed_{{test_id}}      | CREATE CLIENT sink TYPE PULSAR CONFIG { 'addr' = '{{pulsar_addr}}' };                           | PULSAR sink TOPIC keyed_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s   | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   |
      | 1            | RabbitMQ | keyed_{{test_id}}      | CREATE CLIENT sink TYPE RABBITMQ CONFIG { 'addr' = '{{rabbitmq_addr}}' };                       | RABBITMQ sink QUEUE keyed_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s |                   |                   |                   |                   |
      | 3            | RabbitMQ | keyed_{{test_id}}      | CREATE CLIENT sink TYPE RABBITMQ CONFIG { 'addr' = '{{rabbitmq_addr}}' };                       | RABBITMQ sink QUEUE keyed_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                         |                   |                   |                   |                   |
      | 1            | NATS     | keyed_{{test_id}}      | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                               | NATS sink SUBJECT keyed_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                           |                   |                   |                   |                   |
      | 3            | NATS     | keyed_{{test_id}}      | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                               | NATS sink SUBJECT keyed_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                           |                   |                   |                   |                   |
      | 1            | SQS      | keyed_{{test_id}}      | CREATE CLIENT sink TYPE SQS CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1' }; | SQS sink QUEUE keyed_{{test_id}} MODE BATCH RETRY POLICY BACKOFF 50ms MAX 1s                               |                   |                   |                   |                   |
      | 3            | SQS      | keyed_{{test_id}}      | CREATE CLIENT sink TYPE SQS CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1' }; | SQS sink QUEUE keyed_{{test_id}} MODE SINGLE RETRY POLICY BACKOFF 50ms MAX 1s                              |                   |                   |                   |                   |
      | 1            | SQS      | keyed_{{test_id}}.fifo | CREATE CLIENT sink TYPE SQS CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1' }; | SQS sink QUEUE keyed_{{test_id}}.fifo FIFO GROUP FROM BRANCH MODE SINGLE RETRY POLICY BACKOFF 50ms MAX 1s  |                   |                   | {"tenant":"acme"} | {"tenant":"beta"} |
      | 3            | SQS      | keyed_{{test_id}}.fifo | CREATE CLIENT sink TYPE SQS CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1' }; | SQS sink QUEUE keyed_{{test_id}}.fifo FIFO GROUP FROM BRANCH MODE BATCH RETRY POLICY BACKOFF 50ms MAX 1s   |                   |                   | {"tenant":"acme"} | {"tenant":"beta"} |

  @emitter_batch_brokers @sqs_batch_request_answers
  Scenario Outline: An SQS batch request answers for every batch message it carries
    Given the "SQS" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "SQS" emission target "answered_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_reference STRING,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, tenant string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge answered-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink TYPE SQS CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE EMITTER answered FROM events
        TO SQS sink QUEUE answered_{{test_id}}
          MODE BATCH RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        INVOKE write_header('tenant', output.tenant)
        BATCH MAX MESSAGES 2 MAX SIZE 1KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_reference = error.reference,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    # The tenant header divides the records into three messages of two. SQS refuses an empty
    # message attribute, so the message for the empty tenant is rejected with both its members
    # before the request is sent, and the request carries the other two as separate entries. The
    # emitter then learns the answers for the first two messages only: the acme entry and the
    # rejected message. It writes the unanswered beta message again, byte for byte, and never the
    # acme message or the rejected one.
    When the sink of emitter "answered" stalls after resolving 2 records of its next publish
    And http payload is posted to host "answered-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"},{"seq":3,"tenant":""},{"seq":4,"tenant":""},{"seq":5,"tenant":"beta"},{"seq":6,"tenant":"beta"}]
      """
    Then within "30s" the observed broker receives exactly these messages
      | payload                                               | key | headers     | group |
      | [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"}] |     | tenant=acme |       |
      | [{"seq":5,"tenant":"beta"},{"seq":6,"tenant":"beta"}] |     | tenant=beta |       |
      | [{"seq":5,"tenant":"beta"},{"seq":6,"tenant":"beta"}] |     | tenant=beta |       |
    And within "10s" the relay subscription receives payloads containing all fragments that share one "error_reference"
      """
      "seq":3 | "error_code":"external" | "operation":"publish" | attribute 'tenant' has an empty value
      "seq":4 | "error_code":"external" | "operation":"publish" | attribute 'tenant' has an empty value
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_brokers @emitter_batch_destination_limits
  Scenario Outline: A batch within MAX SIZE that the destination cannot carry is rejected before it is written
    Given the "<target>" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<target>" emission target "limited_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_reference STRING,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge limited-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <client>
      CREATE EMITTER limited FROM events
        TO <sink>
          ENCODE USING event_codec
        INHERIT ALL EXCEPT note
        SET note = repeat(input.note, 200000)
        BATCH MAX MESSAGES 3 MAX SIZE 2MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_reference = error.reference,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    # Each of the first three records grows to about 400 KB, so their message of about 1.2 MB fits
    # MAX SIZE 2MiB but not the 1 MB the destination accepts. The connector checks that limit
    # against everything it writes around the payload before it writes anything, rejects every
    # member of the message with one reference, and keeps publishing: the records after them still
    # leave in their own message.
    And http payload is posted to host "limited-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"xy"},{"seq":2,"note":"xy"},{"seq":3,"note":"xy"},{"seq":4,"note":""},{"seq":5,"note":""}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments that share one "error_reference"
      """
      "seq":1 | "error_code":"external" | "operation":"publish" | <limit_message>
      "seq":2 | "error_code":"external" | "operation":"publish" | <limit_message>
      "seq":3 | "error_code":"external" | "operation":"publish" | <limit_message>
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":4,"note":""},{"seq":5,"note":""}]
      """

    Examples:
      | cluster_size | target         | client                                                                           | sink                                                                                                                 | limit_message                        |
      | 1            | Kafka          | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' }; | KAFKA sink TOPIC limited_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s            | Message size too large               |
      | 3            | Kafka          | CREATE CLIENT sink TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' }; | KAFKA sink TOPIC limited_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                    | Message size too large               |
      | 3            | MQTT           | CREATE CLIENT sink TYPE MQTT CONFIG { 'addr' = '{{mqtt_addr}}' };                | MQTT sink TOPIC limited_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 50ms MAX 1s                                      | maximum packet size of 1048576 bytes |
      | 1            | MQTT           | CREATE CLIENT sink TYPE MQTT CONFIG { 'addr' = '{{mqtt_addr}}' };                | MQTT sink TOPIC limited_{{test_id}} MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s       | maximum packet size of 1048576 bytes |
      | 1            | NATS           | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                | NATS sink SUBJECT limited_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s                                   | max payload size exceeded            |
      | 3            | NATS JetStream | CREATE CLIENT sink TYPE NATS CONFIG { 'addr' = '{{nats_addr}}' };                | NATS sink SUBJECT limited_{{test_id}} MODE JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s | max payload size exceeded            |

  @emitter_batch_brokers @emitter_batch_destination_limits
  Scenario Outline: An SQS batch message counts its attributes against the service limit
    Given the "SQS" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "SQS" emission target "attributed_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge attributed-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink TYPE SQS CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE EMITTER attributed FROM events
        TO SQS sink QUEUE attributed_{{test_id}}
          MODE <publishing_mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL EXCEPT note
        SET note = repeat(input.note, 262123)
        INVOKE write_header('source', 'web')
        BATCH MAX MESSAGES 3 MAX SIZE 256KiB
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
    # Record 1 alone fills MAX SIZE 256KiB to the byte, so the emitter builds its message, but its
    # source attribute takes the message past the 256 KiB SQS allows a body and its attributes
    # together. The emitter rejects it before sending anything, and record 2 still leaves.
    And http payload is posted to host "attributed-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"x"},{"seq":2,"note":""}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "error_code":"external" | "operation":"publish" | SQS message is 262159 bytes
      """
    And within "30s" the observed broker receives exactly these messages
      | payload               | key | headers    | group |
      | [{"seq":2,"note":""}] |     | source=web |       |

    Examples:
      | cluster_size | publishing_mode |
      | 1            | SINGLE          |
      | 3            | BATCH           |
