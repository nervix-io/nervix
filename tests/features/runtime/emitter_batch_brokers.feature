Feature: Batching across broker and message emitters
  Kafka, Pulsar, RabbitMQ, Redis Pub/Sub, MQTT, NATS, ZeroMQ and SQS QUEUE emitters publish every batch
  payload as one external message through their own driver, in every publishing mode, and without
  the BATCH clause each record stays its own message. A batch message carries the key, headers and
  FIFO group its members share. An SQS request answers for each batch message it carries. A payload
  that fits MAX SIZE but not the destination's own limit is rejected with every member it carries:
  before it is written where the connector can read the limit, and on the broker's refusal where
  only the broker applies it, as with a Pulsar topic policy or RabbitMQ's max_message_size.

  @emitter_batch_brokers
  Scenario Outline: Every broker publishing mode carries one batch in one external message
    Given the "<target>" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<target>" emission target "<destination>" is observed
    When these Plexus commands are executed on the leader node
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
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <client>
      CREATE <emitter_kind> EMITTER unbatched FROM events
        TO <connector_body>
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE <emitter_kind> EMITTER batched FROM events
        TO <connector_body>
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
      | cluster_size | target         | destination          | client                                                                                                                                                                   | connector_body                                                                                                                              | emitter_kind   |
      | 1            | Kafka          | batched_{{test_id}}  | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                                                                                           | emitter TOPIC 'batched_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                            | KAFKA TOPIC    |
      | 3            | Kafka          | batched_{{test_id}}  | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                                                                                           | emitter TOPIC 'batched_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                    | KAFKA TOPIC    |
      | 1            | Kafka          | batched_{{test_id}}  | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                                                                                           | emitter TOPIC 'batched_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                | KAFKA TOPIC    |
      | 3            | Pulsar         | batched_{{test_id}}  | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };                                                                                                      | emitter TOPIC 'batched_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                            | PULSAR TOPIC   |
      | 1            | Pulsar         | batched_{{test_id}}  | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };                                                                                                      | emitter TOPIC 'batched_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                    | PULSAR TOPIC   |
      | 3            | Pulsar         | batched_{{test_id}}  | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };                                                                                                      | emitter TOPIC 'batched_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                | PULSAR TOPIC   |
      | 1            | RabbitMQ       | batched_{{test_id}}  | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };                                                                                                  | emitter QUEUE 'batched_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                            | RABBITMQ QUEUE |
      | 3            | RabbitMQ       | batched_{{test_id}}  | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };                                                                                                  | emitter QUEUE 'batched_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                    | RABBITMQ QUEUE |
      | 1            | RabbitMQ       | batched_{{test_id}}  | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };                                                                                                  | emitter QUEUE 'batched_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                | RABBITMQ QUEUE |
      | 1            | Redis          | batched_{{test_id}}  | CREATE REDIS CLIENT emitter POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{redis_addr}}' };                                                                                  | emitter CHANNEL 'batched_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                          | REDIS PUBSUB   |
      | 3            | Redis          | batched_{{test_id}}  | CREATE REDIS CLIENT emitter POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = '{{redis_addr}}' };                                                                                  | emitter CHANNEL 'batched_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                          | REDIS PUBSUB   |
      | 3            | MQTT           | batched_{{test_id}}  | CREATE MQTT CLIENT emitter CONFIG { 'addr' = '{{mqtt_addr}}' };                                                                                                          | emitter TOPIC 'batched_{{test_id}}' MODE QOS 0 RETRY POLICY BACKOFF 50ms MAX 1s                                                             | MQTT TOPIC     |
      | 1            | MQTT           | batched_{{test_id}}  | CREATE MQTT CLIENT emitter CONFIG { 'addr' = '{{mqtt_addr}}' };                                                                                                          | emitter TOPIC 'batched_{{test_id}}' MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                              | MQTT TOPIC     |
      | 3            | MQTT           | batched_{{test_id}}  | CREATE MQTT CLIENT emitter CONFIG { 'addr' = '{{mqtt_addr}}' };                                                                                                          | emitter TOPIC 'batched_{{test_id}}' MODE QOS 2 ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                          | MQTT TOPIC     |
      | 1            | NATS           | batched_{{test_id}}  | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                                                                                                          | emitter SUBJECT 'batched_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                          | NATS SUBJECT   |
      | 3            | NATS JetStream | batched_{{test_id}}  | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                                                                                                          | emitter STREAM 'batched_{{test_id}}' SUBJECT 'batched_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s     | NATS STREAM    |
      | 1            | NATS JetStream | batched_{{test_id}}  | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                                                                                                          | emitter STREAM 'batched_{{test_id}}' SUBJECT 'batched_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s | NATS STREAM    |
      | 1            | ZeroMQ         | {{zeromq_emit_addr}} | CREATE ZEROMQ CLIENT emitter CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = 'false' };                                                                               | emitter MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                                                        | ZEROMQ PUSH    |
      | 3            | ZeroMQ         | {{zeromq_emit_addr}} | CREATE ZEROMQ CLIENT emitter CONFIG { 'addr' = '{{zeromq_emit_addr}}', 'bind' = 'false' };                                                                               | emitter MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                                                        | ZEROMQ PUSH    |
      | 3            | SQS            | batched_{{test_id}}  | CREATE SQS CLIENT emitter CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' }; | emitter QUEUE 'batched_{{test_id}}' SEND SINGLE MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                   | SQS QUEUE      |
      | 1            | SQS            | batched_{{test_id}}  | CREATE SQS CLIENT emitter CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' }; | emitter QUEUE 'batched_{{test_id}}' SEND BATCH MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                    | SQS QUEUE      |

  @emitter_batch_brokers
  Scenario Outline: A batch message carries the key, headers and FIFO group its members share
    Given the "<target>" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<target>" emission target "<destination>" is observed
    When these Plexus commands are executed on the leader node
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
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
          INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <client>
      CREATE <emitter_kind> EMITTER keyed FROM events
        TO <connector_body>
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
      | cluster_size | target   | destination            | client                                                                                                                                                                   | connector_body                                                                                                      | acme_key          | beta_key          | acme_group        | beta_group        | emitter_kind   |
      | 1            | Kafka    | keyed_{{test_id}}      | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                                                                                           | emitter TOPIC 'keyed_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s              | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   | KAFKA TOPIC    |
      | 3            | Kafka    | keyed_{{test_id}}      | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };                                                                                           | emitter TOPIC 'keyed_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                      | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   | KAFKA TOPIC    |
      | 1            | Pulsar   | keyed_{{test_id}}      | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };                                                                                                      | emitter TOPIC 'keyed_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                      | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   | PULSAR TOPIC   |
      | 3            | Pulsar   | keyed_{{test_id}}      | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };                                                                                                      | emitter TOPIC 'keyed_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s              | {"tenant":"acme"} | {"tenant":"beta"} |                   |                   | PULSAR TOPIC   |
      | 1            | RabbitMQ | keyed_{{test_id}}      | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };                                                                                                  | emitter QUEUE 'keyed_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s              |                   |                   |                   |                   | RABBITMQ QUEUE |
      | 3            | RabbitMQ | keyed_{{test_id}}      | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };                                                                                                  | emitter QUEUE 'keyed_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                      |                   |                   |                   |                   | RABBITMQ QUEUE |
      | 1            | NATS     | keyed_{{test_id}}      | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                                                                                                          | emitter SUBJECT 'keyed_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                    |                   |                   |                   |                   | NATS SUBJECT   |
      | 3            | NATS     | keyed_{{test_id}}      | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                                                                                                          | emitter SUBJECT 'keyed_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                    |                   |                   |                   |                   | NATS SUBJECT   |
      | 1            | SQS      | keyed_{{test_id}}      | CREATE SQS CLIENT emitter CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' }; | emitter QUEUE 'keyed_{{test_id}}' SEND BATCH MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s                              |                   |                   |                   |                   | SQS QUEUE      |
      | 3            | SQS      | keyed_{{test_id}}      | CREATE SQS CLIENT emitter CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' }; | emitter QUEUE 'keyed_{{test_id}}' SEND SINGLE MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s                             |                   |                   |                   |                   | SQS QUEUE      |
      | 1            | SQS      | keyed_{{test_id}}.fifo | CREATE SQS CLIENT emitter CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' }; | emitter QUEUE 'keyed_{{test_id}}.fifo' FIFO GROUP FROM BRANCH SEND SINGLE MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s |                   |                   | {"tenant":"acme"} | {"tenant":"beta"} | SQS QUEUE      |
      | 3            | SQS      | keyed_{{test_id}}.fifo | CREATE SQS CLIENT emitter CONFIG { 'endpoint' = '{{sqs_endpoint}}', 'region' = 'us-east-1', 'access_key_id' = 'scenario-key', 'secret_access_key' = 'scenario-secret' }; | emitter QUEUE 'keyed_{{test_id}}.fifo' FIFO GROUP FROM BRANCH SEND BATCH MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s  |                   |                   | {"tenant":"acme"} | {"tenant":"beta"} | SQS QUEUE      |

  @emitter_batch_brokers @sqs_batch_request_answers
  Scenario Outline: An SQS batch request answers for every batch message it carries
    Given the "SQS" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "SQS" emission target "answered_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
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
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS CLIENT emitter CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER answered FROM events
        TO emitter QUEUE 'answered_{{test_id}}'
          SEND BATCH MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s
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
    When the emitter of emitter "answered" stalls after resolving 2 records of its next publish
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
  Scenario Outline: A batch within MAX SIZE that the destination cannot carry is rejected with every member
    Given the "<target>" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "<target>" emission target "limited_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
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
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      <client>
      CREATE <emitter_kind> EMITTER limited FROM events
        TO <connector_body>
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
    # MAX SIZE 2MiB but not the 1 MB the destination accepts. Kafka, MQTT, NATS and Pulsar check that
    # limit against everything they write around the payload before they write anything. A RabbitMQ
    # client cannot read its broker's max_message_size, so the broker refuses the message by closing
    # the channel, and the connector rejects the message on that answer and publishes the rest on a
    # new channel. Either way every member of the message is rejected with one reference and
    # publishing continues: the records after them still leave in their own message, exactly once.
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
      | cluster_size | target         | client                                                                         | connector_body                                                                                                                          | limit_message                         | emitter_kind   |
      | 1            | Kafka          | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' }; | emitter TOPIC 'limited_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                | Message size too large                | KAFKA TOPIC    |
      | 3            | Kafka          | CREATE KAFKA CLIENT emitter CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' }; | emitter TOPIC 'limited_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                        | Message size too large                | KAFKA TOPIC    |
      | 3            | MQTT           | CREATE MQTT CLIENT emitter CONFIG { 'addr' = '{{mqtt_addr}}' };                | emitter TOPIC 'limited_{{test_id}}' MODE QOS 0 RETRY POLICY BACKOFF 50ms MAX 1s                                                         | maximum packet size of 1048576 bytes  | MQTT TOPIC     |
      | 1            | MQTT           | CREATE MQTT CLIENT emitter CONFIG { 'addr' = '{{mqtt_addr}}' };                | emitter TOPIC 'limited_{{test_id}}' MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                          | maximum packet size of 1048576 bytes  | MQTT TOPIC     |
      | 1            | NATS           | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                | emitter SUBJECT 'limited_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                      | max payload size exceeded             | NATS SUBJECT   |
      | 3            | NATS JetStream | CREATE NATS CLIENT emitter CONFIG { 'addr' = '{{nats_addr}}' };                | emitter STREAM 'limited_{{test_id}}' SUBJECT 'limited_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s | max payload size exceeded             | NATS STREAM    |
      | 1            | Pulsar         | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };            | emitter TOPIC 'limited_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                | maximum message size of 1048576 bytes | PULSAR TOPIC   |
      | 3            | Pulsar         | CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };            | emitter TOPIC 'limited_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                        | maximum message size of 1048576 bytes | PULSAR TOPIC   |
      | 1            | RabbitMQ       | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };        | emitter QUEUE 'limited_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                                | max_message_size of 1048576 bytes     | RABBITMQ QUEUE |
      | 3            | RabbitMQ       | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };        | emitter QUEUE 'limited_{{test_id}}' MODE ACK PARALLEL MAX 2 ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s                            | max_message_size of 1048576 bytes     | RABBITMQ QUEUE |
      | 1            | RabbitMQ       | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };        | emitter QUEUE 'limited_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                        | max_message_size of 1048576 bytes     | RABBITMQ QUEUE |
      | 3            | RabbitMQ       | CREATE RABBITMQ CLIENT emitter CONFIG { 'addr' = '{{rabbitmq_addr}}' };        | emitter QUEUE 'limited_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s                                                        | max_message_size of 1048576 bytes     | RABBITMQ QUEUE |

  @emitter_batch_brokers @emitter_batch_destination_limits
  Scenario Outline: A Pulsar batch message larger than its topic allows is rejected with every member
    Given the "Pulsar" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "Pulsar" emission target "policed_{{test_id}}" is observed
    And Pulsar topic "policed_{{test_id}}" accepts messages of at most 1000 bytes
    When these Plexus commands are executed on the leader node
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
      CREATE VHOST edge policed-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT emitter CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC EMITTER policed FROM events
        TO emitter TOPIC 'policed_{{test_id}}'
          MODE <publishing_mode> RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL EXCEPT note
        SET note = repeat(input.note, 200)
        BATCH MAX MESSAGES 3 MAX SIZE 64KiB
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
    # Each of the first three records grows to about 420 bytes, so their message of about 1.3 KB
    # fits MAX SIZE and the maximum the broker announces, but not the 1000 bytes the topic's policy
    # allows. Only the broker applies that policy, so the message is written, and the broker's
    # refusal rejects every member with one reference. The records after them still leave in their
    # own message.
    And http payload is posted to host "policed-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"xy"},{"seq":2,"note":"xy"},{"seq":3,"note":"xy"},{"seq":4,"note":""},{"seq":5,"note":""}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments that share one "error_reference"
      """
      "seq":1 | "error_code":"external" | "operation":"publish" | Exceed maximum message size
      "seq":2 | "error_code":"external" | "operation":"publish" | Exceed maximum message size
      "seq":3 | "error_code":"external" | "operation":"publish" | Exceed maximum message size
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":4,"note":""},{"seq":5,"note":""}]
      """

    # Only a confirming mode waits for the broker's answer; NO ACK completes when the producer
    # accepts the message.
    Examples:
      | cluster_size | publishing_mode                    |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 30s     |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 30s |

  @emitter_batch_brokers @emitter_batch_destination_limits
  Scenario Outline: An SQS batch message counts its attributes against the service limit
    Given the "SQS" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "SQS" emission target "attributed_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
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
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS CLIENT emitter CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER attributed FROM events
        TO emitter QUEUE 'attributed_{{test_id}}'
          SEND <publishing_mode> MODE ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL EXCEPT note
        SET note = repeat(input.note, 1048555)
        INVOKE write_header('source', 'web')
        BATCH MAX MESSAGES 3 MAX SIZE 1MiB
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
    # Record 1 alone fills MAX SIZE 1MiB to the byte, so the emitter builds its message, but its
    # source attribute takes the message past the 1 MiB SQS allows a body and its attributes
    # together. The emitter rejects it before sending anything, and record 2 still leaves.
    And http payload is posted to host "attributed-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"x"},{"seq":2,"note":""}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "error_code":"external" | "operation":"publish" | SQS message is 1048591 bytes
      """
    And within "30s" the observed broker receives exactly these messages
      | payload               | key | headers    | group |
      | [{"seq":2,"note":""}] |     | source=web |       |

    Examples:
      | cluster_size | publishing_mode |
      | 1            | SINGLE          |
      | 3            | BATCH           |
