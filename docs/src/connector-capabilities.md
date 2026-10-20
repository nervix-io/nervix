# Connector Capabilities

This reference is generated from the current Plexus connector declarations. Regenerate it with `just connector-matrix`; `just validate-connector-matrix` rejects drift in this chapter and the generated manual regions.

The tables describe the implemented surface. A connector Epic can extend a declaration and regenerate these tables in the same change. Modes below are complete bodies after `MODE`; options retain their declared order, exact value types, presence and defaults. Locator rule names identify the vocabulary's validator; see [External Names](ingestors.md#external-names).

Ingest and emit are separate contracts: consuming a broker position and publishing a record need different acknowledgements. Each mirror column compares every declared entity of the same connector on the other side and explains transport differences. An absent capability requires reviewed explanatory prose; stale or missing explanations fail validation.

Client declarations currently expose pooling, statement options and CONFIG ownership. They do not yet enumerate CONFIG keys, so these tables do not imply that an arbitrary Nervix-owned key is accepted. Read the linked entity manual and [client pool reference](database-client-pools.md) for those keys. Driver-owned CONFIG remains intentional pass-through.

## Ingest

### CLIENT PRODUCER ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | CLIENT PRODUCER | Compare `CLIENT CONSUMER`: modes. Native Arrow batches cross an application session; producers submit batches and consumers explicitly acknowledge delivery attempts, with no broker envelope. |
| Reference | SCHEMA &lt;schema&gt; |  |
| Locator and validation rule | Not offered: The body references the native schema; there is no external topic, queue or table to locate. |  |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Native Arrow batches cross an application session; producers submit batches and consumers explicitly acknowledge delivery attempts, with no broker envelope. |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: Application batches carry their declared schema; there is no separate transport metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### HTTP REQUEST ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | HTTP REQUEST | Compare `HTTP REQUEST`: options, modes. Polling and emitting send requests while an endpoint accepts requests. Response/request headers are transport headers; server endpoints run on every node and have no remote source position. |
| Reference | HTTP client |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `EVERY 1s` (duration); required |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |  |
| Metadata | Not offered: HTTP headers are available through header functions; this implementation declares no typed request or response metadata fields. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### HTTP ENDPOINT ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | HTTP ENDPOINT | Compare `HTTP REQUEST`: options, modes, reference. Polling and emitting send requests while an endpoint accepts requests. Response/request headers are transport headers; server endpoints run on every node and have no remote source position. |
| Reference | Http endpoint |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |  |
| Metadata | Not offered: HTTP headers are available through header functions; this implementation declares no typed request or response metadata fields. |  |
| Every live node | yes |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### KAFKA TOPIC ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | KAFKA TOPIC | Compare `KAFKA TOPIC`: locator, options, modes, ingest metadata. Consumption tracks replayable log offsets and consumer groups; publication uses a producer queue or delivery report. Emit locators currently use the emitter grammar’s unquoted name, while ingest validates a quoted broker topic. |
| Reference | KAFKA client |  |
| Locator and validation rule | TOPIC 'orders' (kafka_topic) |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `OFFSET BY DOMAIN` (keyword choice); `OFFSET BY CONSUMER GROUP 'readers'` (string literal checked by kafka_consumer_group); required<br>`INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | metadata.topic: STRING OPTIONAL<br>metadata.partition: I32 OPTIONAL<br>metadata.offset: I64 OPTIONAL |  |
| Every live node | no; scheduled owner |  |
| Position class | replayable |  |
| Acknowledgement confirmation | answered |  |

### PULSAR TOPIC ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | PULSAR TOPIC | Compare `PULSAR TOPIC`: locator, options, modes. Consumption owns a subscription and broker positions; publication uses a producer receipt. Emit locators currently use the emitter grammar’s unquoted topic, while ingest validates a quoted Pulsar name. |
| Reference | PULSAR client |  |
| Locator and validation rule | TOPIC 'orders' (pulsar_topic) |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `SUBSCRIPTION 'readers' OWNED BY INGESTOR` (string literal checked by pulsar_subscription); `SUBSCRIPTION 'readers' PROVISIONED` (string literal checked by pulsar_subscription); required<br>`INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: The current topic declaration exposes properties as headers but no separate typed broker metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | replayable |  |
| Acknowledgement confirmation | answered |  |

### MQTT TOPIC ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MQTT TOPIC | Compare `MQTT TOPIC`: locator, options, modes. Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |
| Reference | MQTT client |  |
| Locator and validation rule | TOPIC 'devices/+/orders' (mqtt_topic_filter) |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s (requires SESSION PERSISTENT and QOS 1)<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s (requires SESSION PERSISTENT and QOS 1) |  |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1<br>`SESSION CLEAN` (keyword choice); `SESSION PERSISTENT` (keyword choice); optional; default CLEAN<br>`QOS 0` (keyword choice); `QOS 1` (keyword choice); optional; default 0 |  |
| Headers | Not offered: Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND (requires SESSION PERSISTENT and QOS 1)<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | Not offered: The current topic declaration exposes the payload and configured subscription, with no typed publish-property metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | broker_redelivered |  |
| Acknowledgement confirmation | queued |  |

### NATS SUBJECT ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | NATS SUBJECT | Compare `NATS SUBJECT`: locator, options, modes. Subject consumption can join a queue group; publication chooses Core flush or JetStream acknowledgement. Emit subject names currently use the unquoted emitter grammar, and ingest validates quoted subscription subjects. |
| Reference | NATS client |  |
| Locator and validation rule | SUBJECT 'orders.created' (nats_subject) |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `QUEUE GROUP 'workers'` (string literal checked by nats_queue_group); required<br>`INSTANCES 2` (positive count); required |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | Not offered: The current subject declaration exposes message headers but no typed subject or JetStream metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### RABBITMQ QUEUE ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | RABBITMQ QUEUE | Compare `RABBITMQ QUEUE`: locator, options, modes. A queue consumer acknowledges delivery tags on its channel; publishing waits for a channel answer or publisher confirmation. Queue names currently use the unquoted emitter grammar and quoted ingest validator respectively. |
| Reference | RABBITMQ client |  |
| Locator and validation rule | QUEUE 'orders' (rabbitmq_queue) |  |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: The current queue declaration exposes transport headers but no typed AMQP delivery metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | broker_redelivered |  |
| Acknowledgement confirmation | written |  |

### REDIS PUBSUB ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | REDIS PUBSUB | Compare `REDIS PUBSUB`: locator, modes. Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Emit channel names currently use the unquoted emitter grammar. |
| Reference | REDIS client |  |
| Locator and validation rule | CHANNEL 'orders' (redis_channel) |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Emit channel names currently use the unquoted emitter grammar. |  |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | Not offered: The current Pub/Sub declaration delivers payloads without a typed channel metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### PROMETHEUS QUERY ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | PROMETHEUS QUERY | No emit entity is declared. The Prometheus query API is polled for samples; no Prometheus remote-write emitter is declared. |
| Reference | PROMETHEUS client |  |
| Locator and validation rule | Not offered: QUERY is a PromQL expression option rather than an external entity name. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `QUERY 'up'` (string literal); required<br>`EVERY 15s` (duration); required |  |
| Headers | Not offered: The Prometheus query API is polled for samples; no Prometheus remote-write emitter is declared. |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |  |
| Metadata | Not offered: The current query result is decoded as payload; no separate typed query or evaluation-time metadata namespace is declared. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### ZEROMQ PULL ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | ZEROMQ PULL | Compare `ZEROMQ PUSH`: modes. The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |
| Reference | ZEROMQ client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | Not offered: Payload frames have no broker position or typed transport metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### SQS QUEUE ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SQS QUEUE | Compare `SQS QUEUE`: locator, options, modes. Receiving uses a visibility timeout and deletion; publishing uses single or batch service requests and may declare a FIFO group. Emit queue names currently use the unquoted emitter grammar. |
| Reference | SQS client |  |
| Locator and validation rule | QUEUE 'orders' (sqs_queue) |  |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: The current queue declaration exposes message attributes through headers but no separate typed receipt metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | broker_redelivered |  |
| Acknowledgement confirmation | answered |  |

### WEBSOCKETS CONNECTION ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | WEBSOCKETS CONNECTION | No emit entity is declared. An outbound connection or server endpoint receives frames. The declared emitter set has no WebSocket frame publisher; handshake signaling remains distinct from a frame payload. |
| Reference | WEBSOCKETS client |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `HEADERS { 'authorization' = 'Bearer token' }` (composed clause); optional; omitted when unset |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | metadata.frame_type: STRING OPTIONAL |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### WEBSOCKETS ENDPOINT ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | WEBSOCKETS ENDPOINT | No emit entity is declared. An outbound connection or server endpoint receives frames. The declared emitter set has no WebSocket frame publisher; handshake signaling remains distinct from a frame payload. |
| Reference | Websockets endpoint |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `read_header`, `read_headers` |  |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |  |
| Metadata | Not offered: The current frame declaration has no typed frame or handshake metadata namespace. |  |
| Every live node | yes |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

### SYSLOG LISTENER ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SYSLOG LISTENER | Compare `SYSLOG COLLECTOR`: modes, ingest metadata. A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |
| Reference | SYSLOG client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | metadata.peer_addr: STRING OPTIONAL |  |
| Every live node | yes |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |

## Emit

### CLIENT CONSUMER emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | CLIENT CONSUMER | Compare `CLIENT PRODUCER`: modes. Native Arrow batches cross an application session; producers submit batches and consumers explicitly acknowledge delivery attempts, with no broker envelope. |
| Reference | SCHEMA &lt;schema&gt; |  |
| Locator and validation rule | Not offered: The body references the native schema; there is no external topic, queue or table to locate. |  |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Native Arrow batches cross an application session; producers submit batches and consumers explicitly acknowledge delivery attempts, with no broker envelope. |  |
| Body | native Arrow output; no codec |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### HTTP REQUEST emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | HTTP REQUEST | Compare `HTTP REQUEST`, `HTTP ENDPOINT`: options, modes, reference. Polling and emitting send requests while an endpoint accepts requests. Response/request headers are transport headers; server endpoints run on every node and have no remote source position. |
| Reference | HTTP client |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `METHOD 'POST'` (expression); required<br>`PATH '/events'` (expression); required |  |
| Headers | `write_header` |  |
| Body | ENCODE USING &lt;codec&gt; \| WITHOUT BODY |  |
| BATCH | unavailable: send one request per record |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### KAFKA TOPIC emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | KAFKA TOPIC | Compare `KAFKA TOPIC`: locator, options, modes. Consumption tracks replayable log offsets and consumer groups; publication uses a producer queue or delivery report. Emit locators currently use the emitter grammar’s unquoted name, while ingest validates a quoted broker topic. |
| Reference | KAFKA client |  |
| Locator and validation rule | TOPIC orders (topic_name) |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header` |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### PULSAR TOPIC emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | PULSAR TOPIC | Compare `PULSAR TOPIC`: locator, options, modes. Consumption owns a subscription and broker positions; publication uses a producer receipt. Emit locators currently use the emitter grammar’s unquoted topic, while ingest validates a quoted Pulsar name. |
| Reference | PULSAR client |  |
| Locator and validation rule | TOPIC orders (topic_name) |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header` |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### RABBITMQ QUEUE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | RABBITMQ QUEUE | Compare `RABBITMQ QUEUE`: locator, options, modes. A queue consumer acknowledges delivery tags on its channel; publishing waits for a channel answer or publisher confirmation. Queue names currently use the unquoted emitter grammar and quoted ingest validator respectively. |
| Reference | RABBITMQ client |  |
| Locator and validation rule | QUEUE orders (queue_name) |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header` |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### REDIS PUBSUB emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | REDIS PUBSUB | Compare `REDIS PUBSUB`: locator, modes. Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Emit channel names currently use the unquoted emitter grammar. |
| Reference | REDIS client |  |
| Locator and validation rule | CHANNEL orders (channel_name) |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Emit channel names currently use the unquoted emitter grammar. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### MQTT TOPIC emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MQTT TOPIC | Compare `MQTT TOPIC`: locator, options, modes. Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |
| Reference | MQTT client |  |
| Locator and validation rule | TOPIC orders (topic_name) |  |
| Modes (complete bodies after MODE) | QOS 0 RETRY POLICY BACKOFF 1s MAX 30s<br>QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>QOS 2 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### NATS SUBJECT emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | NATS SUBJECT | Compare `NATS SUBJECT`: locator, options, modes. Subject consumption can join a queue group; publication chooses Core flush or JetStream acknowledgement. Emit subject names currently use the unquoted emitter grammar, and ingest validates quoted subscription subjects. |
| Reference | NATS client |  |
| Locator and validation rule | SUBJECT orders (subject_name) |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s<br>JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header` |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### ZEROMQ PUSH emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | ZEROMQ PUSH | Compare `ZEROMQ PULL`: modes. The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |
| Reference | ZEROMQ client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### SQS QUEUE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SQS QUEUE | Compare `SQS QUEUE`: locator, options, modes. Receiving uses a visibility timeout and deletion; publishing uses single or batch service requests and may declare a FIFO group. Emit queue names currently use the unquoted emitter grammar. |
| Reference | SQS client |  |
| Locator and validation rule | QUEUE orders (queue_name) |  |
| Modes (complete bodies after MODE) | SINGLE RETRY POLICY BACKOFF 1s MAX 30s<br>BATCH RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `FIFO GROUP tenant` (expression); `FIFO GROUP FROM BRANCH` (keyword choice); optional; omitted when unset |  |
| Headers | `write_header` |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | 256KiB |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### SENTRY EVENT emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SENTRY EVENT | No ingest entity is declared. The Sentry envelope API accepts one encoded event per request; this connector declares no event-reading ingestor. |
| Reference | SENTRY client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: An event is encoded inside its Sentry envelope; arbitrary per-event HTTP header writes are not supported. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | yes |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### SYSLOG COLLECTOR emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SYSLOG COLLECTOR | Compare `SYSLOG LISTENER`: modes. A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |
| Reference | SYSLOG client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | NO_ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### OTEL LOGS emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | OTEL LOGS | No ingest entity is declared. The OTLP export API accepts typed logs, traces or metrics with value, attribute and resource mappings; this connector declares no OTLP receiver. |
| Reference | OTEL client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ATTRIBUTES {'tenant' = 'eu'}` (named expressions); optional; omitted when unset<br>`RESOURCE {'service.name' = 'nervix'}` (named expressions); optional; omitted when unset<br>`SCOPE 'nervix' VERSION '1.0'` (composed clause); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### OTEL TRACES emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | OTEL TRACES | No ingest entity is declared. The OTLP export API accepts typed logs, traces or metrics with value, attribute and resource mappings; this connector declares no OTLP receiver. |
| Reference | OTEL client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ATTRIBUTES {'tenant' = 'eu'}` (named expressions); optional; omitted when unset<br>`RESOURCE {'service.name' = 'nervix'}` (named expressions); optional; omitted when unset<br>`SCOPE 'nervix' VERSION '1.0'` (composed clause); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### OTEL METRICS emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | OTEL METRICS | No ingest entity is declared. The OTLP export API accepts typed logs, traces or metrics with value, attribute and resource mappings; this connector declares no OTLP receiver. |
| Reference | OTEL client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `METRIC 'temperature' UNIT 'Cel' GAUGE` (composed clause); `METRIC 'requests' UNIT '1' SUM MONOTONIC DELTA` (composed clause); `METRIC 'latency' UNIT 'ms' HISTOGRAM CUMULATIVE` (composed clause); required<br>`ATTRIBUTES {'tenant' = 'eu'}` (named expressions); optional; omitted when unset<br>`RESOURCE {'service.name' = 'nervix'}` (named expressions); optional; omitted when unset<br>`SCOPE 'nervix' VERSION '1.0'` (composed clause); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### CLICKHOUSE TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | CLICKHOUSE TABLE | No ingest entity is declared. The ClickHouse connector inserts mapped rows into an existing table; it declares no change-data-capture or query ingestor. |
| Reference | CLICKHOUSE client |  |
| Locator and validation rule | INSERT TO TABLE orders (table_name) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### POSTGRES TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | POSTGRES TABLE | No ingest entity is declared. The Postgres connector inserts mapped rows and handles declared conflicts; it declares no replication-slot or query ingestor. |
| Reference | POSTGRES client |  |
| Locator and validation rule | INSERT TO TABLE orders (table_name) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ON CONFLICT DO NOTHING` (keyword choice); `ON CONFLICT ('id') DO UPDATE` (keyword choice); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### MYSQL TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MYSQL TABLE | No ingest entity is declared. The MySQL connector inserts mapped rows and handles declared conflicts; it declares no binlog or query ingestor. |
| Reference | MYSQL client |  |
| Locator and validation rule | INSERT TO TABLE orders (table_name) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ON CONFLICT DO NOTHING` (keyword choice); `ON CONFLICT DO UPDATE` (keyword choice); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### MONGODB COLLECTION emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MONGODB COLLECTION | No ingest entity is declared. The MongoDB connector writes mapped documents to an existing collection; it declares no change-stream ingestor. |
| Reference | MONGODB client |  |
| Locator and validation rule | INSERT TO COLLECTION orders (collection_name) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ON CONFLICT ('id') DO NOTHING` (keyword choice); `ON CONFLICT ('id') DO UPDATE` (keyword choice); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

### ICEBERG TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | ICEBERG TABLE | No ingest entity is declared. The Iceberg connector stages data files and commits through an object store and catalog; it declares no table-scanning ingestor. |
| Reference | S3 \| GCS \| AZURE_BLOB store and catalog clients |  |
| Locator and validation rule | TABLE orders (table_name) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `LOCATION 's3://warehouse/orders'` (string literal); required<br>`CATALOG catalog` (ICEBERG_REST client); required<br>`COMMIT EACH 10s MAX SIZE 64MiB` (composed clause); required |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |

## Clients

### HTTP client

| Capability | Declaration |
| --- | --- |
| Kind | HTTP |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### WEBSOCKETS client

| Capability | Declaration |
| --- | --- |
| Kind | WEBSOCKETS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | `WITH SIGNALING PROTOCOL signaling` (composed clause); optional; omitted when unset |

### KAFKA client

| Capability | Declaration |
| --- | --- |
| Kind | KAFKA |
| Connection pooling | unpooled |
| CONFIG owner class | driver |
| Statement options | No entity-specific options. |

### PULSAR client

| Capability | Declaration |
| --- | --- |
| Kind | PULSAR |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### RABBITMQ client

| Capability | Declaration |
| --- | --- |
| Kind | RABBITMQ |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

### REDIS client

| Capability | Declaration |
| --- | --- |
| Kind | REDIS |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

### MQTT client

| Capability | Declaration |
| --- | --- |
| Kind | MQTT |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### NATS client

| Capability | Declaration |
| --- | --- |
| Kind | NATS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

### ZEROMQ client

| Capability | Declaration |
| --- | --- |
| Kind | ZEROMQ |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### SQS client

| Capability | Declaration |
| --- | --- |
| Kind | SQS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### SYSLOG client

| Capability | Declaration |
| --- | --- |
| Kind | SYSLOG |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### PROMETHEUS client

| Capability | Declaration |
| --- | --- |
| Kind | PROMETHEUS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### SENTRY client

| Capability | Declaration |
| --- | --- |
| Kind | SENTRY |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### OTEL client

| Capability | Declaration |
| --- | --- |
| Kind | OTEL |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### CLICKHOUSE client

| Capability | Declaration |
| --- | --- |
| Kind | CLICKHOUSE |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

### POSTGRES client

| Capability | Declaration |
| --- | --- |
| Kind | POSTGRES |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

### MYSQL client

| Capability | Declaration |
| --- | --- |
| Kind | MYSQL |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

### MONGODB client

| Capability | Declaration |
| --- | --- |
| Kind | MONGODB |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

### S3 client

| Capability | Declaration |
| --- | --- |
| Kind | S3 |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; property namespaces delegated to driver |
| Statement options | No entity-specific options. |

### GCS client

| Capability | Declaration |
| --- | --- |
| Kind | GCS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; property namespaces delegated to driver |
| Statement options | No entity-specific options. |

### AZURE_BLOB client

| Capability | Declaration |
| --- | --- |
| Kind | AZURE_BLOB |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; property namespaces delegated to driver |
| Statement options | No entity-specific options. |

### ICEBERG_REST client

| Capability | Declaration |
| --- | --- |
| Kind | ICEBERG_REST |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; property namespaces delegated to driver |
| Statement options | No entity-specific options. |
