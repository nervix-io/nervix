# Connector Capabilities

This reference is generated from the current declarations in Plexus, the Nervix graph definition language. Regenerate it with `just connector-matrix`; `just validate-connector-matrix` rejects drift in this chapter and the generated manual regions.

The tables describe the implemented surface. A connector Epic can extend a declaration and regenerate these tables in the same change. Modes below are complete bodies after `MODE`; options retain their declared order, exact value types, presence and defaults. Locator rule names identify the vocabulary's validator; see [External Names](ingestors.md#external-names).

Ingest and emit are separate contracts: consuming a broker position and publishing a record need different acknowledgements. Each mirror column compares every declared entity of the same connector on the other side and explains transport differences. An absent capability requires reviewed explanatory prose; stale or missing explanations fail validation.

Client CONFIG keys below come from the same declaration used by validation, completion and public inspection. Keys are lowercase and case-sensitive; every key occurs at most once. Required keys, Nervix-owned value types and TLS identity pairs validate before storage. Driver property namespaces and address query options remain intentional pass-through; undeclared driver properties are confidential in public inspection. Kafka properties and Postgres, MySQL, Redis, RabbitMQ and NATS addresses also pass network-free driver checks before commit. Authentication, connectivity and checks requiring a remote service wait for initialization. Nervix-owned file values accept resource mount templates and must resolve to readable files; driver-owned file properties retain their native interpretation. HTTPS retains the driver's built-in trust roots; MQTT requires tls_ca_file for mqtts, as its key's conditional presence declares. tls_ca_file follows the connector's documented augmentation or replacement policy, and a client certificate requires its private key.

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
| Buffers | batches one producer submits ahead of their outcomes: at most 1024 batches, within 32 MiB, per producer grant; when full, holds back what fills it |  |
| Network waits | the acknowledgement of an admitted batch: `ACK TIMEOUT` |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### HTTP REQUEST ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | HTTP REQUEST | Compare `HTTP REQUEST`: options, modes, headers. Polling and emitting send requests while an endpoint accepts requests. Response/request headers are transport headers; server endpoints run on every node and have no remote source position. |
| Reference | HTTP client |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `EVERY 1s` (duration); required |  |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |  |
| Metadata | Not offered: HTTP headers are available through header functions; this implementation declares no typed request or response metadata fields. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | the response body: unbounded |  |
| Network waits | a request: name resolution, connection, TLS and the response: `timeout_ms`; unbounded when omitted |  |
| Driver limits | Response heads above 100 headers or 417,792 bytes are refused.<br>A request follows at most 10 redirects. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

### HTTP ENDPOINT ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | HTTP ENDPOINT | Compare `HTTP REQUEST`: options, modes, headers, reference. Polling and emitting send requests while an endpoint accepts requests. Response/request headers are transport headers; server endpoints run on every node and have no remote source position. |
| Reference | Http endpoint |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK MAX 8 ACK TIMEOUT 5s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |  |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |  |
| Metadata | Not offered: HTTP headers are available through header functions; this implementation declares no typed request or response metadata fields. |  |
| Every live node | yes |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | a request body: unbounded<br>unresolved requests under MODE ACK, per node and ingestor: set by MODE ACK MAX; when full, immediately refuses the request for its sender to retry |  |
| Network waits | a client's request head and body: unbounded<br>graph acknowledgement under MODE ACK: `ACK TIMEOUT, or the exact intake lifetime ending` |  |
| Driver limits | Request heads above 100 headers are refused. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### KAFKA TOPIC ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | KAFKA TOPIC | Compare `KAFKA TOPIC`: options, modes, ingest metadata. Consumption tracks replayable log offsets and consumer groups; publication uses a producer queue or delivery report. Both directions use the same quoted, case-preserving Kafka topic names. |
| Reference | KAFKA client |  |
| Locator and validation rule | TOPIC 'orders' (kafka_topic) |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `OFFSET BY DOMAIN` (keyword choice); `OFFSET BY CONSUMER GROUP 'readers'` (string literal checked by kafka_consumer_group); required<br>`INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | metadata.topic: STRING OPTIONAL<br>metadata.partition: I32 OPTIONAL<br>metadata.offset: I64 OPTIONAL |  |
| Every live node | no; scheduled owner |  |
| Position class | replayable |  |
| Acknowledgement confirmation | answered |  |
| Buffers | messages librdkafka prefetches: driver default, queued.min.messages 100000 per partition, queued.max.messages.kbytes 65536; when full, holds back what fills it<br>admitted synchronous control jobs per node: at most 1024 pending jobs; when full, immediately refuses the request for its sender to retry |  |
| Network waits | name resolution: unbounded<br>connection setup: TCP, TLS and SASL: driver default, socket.connection.setup.timeout.ms, 30 s<br>a fetch: driver default, socket.timeout.ms, 60 s<br>a consumer-group commit under ACK: fixed 30s<br>metadata, partition inspection, offset lookups, watermarks and seeks including executor admission: driver default, socket.timeout.ms, 60 s per control job<br>subscribe, assign and unsubscribe: unbounded<br>destroying a consumer that joined its group, for its LeaveGroup answer: driver default, 5 s per LeaveGroup request |  |
| Driver limits | Every librdkafka key passes through, so a client can change each driver default above.<br>Connector control execution has two workers per node by default. A commit still unanswered after its caller's 30 s deadline retains its worker and the source's one in-flight commit until the native call ends. Native subscribe, assign and unsubscribe have no cancellable deadline; their caller waits socket.timeout.ms at most.<br>A consumer whose last handle is gone is destroyed by one connector control job, which waits for a place rather than being refused; no Nervix key changes librdkafka's 5 s LeaveGroup timeout.<br>receive.message.max.bytes 100000000 |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### PULSAR TOPIC ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | PULSAR TOPIC | Compare `PULSAR TOPIC`: options, modes, headers. Consumption owns a subscription and broker positions; publication uses a producer receipt. Both directions use the same quoted, case-preserving Pulsar topic names. |
| Reference | PULSAR client |  |
| Locator and validation rule | TOPIC 'orders' (pulsar_topic) |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `SUBSCRIPTION 'readers' OWNED BY INGESTOR` (string literal checked by pulsar_subscription); `SUBSCRIPTION 'readers' PROVISIONED` (string literal checked by pulsar_subscription); required<br>`INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: The current topic declaration exposes properties as headers but no separate typed broker metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | replayable |  |
| Acknowledgement confirmation | answered |  |
| Buffers | messages each partition consumer receives ahead of the host: driver default, a receiver queue of 1000 messages; when full, holds back what fills it |  |
| Network waits | name resolution: unbounded<br>connection: TCP, TLS and CONNECT: driver default, 10 s per address, 12 retries<br>topic lookup and subscribe: driver default, 30 s per attempt, retried without limit<br>an acknowledgement under ACK: driver default, 30 s operation timeout<br>close: unbounded |  |
| Driver limits | Inbound frames have no size limit. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

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
| Acknowledgement confirmation | written |  |
| Buffers | each raw MQTT frame, whose protocol bound every CONNECT advertises as its Maximum Packet Size; one packet is decoded before admission: at most 268435460 bytes; when full, its peer, told this capacity, discards what does not fit instead of sending it<br>each admitted publish including its packet headers, copied out of raw framing: at most 10240 bytes; when full, drops and counts what does not fit<br>ACK-mode QoS 1/2 outcomes in the packet-ordered read-ahead queue: set by the Receive Maximum every CONNECT advertises, ACK PARALLEL MAX or 1 under ACK SEQUENTIAL plus 64 up to 65,535; when full, holds back what fills it<br>ACK-mode QoS 0 outcomes alongside the reserved acknowledged capacity in the same packet-ordered read-ahead queue: at most 64 outcome; when full, drops and counts what does not fit<br>NO ACK outcomes in the packet-ordered read-ahead queue: at most 64 outcome; when full, holds back what fills it<br>the next classified packet outcome held for room in read-ahead: at most 1 outcome; when full, holds back what fills it<br>a deferred or in-progress size rejection retained across reads: at most 1 discard; when full, holds back what fills it<br>SUBSCRIBE and PUBACK requests for the connection: set by the Receive Maximum, or 1 under NO ACK; when full, holds back what fills it |  |
| Network waits | connection: name resolution, TCP, TLS and CONNECT: driver default, 5 s<br>SUBACK: unbounded<br>ordinary and oversized-discard acknowledgements written to the connection: driver default, one keep-alive interval of 60 s, or the broker's<br>a silent connection: driver default, keep-alive of 60 s, or the broker's |  |
| Driver limits | A publish above the admission bound is acknowledged before it is counted as oversized; its topic, payload and properties never enter read-ahead. The raw framing bound is MQTT's protocol maximum, separate from the admission bound.<br>ACK-mode QoS 0 overflow is counted immediately and never consumes an acknowledged outcome's reserved capacity. An oversized QoS 0 publish refused here counts as overflow only. NO ACK retains backpressure. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### NATS SUBJECT ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | NATS SUBJECT | Compare `NATS SUBJECT`: locator, options, modes. Subject consumption can join a queue group; publication chooses Core flush or JetStream acknowledgement. Both directions use quoted, case-preserving subjects; publication subjects exclude subscription wildcards. |
| Reference | NATS client |  |
| Locator and validation rule | SUBJECT 'orders.created' (nats_subject) |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `QUEUE GROUP 'workers'` (string literal checked by nats_queue_group); required<br>`INSTANCES 2` (positive count); required |  |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, exact names: every value of a name, together and in the order written; the order of different names is not declared; a name matches only a name spelled the same, letter case included |  |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | Not offered: The current subject declaration exposes message headers but no typed subject or JetStream metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | messages the subscription holds ahead of the host: driver default, 65,536 messages; when full, drops what does not fit without counting it |  |
| Network waits | name resolution: unbounded<br>connection: driver default, 5 s per address<br>the subscription's flush: unbounded<br>a silent connection: driver default, a ping every 60 s; the connection ends after 2 go unanswered |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### RABBITMQ QUEUE ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | RABBITMQ QUEUE | Compare `RABBITMQ QUEUE`: options, modes. A queue consumer acknowledges delivery tags on its channel; publishing waits for a channel answer or publisher confirmation. Both directions use the same quoted, case-preserving RabbitMQ queue names. |
| Reference | RABBITMQ client |  |
| Locator and validation rule | QUEUE 'orders' (rabbitmq_queue) |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers`; `sorted_by_name`, exact names: one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: The current queue declaration exposes transport headers but no typed AMQP delivery metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | broker_redelivered |  |
| Acknowledgement confirmation | written |  |
| Buffers | Lapin's existing consumer delivery queue; broker prefetch bounds it in acknowledged modes, but automatic acknowledgements do not apply that bound: unbounded<br>deliveries gathered for one host read: set by 1 under NO ACK and ACK SEQUENTIAL; ACK PARALLEL MAX otherwise, at most 65,535; when full, holds back what fills it |  |
| Network waits | gathering further deliveries after the first under ACK PARALLEL; cancellation hands over every delivery already gathered: `BATCH TIMEOUT`<br>connection: name resolution, address attempts, TLS, the AMQP handshake, channel open and consume: `connect_timeout_ms`; 30s when omitted<br>a silent connection: driver default, twice the negotiated heartbeat |  |
| Driver limits | Consumer prefetch is 1 under ACK SEQUENTIAL and ACK PARALLEL MAX under ACK PARALLEL, at most 65,535; the broker holds back deliveries beyond that window.<br>NO ACK has no prefetch bound; auto-acknowledged deliveries can accumulate in Lapin's existing consumer queue while the host is not reading.<br>A frame above the negotiated frame_max ends the connection. |  |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |  |

### REDIS PUBSUB ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | REDIS PUBSUB | Compare `REDIS PUBSUB`: modes. Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Both directions use quoted, case-preserving Redis channel names. |
| Reference | REDIS client |  |
| Locator and validation rule | CHANNEL 'orders' (redis_channel) |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Both directions use quoted, case-preserving Redis channel names. |  |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | Not offered: The current Pub/Sub declaration delivers payloads without a typed channel metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | messages read ahead of intake: set by subscription_buffer_messages; when full, holds back what fills it |  |
| Network waits | subscription: name resolution, address attempts, TLS, Redis setup and SUBSCRIBE: `connect_timeout_ms`; 30s when omitted<br>idle subscription activity, including its PING probe: `idle_timeout_ms`; required |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms`<br>`STALLED PEER idle_timeout_ms` |  |

### PROMETHEUS QUERY ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | PROMETHEUS QUERY | No emit entity is declared. The Prometheus query API is polled for samples; no Prometheus remote-write emitter is declared. |
| Reference | PROMETHEUS client |  |
| Locator and validation rule | Not offered: QUERY is a PromQL expression option rather than an external entity name. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `QUERY 'up'` (string literal); required<br>`MAX BODY SIZE 16MiB` (byte size); optional; default 16MiB<br>`EVERY 15s` (duration); required |  |
| Headers | Not offered: The Prometheus query API is polled for samples; no Prometheus remote-write emitter is declared. |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |  |
| Metadata | Not offered: The current query result is decoded as payload; no separate typed query or evaluation-time metadata namespace is declared. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | the query response: set by MAX BODY SIZE; when exceeded, fails the poll; the next poll reads again<br>samples the host takes in as one intake batch: at most 1024 samples; when full, holds back what fills it |  |
| Network waits | a query: name resolution, connection, TLS and the response: `timeout_ms`; required |  |
| Driver limits | Response heads above 100 headers or 417,792 bytes are refused. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

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
| Metadata | Not offered: The driver exposes no peer identity on receive. Only frame 0 is ingested, with no broker position or typed transport metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | the frame a peer is sending: unbounded |  |
| Network waits | connecting, retried without limit: unbounded<br>the ZMTP handshake: unbounded<br>receiving the next message: unbounded |  |
| Driver limits | The driver has no high-water mark, maximum message size or heartbeat. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### SQS QUEUE ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SQS QUEUE | Compare `SQS QUEUE`: options, modes. Receiving uses a visibility timeout and deletion; publishing uses single or batch service requests and may declare a FIFO group. Both directions use the same quoted, case-preserving SQS queue names, including the .fifo suffix. |
| Reference | SQS client |  |
| Locator and validation rule | QUEUE 'orders' (sqs_queue) |  |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1 |  |
| Headers | `read_header`, `read_headers`; `last_wins`, exact names: one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |  |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |  |
| Metadata | Not offered: The current queue declaration exposes message attributes through headers but no separate typed receipt metadata namespace. |  |
| Every live node | no; scheduled owner |  |
| Position class | broker_redelivered |  |
| Acknowledgement confirmation | answered |  |
| Buffers | The connector holds no buffer between its transport and the host. |  |
| Network waits | connection: name resolution, TCP and TLS: driver default, 3.1 s<br>the queue URL lookup, a receive and a delete: unbounded |  |
| Driver limits | A request that stalls below 1 byte a second for 5 s is cancelled. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### WEBSOCKETS CONNECTION ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | WEBSOCKETS CONNECTION | No emit entity is declared. An outbound connection or server endpoint receives frames. The declared emitter set has no WebSocket frame publisher; handshake signaling remains distinct from a frame payload. |
| Reference | WEBSOCKETS client |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK |  |
| Options in statement order | `HEADERS { 'authorization' = 'Bearer token' }` (composed clause); optional; omitted when unset |  |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |  |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |  |
| Metadata | metadata.frame_type: STRING OPTIONAL |  |
| Every live node | no; scheduled owner |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | accepted signaling frames retained for source reads: set by handshake_buffer_capacity messages (default 16), each at most max_message_size bytes; when full, holds back what fills it<br>one accepted frame waiting for handoff room: set by max_message_size bytes (default 67108864); when full, holds back what fills it<br>the incoming message assembled by the transport: set by max_message_size bytes (default 67108864), with each frame bounded by max_frame_size bytes (default 16777216); when exceeded, fails the read and reconnects on the retry policy |  |
| Network waits | connection: name resolution, address attempts, TLS and the opening upgrade: `connect_timeout_ms`; 30s when omitted<br>the signaling handshake: `the signaling protocol's TIMEOUT`<br>reading the next frame: `idle_timeout_ms`; when omitted, three ping_interval_ms intervals, or 30 s without configured pings<br>writing a configured ping: fixed 1s<br>close: fixed 1s |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms`<br>`STALLED PEER idle_timeout_ms` |  |

### WEBSOCKETS ENDPOINT ingest

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | WEBSOCKETS ENDPOINT | No emit entity is declared. An outbound connection or server endpoint receives frames. The declared emitter set has no WebSocket frame publisher; handshake signaling remains distinct from a frame payload. |
| Reference | Websockets endpoint |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | NO ACK<br>ACK MAX 8 ACK TIMEOUT 5s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |  |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |  |
| Metadata | Not offered: The current frame declaration has no typed frame or handshake metadata namespace. |  |
| Every live node | yes |  |
| Position class | none |  |
| Acknowledgement confirmation | none |  |
| Buffers | unresolved data frames under MODE ACK, per node and ingestor: set by MODE ACK MAX; when full, immediately refuses the request for its sender to retry |  |
| Network waits | graph acknowledgement under MODE ACK: `ACK TIMEOUT, or the exact intake lifetime ending`<br>the signaling handshake: `the signaling protocol's TIMEOUT`<br>reading the next frame: unbounded |  |
| Driver limits | A message above 64 MiB or a frame above 16 MiB ends the session. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

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
| Buffers | UDP datagrams admitted after a fixed 65,535-byte receive scratch buffer: set by max_message_size; when full, drops and counts what does not fit<br>TCP and TLS frame assembly across unlimited connections (max_message_size + 11 bytes per connection; an oversized frame closes only that connection): unbounded<br>completed TCP and TLS frames waiting for the intake queue (one frame per connection, at most max_message_size bytes; unlimited connections): unbounded<br>frames read from TCP and TLS connections: at most 64 frames; when full, holds back what fills it |  |
| Network waits | the TLS handshake of an accepted connection: `connect_timeout_ms`; 30s when omitted<br>reading a connection: `idle_timeout_ms`; unbounded when omitted |  |
| Driver limits | Datagrams wait in the kernel's UDP receive buffer, which drops what does not fit. |  |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms`<br>`STALLED PEER idle_timeout_ms` |  |

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
| Buffers | batches retained for consumers on a node: at most 134217728 bytes; when full, holds back what fills it |  |
| Network waits | a delivery attempt: `ACK TIMEOUT` |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### HTTP REQUEST emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | HTTP REQUEST | Compare `HTTP REQUEST`, `HTTP ENDPOINT`: options, modes, headers, reference. Polling and emitting send requests while an endpoint accepts requests. Response/request headers are transport headers; server endpoints run on every node and have no remote source position. |
| Reference | HTTP client |  |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `METHOD 'POST'` (expression); required<br>`PATH '/events'` (expression); required |  |
| Headers | `write_header`; `sorted_by_name`, lowercase names: one value per name, the last one written for it; names in ascending byte order; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |  |
| Body | ENCODE USING &lt;codec&gt; \| WITHOUT BODY |  |
| BATCH | unavailable: send one request per record |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | The connector holds no buffer between its transport and the host. |  |
| Network waits | a request: name resolution within half of it, each address's connection within an equal share of what is left, TLS, writing it and its final response headers: `timeout_ms`; required |  |
| Driver limits | A response head above 74,240 bytes or 128 fields fails the attempt. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

### KAFKA TOPIC emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | KAFKA TOPIC | Compare `KAFKA TOPIC`: options, modes. Consumption tracks replayable log offsets and consumer groups; publication uses a producer queue or delivery report. Both directions use the same quoted, case-preserving Kafka topic names. |
| Reference | KAFKA client |  |
| Locator and validation rule | TOPIC 'orders' (kafka_topic) |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | records the producer queues under ACK: driver default, queue.buffering.max.messages 100000, queue.buffering.max.kbytes 1048576; when full, holds back what fills it<br>records the producer queues under NO ACK: driver default, queue.buffering.max.messages 100000, queue.buffering.max.kbytes 1048576; when full, fails the publish for the host to retry<br>records awaiting confirmation under ACK: set by ACK PARALLEL MAX; when full, holds back what fills it |  |
| Network waits | a produce request: driver default, request.timeout.ms, 30 s<br>delivery under NO ACK: driver default, message.timeout.ms, 300 s<br>delivery under ACK: `ACK TIMEOUT`<br>destroying a producer, for its polling thread and the reports of the records its purge failed: driver default, 100 ms poll, then a 500 ms flush |  |
| Driver limits | Every librdkafka key passes through, so a client can change each driver default above.<br>message.max.bytes 1000000<br>A record whose payload exceeds the whole queue.buffering.max.kbytes is rejected through ON MESSAGE ERROR.<br>A producer whose last handle is gone is destroyed by one connector control job, which waits for a place rather than being refused; no Nervix key changes rust-rdkafka's 100 ms poll or 500 ms flush, and librdkafka then joins its threads, a broker thread still resolving a name included. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### PULSAR TOPIC emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | PULSAR TOPIC | Compare `PULSAR TOPIC`: options, modes, headers. Consumption owns a subscription and broker positions; publication uses a producer receipt. Both directions use the same quoted, case-preserving Pulsar topic names. |
| Reference | PULSAR client |  |
| Locator and validation rule | TOPIC 'orders' (pulsar_topic) |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header`; `last_wins`, exact names: one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | receipts held under NO ACK: at most 100 receipts; when full, holds back what fills it<br>messages the driver queues for its connection: driver default, 100 messages; when full, fails the publish for the host to retry<br>records awaiting confirmation under ACK: set by ACK PARALLEL MAX; when full, holds back what fills it |  |
| Network waits | name resolution: unbounded<br>connection: driver default, 10 s per address<br>topic lookup, producer creation and receipts: driver default, the operation timeout: ACK TIMEOUT under ACK, 30 s under NO ACK |  |
| Driver limits | Messages above the broker's announced maximum are rejected. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### RABBITMQ QUEUE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | RABBITMQ QUEUE | Compare `RABBITMQ QUEUE`: options, modes. A queue consumer acknowledges delivery tags on its channel; publishing waits for a channel answer or publisher confirmation. Both directions use the same quoted, case-preserving RabbitMQ queue names. |
| Reference | RABBITMQ client |  |
| Locator and validation rule | QUEUE 'orders' (rabbitmq_queue) |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header`; `sorted_by_name`, exact names: one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | publishes awaiting confirmation: set by ACK PARALLEL MAX, 1 under ACK SEQUENTIAL; when full, holds back what fills it<br>unroutable messages the broker returns under NO ACK: unbounded |  |
| Network waits | connection: name resolution, address attempts, TLS, the AMQP handshake, channel open, confirm select and the queue check: `connect_timeout_ms`; 30s when omitted<br>a publish: unbounded<br>a confirmation under ACK: `ACK TIMEOUT`<br>the round trip that ends a publish under NO ACK: `timeout_ms`; unbounded when omitted |  |
| Driver limits | A frame above the negotiated frame_max ends the connection. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |  |

### REDIS PUBSUB emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | REDIS PUBSUB | Compare `REDIS PUBSUB`: modes. Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Both directions use quoted, case-preserving Redis channel names. |
| Reference | REDIS client |  |
| Locator and validation rule | CHANNEL 'orders' (redis_channel) |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Both directions use quoted, case-preserving Redis channel names. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | connections the command pool holds: set by POOL SIZE MAX; when full, holds back what fills it |  |
| Network waits | checking out a connection, a new one's name resolution, connection, TLS and Redis setup included: `connect_timeout_ms`; 30s when omitted<br>the answer to a PUBLISH: driver default, 500 ms |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |  |

### MQTT TOPIC emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MQTT TOPIC | Compare `MQTT TOPIC`: locator, options, modes. Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |
| Reference | MQTT client |  |
| Locator and validation rule | TOPIC 'devices/Orders' (mqtt_topic) |  |
| Modes (complete bodies after MODE) | QOS 0 RETRY POLICY BACKOFF 1s MAX 30s<br>QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>QOS 2 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | publishes queued for the driver: set by 1 under QOS 0, ACK PARALLEL MAX under QOS 1 and 2; when full, fails the publish for the host to retry |  |
| Network waits | connection: name resolution, TCP, TLS and CONNECT: driver default, 5 s<br>answers to QOS 1 and 2 publishes: `ACK TIMEOUT`<br>writes to the connection: unbounded |  |
| Driver limits | Packets above the broker's maximum packet size are rejected. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### NATS SUBJECT emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | NATS SUBJECT | Compare `NATS SUBJECT`: locator, options, modes. Subject consumption can join a queue group; publication chooses Core flush or JetStream acknowledgement. Both directions use quoted, case-preserving subjects; publication subjects exclude subscription wildcards. |
| Reference | NATS client |  |
| Locator and validation rule | SUBJECT 'orders.Created' (nats_publish_subject) |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | `write_header`; `repeats_by_name`, exact names: every value of a name, together and in the order written; the order of different names is not declared; a name matches only a name spelled the same, letter case included |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | commands queued for the connection: driver default, 2048 commands; when full, holds back what fills it |  |
| Network waits | name resolution: unbounded<br>connection: driver default, 5 s per address<br>a core publish and its flush to the client's socket, not a server round trip: `timeout_ms`; unbounded when omitted<br>a JetStream acknowledgement: `ACK TIMEOUT` |  |
| Driver limits | Messages above the server's max_payload are rejected. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

### ZEROMQ PUSH emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | ZEROMQ PUSH | Compare `ZEROMQ PULL`: modes. The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |
| Reference | ZEROMQ client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: The PUSH socket sends one payload frame. Multipart envelopes and additional per-record envelope clauses are not offered. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | bytes queued for a peer: unbounded |  |
| Network waits | connecting and the ZMTP handshake: `connect_timeout_ms`; 30s when omitted<br>a send: `timeout_ms`; unbounded when omitted |  |
| Driver limits | The driver has no high-water mark, maximum message size or heartbeat. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |  |

### SQS QUEUE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SQS QUEUE | Compare `SQS QUEUE`: options, modes. Receiving uses a visibility timeout and deletion; publishing uses single or batch service requests and may declare a FIFO group. Both directions use the same quoted, case-preserving SQS queue names, including the .fifo suffix. |
| Reference | SQS client |  |
| Locator and validation rule | QUEUE 'orders' (sqs_queue) |  |
| Modes (complete bodies after MODE) | SINGLE RETRY POLICY BACKOFF 1s MAX 30s<br>BATCH RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `FIFO GROUP tenant` (expression); `FIFO GROUP FROM BRANCH` (keyword choice); optional; omitted when unset |  |
| Headers | `write_header`; `last_wins`, exact names: one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | 256KiB |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | The connector holds no buffer between its transport and the host. |  |
| Network waits | connection: name resolution, TCP and TLS: driver default, 3.1 s<br>the queue URL lookup at start: unbounded<br>a send: `timeout_ms`; unbounded when omitted |  |
| Driver limits | A batch request holds at most 10 entries and 256 KiB; a record above 256 KiB is rejected. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

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
| Buffers | The connector holds no buffer between its transport and the host. |  |
| Network waits | a request: name resolution, connection, TLS and the response headers: `timeout_ms`; unbounded when omitted |  |
| Driver limits | An event above 1,000,000 bytes is rejected. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

### SYSLOG COLLECTOR emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | SYSLOG COLLECTOR | Compare `SYSLOG LISTENER`: modes. A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |
| Reference | SYSLOG client |  |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |  |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |  |
| Body | ENCODE USING &lt;codec&gt; |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | The connector holds no buffer between its transport and the host. |  |
| Network waits | connection: name resolution, address attempts and TLS: `connect_timeout_ms`; 30s when omitted<br>writing and flushing a frame: `timeout_ms`; unbounded when omitted |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |  |

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
| Buffers | the export request of a publish without BATCH: unbounded |  |
| Network waits | an export: name resolution, connection, TLS and the response: `timeout_ms`; required |  |
| Driver limits | gRPC responses above 4 MiB are refused. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |
| VALUES fields, exact types and constraints | LOGS `time`: DATETIME; required; not before the Unix epoch<br>LOGS `severity_text`: STRING; optional; no additional value constraint<br>LOGS `severity_number`: I32; optional; 0..=24<br>LOGS `body`: STRING; required; no additional value constraint<br>LOGS `trace_id`: STRING; optional; 32 hexadecimal characters; not all zero<br>LOGS `span_id`: STRING; optional; 16 hexadecimal characters; not all zero |  |
| ATTRIBUTES values | a string, boolean, number, datetime or an ARRAY or VEC of those; DATETIME becomes RFC 3339 text; ARRAY and VEC may nest; NULL attributes are omitted; BYTES and maps are refused |  |
| RESOURCE values | a literal string, boolean, I64, F64, null or ARRAY; literal expressions only, including nested literal arrays; one resource for the emitter |  |
| Unsupported OTLP features and reasons | exponential histograms and summaries: METRIC declares only GAUGE, SUM and explicit HISTOGRAM<br>per-record resource and scope: RESOURCE is literal-only and SCOPE is fixed for the emitter<br>span events and links: the closed span VALUES table has no event or link mapping<br>trace_state: the closed span VALUES table does not map trace state<br>log event_name and flags: the closed log VALUES table maps neither field<br>exemplars: metric points expose values and attributes without exemplar mappings<br>non-string log bodies: the log body field requires STRING<br>bytes_value and map attributes: attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are unavailable<br>scope attributes: SCOPE declares only a name and optional version<br>OTLP/JSON: the client declares only grpc and http/protobuf |  |

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
| Buffers | the export request of a publish without BATCH: unbounded |  |
| Network waits | an export: name resolution, connection, TLS and the response: `timeout_ms`; required |  |
| Driver limits | gRPC responses above 4 MiB are refused. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |
| VALUES fields, exact types and constraints | TRACES `trace_id`: STRING; required; 32 hexadecimal characters; not all zero<br>TRACES `span_id`: STRING; required; 16 hexadecimal characters; not all zero<br>TRACES `parent_span_id`: STRING; optional; 16 hexadecimal characters; not all zero<br>TRACES `name`: STRING; required; no additional value constraint<br>TRACES `kind`: STRING; optional; SERVER, CLIENT, INTERNAL, PRODUCER or CONSUMER; case-sensitive<br>TRACES `start_time`: DATETIME; required; not before the Unix epoch<br>TRACES `end_time`: DATETIME; required; not before the Unix epoch<br>TRACES `status_code`: STRING; optional; OK, ERROR or UNSET; case-sensitive<br>TRACES `status_message`: STRING; optional; no additional value constraint |  |
| ATTRIBUTES values | a string, boolean, number, datetime or an ARRAY or VEC of those; DATETIME becomes RFC 3339 text; ARRAY and VEC may nest; NULL attributes are omitted; BYTES and maps are refused |  |
| RESOURCE values | a literal string, boolean, I64, F64, null or ARRAY; literal expressions only, including nested literal arrays; one resource for the emitter |  |
| Unsupported OTLP features and reasons | exponential histograms and summaries: METRIC declares only GAUGE, SUM and explicit HISTOGRAM<br>per-record resource and scope: RESOURCE is literal-only and SCOPE is fixed for the emitter<br>span events and links: the closed span VALUES table has no event or link mapping<br>trace_state: the closed span VALUES table does not map trace state<br>log event_name and flags: the closed log VALUES table maps neither field<br>exemplars: metric points expose values and attributes without exemplar mappings<br>non-string log bodies: the log body field requires STRING<br>bytes_value and map attributes: attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are unavailable<br>scope attributes: SCOPE declares only a name and optional version<br>OTLP/JSON: the client declares only grpc and http/protobuf |  |

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
| Buffers | the export request of a publish without BATCH: unbounded |  |
| Network waits | an export: name resolution, connection, TLS and the response: `timeout_ms`; required |  |
| Driver limits | gRPC responses above 4 MiB are refused. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |
| VALUES fields, exact types and constraints | GAUGE / SUM `time`: DATETIME; required; not before the Unix epoch<br>GAUGE / SUM `start_time`: DATETIME; required for DELTA; optional otherwise; not before the Unix epoch<br>GAUGE / SUM `value`: an integer-family, F32, or F64 value; required; integer values must fit I64; floating-point values use OTLP as_double<br>HISTOGRAM `time`: DATETIME; required; not before the Unix epoch<br>HISTOGRAM `start_time`: DATETIME; required for DELTA; optional otherwise; not before the Unix epoch<br>HISTOGRAM `count`: U64; required; the complete U64 range<br>HISTOGRAM `sum`: F32 or F64; optional; no additional value constraint<br>HISTOGRAM `bucket_counts`: a U64 ARRAY or VEC; required; no NULL elements; length equals explicit_bounds length plus one<br>HISTOGRAM `explicit_bounds`: an F32 or F64 ARRAY or VEC; required; no NULL elements<br>HISTOGRAM `min`: F32 or F64; optional; no additional value constraint<br>HISTOGRAM `max`: F32 or F64; optional; no additional value constraint |  |
| ATTRIBUTES values | a string, boolean, number, datetime or an ARRAY or VEC of those; DATETIME becomes RFC 3339 text; ARRAY and VEC may nest; NULL attributes are omitted; BYTES and maps are refused |  |
| RESOURCE values | a literal string, boolean, I64, F64, null or ARRAY; literal expressions only, including nested literal arrays; one resource for the emitter |  |
| Unsupported OTLP features and reasons | exponential histograms and summaries: METRIC declares only GAUGE, SUM and explicit HISTOGRAM<br>per-record resource and scope: RESOURCE is literal-only and SCOPE is fixed for the emitter<br>span events and links: the closed span VALUES table has no event or link mapping<br>trace_state: the closed span VALUES table does not map trace state<br>log event_name and flags: the closed log VALUES table maps neither field<br>exemplars: metric points expose values and attributes without exemplar mappings<br>non-string log bodies: the log body field requires STRING<br>bytes_value and map attributes: attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are unavailable<br>scope attributes: SCOPE declares only a name and optional version<br>OTLP/JSON: the client declares only grpc and http/protobuf |  |

### CLICKHOUSE TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | CLICKHOUSE TABLE | No ingest entity is declared. The ClickHouse connector inserts mapped rows into an existing table; its client declares TCP keep-alive and HTTP pool idle-reuse intervals. It declares no change-data-capture or query ingestor. |
| Reference | CLICKHOUSE client |  |
| Locator and validation rule | TABLE 'sales.Orders' (clickhouse_table) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | No entity-specific options. |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | The connector holds no buffer between its transport and the host. |  |
| Network waits | an insert: connection, TLS, upload and the response: `timeout_ms`; required |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

### POSTGRES TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | POSTGRES TABLE | No ingest entity is declared. The Postgres connector inserts mapped rows and handles declared conflicts; it declares no replication-slot or query ingestor. |
| Reference | POSTGRES client |  |
| Locator and validation rule | TABLE 'Orders' (postgres_table) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ON CONFLICT DO NOTHING` (keyword choice); `ON CONFLICT ('id') DO UPDATE` (keyword choice); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | connections the pool holds: set by POOL SIZE MAX; when full, holds back what fills it |  |
| Network waits | a pool acquire: name resolution, connection, TLS and authentication: fixed 30s<br>an insert and its metadata query: unbounded |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### MYSQL TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MYSQL TABLE | No ingest entity is declared. The MySQL connector inserts mapped rows and handles declared conflicts; it declares no binlog or query ingestor. |
| Reference | MYSQL client |  |
| Locator and validation rule | TABLE 'Orders' (mysql_table) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ON CONFLICT DO NOTHING` (keyword choice); `ON CONFLICT DO UPDATE` (keyword choice); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | connections the pool holds: set by POOL SIZE MAX; when full, holds back what fills it |  |
| Network waits | connection, its handshake and an insert: unbounded |  |
| Driver limits | The driver has no connect, read or write timeout.<br>A prepared insert binds at most 65,535 placeholders; mapped column count narrows its row limit. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### MONGODB COLLECTION emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | MONGODB COLLECTION | No ingest entity is declared. The MongoDB connector writes mapped documents to an existing collection; it declares no change-stream ingestor. |
| Reference | MONGODB client |  |
| Locator and validation rule | COLLECTION 'Orders' (mongodb_collection) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `ON CONFLICT ('id') DO NOTHING` (keyword choice); `ON CONFLICT ('id') DO UPDATE` (keyword choice); optional; omitted when unset |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | required |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | connections the pool holds: set by POOL SIZE MAX; when full, holds back what fills it |  |
| Network waits | connection: name resolution, TCP and TLS: driver default, connectTimeoutMS, 10 s<br>server selection: driver default, 30 s<br>the handshake, a pool wait and a write: unbounded |  |
| Driver limits | A document above the declared BSON document limit is rejected. |  |
| Stalled-peer checks | None: no client deadline key bounds a wait. |  |

### ICEBERG TABLE emit

| Capability | Declaration | Mirror and transport reason |
| --- | --- | --- |
| Entity | ICEBERG TABLE | No ingest entity is declared. The Iceberg connector stages data files and commits through an object store and catalog; it declares no table-scanning ingestor. |
| Reference | S3 \| GCS \| AZURE_BLOB store and catalog clients |  |
| Locator and validation rule | TABLE 'Orders' (iceberg_table) |  |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |  |
| Options in statement order | `NAMESPACE 'Analytics.Events'` (string literal); required<br>`LOCATION 's3://warehouse/orders'` (string literal); required<br>`CATALOG catalog` (ICEBERG_REST client); required<br>`COMMIT EACH 10s MAX SIZE 64MiB` (composed clause); required |  |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |  |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |  |
| BATCH | optional |  |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |  |
| Batch transformation required | no |  |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |  |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |  |
| Buffers | records staged on disk until a commit: set by COMMIT EACH ... MAX SIZE; when full, holds back what fills it |  |
| Network waits | catalog requests: unbounded<br>object-store metadata and delete operations: `timeout_ms`; 1m when omitted<br>object-store reads and writes: `timeout_ms`; 10s when omitted |  |
| Driver limits | None declared. |  |
| Stalled-peer checks | `STALLED PEER timeout_ms` |  |

## Clients

### HTTP client

| Capability | Declaration |
| --- | --- |
| Kind | HTTP |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `endpoint` | required | URL (http or https) | Nervix; strict | no | none |
| `method` | optional | string | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### WEBSOCKETS client

| Capability | Declaration |
| --- | --- |
| Kind | WEBSOCKETS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | `WITH SIGNALING PROTOCOL signaling` (composed clause); optional; omitted when unset |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `connect_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `endpoint` | required | URL (ws or wss) | Nervix; strict | no | none |
| `handshake_buffer_capacity` | optional | positive integer | Nervix; strict | no | none |
| `idle_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `max_frame_size` | optional | positive integer | Nervix; strict | no | none |
| `max_message_size` | optional | positive integer | Nervix; strict | no | none |
| `ping_interval_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### KAFKA client

| Capability | Declaration |
| --- | --- |
| Kind | KAFKA |
| Connection pooling | unpooled |
| CONFIG owner class | declared keys and driver-delegated property namespaces |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `auto.offset.reset` | optional | string | driver; best effort | no | none |
| `bootstrap.servers` | optional | string | driver; best effort | no | none |
| `client.id` | optional | string | driver; best effort | no | none |
| `fetch.wait.max.ms` | optional | unsigned integer | driver; best effort | no | none |
| `heartbeat.interval.ms` | optional | positive integer | driver; best effort | no | none |
| `linger.ms` | optional | unsigned integer | driver; best effort | no | none |
| `message.max.bytes` | optional | positive integer | driver; best effort | no | none |
| `message.timeout.ms` | optional | unsigned integer | driver; best effort | no | none |
| `queue.buffering.max.messages` | optional | unsigned integer | driver; best effort | no | none |
| `sasl.mechanism` | optional | string | driver; best effort | no | none |
| `sasl.mechanisms` | optional | string | driver; best effort | no | none |
| `sasl.oauthbearer.config` | optional | string | driver; best effort | yes | none |
| `sasl.password` | optional | string | driver; best effort | yes | none |
| `sasl.username` | optional | string | driver; best effort | yes | none |
| `security.protocol` | optional | string | driver; best effort | no | none |
| `session.timeout.ms` | optional | positive integer | driver; best effort | no | none |
| `socket.timeout.ms` | optional | positive, schedulable whole milliseconds | driver; best effort | no | none |
| `ssl.ca.location` | optional | file path, with resource mount templates resolved before use | driver; best effort | no | driver file |
| `ssl.certificate.location` | optional | file path, with resource mount templates resolved before use | driver; best effort | no | driver file |
| `ssl.key.location` | optional | file path, with resource mount templates resolved before use | driver; best effort | no | driver file |
| `ssl.key.password` | optional | string | driver; best effort | yes | none |
| `ssl.key.pem` | optional | string | driver; best effort | yes | none |

Delegated property namespaces: all driver property names. Reserved keys: `allow.auto.create.topics`, `delivery.report.only.error`, `enable.auto.commit`, `enable.partition.eof`, `group.id`.

### PULSAR client

| Capability | Declaration |
| --- | --- |
| Kind | PULSAR |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | URL (pulsar or pulsar+ssl) | Nervix; strict | no | none |
| `namespace` | optional | string | Nervix; strict | no | none |
| `tls_allow_insecure_connection` | optional | boolean | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_hostname_verification_enabled` | optional | boolean | Nervix; strict | no | none |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### RABBITMQ client

| Capability | Declaration |
| --- | --- |
| Kind | RABBITMQ |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | driver address, including its delegated query options | Nervix; strict | no | none |
| `connect_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### REDIS client

| Capability | Declaration |
| --- | --- |
| Kind | REDIS |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | driver address, including its delegated query options | Nervix; strict | no | none |
| `connect_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `idle_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `pool_idle_timeout_ms` | optional; default `600000` | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `pool_max_lifetime_ms` | optional; default `1800000` | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `subscription_buffer_messages` | optional | positive integer | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### MQTT client

| Capability | Declaration |
| --- | --- |
| Kind | MQTT |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | URL (mqtt or mqtts) | Nervix; strict | no | none |
| `client_id` | optional | string | Nervix; strict | no | none |
| `tls_ca_file` | required when `addr` uses `mqtts` | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### NATS client

| Capability | Declaration |
| --- | --- |
| Kind | NATS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | driver address, including its delegated query options | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### ZEROMQ client

| Capability | Declaration |
| --- | --- |
| Kind | ZEROMQ |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | string | Nervix; strict | no | none |
| `bind` | optional | boolean | Nervix; strict | no | none |
| `connect_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### SQS client

| Capability | Declaration |
| --- | --- |
| Kind | SQS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `access_key_id` | optional | string | Nervix; strict | yes | none |
| `endpoint` | required | URL (http or https) | Nervix; strict | no | none |
| `region` | optional | string | Nervix; strict | no | none |
| `secret_access_key` | optional | string | Nervix; strict | yes | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### SYSLOG client

| Capability | Declaration |
| --- | --- |
| Kind | SYSLOG |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | string | Nervix; strict | no | none |
| `connect_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `framing` | optional | octet-counting or non-transparent | Nervix; strict | no | none |
| `idle_timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `max_message_size` | optional | positive integer | Nervix; strict | no | none |
| `protocol` | required | udp or tcp or tls | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### PROMETHEUS client

| Capability | Declaration |
| --- | --- |
| Kind | PROMETHEUS |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | URL (http or https) | Nervix; strict | no | none |
| `timeout_ms` | required | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### SENTRY client

| Capability | Declaration |
| --- | --- |
| Kind | SENTRY |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `dsn` | required | URL (http or https) | Nervix; strict | yes | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### OTEL client

| Capability | Declaration |
| --- | --- |
| Kind | OTEL |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `compression` | optional | gzip | Nervix; strict | no | none |
| `endpoint` | required | URL (http or https) | Nervix; strict | no | none |
| `headers` | optional | string | Nervix; strict | yes | none |
| `protocol` | required | grpc or http/protobuf | Nervix; strict | no | none |
| `timeout_ms` | required | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

| Capability | Declaration |
| --- | --- |
| Additional declared constraints | comma-separated key=value pairs; split at the first '='; trim surrounding whitespace; lowercase keys; preserve values literally without percent-decoding; empty input means no headers<br>tls_ca_file, tls_cert_file and tls_key_file require an https endpoint for both grpc and http/protobuf; tls_cert_file and tls_key_file must occur together |

### CLICKHOUSE client

| Capability | Declaration |
| --- | --- |
| Kind | CLICKHOUSE |
| Connection pooling | unpooled |
| CONFIG owner class | Nervix |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | URL (http or https) | Nervix; strict | no | none |
| `database` | optional | string | Nervix; strict | no | none |
| `password` | optional | string | Nervix; strict | yes | none |
| `pool_idle_timeout_ms` | optional; default `2000` | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tcp_keepalive_ms` | optional; default `60000` | whole seconds from 1000 through 32767000 milliseconds | Nervix; strict | no | none |
| `timeout_ms` | required | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `tls_ca_file` | required when `addr` uses `https` | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |
| `user` | optional | string | Nervix; strict | no | none |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### POSTGRES client

| Capability | Declaration |
| --- | --- |
| Kind | POSTGRES |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | driver address, including its delegated query options | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### MYSQL client

| Capability | Declaration |
| --- | --- |
| Kind | MYSQL |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | driver address, including its delegated query options | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### MONGODB client

| Capability | Declaration |
| --- | --- |
| Kind | MONGODB |
| Connection pooling | pooled; explicit POOL SIZE MIN &lt;minimum&gt; MAX &lt;maximum&gt; |
| CONFIG owner class | Nervix; address query delegated to driver |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `addr` | required | driver address, including its delegated query options | Nervix; strict | no | none |
| `database` | optional | string | Nervix; strict | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |

Delegated property namespaces: none; only declared keys are accepted. Reserved keys: none.

### S3 client

| Capability | Declaration |
| --- | --- |
| Kind | S3 |
| Connection pooling | unpooled |
| CONFIG owner class | declared keys and driver-delegated property namespaces |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `access_key_id` | optional | string | Nervix; strict | yes | none |
| `allow_anonymous` | optional | boolean | Nervix; strict | no | none |
| `client_region` | optional | string | Nervix; strict | no | none |
| `disable_config_load` | optional | boolean | Nervix; strict | no | none |
| `disable_ec2_metadata` | optional | boolean | Nervix; strict | no | none |
| `endpoint` | optional | URL (http or https) | Nervix; strict | no | none |
| `path_style_access` | optional | boolean | Nervix; strict | no | none |
| `region` | optional | string | Nervix; strict | no | none |
| `secret_access_key` | optional | string | Nervix; strict | yes | none |
| `session_token` | optional | string | Nervix; strict | yes | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |

Delegated property namespaces: s3.*, client.*. Reserved keys: none.

### GCS client

| Capability | Declaration |
| --- | --- |
| Kind | GCS |
| Connection pooling | unpooled |
| CONFIG owner class | declared keys and driver-delegated property namespaces |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `allow_anonymous` | optional | boolean | Nervix; strict | no | none |
| `credentials_json` | optional | string | Nervix; strict | yes | none |
| `disable_config_load` | optional | boolean | Nervix; strict | no | none |
| `disable_vm_metadata` | optional | boolean | Nervix; strict | no | none |
| `no_auth` | optional | boolean | Nervix; strict | no | none |
| `service_path` | optional | URL (http or https) | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |
| `token` | optional | string | Nervix; strict | yes | none |

Delegated property namespaces: gcs.*. Reserved keys: none.

### AZURE_BLOB client

| Capability | Declaration |
| --- | --- |
| Kind | AZURE_BLOB |
| Connection pooling | unpooled |
| CONFIG owner class | declared keys and driver-delegated property namespaces |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `account_key` | optional | string | Nervix; strict | yes | none |
| `account_name` | optional | string | Nervix; strict | no | none |
| `authority_host` | optional | string | Nervix; strict | no | none |
| `client_id` | optional | string | Nervix; strict | no | none |
| `client_secret` | optional | string | Nervix; strict | yes | none |
| `sas_token` | optional | string | Nervix; strict | yes | none |
| `tenant_id` | optional | string | Nervix; strict | no | none |
| `timeout_ms` | optional | positive, schedulable whole milliseconds | Nervix; strict | no | none |

Delegated property namespaces: adls.*. Reserved keys: none.

### ICEBERG_REST client

| Capability | Declaration |
| --- | --- |
| Kind | ICEBERG_REST |
| Connection pooling | unpooled |
| CONFIG owner class | declared keys and driver-delegated property namespaces |
| Statement options | No entity-specific options. |

| CONFIG key | Presence | Value | Owner | Confidential | File role |
| --- | --- | --- | --- | --- | --- |
| `audience` | optional | string | driver; best effort | no | none |
| `credential` | optional | string | driver; best effort | yes | none |
| `disable-header-redaction` | optional | boolean | driver; best effort | no | none |
| `oauth2-server-uri` | optional | URL (http or https) | Nervix; strict | no | none |
| `prefix` | optional | string | driver; best effort | no | none |
| `resource` | optional | string | driver; best effort | no | none |
| `scope` | optional | string | driver; best effort | no | none |
| `tls_ca_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS trust; follows the connector's documented root policy |
| `tls_cert_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS certificate; requires tls_key_file |
| `tls_key_file` | optional | file path, with resource mount templates resolved before use | Nervix; strict | no | TLS private key; requires tls_cert_file |
| `token` | optional | string | driver; best effort | yes | none |
| `uri` | required | URL (http or https) | Nervix; strict | no | none |
| `warehouse` | optional | string | Nervix; strict | no | none |

Delegated property namespaces: header.*, s3.*, client.*, gcs.*, adls.*. Reserved keys: none.
