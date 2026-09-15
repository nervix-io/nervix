# JAQ codec unfolding

Status: implemented. This document defines the current contract for decoding one payload through a
JAQ-backed codec into a stream of messages.

## Required outcome

A JAQ-backed codec decodes a payload as a stream. The payload is parsed into the values its format
contains, the `ON INGESTION` program runs on each value, and every value the program produces
becomes one message in the ingestor's graph. A payload carrying a JSON array therefore feeds each
element to the graph as its own message when the program is `.[]`, a body of newline-delimited JSON
feeds one message per line with `.`, and a program that selects nothing from a payload feeds no
message at all.

Before this contract, the program had to produce exactly one JSON object per payload, and a payload
had to parse to exactly one value; anything else was rejected. This contract removes both
requirements for ingestion and defines what a source, the graph, and an operator observe when a
payload becomes zero, one, or many messages. The ordering, acknowledgement, failure, and limit rules
below are the complete contract; no part of unfolding is left to an individual source connector.

The contract applies to every JAQ-backed codec: the JAQ-native formats `JSON`, `YAML`, `TOML`,
`XML`, and `CBOR`, and `PROTOBUF`. Schemaful codecs declared over `WIRE JSON|CBOR|AVRO SCHEMA` and the
`SYSLOG` codec keep their one-payload-one-message contract; an array payload remains a decode failure
for them.

Nothing in NSPL changes. `CREATE CODEC` keeps its grammar, `SHOW CREATE CODEC` renders the same
definition, and persisted Models keep their shape. A codec whose program yields exactly one object
per payload behaves as it did before, apart from the diagnostic wording described under "Failure
semantics". The only definitions whose outcome changes are the ones that previously failed.

## Concepts

- **Payload.** The unit a source hands to its codec: one Kafka, Pulsar, RabbitMQ, SQS, MQTT, NATS,
  Redis, or ZeroMQ message, one request body on an HTTP endpoint, one response body of a polling
  HTTP ingestor, one WebSocket frame, one syslog frame, one flattened Prometheus sample, or one line
  of a hash map resource file. The payload is the unit of source acknowledgement, of quiesce
  buffering, and of decode failure.
- **Input value.** One value parsed from the payload in the codec's format. A payload holds a stream
  of zero or more input values.
- **Output.** One value the `ON INGESTION` program produces for one input value. A program produces
  a stream of zero or more outputs per input value.
- **Message.** One output decoded into the codec's internal schema. Messages are what
  `FILTER WHERE`, routes, branches, relays, and acknowledgements operate on.
- **Unfolding.** The decoding of one payload into its message stream: input values in payload
  order, and each value's outputs in program order.

## Decoding contract

### Parsing a payload into input values

Formats differ in how many values one payload can hold:

- `JSON`: a sequence of JSON values separated by whitespace. This covers a single document,
  newline-delimited JSON, and concatenated JSON.
- `CBOR`: a sequence of consecutive CBOR data items.
- `YAML`: a stream of documents.
- `XML`: the document's root element. A declaration, document type, comment, or processing
  instruction outside the root element is not an input value, and a second root element is
  malformed, so an XML payload holds at most one input value.
- `TOML`: exactly one document.
- `PROTOBUF`: exactly one message of the declared type.

A `JSON`, `CBOR`, `YAML`, or `XML` payload that contains no value is a stream of zero input values.
An empty request body or an empty broker message therefore produces no messages and is not a decode
failure. A `TOML` or `PROTOBUF` payload always yields one input value: an empty payload is an empty
document or an all-defaults message, and whether the program's output fits the schema decides the
outcome.

Parsing stops at the first malformed value, and the whole payload is rejected as described under
"Whole-payload atomicity". A text format payload that is not valid UTF-8 is malformed at its first
input value.

### Running the program

`ON INGESTION` runs once per input value, in payload order. All of the program's outputs for that
value join the message stream in the order the program produces them, before any output of the next
input value. jq stream semantics apply without modification: `.[]` yields one output per element,
`select` and `empty` yield nothing for the values they reject, and a comma yields several outputs.

Each output must be a JSON object. It is decoded into the internal schema exactly as a single output
was decoded before: surplus keys are dropped, an absent or `null` value is accepted only for an
`OPTIONAL` field, and every value must match its field's type exactly.

Unfolding exposes no positional metadata. A message that needs the position of its element within
the payload computes it in the program, for example with `range(length) as $i | .[$i] + {index: $i}`.

### Whole-payload atomicity

A payload is decoded as a whole or rejected as a whole. When any step fails, no message from that
payload is delivered, including messages produced before the failure, and the ingest group the
payload was joining is left exactly as it was before the payload arrived. The failures are:

- a malformed input value at any position
- an evaluation error of the program on any input value
- an output that is not an object
- an output that cannot be decoded into the internal schema
- more messages than the unfold limit

A rejected payload is a decode failure of that payload. It is not a route message error, because no
message exists to route. What a decode failure means for each source is stated under "Failure
semantics".

### Unfold limit

One payload unfolds into at most 65,536 messages. A program that produces more is stopped at the
limit, and the payload is rejected with a diagnostic naming the limit. The limit bounds the work and
memory one payload can commit a node to when a program is unbounded, such as `repeat` or an
open-ended `range`. The value is the largest batch size Nervix's message histograms track, so a
payload at the limit is still observable as one batch. A producer whose payloads legitimately carry
more elements splits them.

## Ordering and grouping

Messages unfolded from one payload enter the ingestor's source ingest group contiguously, in unfold
order, after every message from earlier payloads of the same source instance. A payload never spans
two groups.

A group closes as soon as it holds 1,024 or more messages, or when the source goes quiet for 5 ms,
whichever comes first, and a payload that unfolds beyond the remaining capacity closes the group
above 1,024. The first Arrow batch built from external input is therefore bounded by 1,023 messages
plus one payload's messages, at most 66,559. A payload that unfolds into no messages adds nothing to
the group and does not delay its idle close. Route `FLUSH` policies rebatch downstream and are
unaffected.

Because a payload's messages share one group, they share one domain execution snapshot:
`TIMESTAMP NOW` records the same instant for every message of the payload, and admission evaluates
them against the same domain state. `TIMESTAMP AT <field>` reads each message's own field.

## Per-message semantics

Every message unfolded from a payload is an ordinary ingested message:

- `FILTER WHERE` decides per message. Some messages of a payload may be filtered while others
  continue.
- Each route's `INHERIT`, `SET`, and `WHERE` run per message, and `BRANCHED BY` constructs each
  message's branch key from its own fields. Messages from one payload may reach different routes and
  different concrete branches.
- A construction failure is a message error of that message alone, handled by the route's
  `ON MESSAGE ERROR` policy. The other messages of the payload are unaffected.
- `metadata.<field>`, `read_header`, and `read_headers` describe the source payload, so every
  message of a payload sees the same topic, partition, offset, peer address, and headers.
- Ingestor received and sent metrics count messages, so a payload that unfolds into `n` messages
  counts `n`. The quiesce buffer, drop, and reject families keep counting payloads, because they
  measure raw intake.
- Session subscriptions deliver each message on its own.

## Acknowledgement

Acknowledgement stays at payload granularity, because the payload is what the external source knows.

- A payload is acknowledged to its source when every message unfolded from it has been acknowledged
  on every route each message reached, including the handling of any message error a route produced
  for it. A filtered message counts as acknowledged when it is filtered.
- A payload that unfolds into no messages is acknowledged as soon as it decodes.
- A negative acknowledgement of any message of a payload negatively acknowledges the payload. The
  source's delivery mode then treats the payload as it treats any negatively acknowledged payload,
  for example by retrying under its `RETRY POLICY` or by broker redelivery. Messages of that payload
  that had already been acknowledged are delivered again with it. This is the at-least-once contract
  at the granularity the source already has.
- Delivery windows count payloads. `ACK PARALLEL MAX <n>` admits `n` payloads in flight however many
  messages each unfolds into, and `BATCH TIMEOUT` is unchanged.
- Quiesce and planned ownership handoff are unchanged. Buffered payloads decode at delivery with the
  codec then in effect, and a handoff waits for the admitted payloads' acknowledgement roots to
  resolve, which covers all of their messages.

## Failure semantics

A rejected payload is handled exactly as any payload that fails to decode in that source and delivery
mode. The failure is reported as a runtime error event to attached sessions and logged. It is not a
route message error, and no error record is produced for it. `NO_ACK` modes continue with the next
payload, and acknowledged modes apply their retry and redelivery behavior. An HTTP endpoint still
answers `202`, which means that the body was accepted, not that it decoded or how many messages it
produced. A source that hands several payloads over as one unit, such as one Prometheus evaluation,
keeps its all-or-nothing handling of that unit.

The decode diagnostic identifies the codec, the cause, the zero-based position of the input value
within the payload, and the zero-based position of the output within that value's outputs when an
output is at fault. The unfold limit diagnostic names the limit instead of a position.

Payload values never appear in the diagnostic. A value that does not fit its field is described by its
JSON kind, a YAML scalar that conflicts with its tag is described by its position, and a program
evaluation error is reported without the evaluator's own message, which quotes the values it failed
on. That message is available in trace-level logs, where Nervix records payload-bearing detail.

A hash map file line that fails rejects the hash map's creation with the same diagnostic, extended
with the line number.

## Hash maps

A hash map resource file is still read line by line, and each line is one payload. A line decoded
through a JAQ-backed codec contributes every message it unfolds into as an entry, in unfold order,
and a line that unfolds into no messages contributes no entry. When two entries carry the same key,
whether from two lines or from one, the later entry replaces the earlier one, as it does for
duplicate lines. `DESCRIBE HASH MAP` reports the number of entries, which counts distinct keys.

## Unchanged surfaces

- **Emission.** `ON EMITTING` keeps its contract: it runs once per record and must yield exactly one
  value, which becomes one payload. An emitted record is the unit of emitter acknowledgement,
  batching, and retry, and wire batching belongs to the sink connectors. One record becoming several
  payloads is a different feature and is not part of this contract.
- **Signaling protocols.** `SEND JAQ` programs must still produce exactly one value, `WAIT JAQ` and
  `FAIL JAQ` matchers are satisfied by any output, and a handshake frame is read as exactly one value
  under the parsing rules above. Data frames, both after the handshake and those admitted by
  `ACCEPT DATA`, pass through the ingestor's codec and unfold like any payload.
- **Schemaful and syslog codecs.** One payload is one message. A payload that is not a single object
  of the declared shape is a decode failure.
- **Execution placement.** JAQ decoding still runs on blocking workers, and only the transformation
  leaves the async task.
- **Grammar and Models.** No statement, clause, Model, or stored shape changes.

## Behavior changes

The externally observable differences from the release before this contract are:

1. An `ON INGESTION` program may produce zero or several outputs per input value. Zero outputs
   decode the payload into no messages instead of failing it, and several outputs produce several
   messages instead of failing it.
2. A JSON, CBOR, or YAML payload may contain several values, each of which is an input value. An
   empty JSON, CBOR, YAML, or XML payload decodes into no messages instead of failing.
3. An XML payload or handshake frame is read as its root element, so a declaration, document type,
   comment, or processing instruction outside the root element no longer makes it fail.
4. One payload may produce up to 65,536 messages. More is a decode failure.
5. A source ingest group closes at 1,024 or more messages rather than at exactly 1,024, and never
   splits a payload.
6. A hash map line decoded by a JAQ-backed codec may contribute zero or several entries.
7. A decode diagnostic of a JAQ-backed codec names the zero-based input value and output positions,
   describes a mismatched field value by its JSON kind for every JSON-family codec, and omits the
   program evaluator's message.

The public documentation statements that change are the `ON INGESTION` semantics of Schemas And
Codecs, the output wording of the JAQ Transformations quickstart, the 1,024-message group bound in
Ingestors, Domains And Time, and the NSPL Overview, the line-per-entry description in Lookups, the
codec rules of the NSPL skill, and the HTTP Ingestion quickstart's "each POST body is one record",
which remains true for its schemaful codec and is qualified for JAQ-backed codecs.

## Acceptance criteria

1. An endpoint ingestor with a JSON JAQ codec whose program is `.[]` receives one POST carrying a
   three-element array. The destination relay receives three messages in array order, and a
   subscription sees each one.
2. The same ingestor receives a newline-delimited body of three objects with the program `.`. The
   relay receives three messages in line order.
3. A Kafka ingestor in `ACK SEQUENTIAL` mode receives one message whose payload unfolds into several
   messages. The offset advances only after every unfolded message is acknowledged, and a negative
   acknowledgement of one of them leaves the source message eligible for redelivery.
4. A payload whose second element cannot be decoded into the schema produces no message from that
   payload, the runtime error event names the codec and the zero-based positions without quoting the
   element, and the next payload decodes normally.
5. A payload for which the program selects nothing produces no message, is acknowledged, and leaves
   the next payload unaffected.
6. A payload whose program produces more than 65,536 messages is rejected with a diagnostic naming
   the limit, and the ingestor keeps serving.
7. Messages of one payload construct different branch keys and reach different concrete branches,
   each carrying the same source metadata and headers.
8. A hash map created from a file whose single line unfolds into two entries reports two entries, and
   `LOOKUP` finds both keys.
9. Ingestor received and sent metrics count every message a payload unfolds into.
10. Every runtime criterion holds on one-node and three-node clusters.
