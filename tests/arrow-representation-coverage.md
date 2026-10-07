# Arrow, Codec And Interconnect Representation Coverage

Every target is registered in `tests/bolero-targets.toml` for ordinary randomized/corpus checks and
sanitizer libFuzzer. The same bounded generators, production encoders and decoders and complete
assertions run in both modes. The properties run in memory: no target opens a socket, a broker or a
database. All retained inputs contain synthetic data and describe the current shapes.

The server targets build their batches from one test-only generator,
`src/runtime_schema/generated_batches.rs`. It draws a schema of one to four fields over every
current field type, with `VEC` and `ARRAY` nested two levels deep, optional and sensitive fields,
and a batch of zero to five rows cut out of a batch with up to two more rows on either side, so a
carrier meets columns whose data starts inside a larger allocation. Values land on the integer
extremes, on signed zeros, subnormals, the float extremes and NaN payloads, on the whole signed
nanosecond range, on empty and Unicode text with escapes and control characters, and on empty and
arbitrary bytes. A null fixed-size list value owns elements that may themselves be null, and a null
list value may own elements, as Arrow allows. Its oracle compares two batches by their schemas,
field and schema metadata included, their row counts, and every cell as a reader sees it: a null,
or a value with floats compared by their bits and lists compared element by element.

An ordinary randomized run hands a property at most 64 bytes, and a choice read after they run out
takes its first option. Every property here therefore reads what shapes its case before the case:
the damage and the share of the encoding where it lands, the defect a payload carries, the message
damaged bytes are read as, how a stream ends, whether the batch is branched and which rows are
registered, and the format and first damaged payload of a payload group. What can wait is read after
the case, so that it takes none of the bytes an ordinary run has for a schema and its rows: the
further damaged payloads of a group. The batch is generated from the bytes that remain, and the
`boundaries` and `ordered` seeds of every corpus carry a case of full size.

Arrow's Rust writer writes every generated stream here, and it takes none of the freedoms the Arrow
format leaves a writer: it writes a validity bitmap for every column with rows and exactly one
offset more than a column has rows. Arrow's C++, Go and JavaScript writers omit the bitmap of a
column without nulls, the Go and JavaScript writers write a bitmap longer than its rows need, the
JavaScript writer leaves more offsets than a column has rows, and the Java writer writes no offset
for a column without rows. `a_stream_another_arrow_writer_would_write_decodes_to_its_rows` and
`a_generated_pool_another_arrow_writer_would_write_decodes_to_its_column` hold the producer batch
and generated pool decoders, the two that read what another Arrow library wrote, to one hand-built
stream for each of the four.

| Representation | Target | Complete oracle |
| --- | --- | --- |
| Relay body: one Arrow IPC section of a relay batch, decoded against the relay's schema or without one | `relay-arrow-bodies` | The decoded batch is the encoded one; a body of several sections is refused by the decoders that take one section and concatenated by the one that takes any number; no charge outlives a body |
| Sealed snapshot section: one Arrow IPC section of a runtime snapshot | `relay-arrow-bodies` | The decoded section is the encoded batch under its expected schema |
| Damaged relay bodies and snapshot sections | `relay-arrow-bodies-malformed` | `Decode`, `TooManySections`, `NoSection` or `DecodedTooLarge`, never `Admission` or `Execution`; or a batch of the expected schema that encodes and decodes back to itself |
| Producer batch: the canonical Arrow IPC stream the client library writes and the node decodes | `client-producer-batches` | The decoded batch is the submitted one; the client library and the node map the ingestor's fields to one Arrow schema; limits equal to the batch admit it and one row or byte less refuses it with the size and the limit |
| Damaged producer batches | `client-producer-batches-malformed` | A refusal that names a defect of the batch, never a busy node; or a batch of the exact schema within the row limit that the client library writes and the node decodes back to itself |
| Generated column pool: the Arrow IPC stream a WASM guest writes beside its routed outputs | `wasm-generated-pools` | The decoded pool is the written one, unnamed fields with their types, nullability and metadata and every row, value and null, whether the stream ends with the end-of-stream marker or is closed after its record batch |
| Damaged generated pools | `wasm-generated-pools-malformed` | A typed defect of the guest's generated Arrow IPC or of its record batch count; or one record batch of unnamed columns that writes and decodes back to itself; only the empty byte string is no pool |
| `WIRE JSON`, `WIRE CBOR` and `WIRE AVRO` rows | `codec-schemaful-rows` | Every row of a batch, encoded one payload per row by a codec of a shape the registry accepts and decoded into one group builder, is the original row |
| Groups of payloads with damaged members | `codec-schemaful-payload-groups` | Each payload is accepted or refused in a group exactly as alone, with a typed failure of its own bytes, and the group holds exactly the rows its accepted payloads decode to alone |
| `WIRE JSON` and `WIRE CBOR` rows holding non-finite floats | `codec-json-non-finite-floats` | The projection, not a round trip: a non-finite float of an optional top-level field reads back as a null with every other value intact, and one in a required field or a list element refuses the payload with a typed failure |
| Routed relay payload from the sending runtime to the receiving runtime's rows | `remote-relay-rows` | The rows, the concrete branch or its absence with key values to the bit, every row's watermarks and one registration per row |
| Routed payloads whose parts disagree | `remote-relay-rows-malformed` | The first defect in the receiver's order: the body, the watermark count, the registration count, then the branch key, before any row forms a batch |
| Relay grant request, its relay metadata and the payload the receiver rebuilds, grant replies, admission exchanges, acknowledgement resolutions and connection bindings | `relay-wire-messages` | Every field through the bounded rkyv codec under the class and limit the transport uses, branch key floats by their bits, and no charge outlives a message |
| Damaged relay and acknowledgement messages | `relay-wire-messages-malformed` | The codec's typed decode failure within the depth bound, or a message that encodes and decodes back to itself |

The earlier Arrow targets remain the owners of their paths: `client-arrow-rows` and
`client-arrow-selection` for Row subscriptions, `client-binding-rows` and `client-ffi-host-columns`
for the shared binding, the WASM targets in the
[WASM representation coverage map](./wasm-representation-coverage.md) for guest envelopes, and the
backup and storage targets in the
[storage representation coverage map](./storage-representation-coverage.md) for archived and stored
Arrow sections. A remote hash map answer and a client emitter delivery travel as the relay body's
Arrow IPC stream, which `relay-arrow-bodies` covers through its schema-free decoder.

## Codec Domains

- `WIRE JSON` and `WIRE CBOR` preserve every value of every field type except a non-finite float:
  JSON has no number for one, and the CBOR decoder reads values through the JSON model. The JSON
  encoder writes a non-finite float as `null`, which reads back as a null of an optional field and
  refuses a required field or a list element; the CBOR encoder writes the float, and the decoder
  reads it as a null. Their domain therefore holds finite floats of every bit pattern. A JSON or
  CBOR number beyond the range of an `F32` field rounds to an infinity, as the nearest float.
- The JSON and CBOR readers read a number as a 64-bit float before narrowing it to an `F32` field.
  Where that float lies exactly halfway between two `F32` values, the decoder names the one whose
  shortest decimal reads as the same float, so every `F32` the encoder writes reads back to its
  bits. An exhaustive check of all 2^32 bit patterns found two that rounding half to even would
  otherwise change, `±7.038531e-26`; `a_number_reads_as_the_f32_whose_shortest_decimal_it_is` and
  the scenario outline **A `<wire_format>` number reads as the F32 whose shortest decimal it is**
  retain that case.
- An exact `U8` through `I64`, `F32`, `F64` or `DATETIME` JSON or CBOR wire type binds only the
  internal type of its name; the generic `integer`, `number`, `string` with `ENCODE ... AS
  RFC3339`, `boolean`, `bytes` and `array` types bind the types the registry lists for them. The
  generator draws both, and `generated_schemaful_codecs_are_accepted` holds every generated codec
  to the registry's own validation.
- A payload refused while a nested list value is being appended closes that list value inside the
  abandoned row, so its elements never become the first elements of the next row's list. The
  payload-group property and the scenario outline **Kafka NO_ACK keeps the nested lists of a
  collected ingest group exact when a `<wire_format>` message is refused inside one** retain that
  case.
- `WIRE AVRO` preserves every float bit pattern. A top-level field holds a type an Avro wire type
  binds, and an unsigned element of a list is written as an Avro `long`, so the generated domain
  holds unsigned values up to `i64::MAX`; a larger one is refused when it is encoded.
- Every format writes a datetime as RFC 3339 text in UTC, which reads back to the same nanosecond
  from 1677 through 2262, and bytes as padded base64 text in JSON and CBOR and as octets in Avro.
- A wire schema lists its fields in any order. Avro writes them in that order, and every format
  decodes into the internal schema's column order.
- The JAQ-native, protobuf and `SYSLOG` codecs are projections, not lossless round trips: a JAQ
  program decides the shape both ways and numbers pass through serde JSON, the protobuf JSON view
  omits default values, and `SYSLOG` keeps microseconds in UTC and its own header rules. Their
  owning tests state those contracts.

## Contract Boundaries

- Every Arrow IPC stream from outside the node, a relay body, a snapshot section, a producer batch
  or a WASM guest's generated pool, passes one scan before Arrow's reader reads it, and the scan
  alone opens the reader: continuation markers and lengths inside the stream, the schema message
  first with only the field types Nervix carries, uncompressed record batches that declare exactly
  the field nodes and buffers that schema's fields take with every buffer inside its message body,
  and the end-of-stream marker ending the stream. A generated pool may also end where its last
  message ends, as the Arrow format allows a writer that closes its stream, and bytes after its
  marker are the pool's trailing bytes.
- Arrow's reader trusts what a stream declares and panics where the scan now refuses: on a buffer
  past its body, which `relay-arrow-bodies-malformed` found, on a field type or type parameter it
  does not implement, a list without its child, a dictionary encoding without its index type, a
  schema without its field list, a validity bitmap shorter than the nulls it is declared to hold,
  an offsets buffer that ends inside an offset, variadic buffer counts no field takes and a
  fixed-size list too long to count. It also allocates each message's metadata and body from their
  declared lengths before reading them, so a message that declares a body of 2^60 bytes aborts the
  process; the scan refuses a declared length that reaches past the stream before anything is
  allocated. One hand-built stream for each lives in `src/runtime_schema/crafted_streams.rs`, and
  `a_body_arrows_reader_would_panic_on_is_refused_before_it_is_read`,
  `a_stream_arrows_reader_would_panic_on_is_refused_with_its_defect` and
  `a_generated_pool_arrows_reader_would_panic_on_is_refused_however_it_ends` hold every decoder to
  the scan's exact refusal of each; the inputs the malformed targets found are retained corpus
  regressions. On a guest's pool such a panic ended the processor's task; the scenario outline
  **A WASM processor output whose generated column `<defect>` reports a runtime error** retains a
  buffer past its body, an integer of 7 bits, a null counted without a validity bitmap and a body
  declared longer than its stream.
- The transport receives exactly the body length the grant declared, so a relay body cannot lose
  bytes in transit.
- A stream of no record batch decodes, where any number of sections is accepted, as an empty batch
  of the schema it declares.
- A decoder that takes any schema, the schema-free relay decoder and the generated pool's, accepts
  a timestamp whose zone name is empty, which Arrow's writer writes back as a timestamp without a
  zone. No schema of the node's own declares such a column, and every decoder with an expected
  schema refuses it, so the damaged-input properties compare a batch read back from the writer
  with the original as the writer writes it: every field, nullability, nested field and metadata
  entry, every row and every value, with that one zone spelling set aside.
  `a_rewritten_batch_differs_only_in_an_empty_timestamp_zone` holds the oracle to exactly that.
- A schemaful CBOR payload is one data item and an Avro payload is one datum; bytes after them are
  not read. A JSON payload must hold exactly one object, and a JSON object that repeats a key keeps
  one of its values.
- A remote branch key is read field by field and keyed by name, so a key that repeats a field keeps
  its last value, and a key is not checked against the branch its relay declares.
- The relay body travels as HTTP/2 data frames the transport reassembles by appending them, and a
  client consumer stream reassembles its delivery chunks inside the serving loop. Neither has a
  pure reassembly API a property could split arbitrarily; the interconnect simulation owns
  fragmentation of the transport.
- A body, a stream or a message is generated within the property's input limit, so the decoders'
  configured byte limits are reached by the deterministic boundary tests beside the properties
  rather than by generated values.
