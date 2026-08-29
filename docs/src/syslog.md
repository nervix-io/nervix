# Syslog

Nervix separates the syslog message format from syslog network transport:

- a `SYSLOG` codec decodes RFC 3164 and RFC 5424 messages and encodes RFC 5424 messages
- a `TYPE SYSLOG` client declares a UDP, TCP, or TLS socket endpoint
- a `FROM SYSLOG` ingestor receives transport frames and passes each frame to its declared codec
- a `TO SYSLOG` emitter sends codec output with the selected syslog transport framing

The pieces are intentionally independent. A Kafka ingestor can decode a syslog payload with a
`SYSLOG` codec, and a syslog listener can pass its frames to a JSON codec. Pairing the codec and
transport creates a conventional syslog receiver or forwarder. `SYSLOG` is a reserved NSPL
keyword.

## Codec

Create a syslog codec without a wire schema:

```nspl
CREATE CODEC syslog_codec
  FROM SYSLOG
  TO SCHEMA syslog_event;
```

A syslog codec does not accept a wire-schema reference, `WITH JAQ TRANSFORMATIONS`, or
`ENCODE <field> AS RFC3339` rules.

### Fixed field contract

The target schema may declare any subset of these fields. Every declared field must use the exact
name, type, and optionality shown here.

| Field | Required schema shape | Decode source | RFC 5424 encoding |
| --- | --- | --- | --- |
| `facility` | `U8` | PRI divided by 8 | Required for encoding; must be at most 23 |
| `severity` | `U8` | PRI modulo 8 | Required for encoding; must be at most 7 |
| `timestamp` | `DATETIME OPTIONAL` | RFC 5424 timestamp or RFC 3164 timestamp | RFC 3339 UTC, or `-` when null or undeclared |
| `hostname` | `STRING OPTIONAL` | `HOSTNAME` | Value, or `-` when null or undeclared |
| `app_name` | `STRING OPTIONAL` | RFC 5424 `APP-NAME` or RFC 3164 `TAG` | Value, or `-` when null or undeclared |
| `proc_id` | `STRING OPTIONAL` | `PROCID` | Value, or `-` when null or undeclared |
| `msg_id` | `STRING OPTIONAL` | `MSGID` | Value, or `-` when null or undeclared |
| `structured_data` | `STRING OPTIONAL` | Exact RFC 5424 `STRUCTURED-DATA` text | Validated and emitted verbatim, or `-` when null or undeclared |
| `message` | `STRING` | `MSG`, including an empty message | Required for encoding |

A field outside this contract, or a field with different type or optionality, makes `CREATE CODEC`
fail and identifies the offending field and expected shape. Fields may be marked `SENSITIVE` in
the schema; normal sensitivity propagation and explicit external-leakage rules still apply.

Fields omitted from the schema are parsed and discarded during decoding. During encoding, omitted
optional header fields and structured data become the RFC 5424 NILVALUE `-`.

An example schema that supports both decoding and encoding is:

```nspl
CREATE SCHEMA syslog_event (
  facility U8,
  severity U8,
  timestamp DATETIME OPTIONAL,
  hostname STRING OPTIONAL,
  app_name STRING OPTIONAL,
  proc_id STRING OPTIONAL,
  msg_id STRING OPTIONAL,
  structured_data STRING OPTIONAL,
  message STRING
);

CREATE CODEC syslog_codec
  FROM SYSLOG
  TO SCHEMA syslog_event;
```

### Decoding

The codec detects RFC 5424 or RFC 3164 for each message. The choice is not configurable.

- the payload must be non-empty UTF-8 after trailing carriage returns, line feeds, and NUL bytes
  are removed
- an absent or unparseable PRI uses priority 13: facility 1 and severity 5
- an RFC 3164 timestamp uses the receiving node's current year and is interpreted as UTC
- RFC 5424 NILVALUE fields decode as typed nulls
- `structured_data` retains the exact bracketed and escaped RFC 5424 text; RFC 3164 produces null
- `message` may be an empty string

A decode failure skips that frame, records the normal decode diagnostic, and continues with the
next frame.

### Encoding

Encoding always produces RFC 5424 version 1 without a byte-order mark. A codec is encode-capable
only when its schema declares `facility`, `severity`, and `message`. Graph validation rejects a
syslog emitter using a codec that omits any of them and lists the missing fields.

Timestamps are rendered in UTC. Nanosecond values are truncated to RFC 5424's maximum six
fractional digits, and trailing fractional zeroes are omitted.

Record-specific encoding failures follow the emitter's `ON MESSAGE ERROR` policy. These include:

- a facility above 23 or severity above 7
- `hostname`, `app_name`, `proc_id`, or `msg_id` containing characters outside printable US-ASCII
  or exceeding its RFC 5424 length limit
- `structured_data` that is neither `-` nor a well-formed sequence of structured-data elements

## Client

A syslog client describes one direction-dependent socket endpoint:

```nspl,ignore
CREATE CLIENT <name>
  TYPE SYSLOG [MOUNT <resource>]
  CONFIG {
    'protocol' = 'udp'|'tcp'|'tls',
    'addr' = '<host:port>',
    ...
  };
```

One client may be referenced by both ingestors and emitters. An ingestor binds `addr`; an emitter
connects or sends to it.

| Config key | Required | Meaning |
| --- | --- | --- |
| `protocol` | Yes | `udp`, `tcp`, or `tls` |
| `addr` | Yes | `host:port`; bind address for ingestion and destination for emission |
| `max_message_size` | No | Ingested-message byte cap; defaults to `131072` |
| `framing` | No | TCP emitter framing: `octet-counting` by default or `non-transparent` |
| `tls_cert_file` | Directional | TLS ingestor server certificate; optional emitter client certificate |
| `tls_key_file` | Directional | Private key paired with `tls_cert_file` |
| `tls_ca_file` | No | Ingestor client-certificate CA, or emitter server trust root |

TLS file paths use normal client resource mounts. For example:

```nspl
CREATE CLIENT syslog_tls
  TYPE SYSLOG MOUNT syslog_identity
  CONFIG {
    'protocol' = 'tls',
    'addr' = '0.0.0.0:6514',
    'tls_cert_file' = '{{ syslog_identity }}/server.crt',
    'tls_key_file' = '{{ syslog_identity }}/server.key',
    'tls_ca_file' = '{{ syslog_identity }}/clients-ca.crt'
  };
```

For a TLS ingestor, `tls_cert_file` and `tls_key_file` are both required. Supplying
`tls_ca_file` additionally enables mutual TLS and requires every client to present a certificate
that chains to that CA. Without it, the server does not request client certificates.

For a TLS emitter, the certificate and key are optional, but must appear together when used for
client authentication. `tls_ca_file` adds a server trust root; platform roots are used when it is
absent. TLS verifies the server name derived from `addr`.

Configuration fails with a key-specific error for an unknown protocol, malformed address,
`framing` on UDP, non-transparent framing on TLS, a missing TLS server identity, or only one half
of an emitter client identity. TLS keys are invalid with UDP or plain TCP.

## Ingestor

The syslog source has one delivery mode and no `INSTANCES` clause:

```nspl,ignore
FROM SYSLOG <client>
MODE NO_ACK SEQUENTIAL
ON QUIESCE SUSPEND
  | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
DECODE USING <codec>
```

Each scheduled ingestor owns one listener on its node. Routes, branch construction, flush policy,
and error policy follow the normal ingestor contract.

Every received message exposes its remote socket address as optional `STRING`
`metadata.peer_addr`. For UDP this is the datagram source; for TCP and TLS it is the connection
peer. Transport metadata does not propagate through a relay unless a route assigns it to a
schema-backed field:

```nspl,ignore
TO received_syslog
  INHERIT ALL
  SET peer_addr = metadata.peer_addr
  UNBRANCHED
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
```

### UDP

UDP follows RFC 5426. One datagram is one frame. A datagram larger than `max_message_size` is
dropped and logged at debug level.

### TCP

TCP follows RFC 6587. The listener accepts any number of concurrent connections and detects
framing for every frame:

- a frame beginning with a digit uses octet counting
- any other frame is terminated by LF; a CR immediately before the LF is removed
- both framing forms may be interleaved on one connection

An octet count must be a positive decimal number with at most ten digits. A malformed or oversized
octet-counted frame, or an oversized non-transparent frame, closes that connection because the
stream cannot be resynchronized safely. Other connections and the listener continue unaffected.
Per-connection frame memory is bounded by `max_message_size`, and completed frames enter a bounded
intake queue.

### TLS

TLS follows RFC 5425: the TCP listener behavior runs over a TLS session and frames use octet
counting. A failed handshake closes only that connection. The client-authentication behavior is
controlled by `tls_ca_file` as described above.

### Quiesce and failures

`SUSPEND` stops reading. UDP datagrams may accumulate in and overflow the kernel receive buffer;
TCP and TLS apply transport backpressure and retain transport-buffered data for resume. `BUFFER`
and `DROP` continue reading and apply the normal bounded local quiesce behavior.

Syslog transport is at-most-once at intake. The listener sends no application acknowledgment, and
datagrams dropped or stream data lost before intake are not recoverable.

Bind and listener-level socket failures appear as the ingestor transient error and retry with the
standard reconnect backoff. Recovery clears the error and resets the backoff. A malformed message
body is a codec decode failure and skips only that frame.

## Emitter

The syslog sink requires a codec and supports only `NO_ACK`:

```nspl,ignore
TO SYSLOG <client>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  ENCODE USING <codec>
```

Header invocations are not supported. Every emitter route still requires its normal flush policy,
message error policy, and general error policy. Branch identity collapses only after successful
external emission.

### UDP

Each encoded record is one datagram. An encoded payload above 65,507 bytes is a message error.

### TCP

The emitter keeps one persistent connection. `octet-counting` writes the decimal byte length, one
space, and the payload. `non-transparent` writes the payload followed by LF; an encoded payload
that already contains LF is a message error in this mode.

### TLS

The emitter uses octet-counted framing over one verified TLS connection. RFC 5425 does not permit
non-transparent TLS framing.

For every transport, success means the local socket accepted and flushed the complete frame. No
remote delivery acknowledgment exists. Connection establishment and write failures are
infrastructure errors: the current batch is retained, the connection is rebuilt, and delivery is
retried on the declared backoff. Records accepted before a later failure in the same batch may be
delivered more than once.

## Observability and limits

Syslog ingestors and emitters use the standard `DESCRIBE INGESTOR` and `DESCRIBE EMITTER` runtime
state, transient-error, reconnect-backoff, edge-metric, and emitter-metric surfaces. `SHOW CREATE`
renders canonical syslog declarations, and normal ALTER quiesce classification applies. Lifecycle
transitions are info-level events; per-frame and per-connection details are debug or trace and do
not include payload values.

| Limit | Value |
| --- | --- |
| Ingested message | `max_message_size`, default 131,072 bytes |
| UDP datagram intake | 65,535 bytes, further limited by `max_message_size` |
| UDP payload emission | 65,507 bytes |
| Octet-counting prefix | At most 10 digits |
| Concurrent TCP/TLS connections | Unbounded count; bounded per-connection frame memory and bounded intake queue |
