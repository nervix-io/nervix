# Syslog

*Plexus, the Nervix graph definition language.*

Nervix separates the syslog message format from syslog network transport:

- the predefined singleton `SYSLOG` wire schema represents the fixed syslog message format
- a codec that uses that wire schema decodes RFC 3164 and RFC 5424 messages and encodes RFC 5424
  messages
- a `SYSLOG CLIENT` declares a UDP, TCP, or TLS socket endpoint
- a `SYSLOG LISTENER` ingestor receives transport frames and passes each frame to its declared codec
- a `TO SYSLOG` emitter sends codec output with the selected syslog transport framing

The pieces are intentionally independent. A `KAFKA TOPIC` ingestor can decode a syslog payload
with a codec that uses the `SYSLOG` wire schema, and a `SYSLOG LISTENER` ingestor can pass its
frames to a JSON codec.
Pairing the format and transport pieces creates a conventional syslog receiver or forwarder.
`SYSLOG` is a reserved Plexus keyword.

## Predefined wire schema and codec

Reference the predefined wire schema directly from a codec:

```plexus
CREATE CODEC syslog_codec
  FROM SYSLOG
  TO SCHEMA syslog_event;
```

`SYSLOG` itself is the reference to the singleton wire schema. It has no user-defined name or
separate `CREATE`, `SHOW CREATE`, `ALTER`, or `DROP` lifecycle. A codec using it does not accept
`WITH JAQ TRANSFORMATIONS` or `ENCODE <field> AS RFC3339` rules.

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
the target schema; normal sensitivity propagation and explicit external-leakage rules still apply.

Fields omitted from the schema are parsed and discarded during decoding. During encoding, omitted
optional header fields and structured data become the RFC 5424 NILVALUE `-`.

An example schema that supports both decoding and encoding is:

```plexus
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

A codec using the `SYSLOG` wire schema detects RFC 5424 or RFC 3164 for each message. The choice
is not configurable.

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

Encoding through the `SYSLOG` wire schema always produces RFC 5424 version 1 without a byte-order
mark. A codec is encode-capable only when its target schema declares `facility`, `severity`, and
`message`. Graph validation rejects an emitter using such a codec when any field is absent and
lists the missing fields.

Timestamps are rendered in UTC. Nanosecond values are truncated to RFC 5424's maximum six
fractional digits, and trailing fractional zeroes are omitted.

Record-specific encoding failures follow the emitter's `ON MESSAGE ERROR` policy. These include:

- a facility above 23 or severity above 7
- `hostname`, `app_name`, `proc_id`, or `msg_id` containing characters outside printable US-ASCII
  or exceeding its RFC 5424 length limit
- `structured_data` that is neither `-` nor a well-formed sequence of structured-data elements

## Client

A syslog client declares one socket endpoint, which a listener binds and a collector sends to:

```plexus,ignore
CREATE SYSLOG CLIENT <name>
  [MOUNT <resource> VERSION <n>|LATEST]
  CONFIG {
    'protocol' = 'udp'|'tcp'|'tls',
    'addr' = '<host:port>',
    ...
  };
```

One client may be referenced by both ingestors and emitters while it sets only keys both sides read
and its `addr` suits each of them.

A listener binds `addr` as written and asks no resolver: a host name there refuses the statement
that binds the client. A collector resolves the host in `addr` through the node's asynchronous
resolver each time it opens a sender. Literal IPv4 and IPv6 addresses are used directly; hosts-file
names and DNS answers follow the node's configured resolver and TTL policy. Opening a sender spends
one budget, the client's `connect_timeout_ms`, 30 seconds when it is left out: name resolution
first, then the answers in order, each with an equal share of the time left. UDP binds each socket
to the answer's IP family and sends to the first address whose setup succeeds; TCP and TLS use the
first address that accepts a connection, and TLS completes its handshake within that address's
share. A name the resolver cannot answer is the emitter's transient status, with the lookup's
failure, and address attempts that run out of the budget are its transient status naming the key and
its bound. TLS verifies the original host from `addr`, whichever address accepted the connection. A
new sender after a failure resolves again, so an expired answer can be replaced without restarting
the node.

Every key means one thing, and belongs to the side that reads it:

| Config key | Read by | Required | Meaning |
| --- | --- | --- | --- |
| `protocol` | Both | Yes | `udp`, `tcp`, or `tls` |
| `addr` | Both | Yes | `host:port`. A listener's bind address, which must be a literal IP address and port; a collector's destination, which may be a name |
| `framing` | Both | No | TCP or TLS only: `octet-counting` or `non-transparent`, and only `octet-counting` over TLS. A collector writes `octet-counting` without it; a TCP listener reads each frame's own framing without it |
| `connect_timeout_ms` | Both | No | For a collector, positive whole milliseconds opening a new sender may take, name resolution, address attempts and TLS included; for a listener, TLS only, how long each accepted connection's TLS handshake may take. Defaults to 30 seconds |
| `tls_cert_file` | Both | TLS listener | TLS only: a listener's server certificate, or a collector's optional client certificate |
| `tls_key_file` | Both | TLS listener | TLS only: the private key paired with `tls_cert_file` |
| `max_message_size` | Listener | No | The most bytes one message may hold; defaults to `131072` |
| `idle_timeout_ms` | Listener | No | TCP or TLS only: positive whole milliseconds an accepted connection may send nothing while the listener reads it before the listener closes it; unbounded when omitted |
| `tls_client_ca_file` | Listener | No | TLS only: the authority client certificates must chain to, which turns on client-certificate verification |
| `timeout_ms` | Collector | No | Positive whole milliseconds writing and flushing one frame may take; unbounded when omitted |
| `tls_ca_file` | Collector | No | TLS only: a server trust root |

The generated [SYSLOG client reference](connector-capabilities.md#syslog-client) states the same
keys from the client declaration.

The client alone cannot know its side, so the statement that binds it checks the keys that side
reads: `CREATE SYSLOG LISTENER INGESTOR` and `ALTER SYSLOG LISTENER INGESTOR ... SET FROM` for a
listener, `CREATE SYSLOG COLLECTOR EMITTER` and `ALTER SYSLOG COLLECTOR EMITTER ... SET CLIENT` for a
collector, and the transaction that replaces a client its entities reference with `DROP CLIENT` and
`CREATE SYSLOG CLIENT`. A key only the other side reads refuses the statement, naming the key and
the side that reads it.

TLS file paths use normal client resource mounts. For example:

```plexus
CREATE SYSLOG CLIENT syslog_tls
  MOUNT syslog_identity VERSION 1
  CONFIG {
    'protocol' = 'tls',
    'addr' = '0.0.0.0:6514',
    'tls_cert_file' = '{{ syslog_identity }}/server.crt',
    'tls_key_file' = '{{ syslog_identity }}/server.key',
    'tls_client_ca_file' = '{{ syslog_identity }}/clients-ca.crt'
  };
```

For a TLS listener, `tls_cert_file` and `tls_key_file` are both required. Supplying
`tls_client_ca_file` additionally enables mutual TLS and requires every client to present a
certificate that chains to that CA. Without it, the server does not request client certificates.

For a TLS collector, the certificate and key are optional, but must appear together when used for
client authentication. `tls_ca_file` adds a server trust root; platform roots are used when it is
absent. TLS verifies the server name derived from `addr`.

`CREATE SYSLOG CLIENT` refuses, naming the key, what no side can use: an unknown protocol, an `addr`
that is not `host:port`, `framing` or `idle_timeout_ms` over UDP, non-transparent framing over TLS,
a TLS file over UDP or plain TCP, only one half of a TLS identity, or a deadline key that is not a
positive whole number of milliseconds. The statement that binds a client to a listener also refuses
a host name in `addr`, a TLS client without a server identity, and `connect_timeout_ms` over UDP or
TCP, where a listener opens no connection and performs no handshake.

A TLS file that is readable but holds no usable certificate or key fails the listener's or the
collector's start. Startup diagnostics retain the failed TLS file, certificate, or key cause beneath
the Syslog configuration error, with the setting or path that failed. They do not include TLS file
contents.

## Ingestor

The generated [SYSLOG LISTENER ingest table](connector-capabilities.md#syslog-listener-ingest)
and [SYSLOG COLLECTOR emit table](connector-capabilities.md#syslog-collector-emit) record the
Plexus declarations, including peer metadata, header availability, quiesce and batching.

A `SYSLOG LISTENER` ingestor reads through a `SYSLOG CLIENT`. It has one delivery mode,
`NO ACK`, and no `INSTANCES` clause:

```plexus,ignore
CREATE [IF NOT EXISTS] SYSLOG LISTENER INGESTOR <name>
  FROM <client>
  MODE NO ACK
  ON QUIESCE SUSPEND
    | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
    | DROP
  DECODE USING <codec>
```

Every live Nervix node runs the listener on the client's configured `addr`. The listener is not
leader-owned and has no placement owner: leadership changes do not move or restart it. A restarted
node starts its listener again after restoring the active graph, and a node joining the cluster
starts one after receiving that graph. In Kubernetes, a Service can therefore target the syslog
port on every Nervix pod. Routes, branch construction, flush policy, and error policy follow the
normal ingestor contract.

Every received message exposes its remote socket address as optional `STRING`
`metadata.peer_addr`. For UDP this is the datagram source; for TCP and TLS it is the connection
peer. Transport metadata does not propagate through a relay unless a route assigns it to a
schema-backed field:

```plexus,ignore
TO received_syslog
  INHERIT ALL
  SET peer_addr = metadata.peer_addr
  UNBRANCHED
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
```

### UDP

UDP follows RFC 5426. One datagram is one frame. A datagram larger than `max_message_size` is
dropped and counted in `nervix_ingestor_oversized_payloads_total` on the node that received it,
which that node's `DESCRIBE INGESTOR` renders; the listener goes on reading the next datagram.
The listener receives into a fixed 65,535-byte scratch buffer before applying this admission limit.
Datagrams the listener has not read yet wait in the socket's kernel receive buffer, which drops
what does not fit without anything counting it.

### TCP

TCP follows RFC 6587. The listener accepts any number of concurrent connections. Without `framing`,
it detects framing for every frame:

- a frame beginning with a digit uses octet counting
- any other frame is terminated by LF; a CR immediately before the LF is removed
- both framing forms may be interleaved on one connection

With `framing`, every frame of every connection uses that framing. Under `octet-counting`, a frame
that does not begin with an octet count is malformed and closes its connection. Under
`non-transparent`, every frame ends at LF, so a line that begins with a digit is an ordinary frame.

An octet count must be a positive decimal number with at most ten digits. Each connection holds at
most `max_message_size` bytes, plus one octet count and its space, of what it has read but not yet
queued, and hands every frame it completes to an intake queue of 64 frames that all of the
listener's connections share. When the queue is full, a connection keeps its completed frame and
reads nothing more until the queue has room, so TCP flow control holds its sender back and no
frame is dropped.

These are per-connection limits. The listener has no connection-count limit, so the total of its
frame-assembly buffers and completed frames waiting for the queue is unbounded. The shared
64-frame queue does not bound that total; the generated [capability declaration](connector-capabilities.md#syslog-listener-ingest)
reports these buffers separately.

A frame may arrive split across any number of reads, including inside its octet count or between
its CR and LF. The listener keeps the part it has and completes the frame from the following
reads; the frame, and any framing failure, is the same as when one read carries it whole.

Connections fail independently: whatever closes one leaves the listener and its other connections
running. The frames a connection completed before it closed are still delivered, and the part of a
frame it had sent is lost. A malformed or oversized octet-counted frame, or an
oversized non-transparent frame, closes its connection because the stream cannot be
resynchronized safely; nothing after it is read. Such a frame is not counted in
`nervix_ingestor_oversized_payloads_total`, which for a Syslog listener counts UDP datagrams only.
A read that fails, such as a reset, closes its connection, and so does a sender that closes its
connection in the middle of a frame. A connection that fails is not the ingestor's transient error:
the listener records the failure as a debug diagnostic that retains its cause without quoting the
frame's payload.

With `idle_timeout_ms`, a connection that sends nothing for that long while the listener waits to
read from it is closed; the other connections and the listener continue. The wait restarts with
every read. Time the listener spends not reading a connection, because `SUSPEND` stopped it or the
intake queue is full, does not count, so a sender is never closed for the listener's own
backpressure. Without the key, an idle connection stays open until its sender closes it.

### TLS

TLS follows RFC 5425: the TCP listener behavior runs over a TLS session and frames use octet
counting, so a frame that does not begin with an octet count is malformed and closes its
connection. A failed handshake closes only that connection, and so does a handshake that does not
complete within the client's `connect_timeout_ms`, 30 seconds when it is left out, so a client that
opens a connection and never completes its handshake holds nothing past that bound. The
client-authentication behavior is controlled by `tls_client_ca_file` as described above.

### Quiesce

`SUSPEND` stops the listener taking frames, accepting connections and reading the connections it
has. A UDP listener leaves arriving datagrams in the socket's kernel receive buffer, which drops
what does not fit. A TCP or TLS listener keeps every frame it holds:

- frames in the intake queue stay there;
- a frame a connection has completed finishes its hand-off to the queue, or, when the queue is
  full, waits in its connection until the queue has room after resume, rather than being dropped
  when the suspension starts;
- what a connection has read but not yet queued stays with it, and the connection reads nothing
  more, so the kernel buffers fill and TCP flow control holds its sender back;
- new connections wait in the operating system's accept queue.

`idle_timeout_ms` does not run while the listener is not reading, so no connection is closed as
idle because of a suspension. When the listener resumes, it delivers what it kept and goes on
reading; each connection's frames arrive in the order the connection sent them.

A suspended listener delivers what it kept only when it resumes itself, which it does when a hold
ends without replacing it: a memory-pressure pause, or an alteration whose drain fails or whose
hold expires.

Whatever the mode, a change that commits under a hold starts a new listener in place of the old
one: an alteration of the ingestor, or a `DOMAIN_PAUSE` change, which rebuilds the domain. The old
listener then ends as it does on `STOP`, `DROP INGESTOR` or the node's shutdown: it closes its
sockets and connections and discards what its queue and connections held and what the kernel
buffered for them, so a sender whose connection closed has to connect again.

`BUFFER` and `DROP` keep accepting and reading, and apply the normal bounded local quiesce behavior
to what arrives. The frames `BUFFER` retained during an alteration's hold are held outside the
listener, so they are delivered after the alteration commits; see
[Quiesce Modes](ingestors.md#quiesce-modes).

### Failures and recovery

Syslog transport is at-most-once at intake. The listener sends no application acknowledgment, and
datagrams dropped or stream data lost before intake are not recoverable.

A failure of the listener's own socket is that node's ingestor transient error, which
`DESCRIBE INGESTOR` on that node shows with its reconnect backoff:

- a bind that fails, when the listener starts or binds again, leaves it unbound until a later bind
  succeeds;
- a failed TCP or TLS accept closes only the listening socket. Established connections stay open:
  their frames wait in the intake queue, and then in the connections, until the listener is bound
  again, and are delivered then. New connections are refused in between;
- a failed UDP receive closes the socket, losing the datagrams its kernel buffer held.

The listener binds again on the host's reconnect cadence: 250 ms after the first failure, doubling
after each further one up to 30 seconds. A successful bind alone neither clears the failure nor
resets the delay; the next frame the listener delivers does. A failure on one node does not stop
listeners on other nodes.

A malformed message body is a codec decode failure and skips only that frame. A failed connection
closes on its own, as [TCP](#tcp) describes.

## Emitter

The syslog emitter requires a codec and supports only `NO ACK`:

```plexus,ignore
TO SYSLOG <client>
  MODE NO ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  ENCODE USING <codec>
```

Header invocations are not supported. Every emitter route still requires its normal flush policy,
message error policy, and general error policy. Branch identity collapses only after successful
external emission.

### UDP

Each encoded record is one datagram. An encoded payload above 65,507 bytes is a message error.
With an emitter `BATCH` clause and a `SYSLOG` codec, the codec writes one RFC 5424 message per
batch. Its `MSG` is a JSON array of the members' complete RFC 5424 messages, and the outer header
contains the fields every member shares. Each complete batch frame is one datagram and is checked
against the same UDP limit.

### TCP

The emitter keeps one persistent connection. `octet-counting` writes the decimal byte length, one
space, and the payload. It rejects a payload whose count needs more than ten digits before writing.
`non-transparent` writes the payload followed by LF; an encoded payload
that already contains LF is a message error in this mode.

### TLS

The emitter uses octet-counted framing over one verified TLS connection. RFC 5425 does not permit
non-transparent TLS framing. The same ten-digit count limit applies.

For every transport, success means the local socket accepted and flushed the complete frame. No
remote delivery acknowledgment exists. Because every frame is flushed before its publish returns,
the sender holds nothing between publishes, so stopping the emitter leaves it nothing of its own to
send. Connection establishment and write failures are infrastructure errors: the current batch is
retained, the connection is rebuilt, and delivery is retried on the declared backoff. Records
accepted before a later failure in the same batch may be delivered more than once.

With `timeout_ms`, writing and flushing one frame that does not finish within that bound is a write
failure: a server that stops reading fills the connection's buffers, and the write that waits on
them fails naming the key and its bound, which `DESCRIBE EMITTER` shows as the transient error. The
connection the frame was partly written to is abandoned and rebuilt. Without the key, such a write
waits until the server reads again or the connection fails.

A missing name, silent DNS server, or unreachable answer is a connection failure. Resolving an
address does not report record delivery. Shutdown or cancelled startup drops pending DNS and
connection work; the physical connection budget also bounds an attempt if no cancellation occurs.

## Observability and limits

Syslog ingestors and emitters use the standard `DESCRIBE INGESTOR` and `DESCRIBE EMITTER` runtime
state, transient-error, reconnect-backoff, edge-metric, and emitter-metric surfaces. Every node
runs its own listener, so `DESCRIBE INGESTOR` reports the listener of the node that answers it,
with that node's transient error, reconnect backoff and `nervix_ingestor_oversized_payloads_total`.
`SHOW CREATE` renders canonical codec, client, ingestor, and emitter declarations. Normal ALTER
quiesce classification applies. Lifecycle transitions are info-level events; per-frame and
per-connection details, including why a connection closed, are debug or trace and do not include
payload values.

| Limit | Value |
| --- | --- |
| Ingested message | `max_message_size`, default 131,072 bytes |
| UDP datagram intake | 65,535 bytes, further limited by `max_message_size`; a datagram above it is dropped and counted |
| UDP payload emission | 65,507 bytes |
| Octet-counting prefix | At most 10 digits |
| Concurrent TCP/TLS connections | Unbounded count; each holds at most `max_message_size` bytes, plus one octet count and its space, of what it has read but not yet queued |
| TCP/TLS intake queue | 64 frames shared by a listener's connections; a full queue holds the connections back |
| TLS handshake of an accepted connection | `connect_timeout_ms`, default 30 seconds |
| Accepted connection sending nothing | `idle_timeout_ms`; unbounded when omitted |
| Listener rebind after a socket failure | 250 ms after the first failure, doubling up to 30 seconds |
| Opening an emitter's sender, name resolution and TLS included | `connect_timeout_ms`, default 30 seconds |
| Emitter frame write | `timeout_ms`; unbounded when omitted |
