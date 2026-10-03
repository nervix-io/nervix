# Name Resolution

Nervix turns host names into addresses when a node connects to its peers, when a connector opens a
connection to an external system, and when a native client opens a session. Wherever Nervix decides
how a host is resolved, it asks one resolver owned by the `nervix-dns` crate: Hickory's
asynchronous DNS client, configured from a `resolv.conf`-format file and a hosts file and held to
the cache, time and concurrency bounds this chapter describes. A lookup through it never occupies
Tokio's blocking pool and never calls the C library resolver. Some external drivers resolve hosts
inside code that accepts no resolver from its caller; they keep the operating system's resolver,
and [Residual Resolution](#residual-resolution) names every one of them.

This chapter owns the resolver itself: its ownership and lifetime, its configuration, the order in
which it answers, its cache, its bounds and cancellation, the outcomes of a lookup, the hooks that
hand it to client libraries, the dependency features that select it, the paths that resolve
elsewhere, the simulation boundary, and the evidence behind each claim. The boundaries that
resolve through it keep their own facts, and this chapter links to them rather than restating them:

- [Cluster Interconnect](./interconnect.md#where-the-interconnect-resolves) owns when a node
  resolves its own endpoint, its bootstrap seeds and its peers, and how a failed peer lookup is
  classified and counted.
- [Connector Crates And The Connector Contract](./connector-contract.md#dns-for-http-and-iceberg)
  owns how each integration installs the resolver, dials, completes TLS, and reports a connection
  it could not open.
- [Client Session Protocol](./client-session-protocol.md#leader-discovery-redirect-and-reconnect)
  owns native session connections, their seeds, redirects and reconnection.
- [Errors And Diagnostics](./errors-and-diagnostics.md) owns the error types that carry a failed
  lookup through each owner.
- [Shutdown And Recovery](./shutdown.md#stopping-intake) owns how a stopping node ends a pending
  connection attempt.
- [Integration Test Lifecycle](./integration-test-lifecycle.md#dns-authorities) owns the DNS
  fixtures of the scenario harness, and
  [Deterministic Interconnect Simulation](./interconnect-simulation.md#sockets-and-dns) owns the
  simulated names of the Turmoil build.

## Ownership

`nervix-dns` sits in the engines and infrastructure layer. It owns one resolver's configuration
reading, hosts-file snapshot, answer cache and TTL bounds, the bound on concurrent lookups, the
typed outcome of every lookup, the physical connection budget that a lookup shares with the address
attempts after it, and the hooks that hand the resolver to the client libraries a node connects
with: both Reqwest versions, `hyper-util`'s `HttpConnector`, and Smithy's HTTP client. It must not
know Models, peers, connectors, graphs, or any protocol's retry policy. A caller gives it a host, a
port and a time budget, and receives addresses or a typed failure.

| Owner | What it owns about name resolution |
| --- | --- |
| `nervix-dns` | The resolver, its policy and bounds, its failures, its client-library hooks, and the connection budget and ordered address attempts |
| The server, as composition root | Loading the node's resolver once at startup from the node's DNS options, and handing clones to the runtime, the interconnect, and the node's own client sessions |
| `nervix-interconnect` | When and where a peer endpoint is resolved, the TLS and HTTP/2 identity of the connection, and peer failure classification |
| `nervix-connector` | The shared HTTP client configuration that installs the resolver on the Reqwest clients of HTTP polling, Prometheus, Sentry and OTEL HTTP |
| Each connector crate | Installing the resolver its typed plan carries at its driver's hook, or resolving and dialling itself; its TLS, framing, protocol handshake, and the error it reports |
| The host data plane | Retry, acknowledgement, intake, quiesce and shutdown decisions around a connection that could not be opened |
| `nervix-client-core` | The native session client's resolver and the Tonic connector that dials through it |

Connector crates receive the resolver as a value in their typed source or sink plan, so a connector
never loads configuration or chooses a resolver itself, and the registry, which names no connector
crate, never sees DNS at all. The interconnect receives it wrapped in its `PeerResolver`, the one
seam where the Turmoil build substitutes simulated names, as
[Build Modes](#build-modes-and-the-simulation-boundary) describes.

## Resolver Lifetime

A node loads its resolver once, at startup, after its TLS material and before it opens its runtime
state or binds the interconnect. Loading reads the resolver configuration and the hosts file on the
blocking pool, so a slow filesystem never stalls a reactor thread, and then builds Hickory's
resolver on the node's Tokio runtime. The read is one of the declared owners of the blocking pool
outside the bounded executor, because a node loads its resolver before it builds its executor and a
client tool loads the same resolver without one; see
[Blocking work outside the executor](./data-plane-concurrency.md#blocking-work-outside-the-executor). Neither file is read again: a changed resolver configuration
or hosts file takes effect when the node next starts.

Clones of the resolver share its configuration, hosts snapshot, answer cache and concurrency bound.
The node hands one clone to the runtime, which passes it into the typed plan of every source and
sink that resolves through it, one to the interconnect, and one to each client session the node
opens to the leader's session service while it shuts down, and one to its own OTLP trace
exporter when enabled. Nothing about the resolver is process-wide: it lives in no static, its
name-server connections and background tasks run on the runtime that built it, and they end when
its last clone is dropped. A runtime that shuts down with a lookup in flight is not held open by
that lookup.

A runtime built without a resolver exists only in unit tests that start no external connector. A
source or sink that needs the resolver fails to start there with the reason
`the node DNS resolver is not installed`; it never falls back to another resolver.

A native session client loads its resolver once per client, before its first connection, from the
host's files or from the configuration its caller names, and reuses it for the first connection,
seeds, redirects and reconnects. An owner that already holds a resolver, such as a node opening a
session to the leader, hands its own over instead, and the CLI passes its DNS options through. The
shared C binding always loads the host's `/etc/resolv.conf` and `/etc/hosts`.

## Node Trace Export

With `--otel-enabled`, the process installs its subscriber and batch span exporter before node
startup. The lazy Tonic 0.14 connector waits for startup to publish the node's resolver. Creating
the channel performs no lookup or dial. Startup loads DNS at its usual point, after TLS material
and before runtime state, then publishes a clone to this service. Early spans remain in the batch
exporter's existing bounded queue. No second resolver is constructed, and a DNS configuration
failure remains a node startup failure.

Every new collector connection uses the node's configuration, hosts snapshot, cache and concurrency
bound. The endpoint retains its hostname and authority. Waiting for resolver installation stays
outside the connection timeout so slow startup retains its early spans. Once installed, the OTLP
export timeout bounds DNS and TCP address attempts together; the request uses the same timeout.
It defaults to ten seconds; `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` overrides
`OTEL_EXPORTER_OTLP_TIMEOUT`, both in milliseconds, with invalid values ignored as the exporter
defines. Compression and metadata environment settings remain interpreted by `opentelemetry-otlp`.

The node's exporter configures no Tonic TLS roots or identity. An `https` collector therefore fails
at export time with Tonic's TLS-required error even though the connector dependencies enable TLS
support. The OTEL sink's explicit TLS configuration is a separate contract.

Export failures belong to the telemetry SDK. They neither stop a running node nor enter connector
retry or acknowledgement paths. If startup ends before installing DNS, dropping the tracing guard
closes the resolver publication so a pending connection fails without waiting for a resolver that
will never arrive. The guard flushes the provider while the node's Tokio runtime still exists.

## Configuration

Three server options select what the node's resolver reads. The native CLI accepts the same three
options and environment variables for its own resolver, as
[Command Line Client](./client-tools-cli.md#options) lists.

| Option | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--dns-resolver-config` | `NERVIX_DNS_RESOLVER_CONFIG` | `/etc/resolv.conf` | A `resolv.conf`-format file: its `nameserver` lines, its last `search` or `domain` line, and the `ndots`, `timeout`, `attempts`, and `edns0` options |
| `--dns-hosts-file` | `NERVIX_DNS_HOSTS_FILE` | `/etc/hosts` | A hosts-format file consulted before DNS |
| `--dns-name-server` | `NERVIX_DNS_NAME_SERVERS` | none | A name server address with its port, repeatable or comma-separated, that replaces the resolver configuration's `nameserver` lines while the rest of that file still applies |

The resolver configuration is read with the C library's grammar:

- Each `nameserver` line names a server on port 53. A server is asked over UDP and, when an answer
  arrives truncated, again over TCP. A negative answer from a server is final; the resolver does not
  ask another server whether the name exists after all.
- The last `search` or `domain` line is the search list. When the file has neither, the domain of
  the host's own name is the search list, as the C library does; a host name that is not a valid DNS
  name contributes none.
- `ndots`, `timeout` and `attempts` default to 1, 5 seconds and 2, the C library's defaults.
  `attempts` counts the tries of each query, as the C library counts them, and a file that asks for
  none still gets one; `timeout` bounds each try. `edns0` enables EDNS.
- A line the grammar cannot read is logged as the warning
  `ignored a line of the resolver configuration`, with the file and the reason, and skipped. Every
  other directive and option the grammar reads has no effect, including `sortlist`, `rotate`,
  `single-request`, `single-request-reopen`, `use-vc`, `no-aaaa`, `trust-ad` and `inet6`.

`--dns-name-server` replaces only the `nameserver` lines, so the resolver configuration must still
exist and be readable, and its search list and options still apply. The hosts file must exist and be
readable too; an empty file is valid.

Loading fails, and the node does not start, with `failed to load the name resolver configuration`
above one of these causes:

| Cause | When |
| --- | --- |
| `the resolver configuration '<path>' could not be read` | The file is missing or unreadable; the operating system's error is attached |
| `the resolver configuration '<path>' names no name server` | The file has no `nameserver` line and no `--dns-name-server` replaces them |
| `the search domain '<domain>' is not a valid DNS name` | The search list holds a name DNS cannot represent |
| `the hosts file '<path>' could not be read` | The hosts file is missing, unreadable, or cannot be parsed |
| `the resolver could not be constructed` | Hickory rejected the resulting configuration |
| `loading the resolver configuration stopped before it finished` | The blocking read did not complete because its task was cancelled or panicked |

A file without name servers is an error rather than an instruction to ask the local host, which is
what the C library does with one. Nothing falls back to a public resolver or to the C library.

## Resolution Order

A host resolves in this order:

1. A literal IPv4 or IPv6 address, bracketed or not, is its own answer. No query is sent and no
   lookup slot is taken, and the port the caller supplied is kept.
2. A name the hosts file lists, compared exactly as written and without case, answers with every
   address the file gives it, IPv4 addresses first. Nothing is asked of DNS for that name in either
   family, so an entry with only an IPv4 address never waits for an IPv6 query, and search domains
   do not apply to it.
3. Any other name goes to the name servers. A name with a trailing dot is fully qualified and asked
   exactly as written. Any other name is completed by the search list according to `ndots`, as the
   C library completes it.

A lookup that reaches the name servers asks the IPv4 and IPv6 questions in parallel and answers with
every IPv4 address first and then every IPv6 address, each family in the order the server gave it,
without duplicates. Either family is enough: a name with only IPv6 addresses resolves to them, and
only when both questions fail does the lookup fail, with the IPv4 question's outcome. The resolver
does not choose between families and does not sort by reachability; the caller dials the answers,
as [Dialling The Answers](#dialling-the-answers) describes.

`localhost`, and every name under it, is answered with `127.0.0.1` and `::1` without a query when
the hosts file does not list it, as RFC 6761 asks of a resolver. Names under `.local` are asked of
the configured name servers like any other name; nothing is resolved by multicast DNS. A host that
is neither an address nor a valid DNS name fails as an invalid name before anything is asked.

When the configuration names several servers, a query goes to two of them at once, preferring those
that have answered fastest so far, and the first usable response wins. The C library instead asks
its servers one at a time in file order.

## Caching And TTLs

Answers from the name servers are cached per name and record type, in a cache of 4,096 entries,
for the TTL the answer carries. The TTL is capped at one hour for an address answer and at thirty
seconds for a name that does not exist or has no address. A node therefore keeps using an address
its name no longer has for at most an hour, or for the answer's own TTL when that is shorter, and a
name published after a node was told it does not exist resolves on that node within thirty seconds,
or sooner when the negative answer's own TTL is shorter. There is no minimum: an answer with a TTL
of zero is not reused. Literal addresses and the hosts file are answered from memory and never
cached.

An expired answer is asked again on the next resolution. Expiry never closes a connection: an
established connection stays open until its own owner ends it, and a changed answer or an expired
TTL takes effect only when that owner opens its next connection.
[Where Nervix Resolves](#where-nervix-resolves) links each owner's own statement of this rule.

## Bounds, Deadlines And Cancellation

Every lookup runs within a budget its caller gives it, and a lookup that exhausts that budget fails
as a timeout. The budget covers the whole lookup: waiting for a lookup slot, every name the search
list produces, both address families, and every try against every server. The resolver
configuration's `timeout` bounds each try and its `attempts` count the tries, so a configuration
with short tries ends a lookup against a silent server well inside a generous budget.

At most 64 lookups past the hosts file run at once on one resolver, a lookup answered from the
cache included, which is enough for every peer of a full 64-peer topology to reconnect together. A
further lookup waits for a slot inside its own budget. Literal addresses and hosts-file names never
take a slot.

A caller that dials for itself shares one physical connection budget between the lookup and the
address attempts after it: the lookup receives the budget, and each answer, dialled in resolution
order, receives an equal share of whatever time remains when its attempt starts. An address that
refuses at once leaves its unused share to the attempts after it, and one that never answers cannot
consume their time. Handshakes that the caller performs after an address accepts use what is left
of the same budget.

A client library that resolves through one of the resolver's hooks cannot pass the deadline of the
request it resolves for, so every hook lookup has a budget of thirty seconds, and the library's own
request or connection deadline, where it has one, can end the lookup sooner. A lookup cut short by
that deadline reports the library's timeout rather than a lookup failure.

Cancelling a lookup, by dropping it, aborting its task or ending its budget, releases its slot at
once. The resolver never retries a failed lookup; the only retries inside a lookup are the tries its
configuration asks for, all within the budget. Whether and when a failed connection is attempted
again belongs to the caller's existing policy, so name resolution never multiplies a retry policy.

## Lookup Outcomes

A lookup ends with at least one address, or with a `DnsLookupError` that names the host as the
caller wrote it and one `DnsLookupFailure`. Its message is `resolving '<host>' failed: <reason>`.

| Failure | Reason in the message | Meaning |
| --- | --- | --- |
| `NameNotFound` | `the name does not exist` | A name server answered that the name does not exist |
| `NoAddresses` | `the name has no IPv4 or IPv6 address` | The name exists but has no address in either family |
| `Timeout` | `no answer arrived in time` | The budget ended, or every try of the configured servers timed out |
| `Refused` | `a name server refused or failed the query` | A server answered with an error such as a refusal or a server failure |
| `Unreachable` | `no name server could be reached` | No server could be asked, or none answered with a usable response |
| `InvalidName` | `the host is not a valid DNS name` | The host is neither an address nor a valid DNS name |

With a search list, the failure is the outcome of the last name that was asked, while the error
still names the host as written. The error carries the host and the resolver's own description of
the failure, and nothing from a request, a record or a credential.

A client library that resolved through a hook receives the `DnsLookupError` itself as the failure of
its lookup and keeps it among the causes of its own connection error, however many errors of its
own it wraps around it. `DnsLookupError::find_in` finds it there again, including through the
`Arc` and `std::io::Error` wrappers Redis places around its causes, so a connector can keep the
typed lookup failure beneath its own context. Which paths do so is listed under
[Operator Diagnostics](#operator-diagnostics).

## Where Nervix Resolves

Every path below resolves through the node's resolver, or through the native client's own resolver
for a session. Apart from the interconnect's lookups at startup, each path resolves its configured
name again for every new connection it opens.

| Path | How it reaches the resolver | What bounds the lookup | Canonical description |
| --- | --- | --- | --- |
| Interconnect at startup: the node's own advertised endpoint, its bootstrap endpoint, and the recovered Raft members' endpoints | `PeerResolver`, which calls the resolver directly | The connection setup timeout, five seconds, for each lookup | [Where The Interconnect Resolves](./interconnect.md#where-the-interconnect-resolves) |
| Interconnect: every pool connection to a discovered peer | `PeerResolver` inside each connection attempt | The attempt's five-second connection setup timeout | [Where The Interconnect Resolves](./interconnect.md#where-the-interconnect-resolves) |
| HTTP polling and Prometheus sources, Sentry and OTEL HTTP sinks | Reqwest 0.13's `dns_resolver`, installed by the shared `HttpClientConfig` | The client's `timeout_ms` for the whole request when it is set, otherwise the thirty-second hook budget | [DNS for HTTP and Iceberg](./connector-contract.md#dns-for-http-and-iceberg) |
| Iceberg REST catalog and its OAuth token request | Reqwest 0.12's `dns_resolver` on the client given to `RestCatalogBuilder::with_client` | The thirty-second hook budget | [DNS for HTTP and Iceberg](./connector-contract.md#dns-for-http-and-iceberg) |
| Iceberg S3, GCS and Azure object storage and its credential providers | Reqwest 0.13's `dns_resolver` on the client the connector installs through OpenDAL's `HttpClientLayer` | The thirty-second hook budget, inside OpenDAL's own operation timeouts | [DNS for HTTP and Iceberg](./connector-contract.md#dns-for-http-and-iceberg) |
| HTTP request emitter | The connector resolves each request's target host itself | The request's one physical `timeout_ms` | [Request Preparation And Transport Ownership](./http-emitter-architecture.md#request-preparation-and-transport-ownership) |
| RabbitMQ sources and sinks | The connector resolves and dials, then hands the transport to Lapin's `Connection::connector` | Thirty seconds shared with the address attempts and TLS | [DNS for RabbitMQ](./connector-contract.md#dns-for-rabbitmq) |
| Syslog UDP, TCP and TLS senders | The connector resolves and dials each new sender | Thirty seconds shared with socket setup, or with the attempts and TLS | [Integration-specific boundaries](./connector-contract.md#integration-specific-boundaries) |
| WebSocket `ws` and `wss` client sources | The connector resolves and dials each connection and resume | Thirty seconds shared with the attempts, TLS and the upgrade | [Integration-specific boundaries](./connector-contract.md#integration-specific-boundaries) |
| ClickHouse sinks, with and without TLS entries | Hyper's resolver service on `hyper-util`'s `HttpConnector` | The insert's `timeout_ms` when it is set, otherwise the thirty-second hook budget | [DNS for ClickHouse and SQS](./connector-contract.md#dns-for-clickhouse-and-sqs) |
| SQS sources and sinks, with and without a custom CA | Smithy's `ResolveDns`, installed with `build_with_resolver` | The SDK's 3.1-second connect timeout, which encloses the lookup, and a sink's `timeout_ms` as its attempt and operation timeouts | [DNS for ClickHouse and SQS](./connector-contract.md#dns-for-clickhouse-and-sqs) |
| The node's own OTLP trace export | Tonic 0.14's lazy custom connector, which waits for the node resolver installed during startup | The OTLP export timeout, ten seconds by default, enclosing DNS and TCP attempts after resolver installation; the same timeout bounds the request | [Node Trace Export](#node-trace-export) |
| OTEL gRPC sinks | Tonic 0.14's lazy channel over `hyper-util`'s `HttpConnector` with Hyper's resolver service | `timeout_ms` when it is set, as the connection and request timeout; otherwise the thirty-second hook budget alone | [DNS for OTEL gRPC](./connector-contract.md#dns-for-otel-grpc) |
| Redis command pools | Redis's `AsyncConnectionConfig::set_dns_resolver`, installed for every new physical `bb8` connection | The thirty-second hook budget, inside the thirty-second connection timeout the pool sets on the driver | [DNS for Redis](./connector-contract.md#dns-for-redis) |
| Redis Pub/Sub sources | The connector resolves and dials a dedicated stream, then hands it to Redis's `PubSub::new` | Thirty seconds shared with the attempts, TLS, Redis setup and `SUBSCRIBE` | [DNS for Redis](./connector-contract.md#dns-for-redis) |
| MQTT sources and sinks | The connector's socket connector, installed with `MqttOptions::set_socket_connector`, for every connection the driver's event loop opens | The driver's five-second connect timeout | [DNS for MQTT](./connector-contract.md#dns-for-mqtt) |
| Native CLI and SDK sessions | The client's resolver, as Hyper's resolver service under Tonic 0.13's connector, for the first connection, seeds, redirects and reconnects | The session's connect timeout, ten seconds by default, which encloses the lookup, the attempts and TLS | [Leader Discovery, Redirect, And Reconnect](./client-session-protocol.md#leader-discovery-redirect-and-reconnect) |
| A node's session to the leader's session service while it shuts down | The same session client, given the node's resolver | The session's ten-second connect timeout, within the drain and shutdown deadlines | [Topology Cases](./shutdown.md#topology-cases) |

Two interconnect uses are resolved once, when the node starts: the address that identifies the
node in gossip, which is the first answer for its own advertised endpoint, and its bootstrap seeds,
which gossip keeps dialling at the addresses the startup lookup produced.
[Where The Interconnect Resolves](./interconnect.md#where-the-interconnect-resolves) describes
both. No path writes a resolved address into its configuration, a plan, a Model or persisted state,
so a service can move to new addresses without an alteration.

## Dialling The Answers

The resolver returns every answer; the caller decides how to dial them. A caller that dials for
itself shares one connection budget between the lookup and its attempts, tries the answers strictly
in resolution order, one at a time, and keeps the first that succeeds, so every IPv4 answer is tried
before any IPv6 answer and no attempt races another:

| Caller | Budget | One address attempt | After an address succeeds |
| --- | --- | --- | --- |
| Interconnect pool connection | The connection setup timeout, five seconds | TCP connection | TLS, HTTP/2 and the connection hello within the same timeout; a failure there fails the attempt |
| RabbitMQ source or sink | Thirty seconds | TCP connection | TLS within the same budget, then Lapin's AMQP handshake, which has no deadline of its own |
| Syslog TCP sender | Thirty seconds | TCP connection | No further step |
| Syslog TLS sender | Thirty seconds | TCP connection and TLS | No further step |
| WebSocket client source | Thirty seconds | TCP connection, TLS and the opening upgrade | The client's signaling protocol, when it declares one, under that protocol's own `TIMEOUT` |
| Redis Pub/Sub source | Thirty seconds | TCP connection and TLS | Redis setup and `SUBSCRIBE` within the same budget |
| MQTT source or sink | The driver's connect timeout, five seconds | TCP connection with the driver's network options | TLS and the MQTT handshake by the driver within the same timeout |

Syslog UDP has no connection to confirm, so a sender binds a socket in the family of each answer in
turn and sends to the first answer whose socket setup succeeds. The HTTP request emitter tries the
answers in order inside its one physical `timeout_ms`, which it does not divide: an answer that
never responds uses the rest of that attempt.

A client library that resolves through a hook dials with its own connector. Reqwest, Smithy,
ClickHouse's client and Tonic all dial through `hyper-util`'s `HttpConnector`, which tries the
answers of the first answer's family in order, starts the other family's answers when 300
milliseconds pass without a connection, as RFC 6555 describes, and divides a configured connect
timeout evenly across the answers. The Redis driver dials every answer of a new command connection
at once, TLS included, and keeps the first that completes.

## Connection Identity

Resolution chooses where to connect and nothing else. Every caller keeps the configured host as
the identity of the connection, whichever address accepted it:

- **TLS.** The server name sent in the handshake and the name the certificate must carry are the
  configured host. A certificate that names only the dialled address, or another host, is rejected,
  and a literal IPv4 or IPv6 host is verified as an IP address. The interconnect additionally
  requires the peer's cluster identity, as
  [Peer Identity And Authentication](./interconnect.md#peer-identity-and-authentication) describes.
- **HTTP authority.** The `Host` header, the HTTP/2 `:authority`, the WebSocket upgrade's URL, and
  the request path and query stay as configured. A literal IPv6 host is written in brackets there.
- **Proxies.** Every Reqwest client Nervix builds, for HTTP polling, Prometheus, Sentry, OTEL HTTP
  and both Iceberg clients, keeps Reqwest's default of honoring the proxy environment variables
  `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`. When a request goes through a proxy, the
  node's resolver resolves only the proxy's host; the proxy resolves the service, an HTTPS request
  tunnels through it with `CONNECT`, and TLS still verifies the configured host. An SQS client
  without a custom CA takes its proxy from the same variables under the same rule, and one with a
  custom CA uses no proxy. No other path uses a proxy.
- **Request signing.** An SQS request is signed with SigV4 for its configured endpoint before its
  host is resolved, so the signature, the `Host` header and the certificate check name the same
  host.
- **Redirects.** A Reqwest client that follows a redirect resolves the new host through the node's
  resolver like the first, and a native session that follows a leader redirect resolves the
  leader's advertised host through the client's resolver. The HTTP request emitter follows no
  redirect.

## Failure Ownership

A failed lookup is a connection failure of the path that asked, reported through that owner's
existing error and handled by its existing policy. It never rejects a record, never acknowledges or
commits input, and never marks a message delivered: a source that cannot connect holds no delivery,
and a sink that cannot connect confirms nothing, so the input stays unacknowledged until a later
connection delivers it.

| Owner | What a failed lookup does | What tries again |
| --- | --- | --- |
| Interconnect pool slot | Fails that connection attempt and counts it with reason `resolution` | The slot's reconnect backoff, described in [Connection And Credential Lifecycle](./interconnect.md#connection-and-credential-lifecycle) |
| Node startup | Fails startup when the node's own endpoint or its bootstrap endpoint does not resolve; a recovered Raft member that does not resolve is logged and skipped | The operator or the process supervisor |
| A source | Fails its connection or its resume | The ingestor host, on the source's retry policy or, for a `NO_ACK` source, its reconnect cadence |
| A sink | Fails its start, its reopening or its publish | The emitter host, on its backoff and retry policy |
| A Redis command pool | Fails the physical connection the pool was opening | The emitter that borrows from the pool, on its retry policy |
| A native session | Fails the connection attempt as `ClientError::ConnectServer` | The session's bounded reconnect policy until its retry deadline |
| A node's session to the leader while it shuts down | Logs the failure at `warn` and gives the request up: the node drains without the leader's help, or leaves clearing its cordon abandoned | Nothing; shutdown continues within its own deadlines |

A shutdown or quiesce that arrives while a source is resuming cancels the resume, together with
every lookup, dial and handshake in it, as [Stopping Intake](./shutdown.md#stopping-intake)
describes. [Errors And Diagnostics](./errors-and-diagnostics.md) names the error each owner reports
and how the lookup failure stays beneath it.

## Dependency Selection

Nervix selects each library's resolver deliberately. The locked versions and selections are:

| Library | Locked version | Selection |
| --- | --- | --- |
| Hickory: `hickory-resolver`, `hickory-proto`, `hickory-net` | 0.26.3 | The node's resolver, on Hickory's Tokio runtime. The resolver is built from the configuration `nervix-dns` reads itself, never from Hickory's own system configuration reader, which other dependencies compile in. DNS over TLS, HTTPS or QUIC and DNSSEC validation are not selected anywhere in the dependency graph, so resolution adds no second TLS provider beside AWS-LC |
| Reqwest | 0.13.5 | `hickory-dns` is selected. Every client Nervix builds replaces Reqwest's own Hickory adapter with the node's resolver before its first request, so that adapter never reads the host's configuration; left to itself it would fall back to Google's public name servers when it could not |
| Reqwest, for Iceberg REST | 0.12.28, with Hickory 0.25.2 | Required by `iceberg-catalog-rest` 0.10.1. `hickory-dns` is selected and the client installs the node's resolver the same way; its TLS stays on AWS-LC with bundled roots |
| OpenDAL | 0.57.0 | Iceberg object storage receives a Reqwest 0.13 client through `HttpClientLayer`, which also serves OpenDAL's credential providers. OpenDAL's standalone S3 `detect_region` helper builds its own client, and Nervix never calls it |
| `hyper-util` | 0.1.21 | `HttpConnector::new_with_resolver` with the node's resolver as its resolver service, for ClickHouse, OTEL gRPC sinks, native sessions and the node's own trace exporter |
| Smithy's HTTP client | 1.4.2 | `build_with_resolver` with the node's resolver as its `ResolveDns`, for SQS |
| Tonic | 0.13.1 | `connect_with_connector` over that `HttpConnector`, for native sessions |
| Tonic, for OpenTelemetry | 0.14.6 | `connect_with_connector_lazy` over that `HttpConnector`, for OTEL gRPC sinks and the node's own trace exporter |
| Lapin and `async-rs` | 4.12.0 and 0.8.12 | `hickory-dns` deliberately off: it resolves through one process-wide resolver built on first use from `/etc/resolv.conf`, which ignores the node's configuration and bounds and keeps name-server connections of the first Tokio runtime that used it. The connector hands Lapin an established transport through `Connection::connector` instead |
| Redis | 1.7.1 | `AsyncConnectionConfig::set_dns_resolver` for pooled command connections; the Pub/Sub source hands `PubSub::new` a stream it dialled itself |
| `rumqttc-next` | 0.34.0 | `MqttOptions::set_socket_connector` in place of the driver's `lookup_host` connector; its proxy and WebSocket transports are not selected |
| MongoDB | 3.9.1 | `dns-resolver` selected for SRV and TXT discovery; see [Residual Resolution](#residual-resolution) |

A feature selected anywhere in a workspace build is unified into every crate of that build, so a
selection missing from one connector can be hidden by another. `just validate-dns-dependencies`
therefore checks each affected crate's own dependency graph, and runs in `just validate` and in CI:

- The HTTP, Prometheus, Sentry, OTEL and Iceberg connectors select Reqwest 0.13's `hickory-dns`, and
  the Iceberg connector also selects Reqwest 0.12's without Reqwest 0.12's Ring provider.
- The RabbitMQ, Syslog, WebSocket, Redis, MQTT, ClickHouse and SQS connectors, and for RabbitMQ also
  the server, depend on the node's resolver and Hickory's Tokio runtime.
- Neither the RabbitMQ connector nor the server selects the Hickory feature of Lapin,
  `amq-protocol`, `amq-protocol-tcp` or `async-rs`.
- RabbitMQ, Redis, MQTT, ClickHouse and SQS keep their TLS on AWS-LC.
- The MongoDB connector selects the driver's `dns-resolver` feature.

The interconnect, the native client crates and `nervix-dns` itself are not checked as roots: each
depends on `nervix-dns` directly and selects no optional resolver feature. The check has no rule for
the connectors that resolve through their drivers.

Nervix-authored code cannot reach the operating system's resolver at all.
`just validate-primitive-boundary`, also part of `just validate`, rejects `tokio::net::lookup_host`
and the `ToSocketAddrs` traits wherever Nervix code names them, and the primitive boundary's sockets
offer a lookup only in the Turmoil build, where it answers from the simulated DNS table, as
[Execution-Sensitive Primitives](./data-plane-concurrency.md#execution-sensitive-primitives)
describes. A dependency that resolves inside itself is outside that check, which is why [Residual
Resolution](#residual-resolution) lists each one.

## Residual Resolution

The paths below resolve names without the node's resolver, because the library that owns their
connection takes no resolver, transport or socket from its caller at that boundary in the locked
version. They keep working exactly as their drivers define, and each is named here so that no path
is assumed to be covered:

| Path | Who resolves, and how | Why the node's resolver is not used |
| --- | --- | --- |
| MongoDB sources and sinks, `mongodb` 3.9.1 | For a `mongodb+srv` address, the driver's own Hickory resolver, selected by its `dns-resolver` feature and built from the host's `/etc/resolv.conf`, looks up the SRV and TXT records, and repeats the SRV lookup while it monitors the deployment. The host of every server address it connects to, whether listed in the address or discovered through SRV, is resolved with Tokio's `lookup_host` | The driver accepts no resolver for the addresses its sockets connect to. Its Hickory feature covers discovery only and does not replace that lookup |
| NATS sources and sinks, `async-nats` 0.49.1 | The driver resolves each server address with Tokio's `lookup_host` before its per-address connection timeout | No resolver or socket hook |
| PostgreSQL pools, `sqlx-postgres` 0.9.0 | The driver connects with Tokio's `TcpStream::connect((host, port))` | No resolver or socket hook at the pool's connection boundary |
| MySQL pools, `mysql_async` 0.36.2 | The driver connects with Tokio's `TcpStream::connect((host, port))` | Its `resolved_ips` option fixes one address list for the life of the pool, which would pin a name to one answer |
| Pulsar sources and sinks, `pulsar` 6.9.0 | The driver resolves broker and proxy URLs with `Url::socket_addrs` on a blocking worker, then dials the addresses | No resolver hook |
| Kafka sources and sinks, `rdkafka` 0.39.0 with librdkafka 2.12.1 | librdkafka resolves bootstrap and broker hosts in its own native threads | librdkafka's resolver callback is not exposed by the Rust binding |
| ZeroMQ sources and sinks, `zeromq` 0.4.1 | The driver's TCP transport connects with Tokio's `TcpStream::connect((host, port))` | No socket hook |
| The web console | The browser resolves the page's origin and its WebSocket and fetch requests | The browser owns resolution |

Tokio's `lookup_host` and `TcpStream::connect` with a host name, and `Url::socket_addrs`, resolve
through the C library on a thread of Tokio's blocking pool, and librdkafka calls it on its own
threads. Such a lookup follows the host's `nsswitch.conf`, NSS
modules and multicast DNS where the host provides them, the node's `--dns-*` options do not apply to
it, and only the driver's own connection timeout bounds it. A thread stays occupied until the C
library returns.

## Deployment Limits

The node's resolver implements the `resolv.conf` and hosts-file behavior this chapter describes and
nothing else of the operating system's name service:

- It does not consult `nsswitch.conf`, so the hosts file always answers before DNS and no other
  source is ever asked.
- It loads no NSS module. Names served only by LDAP, NIS, `mdns` or `myhostname` do not resolve; in
  particular a node's own host name resolves only when the hosts file or DNS lists it, which
  container runtimes arrange by writing it into the container's hosts file.
- It sends no multicast DNS, and it applies no `sortlist` and none of the options
  [Configuration](#configuration) lists as having no effect.
- It reads its files once. A change that a DHCP client, a VPN or a network manager makes to
  `/etc/resolv.conf` reaches the node only when it next starts. A local stub such as the
  `systemd-resolved` listener at `127.0.0.53` keeps a stable address while its own upstream servers
  change, so a node that names the stub follows such changes without a restart.
- A platform split-DNS policy applies only as far as the name server the configuration names
  applies it, which is the case for the `systemd-resolved` stub.
- It speaks plain DNS over UDP and TCP to the servers it is given. It does not validate DNSSEC and
  does not use DNS over TLS or HTTPS.

Answers are not authenticated, so an address is only as trustworthy as the DNS path that returned
it. Every TLS connection verifies the certificate against the configured host rather than the
dialled address, so a forged answer cannot impersonate a TLS server, and the interconnect always
uses mutually authenticated TLS. A plain-text connection trusts whatever address it was given.

Docker's embedded DNS and Kubernetes cluster DNS, with their search lists and `ndots`, are reached
through the `resolv.conf` those platforms write into each container. The Compose cluster in
[Docker](./installation-docker.md) addresses nodes by container name through Docker's embedded DNS.
The Kubernetes resources in the repository advertise each pod's fully qualified name under a
headless service that publishes pods before they are ready, so a pod's name resolves as soon as the
pod has an address. Such a name has more labels than the platform's `ndots:5` and is asked as
written before any search domain.

Because the interconnect gives a lookup at most its five-second connection setup timeout, keep the
resolver configuration's `timeout` well below five seconds when a node has more than two name
servers: a try goes to two servers at once, and under the default five-second `timeout` two silent
servers consume the whole budget before a third is asked.

## Build Modes And The Simulation Boundary

| Build | Peer names | Connector and client names |
| --- | --- | --- |
| Normal | The node's resolver through `PeerResolver::new` | The node's resolver, or the client's own |
| `turmoil` | `PeerResolver::simulated`, which answers from the simulated host's DNS table through the lookup the primitive boundary offers only in this build | Not part of the simulation |
| `shuttle` | The production `PeerResolver`, but sockets are Tokio's and outside every model, and a Shuttle execution has no Tokio reactor, so no Shuttle check opens a socket or resolves a name | The same: no Shuttle check opens a connector or a session |
| `loom` | Not modeled | Not modeled |
| Browser | The web console runs no interconnect | The browser resolves; `nervix-dns` and Hickory are absent from the web console's dependency graph |

The Turmoil build still compiles `nervix-dns` for its typed failures and its connection budget,
but it never constructs a resolver, reads a resolver configuration or hosts file, or sends a DNS
packet, so a simulated host cannot reach the real network and shares no resolver state with another
host. A simulated name that the table does not hold fails as a name that does not exist, and a
simulated lookup does not wait on the caller's budget. The simulation therefore exercises the
production transport over names, while Hickory's protocol, cache and failure handling are checked
against local DNS authorities outside it, as [Evidence](#evidence) describes.
[Deterministic Interconnect Simulation](./interconnect-simulation.md#sockets-and-dns) owns that
build.

The resolver bounds its lookups with the primitive boundary's semaphore and measures its budgets
with the boundary's timers, and it publishes no state to a data-plane protocol, so it has no Shuttle
check or Loom model of its own; the concurrency bound and cancellation are checked by its own tests
against real lookups. `nervix-dns` cannot be built for the
browser at all, because it requires the native capability of `nervix-primitives`.

## Evidence

Each form of evidence establishes a stated part of this contract and nothing more.
`tests/dns-resolution-ledger.md` is the acceptance record: it names every path, the delivery that
moved it, and each test and scenario by name.

| Evidence | Command | What it establishes | What it does not |
| --- | --- | --- | --- |
| The resolver's own checks against an in-process DNS authority | `just test-dns` | Literal and hosts-file answers without a question; search completion and fully qualified names; IPv4-first dual-stack answers; positive and negative TTL reuse and expiry; missing names, empty answers, refusals and invalid names as distinct failures; a silent server ended by the budget and, sooner, by the configured `timeout` and `attempts`; lookups beyond 64 waiting and freed by cancellation; a resolver rebuilt and torn down with its runtime; the configuration grammar and its rejections; both Reqwest hooks, the Hyper connector and the Smithy hook dialling a later answer, keeping the URL authority, following a redirect, reconnecting after TTL expiry, being cancelled by a request timeout, and keeping the typed failure as a cause | The authority answers over UDP only, so truncation and DNS over TCP, a name server that cannot be reached, and the order in which several servers are asked are not exercised |
| Connector and client unit checks | `just test-package-lib <package>` | Each dialling connector's ordered attempts, the time an unresponsive address leaves for the next, literal addresses without a lookup, typed lookup failures beneath the connector's error, TLS failures and budgets; the native client's ordered answers, deadline and typed failure; SigV4 headers naming the configured host | Behavior through a whole node |
| Public scenarios | `just test-scenarios --input <feature>` | Nodes whose resolver asks the harness's DNS authority, described in [DNS Authorities](./integration-test-lifecycle.md#dns-authorities), on one- and three-node clusters, with topology-specific cases on three: cluster formation through names and IPv6 literals, hosts-file and single-label names, a peer followed to a new address after its name stopped resolving, and a stopped leader rejoining through its advertised name; every migrated connector over plain and TLS transports through a fixture name; certificates checked against the configured host; outages answered as a missing name, no address or silence, with offsets held and delivery after recovery; changed answers followed after a restart; the CLI and a client's named seeds | Resolution through a real recursive resolver, and redirects to a named session endpoint, because every advertised client endpoint in the harness is a literal address |
| The Turmoil simulation | `just test-turmoil` | The production transport resolving peer names through the simulated table on every connection, deterministically and under replay | Anything about Hickory, which the simulation never constructs |
| Dependency graph checks | `just validate-dns-dependencies` | The feature selections of [Dependency Selection](#dependency-selection) in each affected crate's own graph | Runtime behavior |
| The primitive boundary check | `just validate-primitive-boundary` | No Nervix-authored code calls the operating system's resolver through `tokio::net::lookup_host` or `ToSocketAddrs` | Resolution inside a dependency |
| The external Chaos suite | `just chaos` | Three node containers that advertise and bootstrap by container name resolving each other through Docker's embedded DNS with the production resolver, across crashes, restarts, partitions and pauses | Changing answers, DNS failures, and the connectors' names |

The scenario features are `cluster/dns_resolution.feature`, the `runtime/*_dns_resolution.feature`
files for RabbitMQ, Redis, MQTT, Syslog, WebSocket, ClickHouse and SQS, the fixture-name scenarios
in the HTTP polling, Prometheus, Sentry, OTEL, Iceberg, HTTP emitter and WebSocket client features,
`tools/cli_session.feature`, and `runtime/client_wire_qualification.feature`.

## Guarantees And Non-Guarantees

The evidence above establishes these guarantees:

- A lookup through the node's resolver never occupies Tokio's blocking pool or calls the C library,
  and a configuration that cannot be loaded fails startup instead of selecting another resolver.
- Every lookup ends within its caller's budget, at most 64 run at once on a node, and a cancelled
  lookup frees its slot at once.
- An address answer is reused for at most one hour and a negative answer for at most thirty
  seconds, and neither beyond its own TTL.
- Apart from the interconnect's startup lookups, every path resolves its configured name again for
  each new connection.
- TLS verification, the HTTP authority and SQS request signing name the configured host, whichever
  address accepted the connection.
- A failed lookup never rejects a record, acknowledges input or commits an offset.
- The Turmoil build never constructs the production resolver, reads a resolver configuration or
  hosts file, or sends a DNS packet.

Nervix does not guarantee:

- Any part of the operating system's name service beyond the resolver configuration and the hosts
  file, as [Deployment Limits](#deployment-limits) lists.
- That the paths [Residual Resolution](#residual-resolution) lists follow the node's DNS options,
  cache or bounds.
- Authenticated or encrypted DNS.
- That HTTP polling, Prometheus, Sentry, OTEL and Iceberg name the lookup failure in their
  diagnostics.
- That an established connection follows a changed answer before its owner ends it.
- That the HTTP request emitter reaches a later answer when an earlier one never responds within
  its `timeout_ms`.

## Operator Diagnostics

A node that loads its resolver logs at `info`:

```text
loaded the name resolver configuration resolver_configuration=/etc/resolv.conf hosts_file=/etc/hosts name_servers=ResolverConfiguration
```

`name_servers` reads `Explicit([...])` with the servers `--dns-name-server` named. A line of the
resolver configuration that could not be read is logged at `warn` as
`ignored a line of the resolver configuration`, with the file and the parser's reason, and a node
that cannot load its resolver exits with the startup failure [Configuration](#configuration)
describes.

A failed lookup is visible through the path that asked. The resolver itself logs no individual
lookup and exports no metric of its own.

| Where | What shows the failure |
| --- | --- |
| RabbitMQ, Redis, MQTT, Syslog, WebSocket, ClickHouse and SQS, and the HTTP request emitter | `DESCRIBE INGESTOR` or `DESCRIBE EMITTER` shows the lookup failure on its `transient error:` line, with the host and the reason, such as `resolving 'rabbitmq.example.internal' failed: the name does not exist`. An SQS source looks its queue up when the ingestor starts, and a lookup failure there is the text of that start's failure |
| HTTP polling, Prometheus, Sentry, OTEL and Iceberg | `DESCRIBE INGESTOR` or `DESCRIBE EMITTER` reports the failed request, export or catalog call, such as `OTEL gRPC export ended without an answer from the receiver`, without the lookup failure beneath it |
| The interconnect | `nervix_interconnect_connection_failures_total{reason="resolution"}` counts failed pool connection attempts by pool class, and each attempt is logged at `debug` as `interconnect pool connection failed` with the peer's node, endpoint, pool class and slot and an error reading `resolving <endpoint> failed: <reason>` |
| Node startup | A recovered Raft member that does not resolve is logged at `warn` as `could not resolve a recovered Raft peer for gossip`; an own or bootstrap endpoint that does not resolve fails startup as `failed to start cluster membership` |
| The CLI | The command fails with `failed to connect to server` and prints its causes, the last of which is the lookup failure, such as `resolving 'nervix.example.internal' failed: the name does not exist` |
| The shared C binding | The call fails with `NX_ERROR_CONNECT`, and its message joins the same causes |

To see what a node sees, ask the name servers its resolver configuration names, remembering that
the node answers a name its hosts file lists without asking DNS and never consults
`nsswitch.conf`.

## Recovery Examples

These follow from the contract above; [Evidence](#evidence) names the checks behind each.

- **A broker's name stops resolving.** A source fails its resume with the lookup failure, which
  `DESCRIBE INGESTOR` shows, and tries again on its retry policy. It receives nothing and
  acknowledges nothing meanwhile, so what its system keeps for it, such as a queue's messages, is
  delivered after it reconnects, while a system that keeps nothing for an absent subscriber, such
  as Redis Pub/Sub, delivers only what is published after the reconnection. A sink that cannot
  reconnect confirms nothing, so its input stays unacknowledged and a Kafka source feeding it holds
  its offset, while `DESCRIBE EMITTER` shows the failure. Once the name is published again, the
  first attempt after the cached negative answer expires, at most thirty seconds after it was
  cached, connects and delivers.
- **A name server goes silent.** Each lookup ends at the caller's budget, or earlier at the
  configuration's `timeout` and `attempts`, as `no answer arrived in time`, and the caller retries
  on its own schedule. Established connections keep working, because nothing re-resolves an open
  connection.
- **A service or peer moves to new addresses.** Open connections stay where they are until their
  owner ends them. The next connection resolves again once the cached answer expires and dials the
  new addresses; a peer that returns at a new address after its name stopped resolving is reached
  again the same way.
- **The first answer cannot be reached.** A caller that dials for itself moves to the next answer
  when the first refuses or its share of the budget ends; a caller that dials through a library
  moves on under that library's policy, as [Dialling The Answers](#dialling-the-answers) describes.
- **The DNS configuration must change.** Edit the resolver configuration or hosts file, or change
  `--dns-name-server`, and restart the node; a rolling restart applies it across a cluster. A node
  that names a local stub such as `systemd-resolved` follows the stub's upstream changes without a
  restart.
- **A node will not start.** `failed to load the name resolver configuration` names the file and
  the reason. Make the file readable, give it a `nameserver` line or pass `--dns-name-server`, or
  correct the search domain, and start the node again.
