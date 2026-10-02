# Asynchronous DNS resolution ledger

This ledger is the acceptance record for the
[asynchronous DNS epic](https://app.clickup.com/t/86bc7znqw). It lists every path on which Nervix
turns a host name into an address, the resolver that path uses on the current source, the delivery
that owns moving it, and the evidence that proves what it does now. A path is recorded as using the
node resolver only once its delivery has merged, and later deliveries extend the matrix rather than
starting another.

Run the named Cucumber evidence with `just test-scenarios --input <feature>`, the resolver's checks
with `just test-dns`, the interconnect's with `just test-interconnect`, and focused connector and
native-client units with `just test-package-lib <package>`. Check dependency graphs with
`just validate-dns-dependencies` and the simulation with `just test-turmoil`.

## The node resolver

`nervix-dns` owns one resolver per node, loaded at startup from a `resolv.conf`-format file and a
hosts file on the blocking pool and cloned into every owner that resolves through it. It answers
literal addresses without a query, answers a name the hosts file lists from that file alone, and
asks the configured name servers for everything else through Hickory's asynchronous client, IPv4 and
IPv6 together. Every lookup runs inside the budget its caller gives it, at most 64 run at once, and
answers are cached for their TTL, at most one hour for an address and thirty seconds for a negative
answer. A configuration that cannot be read or names no name server fails startup; nothing falls
back to a public resolver or to the C library. [Name Resolution](../docs/src/name-resolution.md)
is the public account.

## Path matrix

| Path | Resolver on the current source | Owning delivery | Evidence |
| --- | --- | --- | --- |
| Interconnect: the node's own advertised endpoint and its bootstrap endpoint, at startup | Node resolver, within the connection setup timeout | [Hickory DNS 01](https://app.clickup.com/t/86bc7zpmz) | `cluster/dns_resolution.feature`: *Nodes form a cluster through DNS names* and *through literal IPv6 endpoints*, one and three nodes |
| Interconnect: every outbound pool connection to a discovered peer | Node resolver, again for each attempt inside its setup deadline; every answer dialled in order | Hickory DNS 01 | `cluster/dns_resolution.feature`: *Peers reach a node through the answer that connects*, *Single-label names are completed by the search domain*, *Names the hosts file lists resolve without asking DNS*, *Peers follow a node that returns at a new address after its name stopped resolving*, *A stopped leader rejoins through its advertised name*; `just test-interconnect` |
| Interconnect: a gossip bootstrap exchange | The exact seed address the startup lookup produced | Hickory DNS 01 | The three-node examples above join through a named bootstrap endpoint |
| Interconnect in the `turmoil` build | Turmoil's simulated DNS table behind `PeerResolver::simulated`; no Hickory resolver is built | Hickory DNS 01 | `just test-turmoil`: every committed seed, run twice in fresh processes |
| HTTP polling ingestion, Prometheus, Sentry and OTEL HTTP export (Reqwest 0.13) | Node resolver injected through `HttpClientConfig`, including TLS and request timeout setup | [Hickory DNS 02](https://app.clickup.com/t/86bc7zpn7) | `runtime/http_client_ingestion.feature`: *HTTP polling resolves its endpoint with the node DNS fixture*; the hostname endpoint scenarios in `runtime/prometheus_ingestion.feature`, `runtime/sentry_emission.feature`, and `runtime/otel_emission.feature`; `just validate-dns-dependencies` |
| Iceberg REST catalog and its OAuth request (Reqwest 0.12) | Node resolver injected through `RestCatalogBuilder::with_client`; both catalog and token requests use that client | Hickory DNS 02 | `runtime/iceberg_emission.feature`: *Iceberg catalog and object storage resolve through the node DNS fixture*, one and three nodes; `just validate-dns-dependencies` |
| Iceberg S3, GCS and Azure object storage and its credential HTTP path (OpenDAL 0.57, Reqwest 0.13) | Node resolver injected through OpenDAL's `HttpClientLayer`; `AccessorInfoHttpSend` shares the client used by object requests | Hickory DNS 02 | The Iceberg scenario above writes and commits to a hostname S3 endpoint; `just validate-dns-dependencies` verifies the isolated connector feature graph |
| RabbitMQ source and sink (Lapin) | Node resolver, again for every connection: each answer dialled in order within a 30 second budget shared with TLS, and the transport handed to Lapin's `Connection::connector`. Lapin's `hickory-dns` feature, a process-wide resolver read from `/etc/resolv.conf`, stays off | [Hickory DNS 03](https://app.clickup.com/t/86bc7zpnc) | `runtime/rabbitmq_dns_resolution.feature`, every scenario, one and three nodes; `just test-package-lib nervix-connector-rabbitmq`; `just validate-dns-dependencies` |
| Syslog UDP, TCP and TLS emission | Node resolver on each new sender, inside a 30-second budget shared with UDP setup or ordered TCP/TLS address attempts and the TLS handshake | [Hickory DNS 04](https://app.clickup.com/t/86bc7zpnf) | `runtime/syslog_dns_resolution.feature`, one and three nodes; `just validate-dns-dependencies` |
| WebSocket `ws` and `wss` client ingestion | Node resolver on every initial connection and resume, inside a 30-second budget shared with ordered TCP/TLS and upgrade attempts; the configured URL remains the request authority | [Hickory DNS 04](https://app.clickup.com/t/86bc7zpnf) | `runtime/websocket_client_ingestion.feature`, `runtime/websocket_client_tls_resource_mounts.feature`, and `runtime/websocket_dns_resolution.feature`, one and three nodes; `just validate-dns-dependencies` |
| ClickHouse emission, plain HTTP and TLS clients (Hyper `HttpConnector`) | Node resolver injected as the connector's resolver service on every new connection, in both the plain-HTTP client that replaces the driver's default and the TLS client; the insert's `timeout_ms` wait covers the connection | [Hickory DNS 05](https://app.clickup.com/t/86bc7zpng) | `runtime/clickhouse_dns_resolution.feature`, one and three nodes; `just test-package-lib nervix-connector-clickhouse`; `just validate-dns-dependencies` |
| SQS source and sink, default-root and custom-CA clients (Smithy HTTP client) | Node resolver injected through Smithy's `ResolveDns` with `build_with_resolver` on every new connection, in both the default-root client, which keeps the SDK default's proxy environment, and the custom-CA client; static credentials and region leave `aws-config`'s default provider chains unused, and its SSO token chain, never asked by SigV4 SQS, shares the same client | [Hickory DNS 05](https://app.clickup.com/t/86bc7zpng) | `runtime/sqs_dns_resolution.feature`, one and three nodes; `just test-package-lib nervix-connector-sqs`; `just validate-dns-dependencies` |
| Native CLI and SDK sessions (Tonic) | Hickory resolver loaded once from the system or explicit client configuration; a server-internal session shares its node's resolver. Tonic's custom eager connector resolves each new connection and retains the original URI authority and TLS name | [Hickory DNS 06](https://app.clickup.com/t/86bc7zpnk) | `tools/cli_session.feature`: hostname HTTP/HTTPS and wrong-name TLS cases; `runtime/client_wire_qualification.feature`: named-seed subscription and transaction recovery; `nervix-client-core`'s `native_session_` tests |
| OTEL gRPC export (Tonic) | Node resolver through Tonic's custom lazy connector, asked only when an export needs a connection | [Hickory DNS 06](https://app.clickup.com/t/86bc7zpnk) | `runtime/otel_emission.feature`: hostname log, trace and metric exports, DNS failure and recovery, ordered addresses; `nervix-connector-otel`'s lazy channel test |
| The node's own OTLP trace export (`opentelemetry-otlp` 0.31.1, Tonic 0.14.6) | A lazy custom connector waits for the node's shared resolver; the export timeout bounds DNS/TCP after installation and requests | [Hickory DNS 09](https://app.clickup.com/t/86bc9yev3) | `cluster/node_trace_dns_resolution.feature`, one and three server processes; focused tracing setup tests |
| Redis command pool (`redis` 1.7.1) | Node resolver through `AsyncConnectionConfig::set_dns_resolver` on every new physical `bb8` connection; Redis keeps the configured URL, auth, database, protocol and TLS policy | [Hickory DNS 07](https://app.clickup.com/t/86bc7zpnn) | `runtime/redis_dns_resolution.feature`, one and three nodes, plain and TLS; `just test-package-lib nervix-connector-redis` |
| Redis Pub/Sub subscription | Node resolver before every dedicated connection or resume; ordered address attempts within a shared DNS/TCP/TLS budget, then the stream and original settings go to `PubSub::new` | Hickory DNS 07 | `runtime/redis_dns_resolution.feature`, one and three nodes, plain and TLS, outage and changed-answer recovery; `runtime/redis_tls_resource_mounts.feature` |
| MQTT source and sink (`rumqttc-v5-next` 0.34.0) | Node resolver through `MqttOptions::set_socket_connector` on every connection the event loop opens, first and after each lost connection: every answer dialled in order with the driver's `connect_socket_addr`, which applies its `NetworkOptions`, within the driver's five-second connect timeout; the driver still completes TLS against the configured host and the MQTT handshake over the stream | [Hickory DNS 07a](https://app.clickup.com/t/86bc9jd87) | `runtime/mqtt_dns_resolution.feature`, one and three nodes, plain and TLS; `just test-package-lib nervix-connector-mqtt`; `just validate-dns-dependencies` |
| MongoDB (`mongodb` 3.9.1) | Driver's Hickory resolver for SRV and TXT discovery through its `dns-resolver` feature; Tokio `lookup_host` for the addresses its sockets connect to | Residual driver boundary | `mongodb::runtime::resolve_address` owns the socket lookup; `just validate-dns-dependencies` keeps the discovery feature selected |
| NATS (`async-nats` 0.49.1) | Driver-owned Tokio `lookup_host` in `ServerAddr::socket_addrs`, before its per-address connection timeout | Residual driver boundary | Driver connector calls `socket_addrs` before dialing; no resolver injection contract in this version |
| PostgreSQL (`sqlx-postgres` 0.9.0) | SQLx socket path resolves nonliteral addresses with blocking `ToSocketAddrs` on Tokio's blocking pool | Residual driver boundary | SQLx `net::socket` owns the address conversion; no supported resolver injection at the client configuration boundary |
| MySQL (`mysql_async` 0.36.2) | Driver calls `TcpStream::connect((host, port))`, which uses Tokio's system resolver | Residual driver boundary | `mysql_async::io::Stream::connect_tcp` owns the dial; no supported resolver injection in this version |
| Pulsar (`pulsar` 6.9.0, Nervix fork) | Driver calls `Url::socket_addrs` on a blocking worker for broker and proxy endpoints, then dials the resulting addresses through Tokio | Residual driver boundary | `pulsar::connection::Connection::new` owns resolution and reconnect; no supported resolver injection |
| Kafka (`rdkafka` 0.39.0, librdkafka 2.12.1) | Native librdkafka resolves bootstrap and broker hosts | Residual native boundary | Rust connector passes broker names into librdkafka; no Rust async resolver hook in the native client |
| ZeroMQ (`zeromq` 0.4.1) | Driver transport calls `TcpStream::connect((host, port))`, which uses Tokio's system resolver | Residual driver boundary | `zeromq::transport::tcp::connect` owns the dial and exposes no socket injection hook |
| Web console | The browser | Browser-owned | Outside the node |

## Deployment limits

[Deployment Limits](../docs/src/name-resolution.md#deployment-limits) is the account of what the
node resolver implements of the operating system's name service and what it does not.

## Hickory DNS 01 acceptance

| Acceptance item | Evidence |
| --- | --- |
| Bootstrap and reconnect with fixture names and literal IPv4 and IPv6 endpoints, TLS identity preserved, an available answer reached when another cannot connect | `cluster/dns_resolution.feature`, every scenario; literal IPv4 is every other cluster scenario; every connection verifies the peer certificate against the advertised host, whichever answer it dialled |
| Topology-specific bootstrap, rejoin and leader failover | `cluster/bootstrap.feature`, `cluster/rejoin.feature`, `cluster/leader_failover.feature`, and *A stopped leader rejoins through its advertised name* |
| Hosts, search and fully qualified names; positive and negative TTL expiry; a changed answer on a later lookup; name-not-found and no-address outcomes; silence; bounded retries and concurrency; the budget; cancellation; runtime teardown | `just test-dns`: `hosts_file_names_answer_without_dns`, `search_domains_complete_unqualified_names`, `fully_qualified_names_skip_the_search_list`, `positive_answers_are_reused_within_their_ttl`, `a_changed_answer_is_used_once_its_ttl_expires`, `missing_names_are_reused_within_their_negative_ttl`, `a_published_name_resolves_once_its_negative_ttl_expires`, `names_without_addresses_and_refusals_are_distinct_failures`, `a_silent_name_server_ends_the_lookup_at_its_budget`, `the_host_configuration_bounds_retries_below_the_budget`, `lookups_beyond_the_concurrency_bound_wait_and_cancellation_frees_their_slots`, `resolvers_are_rebuilt_and_torn_down_with_their_runtime`; the configuration checks in `configuration::tests` |
| Establishment cannot wait indefinitely before the transport deadline starts | Resolution runs inside the connection setup deadline; *Peers follow a node that returns at a new address after its name stopped resolving*, example `silence` |
| Turmoil exchange, identity rejection, partition and reconnect, replay and recorded seeds stay deterministic with simulated names | `just test-turmoil` and `just test-turmoil-replay-check`; the scenarios register peers by name, so every connection resolves through the simulated table |
| Production builds select Hickory and contain no simulation scheduler; the combined-mode diagnostic still works | `just validate-execution-mode-dependencies`, `just validate-execution-mode-conflicts` |
| Resolver protocol checks use local DNS authorities outside the Turmoil boundary | `nervix-test-environment`'s `dns_authority`, used by `just test-dns` and the Cucumber harness |

## Hickory DNS 02 acceptance

The node passes its validated resolver to every migrated client. Reqwest 0.13 and the separate
Reqwest 0.12 Iceberg dependency explicitly select `hickory-dns`; isolated connector roots are
checked by `just validate-dns-dependencies`. The custom adapters do not construct Reqwest's
default Hickory resolver, whose system-configuration error path can choose a public name server.
The Iceberg REST client's OAuth exchange uses its injected client. OpenDAL's S3 `detect_region`
helper has a separate client, but no Nervix path calls that helper; operator construction uses its
configured region or the driver's environment policy. The object and credential paths use the
operator's injected HTTP client.

| Acceptance item | Evidence |
| --- | --- |
| HTTP polling, Prometheus, Sentry, OTEL HTTP, and Iceberg catalog/object storage reach fixture names without changing request authority or commit behavior | The hostname endpoint scenarios in `runtime/http_client_ingestion.feature`, `runtime/prometheus_ingestion.feature`, `runtime/sentry_emission.feature`, `runtime/otel_emission.feature`, and `runtime/iceberg_emission.feature`, each with one and three node examples |
| Reqwest 0.13 and 0.12 use configured DNS, multiple addresses, redirect destinations, timeout cancellation and TTL reconnect | `just test-dns`: `http_clients` integration checks |
| A bad resolver configuration has no fallback | `just test-dns` configuration checks; node startup loads `DnsResolver` before any connector starts |
| Isolated consumer builds retain the selected Reqwest features | `just validate-dns-dependencies`; `just check-package` for each affected connector |

## Hickory DNS 03 acceptance

RabbitMQ connects through the node resolver rather than through Lapin's `hickory-dns` feature.
That feature resolves with one async-rs resolver per process, built on first use from the host's
`/etc/resolv.conf` and kept in a static together with name-server connections whose tasks ran on
the Tokio runtime of its first lookup. It would ignore the node's resolver configuration, hosts
snapshot, budget and concurrency bound, and a runtime created after that one stopped would inherit
its connections. Lapin's `Connection::connector` hook takes a transport instead, so the connector
resolves, dials and completes TLS itself and hands Lapin the established stream; with Lapin's own
reconnection off, the hook is asked once per connection. Lapin's URI grammar reads an IPv6 literal
host as `localhost`, so the connector reads the host with the URL grammar.

| Acceptance item | Evidence |
| --- | --- |
| Source consumption and sink publication over AMQP and AMQPS through fixture names, with explicitly provisioned queues | `runtime/rabbitmq_dns_resolution.feature`: *RabbitMQ sources and sinks reach a broker named by the node DNS fixture over AMQP* and *over AMQPS*, one and three nodes |
| The broker certificate is verified against the configured host, not the dialled address | *An AMQPS client rejects a broker certificate that does not name the host it resolved*; the scenario certificate names `*.nervix.test` and `127.0.0.1`, so only the host name can fail it |
| DNS outage and recovery restart consumers without leaking any, and the sink reopens | *RabbitMQ sources and sinks reconnect once their broker name resolves again after name not found* and *after silence*: while the name fails, the address its cached answer names stops, the queue loses both consumers, and `DESCRIBE INGESTOR` shows the lookup failure once that answer expires; exactly two consumers return through the next answer, and the record published meanwhile is delivered |
| Publisher confirmations hold the input offset while the broker name does not resolve | *RabbitMQ publisher confirms hold the input offset while the broker name does not resolve*: the Kafka offset stays below the record while `DESCRIBE EMITTER` shows the lookup failure, and advances once the record is confirmed |
| Multiple answers, a changed answer on the next connection, and runtime replacement | *RabbitMQ connections dial the answer that connects, follow a changed answer and resolve again after a restart*; `just test-package-lib nervix-connector-rabbitmq`: `connections_dial_the_first_answer_that_accepts_and_hand_lapin_the_transport`, `an_address_that_never_answers_leaves_time_for_the_next`, `no_answer_that_accepts_ends_within_the_budget`, `every_runtime_connects_through_the_resolver_it_was_given` |
| Numeric IPv4 and IPv6 endpoints, typed DNS failures, TLS failures and the budget | `hosts_are_read_with_the_url_grammar`, `ipv6_literals_are_dialled_as_written`, `names_that_do_not_resolve_keep_their_dns_failure`, `amqps_handshakes_that_fail_or_stall_are_tls_failures`, `invalid_addresses_and_ca_files_are_configuration_failures`, `failed_connections_keep_their_typed_cause_and_leave_the_source_to_resume` |
| No leaked connections or threads | A connection that fails before the AMQP handshake has started no Lapin thread; the stand-in brokers of the connector checks observe the client close its socket once the handshake fails; the outage scenarios count consumers exactly |
| The isolated connector and the server select the node resolver, keep AWS-LC, and keep production free of simulation schedulers | `just validate-dns-dependencies`, `just validate-execution-mode-dependencies`; `just check-package nervix-connector-rabbitmq` |

## Hickory DNS 04 acceptance

Syslog senders and WebSocket client sources resolve through the node resolver for every new
connection and share one 30-second budget between the lookup, the ordered address attempts and,
for TLS, the handshake; a WebSocket attempt also completes the opening upgrade. Syslog UDP binds a
socket in each answer's family and sends to the first whose setup succeeds. TLS verifies the
configured host, and the WebSocket upgrade keeps the configured URL's authority, path and query.
The source host cancels a pending resume when shutdown or a quiesce change arrives.

| Acceptance item | Evidence |
| --- | --- |
| Syslog UDP, TCP with octet framing, and TLS with mutual TLS reach a fixture name, and TLS verifies the fixture hostname | `runtime/syslog_dns_resolution.feature`: *Syslog UDP emitter reaches an endpoint named by the node DNS fixture*, *Syslog TCP emitter reaches a fixture-named listener with octet framing* and *Syslog TLS emitter verifies the fixture hostname and mutual TLS identity*, one and three nodes |
| Syslog output waits out a name that does not exist or a silent name server and delivers through the next answer | *Syslog output waits for name recovery before delivery*: `DESCRIBE EMITTER` shows `resolving 'syslog.nervix.test' failed`, and the record arrives once the name resolves, one and three nodes, name not found and silence |
| WebSocket clients dial the answer that connects, follow changed answers, and recover from no addresses, a name that does not exist and silence | `runtime/websocket_dns_resolution.feature`: *WebSocket clients reconnect through a changed DNS answer*, one node with name not found and three nodes with silence |
| Existing WebSocket client and resource-mounted TLS behavior through fixture names | `runtime/websocket_client_ingestion.feature` and `runtime/websocket_client_tls_resource_mounts.feature` |
| Ordered answers, hosts-file names, typed missing-name failures, literal IPv6 endpoints, the URL authority and default ports | `just test-package-lib nervix-connector-syslog`: `tcp_tries_dns_answers_in_order_and_writes_to_the_reachable_address`, `tcp_uses_the_hosts_file_before_dns`, `missing_name_is_an_initialization_failure_with_the_dns_cause`; `just test-package-lib nervix-connector-websockets`: `resume_tries_the_next_address_and_preserves_host_path_and_query`, `resume_connects_to_a_literal_ipv6_endpoint_without_a_dns_question`, `source_plan_uses_url_default_ports_and_rejects_other_schemes` |
| Shutdown and quiesce cancel a pending resume with its lookup, dial and handshake | `just test-lib`: `source_shutdown_cancels_pending_resume_and_closes_the_source`, `source_quiesce_change_cancels_pending_resume_before_retrying` |
| The isolated connectors select the node resolver | `just validate-dns-dependencies` |

## Hickory DNS 05 acceptance

ClickHouse and SQS keep their drivers' HTTP clients and install the node resolver at each driver's
own DNS hook: `nervix-dns` implements Hyper's resolver service for `hyper-util`'s `HttpConnector`
and Smithy's `ResolveDns`. The ClickHouse connector builds that connector for both of its clients,
the plain HTTP client that replaces the driver's default, with the driver's keepalive and idle pool
timeout, and the TLS client. The SQS connector builds its Smithy client with `build_with_resolver`
for both of its clients: the default-root client keeps the SDK default's AWS-LC, native roots and
proxy environment, and the custom-CA client trusts that CA alone with no proxy. The SDK signs a
request before the connector resolves its host. The static credentials and region leave
`aws-config`'s default credential and region chains unused; its SSO token chain, which SigV4 SQS
never asks, would share the same HTTP client. A failed lookup reaches each connector as a cause of
the driver's error, where `DnsLookupError::find_in` recovers it as the report's typed context.

| Acceptance item | Evidence |
| --- | --- |
| ClickHouse inserts into an explicitly provisioned table through a fixture name over HTTP and over HTTPS with a custom CA | `runtime/clickhouse_dns_resolution.feature`: *ClickHouse emitters insert through a host named by the node DNS fixture over HTTP* and *over HTTPS*, one and three nodes |
| SQS sources and sinks consume and send through a fixture name over HTTP with the default-root client and over HTTPS with the custom-CA client | `runtime/sqs_dns_resolution.feature`: *SQS sources and sinks reach a service named by the node DNS fixture over HTTP* and *over HTTPS*, one and three nodes, `SINGLE` and `BATCH` |
| Certificate checks name the configured host, whichever address was dialled | *An HTTPS ClickHouse client rejects a certificate that does not name the host it resolved* and *An HTTPS SQS client rejects a certificate that does not name the host it resolved*: the scenario certificate names `*.nervix.test` and `127.0.0.1`, so only the host name can fail it |
| Signed SQS requests keep the configured host while connecting to a resolved address | `just test-package-lib nervix-connector-sqs`: `requests_reach_the_answer_that_accepts_signed_for_the_configured_host` reads the `Host` header and the SigV4 signed headers of the sink's and the source's requests |
| Multiple answers, typed lookup failures, transport failures, literal addresses and the default client's plain-HTTP boundary | `just test-package-lib nervix-connector-clickhouse`: `inserts_reach_the_answer_that_accepts_and_keep_the_configured_authority`, `literal_addresses_are_dialled_without_a_lookup`, `a_host_that_does_not_resolve_is_the_typed_cause_of_the_insert_failure`, `a_refused_connection_is_described_by_its_causes`, `a_client_without_tls_entries_speaks_plain_http_only`; `just test-package-lib nervix-connector-sqs`: `a_literal_endpoint_is_dialled_without_a_lookup`, `a_host_that_does_not_resolve_is_the_typed_cause_of_the_queue_lookup_failure`, `a_refused_connection_is_described_by_its_causes`, `a_service_response_keeps_its_short_description`; `just test-dns`: `hyper_connector_uses_all_answers_and_keeps_the_url_authority`, `hyper_connector_failures_keep_the_typed_lookup_failure`, `smithy_hook_answers_every_address_and_fails_with_the_typed_lookup_failure`, `reqwest_failures_keep_the_typed_lookup_failure` |
| DNS outage and silence hold the input offset, survive a restart and follow a changed answer | *ClickHouse inserts wait out name not found for their host name, through a restart, and follow the next answer* and *wait out silence*: the first answer refuses and the second accepts, `DESCRIBE EMITTER` shows the lookup failure while the Kafka offset stays below the record, a restart keeps it there, and the record lands through the next answer; *SQS sends hold the input offset while the service name does not resolve* |
| SQS sources resume after a failed lookup without deleting or losing a message | *SQS sources resume once their service name resolves again after name not found* and *after silence*: `DESCRIBE INGESTOR` shows the lookup failure, and the message published meanwhile is delivered through the next answer |
| Request deadlines include the lookup, and SDK retries stay as configured | `configured_timeout_bounds_clickhouse_insert_completion`; `client_timeout_bounds_each_request_while_sdk_retries_stay_disabled`; `request_timeout_cancels_a_silent_dns_lookup` for the shared hook budget |
| The isolated connectors select the node resolver and AWS-LC | `just validate-dns-dependencies`; `just check-package nervix-connector-clickhouse`, `just check-package nervix-connector-sqs` |

## Hickory DNS 06 acceptance

Native sessions reuse one Hickory resolver across the first connection, seeds, redirects, and
reconnects. A node that opens a peer session for shutdown drain passes its own loaded resolver.
OTEL gRPC installs that same node resolver under Tonic's lazy channel; constructing the channel
does not open a socket or ask DNS. Tonic keeps the configured URI for HTTP/2 authority and TLS
verification, while its connection deadline includes lookup and ordered address attempts.

| Acceptance item | Evidence |
| --- | --- |
| Native CLI commands reach a fixture hostname over HTTP and HTTPS in one- and three-node clusters | `tools/cli_session.feature`: *CLI connects by hostname over <mode> through the configured DNS fixture* |
| A certificate for another DNS name is rejected after a successful lookup | `tools/cli_session.feature`: *CLI rejects a TLS certificate for a different DNS hostname* |
| A subscription and open transaction retain their identities through leader loss and named-seed recovery | `runtime/client_wire_qualification.feature`: *Subscription restoration and typed transaction inspection survive the same leader loss* |
| Native DNS errors remain typed, a silent lookup is canceled by the connection deadline, and a second address can connect | `just test-package-lib nervix-client-core native_session_` |
| OTEL exports logs, traces and metrics to a hostname, retries through an outage, and tries a usable address after an unreachable first address | `runtime/otel_emission.feature`: the log/trace and HTTP/gRPC metric outlines, each with one and three nodes |
| Constructing OTEL's lazy gRPC channel makes no DNS query; its first export does | `just test-package-lib nervix-connector-otel lazy_grpc_channel_resolves_only_when_an_export_needs_a_connection` |
| The production, Shuttle, Turmoil and browser builds keep their boundaries | `just validate`, `just validate-dns-dependencies`, `just test-turmoil`; the web-console build is part of `just test-scenarios` setup |

## Hickory DNS 07 acceptance

Redis's command pool installs the node resolver into Redis's supported DNS hook for each new
physical connection. The Pub/Sub source resolves its own dedicated stream before giving it and
the client's original settings to Redis for authentication, database selection, protocol setup,
and subscription. The source opens that stream again on each resume. Established connections are
not closed merely because their DNS answer expires.

| Acceptance item | Evidence |
| --- | --- |
| Plain and TLS source-to-relay and relay-to-channel behavior use fixture hostnames on one and three nodes | `runtime/redis_dns_resolution.feature`: *Redis source and sink use the node DNS fixture over Redis* and *over Rediss*, each with pool minimum and maximum one; `runtime/redis_tls_resource_mounts.feature` preserves existing resource-mounted TLS behavior |
| Pool pressure cannot occupy or break the dedicated subscription, and TLS validates the original hostname | The source-and-sink scenarios succeed with a one-socket command pool while Pub/Sub stays subscribed; *A Rediss subscription rejects a certificate for another DNS hostname* observes the name failure on one and three nodes |
| DNS failures, ordered answers, reconnect, outage recovery, changed answers, and stop/restart preserve delivery and host retries | *Redis subscriptions and command pools follow a changed answer after name not found* and *after silence* on one and three nodes; `just test-package-lib nervix-connector-redis` covers ordered addresses, typed missing/empty/silent results, cancellation, IPv6 and Unix paths |
| Redis client settings and pool semantics survive the resolver injection | `a_subscription_keeps_the_clients_auth_database_and_protocol_settings`, `only_definitive_redis_command_rejections_are_record_failures`, the source-and-sink scenarios with maximum pool size one, and `just validate-dns-dependencies` for the isolated Redis connector and AWS-LC TLS graph |
| Remaining system lookups are identified against the current lockfile | The path matrix above records the exact driver method or native boundary for MongoDB, NATS, PostgreSQL, MySQL, Pulsar, Kafka, and ZeroMQ, which expose no supported resolver hook at their connection boundary in the locked versions. MQTT's supported socket hook is installed by [Hickory DNS 07a](#hickory-dns-07a-acceptance) |

## Hickory DNS 07a acceptance

Both MQTT clients, a source instance's and a sink's, install the node resolver through the
driver's supported socket hook, `MqttOptions::set_socket_connector`, in place of its default
connector, which resolved with Tokio's `lookup_host`. The event loop asks the hook for a TCP
stream on every connection, first and after each lost connection, so every connection resolves
again. The hook dials the answers in order with the driver's own per-address dialer, which applies
the driver's `NetworkOptions`, inside the driver's connect timeout. The driver then completes TLS
against the configured host and runs the MQTT handshake exactly as before. Nervix's build selects
neither the driver's proxy nor its WebSocket transport.

| Acceptance item | Evidence |
| --- | --- |
| Plain and TLS source-to-relay and relay-to-topic behavior use fixture hostnames on one and three nodes | `runtime/mqtt_dns_resolution.feature`: *MQTT sources and sinks reach a broker named by the node DNS fixture over MQTT* and *over MQTTS* |
| TLS validates the configured hostname for both source and sink | *An MQTTS client rejects a broker certificate that does not name the host it resolved*: the scenario certificate names `*.nervix.test` and `127.0.0.1`, so only the host name can fail it, and `DESCRIBE INGESTOR` and `DESCRIBE EMITTER` show the name check that failed |
| DNS outage and recovery, with the typed failure visible, on one and three nodes | *MQTT sources and sinks reconnect once their broker name resolves again after name not found* and *after silence*: while the name fails, `DESCRIBE INGESTOR` shows the resolver's lookup error and `DESCRIBE EMITTER` shows `MqttConnectionError::Resolve`; both reconnect through the next answer and deliver |
| Host acknowledgement and retry semantics survive a lookup failure | *MQTT QoS 1 publishes hold the input offset while the broker name does not resolve*: the Kafka offset stays below the record while the sink cannot connect, and advances once the broker acknowledges it through the next answer |
| Ordered answers, a changed answer on the next connection, and stop/restart | *MQTT connections dial the answer that connects, follow a changed answer and resolve again after a restart*, whose emitter publishes at QoS 0: a restarted QoS 1 or 2 emitter cannot reconnect while the broker retains its persistent session, because the driver rejects a resumed session it holds no local state for, however the broker was resolved; `just test-package-lib nervix-connector-mqtt`: `a_connection_dials_the_answers_in_order_until_one_accepts`, `an_address_that_never_answers_leaves_time_for_the_next`, `no_answer_that_accepts_ends_within_the_budget_with_the_last_failure` |
| Typed lookup failures, IPv4 and IPv6 answers and literals, the driver's network options and its event loop | `names_that_do_not_resolve_keep_their_typed_lookup_failure`, `a_name_with_both_address_families_reaches_its_ipv6_answer`, `literal_addresses_are_dialled_without_a_lookup`, `every_attempt_applies_the_drivers_network_options`, `the_event_loop_connects_through_the_node_resolver`, `an_event_loop_whose_broker_does_not_resolve_reports_the_lookup`, and the source's and sink's resume and event-loop checks |
| The isolated connector selects the node resolver | `just validate-dns-dependencies`; `just check-package nervix-connector-mqtt` |

## Hickory DNS 08 acceptance

[Name Resolution](../docs/src/name-resolution.md) is the consolidated architecture account of this
matrix: resolver ownership and lifetime, configuration, resolution order, cache and bounds, lookup
outcomes, every call site and client-library hook, dialling policy, connection identity, failure
ownership, dependency selection, residual resolution, deployment limits, build modes, evidence,
operator diagnostics and recovery. The interconnect, connector, session, shutdown, simulation and
test-lifecycle chapters keep their own facts and link to it. The matrix and chapter include
the node's own OTLP trace exporter and its node-owned resolution boundary.

| Acceptance item | Evidence |
| --- | --- |
| The chapter is registered as the authoritative reference and reachable from the book | `AGENTS.md`, `docs/src/SUMMARY.md` and the Architecture And Internals index; `just book dev` |
| Operator diagnostics are the product's own | The startup messages, the `info` line and the CLI's lookup failure quoted by the chapter were captured from `nervix-server` and `nervix-cli` built from this source |
| The feature graph matches the chapter | `just validate-dns-dependencies`; the server's normal dependency graph enables only `default`, `system-config` and `tokio` on Hickory 0.26.3 and 0.25.2, so no DNSSEC or encrypted DNS transport is compiled |

## Hickory DNS 09 acceptance

| Acceptance item | Evidence |
| --- | --- |
| Every node exports startup spans through its configured resolver | `cluster/node_trace_dns_resolution.feature`: *Every node exports its own startup traces through fixture DNS*, one and three real server processes, capturing the node service identities and DNS queries |
| The channel is lazy and uses the shared resolver after installation | Tracing setup's lazy channel and pending-resolver tests |
| Lookup failures and silent DNS remain bounded by the export timeout | Tracing setup's early-startup wait, closed-publication and silent-DNS tests |
| DNS configuration failure timing, startup tracing and shutdown remain owned by startup | The existing DNS load boundary publishes its resolver; the tracing guard closes publication before flushing |
