# Asynchronous DNS resolution ledger

This ledger is the acceptance record for the
[asynchronous DNS epic](https://app.clickup.com/t/86bc7znqw). It lists every path on which Nervix
turns a host name into an address, the resolver that path uses on the current source, the delivery
that owns moving it, and the evidence that proves what it does now. A path is recorded as using the
node resolver only once its delivery has merged, and later deliveries extend the matrix rather than
starting another.

Run the named Cucumber evidence with `just test-scenarios --input <feature>`, the resolver's checks
with `just test-dns`, the interconnect's with `just test-interconnect`, the RabbitMQ, Syslog,
WebSocket, ClickHouse and SQS connectors' with `just test-package-lib <package>`, the dependency
graphs with `just validate-dns-dependencies`, and the simulation with `just test-turmoil`.

## The node resolver

`nervix-dns` owns one resolver per node, loaded at startup from a `resolv.conf`-format file and a
hosts file on the blocking pool and cloned into every owner that resolves through it. It answers
literal addresses without a query, answers a name the hosts file lists from that file alone, and
asks the configured name servers for everything else through Hickory's asynchronous client, IPv4 and
IPv6 together. Every lookup runs inside the budget its caller gives it, at most 64 run at once, and
answers are cached for their TTL, at most one hour for an address and thirty seconds for a negative
answer. A configuration that cannot be read or names no name server fails startup; nothing falls
back to a public resolver or to the C library. [Peer Name
Resolution](../docs/src/interconnect.md#peer-name-resolution) is the public account.

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
| Native client sessions and OTEL gRPC export (Tonic) | Tonic's default connector | [Hickory DNS 06](https://app.clickup.com/t/86bc7zpnk) | Not yet on the node resolver |
| Redis pool and Pub/Sub | The driver's default resolver | [Hickory DNS 07](https://app.clickup.com/t/86bc7zpnn) | Not yet on the node resolver |
| MongoDB | Hickory for SRV and TXT discovery inside the driver, through its `dns-resolver` feature; Tokio's `lookup_host` for the addresses it connects to | Residual driver boundary | `just validate-dns-dependencies` keeps the discovery feature selected; the address lookups are not replaceable without a supported injection contract |
| NATS, MQTT, PostgreSQL and MySQL (SQLx, `mysql_async`), Pulsar, Kafka (librdkafka), ZeroMQ | Driver-owned system resolution | Residual driver boundary | Not replaceable without a supported injection contract |
| Web console | The browser | Browser-owned | Outside the node |

## Deployment limits

The node resolver implements the `resolv.conf` and hosts-file behavior the public chapter describes
and nothing else of the operating system's name service. It does not read `nsswitch.conf`, load NSS
modules such as LDAP, NIS, `mdns` or `myhostname`, answer `.local` names by multicast DNS, or apply
the `rotate`, `single-request`, `use-vc`, `no-aaaa` or `trust-ad` options, and a platform split-DNS
policy reaches it only through the name server its configuration names. Docker's embedded DNS,
Kubernetes cluster DNS, and the `systemd-resolved` stub are reached through the `resolv.conf` those
platforms write.

## Hickory DNS 01 acceptance

| Acceptance item | Evidence |
| --- | --- |
| Bootstrap and reconnect with fixture names and literal IPv4 and IPv6 endpoints, TLS identity preserved, an available answer reached when another cannot connect | `cluster/dns_resolution.feature`, every scenario; literal IPv4 is every other cluster scenario; every connection verifies the peer certificate against the advertised host, whichever answer it dialled |
| Topology-specific bootstrap, rejoin and leader failover | `cluster/bootstrap.feature`, `cluster/rejoin.feature`, `cluster/leader_failover.feature`, and *A stopped leader rejoins through its advertised name* |
| Hosts, search and fully qualified names; positive and negative TTL expiry; a changed answer on a later lookup; name-not-found and no-address outcomes; silence; bounded retries and concurrency; the budget; cancellation; runtime teardown | `just test-dns`: `hosts_file_names_answer_without_dns`, `search_domains_complete_unqualified_names`, `fully_qualified_names_skip_the_search_list`, `positive_answers_are_reused_within_their_ttl`, `a_changed_answer_is_used_once_its_ttl_expires`, `missing_names_are_reused_within_their_negative_ttl`, `a_published_name_resolves_once_its_negative_ttl_expires`, `names_without_addresses_and_refusals_are_distinct_failures`, `a_silent_name_server_ends_the_lookup_at_its_budget`, `the_host_configuration_bounds_retries_below_the_budget`, `lookups_beyond_the_concurrency_bound_wait_and_cancellation_frees_their_slots`, `resolvers_are_rebuilt_and_torn_down_with_their_runtime`; the configuration checks in `configuration::tests` |
| Establishment cannot wait indefinitely before the transport deadline starts | Resolution runs inside the connection setup deadline; *Peers follow a node that returns at a new address after its name stopped resolving*, example `silence` |
| Turmoil exchange, identity rejection, partition and reconnect, replay and recorded seeds stay deterministic with simulated names | `just test-turmoil` and `just test-turmoil-replay-check`; the scenarios register peers by name, so every connection resolves through the simulated table |
| Production builds select Hickory and contain no simulation scheduler; the combined-mode diagnostic still works | `just validate-turmoil-dependencies`, `just validate-shuttle-dependencies`, `just validate-execution-mode-conflicts` |
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
| The isolated connector and the server select the node resolver, keep AWS-LC, and keep production free of simulation schedulers | `just validate-dns-dependencies`, `just validate-shuttle-dependencies`, `just validate-turmoil-dependencies`; `just check-package nervix-connector-rabbitmq` |

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
