# Asynchronous DNS resolution ledger

This ledger is the acceptance record for the
[asynchronous DNS epic](https://app.clickup.com/t/86bc7znqw). It lists every path on which Nervix
turns a host name into an address, the resolver that path uses on the current source, the delivery
that owns moving it, and the evidence that proves what it does now. A path is recorded as using the
node resolver only once its delivery has merged, and later deliveries extend the matrix rather than
starting another.

Run the named Cucumber evidence with `just test-scenarios --input <feature>`, the resolver's checks
with `just test-dns`, the interconnect's with `just test-interconnect`, and the simulation with
`just test-turmoil`.

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
| HTTP polling ingestion, Prometheus, Sentry and OTEL HTTP export (Reqwest 0.13) | Node resolver injected through `HttpClientConfig`, including TLS and request timeout setup | [Hickory DNS 02](https://app.clickup.com/t/86bc7zpn7) | `runtime/http_client_ingestion.feature`: *HTTP polling resolves its endpoint with the node DNS fixture*; the hostname endpoint scenarios in `runtime/prometheus_ingestion.feature`, `runtime/sentry_emission.feature`, and `runtime/otel_emission.feature`; `just validate-http-dns-dependencies` |
| Iceberg REST catalog and its OAuth request (Reqwest 0.12) | Node resolver injected through `RestCatalogBuilder::with_client`; both catalog and token requests use that client | Hickory DNS 02 | `runtime/iceberg_emission.feature`: *Iceberg catalog and object storage resolve through the node DNS fixture*, one and three nodes; `just validate-http-dns-dependencies` |
| Iceberg S3, GCS and Azure object storage and its credential HTTP path (OpenDAL 0.57, Reqwest 0.13) | Node resolver injected through OpenDAL's `HttpClientLayer`; `AccessorInfoHttpSend` shares the client used by object requests | Hickory DNS 02 | The Iceberg scenario above writes and commits to a hostname S3 endpoint; `just validate-http-dns-dependencies` verifies the isolated connector feature graph |
| RabbitMQ source and sink (Lapin) | The driver's default resolver | [Hickory DNS 03](https://app.clickup.com/t/86bc7zpnc) | Not yet on the node resolver |
| Syslog UDP, TCP and TLS emission, WebSocket ingestion | Tokio's `lookup_host` and host-name dials, on the blocking pool | [Hickory DNS 04](https://app.clickup.com/t/86bc7zpnf) | Not yet on the node resolver |
| ClickHouse and SQS | Hyper's default connector and the AWS SDK's default client | [Hickory DNS 05](https://app.clickup.com/t/86bc7zpng) | Not yet on the node resolver |
| Native client sessions and OTEL gRPC export (Tonic) | Tonic's default connector | [Hickory DNS 06](https://app.clickup.com/t/86bc7zpnk) | Not yet on the node resolver |
| Redis pool and Pub/Sub | The driver's default resolver | [Hickory DNS 07](https://app.clickup.com/t/86bc7zpnn) | Not yet on the node resolver |
| MongoDB | Hickory for SRV and TXT discovery inside the driver; Tokio's `lookup_host` for TCP addresses | Residual driver boundary | Not replaceable without a supported injection contract |
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
| Production builds select Hickory and contain no simulation scheduler; the combined-mode diagnostic still works | `just validate-turmoil-dependencies`, `just validate-shuttle-dependencies`, `just validate-simulation-feature-conflict` |
| Resolver protocol checks use local DNS authorities outside the Turmoil boundary | `nervix-test-environment`'s `dns_authority`, used by `just test-dns` and the Cucumber harness |

## Hickory DNS 02 acceptance

The node passes its validated resolver to every migrated client. Reqwest 0.13 and the separate
Reqwest 0.12 Iceberg dependency explicitly select `hickory-dns`; isolated connector roots are
checked by `just validate-http-dns-dependencies`. The custom adapters do not construct Reqwest's
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
| Isolated consumer builds retain the selected Reqwest features | `just validate-http-dns-dependencies`; `just check-package` for each affected connector |
