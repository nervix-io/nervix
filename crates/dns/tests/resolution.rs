//! Focused resolver protocol checks against local DNS authorities.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** Resolver behaviour observed through real DNS messages on loopback: literal and
//!   hosts-file answers, search and fully qualified names, dual-stack answers, positive and negative
//!   TTLs, failures, budgets, the concurrency bound, cancellation, and runtime teardown.
//! - **Depends on.** `nervix-dns` and the in-process DNS authority from the test environment.
//! - **Must not know.** Any caller of the resolver.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    time::{Duration, Instant},
};

use meticulous::ResultExt as _;
use nervix_dns::{
    DnsConfiguration, DnsLookupFailure, DnsResolver, MAX_CONCURRENT_LOOKUPS, NameServers,
};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;

/// A budget no check expects to exhaust: a lookup that ends ends well before it.
const GENEROUS_BUDGET: Duration = Duration::from_secs(30);
/// How long a check waits for a resolver to observe a zone change after a TTL expired.
const CHANGE_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const PORT: u16 = 7443;
/// One-second queries without retries, completed by the fixture's search domain.
const QUICK_OPTIONS: &str = "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n";

fn v4(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(192, 0, 2, last))
}

fn v6(last: u16) -> IpAddr {
    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, last))
}

fn sockets(addresses: &[IpAddr]) -> Vec<SocketAddr> {
    let mut sockets = Vec::with_capacity(addresses.len());
    for address in addresses {
        sockets.push(SocketAddr::new(*address, PORT));
    }
    sockets
}

/// A resolver asking one local authority, with the resolver configuration and hosts file it read.
struct Fixture {
    authority: DnsAuthority,
    resolver: DnsResolver,
    _files: TempDir,
}

impl Fixture {
    async fn start(resolver_options: &str, hosts: &str) -> Self {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("a loopback UDP port is available");
        let (files, configuration) = Self::files(resolver_options, hosts, authority.address());
        let resolver = DnsResolver::load(configuration)
            .await
            .assured("the fixture configuration is valid");
        Self {
            authority,
            resolver,
            _files: files,
        }
    }

    fn files(
        resolver_options: &str,
        hosts: &str,
        name_server: SocketAddr,
    ) -> (TempDir, DnsConfiguration) {
        let files = tempfile::tempdir().assured("a temporary directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file: PathBuf = files.path().join("hosts");
        std::fs::write(&resolver_configuration, resolver_options)
            .assured("the resolver configuration can be written");
        std::fs::write(&hosts_file, hosts).assured("the hosts file can be written");
        let configuration = DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![name_server]),
        };
        (files, configuration)
    }

    async fn resolve(&self, host: &str) -> Result<Vec<SocketAddr>, DnsLookupFailure> {
        self.resolve_within(host, GENEROUS_BUDGET).await
    }

    async fn resolve_within(
        &self,
        host: &str,
        budget: Duration,
    ) -> Result<Vec<SocketAddr>, DnsLookupFailure> {
        match self.resolver.resolve(host, PORT, budget).await {
            Ok(addresses) => Ok(addresses),
            Err(report) => {
                assert_eq!(report.current_context().name(), host);
                Err(report.current_context().failure())
            }
        }
    }

    /// Resolve `host` until it answers `expected`, and return when that first happened.
    async fn wait_for_answer(&self, host: &str, expected: &[SocketAddr]) -> Instant {
        let deadline = Instant::now() + CHANGE_DEADLINE;
        loop {
            let resolved = self.resolve(host).await;
            if resolved.as_deref() == Ok(expected) {
                return Instant::now();
            }
            assert!(
                Instant::now() < deadline,
                "{host} still resolved to {resolved:?} instead of {expected:?}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

#[tokio::test]
async fn dual_stack_answers_resolve_ipv4_first() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture.authority.set(
        "peer.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v6(1), v4(1), v4(2)],
            ttl: Duration::from_secs(30),
        },
    );
    let resolved = fixture.resolve("peer.nervix.test").await;
    assert_eq!(resolved, Ok(sockets(&[v4(1), v4(2), v6(1)])));
}

#[tokio::test]
async fn literal_addresses_answer_without_a_question() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    let ipv6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
    assert_eq!(fixture.resolve("192.0.2.7").await, Ok(sockets(&[v4(7)])));
    assert_eq!(fixture.resolve("::1").await, Ok(sockets(&[ipv6])));
    assert_eq!(fixture.resolve("[::1]").await, Ok(sockets(&[ipv6])));
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[tokio::test]
async fn hosts_file_names_answer_without_dns() {
    let hosts =
        "192.0.2.9 listed.nervix.test\n2001:db8::9 dual.nervix.test\n192.0.2.10 dual.nervix.test\n";
    let fixture = Fixture::start(QUICK_OPTIONS, hosts).await;
    fixture
        .authority
        .set("listed.nervix.test", DnsAnswer::Silent);
    fixture.authority.set("dual.nervix.test", DnsAnswer::Silent);
    let listed = fixture.resolve("LISTED.nervix.test").await;
    let dual = fixture.resolve("dual.nervix.test").await;
    assert_eq!(listed, Ok(sockets(&[v4(9)])));
    assert_eq!(dual, Ok(sockets(&[v4(10), v6(9)])));
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[tokio::test]
async fn search_domains_complete_unqualified_names() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture.authority.set(
        "peer.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(3)],
            ttl: Duration::from_secs(30),
        },
    );
    assert_eq!(fixture.resolve("peer").await, Ok(sockets(&[v4(3)])));
    assert!(fixture.authority.questions_for("peer.nervix.test") > 0);
}

#[tokio::test]
async fn fully_qualified_names_skip_the_search_list() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture.authority.set(
        "peer.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(4)],
            ttl: Duration::from_secs(30),
        },
    );
    assert_eq!(
        fixture.resolve("peer.").await,
        Err(DnsLookupFailure::NameNotFound)
    );
    assert!(fixture.authority.questions_for("peer.") > 0);
    assert_eq!(fixture.authority.questions_for("peer.nervix.test"), 0);
}

#[tokio::test]
async fn positive_answers_are_reused_within_their_ttl() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture.authority.set(
        "cached.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(5), v6(5)],
            ttl: Duration::from_secs(300),
        },
    );
    let first = fixture.resolve("cached.nervix.test.").await;
    let asked = fixture.authority.questions_for("cached.nervix.test");
    let second = fixture.resolve("cached.nervix.test.").await;
    assert_eq!(first, Ok(sockets(&[v4(5), v6(5)])));
    assert_eq!(second, first);
    assert_eq!(fixture.authority.questions_for("cached.nervix.test"), asked);
}

#[tokio::test]
async fn a_changed_answer_is_used_once_its_ttl_expires() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    let ttl = Duration::from_secs(1);
    fixture.authority.set(
        "moving.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(6)],
            ttl,
        },
    );
    let cached_from = Instant::now();
    assert_eq!(
        fixture.resolve("moving.nervix.test.").await,
        Ok(sockets(&[v4(6)]))
    );
    fixture.authority.set(
        "moving.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(16)],
            ttl,
        },
    );
    let changed_at = fixture
        .wait_for_answer("moving.nervix.test.", &sockets(&[v4(16)]))
        .await;
    assert!(
        changed_at.duration_since(cached_from) >= ttl,
        "the changed answer arrived before the cached one expired"
    );
}

#[tokio::test]
async fn missing_names_are_reused_within_their_negative_ttl() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture.authority.set(
        "gone.nervix.test",
        DnsAnswer::NameNotFound {
            negative_ttl: Duration::from_secs(300),
        },
    );
    let first = fixture.resolve("gone.nervix.test.").await;
    let asked = fixture.authority.questions_for("gone.nervix.test");
    let second = fixture.resolve("gone.nervix.test.").await;
    assert_eq!(first, Err(DnsLookupFailure::NameNotFound));
    assert_eq!(second, first);
    assert_eq!(fixture.authority.questions_for("gone.nervix.test"), asked);
}

#[tokio::test]
async fn a_published_name_resolves_once_its_negative_ttl_expires() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    let negative_ttl = Duration::from_secs(1);
    fixture
        .authority
        .set("late.nervix.test", DnsAnswer::NameNotFound { negative_ttl });
    let cached_from = Instant::now();
    assert_eq!(
        fixture.resolve("late.nervix.test.").await,
        Err(DnsLookupFailure::NameNotFound)
    );
    fixture.authority.set(
        "late.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(8)],
            ttl: Duration::from_secs(30),
        },
    );
    let published_at = fixture
        .wait_for_answer("late.nervix.test.", &sockets(&[v4(8)]))
        .await;
    assert!(
        published_at.duration_since(cached_from) >= negative_ttl,
        "the name resolved before its negative answer expired"
    );
}

#[tokio::test]
async fn names_without_addresses_and_refusals_are_distinct_failures() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture.authority.set(
        "empty.nervix.test",
        DnsAnswer::NoAddresses {
            negative_ttl: Duration::from_secs(30),
        },
    );
    fixture
        .authority
        .set("refused.nervix.test", DnsAnswer::Refused);
    assert_eq!(
        fixture.resolve("empty.nervix.test.").await,
        Err(DnsLookupFailure::NoAddresses)
    );
    assert_eq!(
        fixture.resolve("refused.nervix.test.").await,
        Err(DnsLookupFailure::Refused)
    );
    assert_eq!(
        fixture.resolve("bad..name").await,
        Err(DnsLookupFailure::InvalidName)
    );
}

#[tokio::test]
async fn a_silent_name_server_ends_the_lookup_at_its_budget() {
    let fixture = Fixture::start("options timeout:30 attempts:3\n", "").await;
    fixture
        .authority
        .set("quiet.nervix.test", DnsAnswer::Silent);
    let budget = Duration::from_millis(300);
    let started = Instant::now();
    let resolved = fixture.resolve_within("quiet.nervix.test.", budget).await;
    assert_eq!(resolved, Err(DnsLookupFailure::Timeout));
    assert!(started.elapsed() >= budget);
    assert!(fixture.authority.questions_for("quiet.nervix.test") > 0);
}

#[tokio::test]
async fn the_host_configuration_bounds_retries_below_the_budget() {
    let fixture = Fixture::start(QUICK_OPTIONS, "").await;
    fixture
        .authority
        .set("quiet.nervix.test", DnsAnswer::Silent);
    let started = Instant::now();
    let resolved = fixture.resolve("quiet.nervix.test.").await;
    assert_eq!(resolved, Err(DnsLookupFailure::Timeout));
    assert!(
        started.elapsed() < GENEROUS_BUDGET,
        "the configured one-second timeout without retries should end the lookup"
    );
}

#[tokio::test]
async fn lookups_beyond_the_concurrency_bound_wait_and_cancellation_frees_their_slots() {
    let fixture = Fixture::start("options timeout:30 attempts:1\n", "").await;
    let mut holders = Vec::with_capacity(MAX_CONCURRENT_LOOKUPS);
    for index in 0..MAX_CONCURRENT_LOOKUPS {
        let name = format!("hold-{index}.nervix.test");
        fixture.authority.set(&name, DnsAnswer::Silent);
        let resolver = fixture.resolver.clone();
        holders.push(tokio::spawn(async move {
            resolver
                .resolve(&format!("{name}."), PORT, GENEROUS_BUDGET)
                .await
        }));
    }
    let deadline = Instant::now() + CHANGE_DEADLINE;
    for index in 0..MAX_CONCURRENT_LOOKUPS {
        let name = format!("hold-{index}.nervix.test");
        while fixture.authority.questions_for(&name) == 0 {
            assert!(Instant::now() < deadline, "{name} was never asked");
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
    fixture.authority.set(
        "waiting.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(11)],
            ttl: Duration::from_secs(30),
        },
    );
    let waited = fixture
        .resolve_within("waiting.nervix.test.", Duration::from_millis(500))
        .await;
    assert_eq!(waited, Err(DnsLookupFailure::Timeout));
    assert_eq!(fixture.authority.questions_for("waiting.nervix.test"), 0);

    for holder in &holders {
        holder.abort();
    }
    for holder in holders {
        assert!(holder.await.is_err_and(|error| error.is_cancelled()));
    }
    assert_eq!(
        fixture.resolve("waiting.nervix.test.").await,
        Ok(sockets(&[v4(11)]))
    );
}

#[test]
fn resolvers_are_rebuilt_and_torn_down_with_their_runtime() {
    let authority_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .assured("the authority runtime can be built");
    let authority = authority_runtime
        .block_on(DnsAuthority::start_on_loopback())
        .assured("a loopback UDP port is available");
    authority.set(
        "peer.nervix.test",
        DnsAnswer::Addresses {
            addresses: vec![v4(12)],
            ttl: Duration::from_secs(30),
        },
    );
    authority.set("quiet.nervix.test", DnsAnswer::Silent);
    for _ in 0..3 {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .assured("a node runtime can be built");
        let (_files, configuration) = Fixture::files(QUICK_OPTIONS, "", authority.address());
        runtime.block_on(async {
            let resolver = DnsResolver::load(configuration)
                .await
                .assured("the fixture configuration is valid");
            let resolved = resolver
                .resolve("peer.nervix.test.", PORT, GENEROUS_BUDGET)
                .await
                .assured("the authority answers");
            assert_eq!(resolved, sockets(&[v4(12)]));
            let in_flight = resolver.clone();
            tokio::spawn(async move {
                in_flight
                    .resolve("quiet.nervix.test.", PORT, GENEROUS_BUDGET)
                    .await
            });
        });
        let started = Instant::now();
        runtime.shutdown_timeout(Duration::from_secs(5));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a lookup in flight held its runtime's shutdown"
        );
    }
    authority_runtime.block_on(authority.stop());
}
