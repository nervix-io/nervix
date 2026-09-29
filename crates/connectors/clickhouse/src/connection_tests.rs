//! How a ClickHouse client reaches its server, observed against a local DNS authority and loopback
//! listeners that stand in for the server's addresses.
//!
//! A stand-in reads one insert request whole and answers it with an empty success, which is all the
//! driver needs to finish an insert. The real server, and the HTTPS client's certificate checks, are
//! the Cucumber suite's.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use nervix_dns::{DnsConfiguration, DnsLookupFailure, NameServers};
use nervix_primitives::{net::TcpListener, task::JoinHandle};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

/// The fixture name every check resolves.
const SERVER: &str = "clickhouse.nervix.test";
/// A request timeout no check expects to reach.
const GENEROUS_TIMEOUT: Duration = Duration::from_secs(30);
/// The end of a chunked request body.
const LAST_CHUNK: &[u8] = b"0\r\n\r\n";

/// A resolver asking one local authority, with the files it was loaded from.
pub(crate) struct Fixture {
    authority: DnsAuthority,
    dns: DnsResolver,
    _files: TempDir,
}

impl Fixture {
    pub(crate) async fn start() -> Self {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .expect("a loopback UDP port is available");
        let files = tempfile::tempdir().expect("a temporary directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .expect("the resolver configuration can be written");
        std::fs::write(&hosts_file, "").expect("the hosts file can be written");
        let dns = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .expect("the fixture configuration is valid");
        Self {
            authority,
            dns,
            _files: files,
        }
    }

    pub(crate) fn dns(&self) -> DnsResolver {
        self.dns.clone()
    }

    fn answer(&self, addresses: Vec<IpAddr>) {
        self.authority.set(
            SERVER,
            DnsAnswer::Addresses {
                addresses,
                ttl: Duration::from_secs(30),
            },
        );
    }

    fn client(&self, addr: &str) -> ClickHouseClient {
        let config = [ClientConfigEntry {
            key: "addr".to_string(),
            value: addr.to_string(),
        }];
        let (client, _) = ClickHouseSink::client_from_config(&config, self.dns())
            .expect("the client configuration is valid");
        client
    }
}

/// A server on `listener` that accepts one connection, reads one insert request whole, answers it
/// with an empty success, and returns the request as it arrived.
fn stand_in(listener: TcpListener) -> JoinHandle<String> {
    nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("the client under test dials this listener");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4_096];
        while !request.ends_with(LAST_CHUNK) {
            let read = stream
                .read(&mut buffer)
                .await
                .expect("reading a loopback request cannot fail");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("the client under test waits for the response");
        String::from_utf8_lossy(&request).into_owned()
    })
}

async fn insert(client: &ClickHouseClient) -> Result<(), ClickHouseWriteError> {
    ClickHouseSink::insert(
        client,
        "events",
        Bytes::from_static(b"{\"id\":1}\n"),
        Some(GENEROUS_TIMEOUT),
    )
    .await
}

#[nervix_primitives::test]
async fn inserts_reach_the_answer_that_accepts_and_keep_the_configured_authority() {
    let fixture = Fixture::start().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    // Nothing listens on the first answer, so it refuses the connection and the next is dialled.
    fixture.answer(vec![
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
    ]);
    let server = stand_in(listener);

    insert(&fixture.client(&format!("http://{SERVER}:{port}")))
        .await
        .expect("the answer that accepts completes the insert");

    let request = server.await.expect("the stand-in does not panic");
    assert!(
        request.starts_with("POST /?"),
        "the insert is an HTTP POST: {request}"
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("host: {SERVER}:{port}")),
        "the request keeps the configured authority: {request}"
    );
    assert!(fixture.authority.questions_for(SERVER) > 0);
}

#[nervix_primitives::test]
async fn literal_addresses_are_dialled_without_a_lookup() {
    let fixture = Fixture::start().await;
    for (ip, host) in [
        (IpAddr::V4(Ipv4Addr::LOCALHOST), "127.0.0.1"),
        (IpAddr::V6(Ipv6Addr::LOCALHOST), "[::1]"),
    ] {
        let listener = TcpListener::bind((ip, 0))
            .await
            .expect("a loopback port is available");
        let port = listener
            .local_addr()
            .expect("a bound listener has an address")
            .port();
        let server = stand_in(listener);

        insert(&fixture.client(&format!("http://{host}:{port}")))
            .await
            .expect("the literal address completes the insert");

        let request = server.await.expect("the stand-in does not panic");
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("host: {host}:{port}")),
            "the request keeps the configured authority: {request}"
        );
    }
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[nervix_primitives::test]
async fn a_host_that_does_not_resolve_is_the_typed_cause_of_the_insert_failure() {
    let fixture = Fixture::start().await;
    fixture.authority.set(
        SERVER,
        DnsAnswer::NameNotFound {
            negative_ttl: Duration::from_secs(30),
        },
    );

    let error = insert(&fixture.client(&format!("http://{SERVER}:8123")))
        .await
        .expect_err("a host that does not resolve cannot be reached");

    let lookup = error
        .lookup_failure()
        .expect("resolving the host is what failed the insert");
    assert_eq!(lookup.name(), SERVER);
    assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
    assert!(!error.is_record_error());
    let report = error.into_report();
    assert_eq!(
        report.current_context(),
        &SinkPublishError::Publish { sink: CLICKHOUSE }
    );
    assert_eq!(
        report
            .downcast_ref::<DnsLookupError>()
            .map(DnsLookupError::failure),
        Some(DnsLookupFailure::NameNotFound)
    );
    let message = report
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>());
    assert_eq!(
        message.map(String::as_str),
        Some(
            "ClickHouse insert request failed: resolving 'clickhouse.nervix.test' failed: the \
             name does not exist"
        )
    );
}

#[nervix_primitives::test]
async fn a_refused_connection_is_described_by_its_causes() {
    let fixture = Fixture::start().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    drop(listener);

    let error = insert(&fixture.client(&format!("http://127.0.0.1:{port}")))
        .await
        .expect_err("a closed port refuses the connection");

    assert!(error.lookup_failure().is_none());
    let report = error.into_report();
    assert!(report.downcast_ref::<DnsLookupError>().is_none());
    let message = report
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>())
        .expect("the failure carries its description");
    assert!(
        message.starts_with("ClickHouse insert request failed: client error (Connect): "),
        "{message}"
    );
    assert!(message.contains("tcp connect error"), "{message}");
}

#[nervix_primitives::test]
async fn a_client_without_tls_entries_speaks_plain_http_only() {
    let fixture = Fixture::start().await;
    fixture.answer(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);

    let error = insert(&fixture.client(&format!("https://{SERVER}:8443")))
        .await
        .expect_err("an https address needs the client's TLS entries");

    assert!(
        matches!(error.0, ClickHouseError::Unsupported(_)),
        "unexpected error: {error:?}"
    );
    assert_eq!(fixture.authority.total_questions(), 0);
}

fn message_of(report: &Report<SinkPublishError>) -> Option<&str> {
    report
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>())
        .map(String::as_str)
}

#[test]
fn only_a_request_that_never_reached_clickhouse_is_described_by_its_causes() {
    let rejected = ClickHouseWriteError(ClickHouseError::BadResponse(
        "Code: 27. DB::Exception: Cannot parse input (CANNOT_PARSE_TEXT)".to_string(),
    ));
    assert!(rejected.lookup_failure().is_none());
    assert!(rejected.transport_failure().is_none());
    assert_eq!(
        message_of(&rejected.into_report()),
        Some("ClickHouse insert request failed with CANNOT_PARSE_TEXT")
    );

    let timed_out = ClickHouseWriteError(ClickHouseError::TimedOut);
    assert!(timed_out.transport_failure().is_none());
    assert_eq!(
        message_of(&timed_out.into_report()),
        Some("ClickHouse insert request failed")
    );
}
