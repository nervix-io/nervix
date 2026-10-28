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
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
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
        let config = [
            ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            },
            ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "5000".to_string(),
            },
        ];
        let (client, _) = ClickHouseTableEmitter::client_from_config(&config, self.dns())
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

async fn insert(client: &ClickHouseClient) -> error_stack::Result<(), ClickHouseWriteError> {
    ClickHouseTableEmitter::insert(
        client,
        &ClickHouseTableName::parse("events").expect("valid table name"),
        Bytes::from_static(b"{\"id\":1}\n"),
        GENEROUS_TIMEOUT,
    )
    .await
}

/// Completes an actual chunked insert, including any byte already read by the reuse probe.
async fn answer_insert(stream: &mut TcpStream, prefix: &[u8]) {
    let mut request = prefix.to_vec();
    let mut buffer = [0_u8; 4096];
    while !request.ends_with(LAST_CHUNK) {
        nervix_primitives::task::consume_budget().await;
        let read = stream
            .read(&mut buffer)
            .await
            .assured("the loopback peer sends an insert");
        assert_ne!(
            read, 0,
            "the insert body must finish before the connection ends"
        );
        request.extend_from_slice(&buffer[..read]);
    }
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        .await
        .assured("the loopback peer waits for its insert result");
}

#[nervix_primitives::test]
async fn configured_pool_idle_timeout_controls_connection_reuse() {
    struct Case {
        configured: Option<&'static str>,
        idle_seconds: u64,
        expect_new: bool,
    }
    let fixture = Fixture::start().await;
    for Case {
        configured,
        idle_seconds,
        expect_new,
    } in [
        Case {
            configured: Some("100"),
            idle_seconds: 1,
            expect_new: true,
        },
        Case {
            configured: Some("10000"),
            idle_seconds: 3,
            expect_new: false,
        },
        Case {
            configured: None,
            idle_seconds: 3,
            expect_new: true,
        },
    ] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("a loopback TCP port is available");
        let addr = listener
            .local_addr()
            .assured("a bound listener has an address");
        let mut config = vec![
            ClientConfigEntry {
                key: "addr".into(),
                value: format!("http://{addr}"),
            },
            ClientConfigEntry {
                key: "timeout_ms".into(),
                value: "30000".into(),
            },
        ];
        if let Some(value) = configured {
            config.push(ClientConfigEntry {
                key: "pool_idle_timeout_ms".into(),
                value: value.into(),
            });
        }
        let (client, _) = ClickHouseTableEmitter::client_from_config(&config, fixture.dns())
            .assured("the connection configuration is valid");
        let server = nervix_primitives::task::spawn(async move {
            let (mut first, _) = listener.accept().await.assured("the first insert connects");
            answer_insert(&mut first, &[]).await;
            let mut prefix = [0_u8; 1];
            nervix_primitives::select! {
                accepted = listener.accept() => {
                    let (mut second, _) = accepted.assured("the second insert opens a connection");
                    answer_insert(&mut second, &[]).await;
                    true
                }
                read = first.read(&mut prefix) => {
                    let read = read.assured("the first connection can be observed");
                    if read == 0 {
                        let (mut second, _) = listener.accept().await
                            .assured("the second insert connects after idle retirement");
                        answer_insert(&mut second, &[]).await;
                        true
                    } else {
                        answer_insert(&mut first, &prefix[..read]).await;
                        false
                    }
                }
            }
        });
        insert(&client).await.assured("the first insert succeeds");
        // Move only the elapsed-time choice under a paused clock. Network I/O stays on the real
        // reactor, and neither insert races an automatically advanced request timeout.
        nervix_primitives::time::pause();
        nervix_primitives::time::advance(Duration::from_secs(idle_seconds)).await;
        nervix_primitives::time::resume();
        insert(&client).await.assured("the second insert succeeds");
        let used_new = server.await.assured("the connection probe completes");
        assert_eq!(
            used_new, expect_new,
            "pool_idle_timeout_ms={configured:?}, idle={idle_seconds}s"
        );
    }
}

#[nervix_primitives::test]
async fn connection_intervals_preserve_typed_validation_failures() {
    let fixture = Fixture::start().await;
    for (key, value) in [("tcp_keepalive_ms", "1001"), ("pool_idle_timeout_ms", "0")] {
        let config = [
            ClientConfigEntry {
                key: "addr".into(),
                value: "http://127.0.0.1:8123".into(),
            },
            ClientConfigEntry {
                key: "timeout_ms".into(),
                value: "5000".into(),
            },
            ClientConfigEntry {
                key: key.into(),
                value: value.into(),
            },
        ];
        let report = match ClickHouseTableEmitter::client_from_config(&config, fixture.dns()) {
            Ok(_) => panic!("an unrepresentable connection interval must fail before dialing"),
            Err(report) => report,
        };
        assert!(matches!(
            report.current_context(),
            EmitterOpenError::InvalidConfiguration { .. }
        ));
        assert!(matches!(
            report.downcast_ref::<nervix_models::ClientConfigSchemaError>(),
            Some(nervix_models::ClientConfigSchemaError::InvalidValue { key: invalid, .. }) if invalid == key
        ));
    }
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
async fn insert_request_quotes_each_case_preserving_table_part() {
    let fixture = Fixture::start().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("bound listener has an address")
        .port();
    let server = stand_in(listener);
    let table =
        ClickHouseTableName::parse("Sales.Order-Items").expect("qualified table name is valid");

    ClickHouseTableEmitter::insert(
        &fixture.client(&format!("http://127.0.0.1:{port}")),
        &table,
        Bytes::from_static(b"{\"id\":1}\n"),
        GENEROUS_TIMEOUT,
    )
    .await
    .expect("the insert succeeds");

    let request = server.await.expect("the stand-in does not panic");
    assert!(
        request.contains("%60Sales%60.%60Order-Items%60")
            || request.contains("`Sales`.`Order-Items`"),
        "the insert SQL quotes and preserves both parts: {request}"
    );
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
        .current_context()
        .lookup_failure()
        .expect("resolving the host is what failed the insert");
    assert_eq!(lookup.name(), SERVER);
    assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
    assert!(!error.current_context().is_record_error());
    let report = ClickHouseWriteError::into_report(error);
    assert_eq!(
        report.current_context(),
        &EmitterWriteError::Publish {
            connector: CLICKHOUSE
        }
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

    assert!(error.current_context().lookup_failure().is_none());
    let report = ClickHouseWriteError::into_report(error);
    assert!(report.contains::<ClickHouseWriteError>());
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
        matches!(
            error.current_context(),
            ClickHouseWriteError::Driver(ClickHouseError::Unsupported(_))
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(fixture.authority.total_questions(), 0);
}

fn message_of(report: &Report<EmitterWriteError>) -> Option<&str> {
    report
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>())
        .map(String::as_str)
}

#[test]
fn only_a_request_that_never_reached_clickhouse_is_described_by_its_causes() {
    let rejected = ClickHouseWriteError::report(ClickHouseError::BadResponse(
        "Code: 27. DB::Exception: secret-row-value (CANNOT_PARSE_TEXT)".to_string(),
    ));
    assert!(rejected.current_context().lookup_failure().is_none());
    assert!(rejected.current_context().transport_failure().is_none());
    assert!(!format!("{rejected:?}").contains("secret-row-value"));
    assert_eq!(
        message_of(&ClickHouseWriteError::into_report(rejected)),
        Some("ClickHouse insert request failed with CANNOT_PARSE_TEXT")
    );

    let timed_out = ClickHouseWriteError::report(ClickHouseError::TimedOut);
    assert!(timed_out.current_context().transport_failure().is_none());
    assert_eq!(
        message_of(&ClickHouseWriteError::into_report(timed_out)),
        Some("ClickHouse insert request failed")
    );
}
