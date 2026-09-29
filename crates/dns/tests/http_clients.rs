//! HTTP client checks against the configured node resolver and a local DNS authority.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** Observable DNS, connection, TTL, and timeout evidence for both Reqwest hooks, the
//!   Hyper connector hook, and the Smithy hook, and the typed lookup failure each hands its library.
//! - **Depends on.** `nervix-dns`, Reqwest, `hyper-util`, the Smithy DNS trait, Tokio, and the
//!   in-process DNS authority.
//! - **Must not know.** Connector plans, graph execution, or control-plane state.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use aws_smithy_runtime_api::client::dns::ResolveDns as _;
use bytes::Bytes;
use http_body_util::Empty;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor,
};
use meticulous::ResultExt as _;
use nervix_dns::{DnsConfiguration, DnsLookupError, DnsLookupFailure, DnsResolver, NameServers};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const NAME: &str = "http-client.nervix.test";

struct Fixture {
    authority: DnsAuthority,
    resolver: DnsResolver,
    _files: TempDir,
}

impl Fixture {
    async fn start() -> Self {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("a loopback DNS port is available");
        let files = tempfile::tempdir().assured("a fixture directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .assured("the fixture resolver configuration can be written");
        std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
        let resolver = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .assured("the fixture resolver configuration is valid");
        Self {
            authority,
            resolver,
            _files: files,
        }
    }

    fn answer(&self, addresses: Vec<IpAddr>, ttl: Duration) {
        self.authority
            .set(NAME, DnsAnswer::Addresses { addresses, ttl });
    }
}

async fn serve_once(address: SocketAddr, body: &'static str) {
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .assured("the loopback HTTP endpoint is available");
    serve_on(listener, body).await;
}

/// Answer one request on `listener` with `body`, after checking it kept the fixture authority.
async fn serve_on(listener: tokio::net::TcpListener, body: &'static str) {
    let (mut stream, _) = listener.accept().await.assured("the test client connects");
    let mut request = [0_u8; 2048];
    let length = stream
        .read(&mut request)
        .await
        .assured("the test request is readable");
    let request = String::from_utf8_lossy(&request[..length]);
    assert!(
        request
            .to_ascii_lowercase()
            .contains("host: http-client.nervix.test"),
        "request authority changed: {request}"
    );
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .assured("the test response can be written");
}

#[nervix_primitives::test]
async fn reqwest_13_uses_all_answers_and_keeps_the_url_authority() {
    let fixture = Fixture::start().await;
    let reachable = IpAddr::V4(Ipv4Addr::LOCALHOST);
    fixture.answer(
        vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), reachable],
        Duration::from_secs(1),
    );
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test endpoint can bind");
    let port = listener
        .local_addr()
        .assured("the listener has an address")
        .port();
    let server = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener.accept().await.assured("the test client connects");
        let mut request = [0_u8; 2048];
        let length = stream
            .read(&mut request)
            .await
            .assured("the test request is readable");
        assert!(
            String::from_utf8_lossy(&request[..length])
                .to_ascii_lowercase()
                .contains("host: http-client.nervix.test")
        );
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .assured("the test response can be written");
    });
    let client = reqwest::Client::builder()
        .dns_resolver(fixture.resolver.clone())
        .timeout(Duration::from_secs(3))
        .build()
        .assured("the test HTTP client is valid");
    let body = client
        .get(format!("http://{NAME}:{port}/test"))
        .send()
        .await
        .assured("the second DNS answer connects")
        .text()
        .await
        .assured("the response body is readable");
    assert_eq!(body, "ok");
    assert!(fixture.authority.questions_for(NAME) > 0);
    server.await.assured("the HTTP server task finishes");
}

#[nervix_primitives::test]
async fn reqwest_12_uses_the_same_resolver_without_a_public_fallback() {
    let fixture = Fixture::start().await;
    fixture.answer(
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        Duration::from_secs(1),
    );
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test endpoint can bind");
    let port = listener
        .local_addr()
        .assured("the listener has an address")
        .port();
    let server = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener.accept().await.assured("the test client connects");
        let mut request = [0_u8; 2048];
        stream
            .read(&mut request)
            .await
            .assured("the test request is readable");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .assured("the test response can be written");
    });
    let client = reqwest_iceberg::Client::builder()
        .dns_resolver(std::sync::Arc::new(fixture.resolver.clone()))
        .use_preconfigured_tls(
            rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .assured("AWS-LC supports the safe TLS protocol versions")
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            })
            .with_no_client_auth(),
        )
        .timeout(Duration::from_secs(3))
        .build()
        .assured("the test catalog client is valid");
    let body = client
        .get(format!("http://{NAME}:{port}/test"))
        .send()
        .await
        .assured("the configured DNS answer connects")
        .text()
        .await
        .assured("the response body is readable");
    assert_eq!(body, "ok");
    assert!(fixture.authority.questions_for(NAME) > 0);
    server.await.assured("the HTTP server task finishes");
}

#[nervix_primitives::test]
async fn request_timeout_cancels_a_silent_dns_lookup() {
    let fixture = Fixture::start().await;
    fixture.authority.set(NAME, DnsAnswer::Silent);
    let client = reqwest::Client::builder()
        .dns_resolver(fixture.resolver.clone())
        .timeout(Duration::from_millis(150))
        .build()
        .assured("the test HTTP client is valid");
    let start = Instant::now();
    let error = client
        .get(format!("http://{NAME}:12345/test"))
        .send()
        .await
        .expect_err("the silent DNS authority cannot resolve the request");
    assert!(
        error.is_timeout(),
        "expected a request timeout, got {error}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "DNS outlived the request deadline"
    );
    assert!(fixture.authority.questions_for(NAME) > 0);
}

#[nervix_primitives::test]
async fn ttl_expiry_reconnects_to_a_changed_answer() {
    let fixture = Fixture::start().await;
    let first_ip = Ipv4Addr::new(127, 0, 0, 1);
    let second_ip = Ipv4Addr::new(127, 0, 0, 2);
    let first = tokio::net::TcpListener::bind((first_ip, 0))
        .await
        .assured("the first loopback endpoint can bind");
    let port = first
        .local_addr()
        .assured("the first endpoint has an address")
        .port();
    let first_server = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = first.accept().await.assured("the first request connects");
        let mut request = [0_u8; 2048];
        stream
            .read(&mut request)
            .await
            .assured("the first request is readable");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\none")
            .await
            .assured("the first response can be written");
    });
    let second_server = nervix_primitives::task::spawn(serve_once(
        SocketAddr::new(IpAddr::V4(second_ip), port),
        "two",
    ));
    fixture.answer(vec![IpAddr::V4(first_ip)], Duration::from_secs(1));
    let client = reqwest::Client::builder()
        .dns_resolver(fixture.resolver.clone())
        .timeout(Duration::from_secs(3))
        .build()
        .assured("the test HTTP client is valid");
    let url = format!("http://{NAME}:{port}/test");
    assert_eq!(
        client
            .get(&url)
            .send()
            .await
            .assured("the first request succeeds")
            .text()
            .await
            .assured("the first body is readable"),
        "one"
    );
    first_server
        .await
        .assured("the first HTTP server task finishes");
    fixture.answer(vec![IpAddr::V4(second_ip)], Duration::from_secs(1));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        client
            .get(&url)
            .send()
            .await
            .assured("the second request succeeds")
            .text()
            .await
            .assured("the second body is readable"),
        "two"
    );
    second_server
        .await
        .assured("the second HTTP server task finishes");
    assert!(
        fixture.authority.questions_for(NAME) >= 2,
        "the changed name was not queried again"
    );
}

#[nervix_primitives::test]
async fn redirect_destination_uses_the_configured_resolver() {
    const SOURCE: &str = "redirect.nervix.test";
    const DESTINATION: &str = "destination.nervix.test";
    let fixture = Fixture::start().await;
    for name in [SOURCE, DESTINATION] {
        fixture.authority.set(
            name,
            DnsAnswer::Addresses {
                addresses: vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
                ttl: Duration::from_secs(1),
            },
        );
    }
    let source = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the redirect endpoint can bind");
    let target = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the destination endpoint can bind");
    let source_port = source
        .local_addr()
        .assured("the source has an address")
        .port();
    let target_port = target
        .local_addr()
        .assured("the destination has an address")
        .port();
    let source_server = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = source
            .accept()
            .await
            .assured("the source receives a request");
        let mut request = [0_u8; 2048];
        stream
            .read(&mut request)
            .await
            .assured("the source request is readable");
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: http://{DESTINATION}:{target_port}/test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stream
            .write_all(response.as_bytes())
            .await
            .assured("the redirect is writable");
    });
    let target_server = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = target
            .accept()
            .await
            .assured("the destination receives a request");
        let mut request = [0_u8; 2048];
        stream
            .read(&mut request)
            .await
            .assured("the destination request is readable");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\narrived")
            .await
            .assured("the destination response is writable");
    });
    let client = reqwest::Client::builder()
        .dns_resolver(fixture.resolver.clone())
        .timeout(Duration::from_secs(3))
        .build()
        .assured("the test HTTP client is valid");
    let body = client
        .get(format!("http://{SOURCE}:{source_port}/test"))
        .send()
        .await
        .assured("the redirected request succeeds")
        .text()
        .await
        .assured("the destination body is readable");
    assert_eq!(body, "arrived");
    assert!(fixture.authority.questions_for(SOURCE) > 0);
    assert!(fixture.authority.questions_for(DESTINATION) > 0);
    source_server.await.assured("the redirect server finishes");
    target_server
        .await
        .assured("the destination server finishes");
}

impl Fixture {
    fn answer_name_not_found(&self) {
        self.authority.set(
            NAME,
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
    }
}

#[nervix_primitives::test]
async fn hyper_connector_uses_all_answers_and_keeps_the_url_authority() {
    let fixture = Fixture::start().await;
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .assured("the test endpoint can bind");
    let port = listener
        .local_addr()
        .assured("the listener has an address")
        .port();
    // Nothing listens on the first answer, so it refuses the connection and the next is dialled.
    fixture.answer(
        vec![
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        ],
        Duration::from_secs(1),
    );
    let server = nervix_primitives::task::spawn(serve_on(listener, "hyper"));
    let client = HyperClient::builder(TokioExecutor::new())
        .build::<_, Empty<Bytes>>(HttpConnector::new_with_resolver(fixture.resolver.clone()));
    let uri = format!("http://{NAME}:{port}/test")
        .parse::<http::Uri>()
        .assured("the test URL is valid");

    let response = client
        .get(uri)
        .await
        .assured("the second DNS answer connects");

    assert_eq!(response.status(), http::StatusCode::OK);
    server.await.assured("the HTTP server task finishes");
    assert!(fixture.authority.questions_for(NAME) > 0);
}

#[nervix_primitives::test]
async fn hyper_connector_failures_keep_the_typed_lookup_failure() {
    let fixture = Fixture::start().await;
    fixture.answer_name_not_found();
    let client = HyperClient::builder(TokioExecutor::new())
        .build::<_, Empty<Bytes>>(HttpConnector::new_with_resolver(fixture.resolver.clone()));
    let uri = format!("http://{NAME}:12345/test")
        .parse::<http::Uri>()
        .assured("the test URL is valid");

    let error = client
        .get(uri)
        .await
        .expect_err("a name that does not exist cannot be connected");

    let lookup = DnsLookupError::find_in(&error).expect("the lookup failure is a cause");
    assert_eq!(lookup.name(), NAME);
    assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
}

#[nervix_primitives::test]
async fn smithy_hook_answers_every_address_and_fails_with_the_typed_lookup_failure() {
    const MISSING: &str = "missing.nervix.test";
    let fixture = Fixture::start().await;
    let addresses = vec![
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
    ];
    fixture.answer(addresses.clone(), Duration::from_secs(1));
    fixture.authority.set(
        MISSING,
        DnsAnswer::NameNotFound {
            negative_ttl: Duration::from_secs(1),
        },
    );

    let resolved = fixture
        .resolver
        .resolve_dns(NAME)
        .await
        .assured("the fixture name resolves");
    let error = fixture
        .resolver
        .resolve_dns(MISSING)
        .await
        .expect_err("a name that does not exist has no address");

    assert_eq!(resolved, addresses);
    let lookup = DnsLookupError::find_in(&error).expect("the lookup failure is a cause");
    assert_eq!(lookup.name(), MISSING);
    assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
}

#[nervix_primitives::test]
async fn reqwest_failures_keep_the_typed_lookup_failure() {
    let fixture = Fixture::start().await;
    fixture.answer_name_not_found();
    let client = reqwest::Client::builder()
        .dns_resolver(fixture.resolver.clone())
        .timeout(Duration::from_secs(3))
        .build()
        .assured("the test HTTP client is valid");

    let error = client
        .get(format!("http://{NAME}:12345/test"))
        .send()
        .await
        .expect_err("a name that does not exist cannot be connected");

    let lookup = DnsLookupError::find_in(&error).expect("the lookup failure is a cause");
    assert_eq!(lookup.failure(), DnsLookupFailure::NameNotFound);
}
