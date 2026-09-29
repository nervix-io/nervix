//! How an SQS client reaches the service, observed against a local DNS authority and loopback
//! listeners that stand in for the service's addresses.
//!
//! A stand-in answers every request with the `GetQueueUrl` result of the AWS JSON protocol and
//! closes the connection after it, so each request opens a new connection. The real service, and
//! the HTTPS clients' certificate checks, are the Cucumber suite's.

use std::{
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use aws_sdk_sqs::{
    error::SdkError, operation::get_queue_url::GetQueueUrlError, types::error::QueueDoesNotExist,
};
use nervix_connector::{
    RecordSink as _, SinkPublishError, SinkRecord, SinkRecordId, SinkStartError,
};
use nervix_dns::{DnsConfiguration, DnsLookupError, DnsLookupFailure, DnsResolver, NameServers};
use nervix_models::{ClientConfigEntry, QueueName, Timestamp};
use nervix_primitives::task::JoinHandle;
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
};

use crate::{
    SQS, SqsPublishingMode, SqsSink, SqsSourceError, SqsSourcePlan, connection::FailedRequest,
};

/// The fixture name every check resolves.
const SERVICE: &str = "sqs.nervix.test";
/// The access key every check signs with.
const ACCESS_KEY_ID: &str = "nervix-access-key";
/// The queue every check looks up.
const QUEUE: &str = "orders";

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
            SERVICE,
            DnsAnswer::Addresses {
                addresses,
                ttl: Duration::from_secs(30),
            },
        );
    }
}

/// The entries of a client for `endpoint` that signs with a known access key.
fn client_config(endpoint: &str) -> Vec<ClientConfigEntry> {
    let entry = |key: &str, value: &str| ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    };
    vec![
        entry("endpoint", endpoint),
        entry("access_key_id", ACCESS_KEY_ID),
        entry("secret_access_key", "nervix-secret"),
    ]
}

/// A service on `listener` that answers `connections` connections, one request each, with the URL
/// of the queue, and returns the requests as they arrived.
fn stand_in(listener: TcpListener, connections: usize) -> JoinHandle<Vec<String>> {
    nervix_primitives::task::spawn(async move {
        let mut requests = Vec::with_capacity(connections);
        for _ in 0..connections {
            let (stream, _) = listener
                .accept()
                .await
                .expect("the client under test dials this listener");
            requests.push(answer_queue_url(stream).await);
        }
        requests
    })
}

async fn answer_queue_url(mut stream: TcpStream) -> String {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4_096];
    while !holds_whole_request(&request) {
        let read = stream
            .read(&mut buffer)
            .await
            .expect("reading a loopback request cannot fail");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let body = format!(r#"{{"QueueUrl":"http://{SERVICE}/000000000000/{QUEUE}"}}"#);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: \
         {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .expect("the client under test waits for the response");
    String::from_utf8_lossy(&request).into_owned()
}

/// Whether `request` holds its whole head and the whole body its `Content-Length` announces.
fn holds_whole_request(request: &[u8]) -> bool {
    let text = String::from_utf8_lossy(request);
    let Some(head_length) = text.find("\r\n\r\n") else {
        return false;
    };
    let head = text[..head_length].to_ascii_lowercase();
    let mut body_length = 0;
    for line in head.lines() {
        if let Some(value) = line.strip_prefix("content-length:") {
            body_length = value
                .trim()
                .parse::<usize>()
                .expect("the SDK announces a numeric body length");
        }
    }
    request.len() >= head_length + 4 + body_length
}

/// The headers `request` signed, from its SigV4 `Authorization` header.
fn signed_headers(request: &str) -> Vec<String> {
    let head = request.to_ascii_lowercase();
    let authorization = head
        .lines()
        .find(|line| line.starts_with("authorization:"))
        .expect("every request is signed");
    let signed = authorization
        .split("signedheaders=")
        .nth(1)
        .expect("a SigV4 authorization names the headers it signed");
    let signed = signed
        .split(',')
        .next()
        .expect("splitting yields at least one part");
    let mut names = Vec::new();
    for name in signed.split(';') {
        names.push(name.trim().to_string());
    }
    names
}

fn message_of<C>(report: &error_stack::Report<C>) -> Option<&str> {
    report
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>())
        .map(String::as_str)
}

#[nervix_primitives::test]
async fn requests_reach_the_answer_that_accepts_signed_for_the_configured_host() {
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
    let service = stand_in(listener, 2);
    let config = client_config(&format!("http://{SERVICE}:{port}"));

    let client = SqsSink::client_from_config(&config, fixture.dns())
        .await
        .expect("the sink client configuration is valid");
    let queue_url = SqsSink::queue_url(&client, QUEUE)
        .await
        .expect("the answer that accepts returns the queue URL");
    assert_eq!(queue_url, format!("http://{SERVICE}/000000000000/{QUEUE}"));
    let queue = QueueName::try_from(QUEUE).expect("a fixed queue name is valid");
    SqsSourcePlan::connect(&config, &queue, fixture.dns())
        .await
        .expect("the source reaches the same answer");

    let requests = service.await.expect("the stand-in does not panic");
    assert_eq!(requests.len(), 2);
    for request in requests {
        let head = request.to_ascii_lowercase();
        assert!(
            head.contains(&format!("\r\nhost: {SERVICE}:{port}\r\n")),
            "the request keeps the configured authority: {request}"
        );
        assert!(
            head.contains("x-amz-target: amazonsqs.getqueueurl"),
            "{request}"
        );
        assert!(
            head.contains(&format!(
                "credential={}/",
                ACCESS_KEY_ID.to_ascii_lowercase()
            )),
            "the request is signed with the configured credentials: {request}"
        );
        assert!(head.contains("/us-east-1/sqs/aws4_request"), "{request}");
        assert!(
            signed_headers(&request).contains(&"host".to_string()),
            "the signature covers the configured host: {request}"
        );
    }
    assert!(fixture.authority.questions_for(SERVICE) > 0);
}

#[nervix_primitives::test]
async fn a_literal_endpoint_is_dialled_without_a_lookup() {
    let fixture = Fixture::start().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    let service = stand_in(listener, 1);

    let client = SqsSink::client_from_config(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await
    .expect("the sink client configuration is valid");
    SqsSink::queue_url(&client, QUEUE)
        .await
        .expect("the literal address returns the queue URL");

    let requests = service.await.expect("the stand-in does not panic");
    assert!(
        requests[0]
            .to_ascii_lowercase()
            .contains(&format!("\r\nhost: 127.0.0.1:{port}\r\n")),
        "{}",
        requests[0]
    );
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[nervix_primitives::test]
async fn a_host_that_does_not_resolve_is_the_typed_cause_of_the_queue_lookup_failure() {
    let fixture = Fixture::start().await;
    fixture.authority.set(
        SERVICE,
        DnsAnswer::NameNotFound {
            negative_ttl: Duration::from_secs(30),
        },
    );
    let client = SqsSink::client_from_config(
        &client_config(&format!("http://{SERVICE}:9324")),
        fixture.dns(),
    )
    .await
    .expect("the sink client configuration is valid");

    let report = SqsSink::queue_url(&client, QUEUE)
        .await
        .expect_err("a host that does not resolve cannot be reached");

    assert_eq!(
        report.current_context(),
        &SinkStartError::Initialize { sink: SQS }
    );
    assert_eq!(
        report
            .downcast_ref::<DnsLookupError>()
            .map(DnsLookupError::failure),
        Some(DnsLookupFailure::NameNotFound)
    );
    assert_eq!(
        message_of(&report),
        Some("SQS GetQueueUrl failed: resolving 'sqs.nervix.test' failed: the name does not exist")
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
    let client = SqsSink::client_from_config(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await
    .expect("the sink client configuration is valid");

    let report = SqsSink::queue_url(&client, QUEUE)
        .await
        .expect_err("a closed port refuses the connection");

    assert!(report.downcast_ref::<DnsLookupError>().is_none());
    let message = message_of(&report).expect("the failure carries its description");
    assert!(
        message.starts_with("SQS GetQueueUrl failed: dispatch failure: "),
        "{message}"
    );
    assert!(message.contains("tcp connect error"), "{message}");
}

#[test]
fn a_service_response_keeps_its_short_description() {
    let missing = QueueDoesNotExist::builder()
        .message("the queue orders does not exist")
        .build();
    let error = SdkError::<GetQueueUrlError, ()>::service_error(
        GetQueueUrlError::QueueDoesNotExist(missing),
        (),
    );

    let failure = FailedRequest::new(&error);

    assert!(failure.lookup_failure().is_none());
    assert_eq!(failure.description(), "service error");
}

/// A port on the loopback address nothing listens on, so every connection to it is refused.
async fn closed_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    drop(listener);
    port
}

#[nervix_primitives::test]
async fn a_send_that_cannot_connect_fails_the_publish_without_answering_a_record() {
    let fixture = Fixture::start().await;
    let port = closed_port().await;
    for (mode, operation) in [
        (
            SqsPublishingMode::Single,
            "SQS SendMessage failed: dispatch failure: ",
        ),
        (
            SqsPublishingMode::Batch,
            "SQS SendMessageBatch failed: dispatch failure: ",
        ),
    ] {
        let client = SqsSink::client_from_config(
            &client_config(&format!("http://127.0.0.1:{port}")),
            fixture.dns(),
        )
        .await
        .expect("the sink client configuration is valid");
        let mut sink = SqsSink {
            client,
            queue_url: format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"),
            mode,
        };
        let record = SinkRecord::new(
            SinkRecordId::new(0),
            None,
            b"{\"user_id\":42}".to_vec(),
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        );

        let outcome = sink.publish(vec![record]).await.into_parts();

        assert!(outcome.delivered.is_empty());
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("a refused connection fails the publish");
        assert_eq!(
            failure.current_context(),
            &SinkPublishError::Publish { sink: SQS }
        );
        let message = message_of(&failure).expect("the failure carries its description");
        assert!(message.starts_with(operation), "{message}");
        assert!(message.contains("tcp connect error"), "{message}");
    }
}

#[nervix_primitives::test]
async fn a_ca_file_that_cannot_be_read_is_a_configuration_failure() {
    let fixture = Fixture::start().await;
    let mut config = client_config("https://sqs.nervix.test:9325");
    config.push(ClientConfigEntry {
        key: "tls_ca_file".to_string(),
        value: "/nonexistent/nervix-sqs-ca.pem".to_string(),
    });

    let report = SqsSink::client_from_config(&config, fixture.dns())
        .await
        .expect_err("a CA file that cannot be read fails the sink configuration");

    assert_eq!(
        report.current_context(),
        &SinkStartError::InvalidConfiguration { sink: SQS }
    );
    let message = message_of(&report).expect("the failure names the unreadable file");
    assert!(message.contains("TLS CA certificate"), "{message}");
}

#[nervix_primitives::test]
async fn a_source_that_cannot_reach_the_service_fails_to_open_its_queue() {
    let fixture = Fixture::start().await;
    let port = closed_port().await;
    let queue = QueueName::try_from(QUEUE).expect("a fixed queue name is valid");

    let Err(report) = SqsSourcePlan::connect(
        &client_config(&format!("http://127.0.0.1:{port}")),
        &queue,
        fixture.dns(),
    )
    .await
    else {
        panic!("a refused connection cannot return the queue URL");
    };

    assert!(matches!(
        report.current_context(),
        SqsSourceError::ResolveQueue { queue } if queue == QUEUE
    ));
    assert!(report.downcast_ref::<DnsLookupError>().is_none());
    let message = message_of(&report).expect("the failure carries its description");
    assert!(
        message.starts_with("SQS GetQueueUrl failed: dispatch failure: "),
        "{message}"
    );
}
