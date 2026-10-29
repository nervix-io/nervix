//! How an SQS client reaches the service, observed against a local DNS authority and loopback
//! listeners that stand in for the service's addresses.
//!
//! A stand-in answers every request with one `GetQueueUrl` answer of the AWS JSON protocol and
//! closes the connection after it, so each request opens a new connection. The real service, and
//! the HTTPS clients' certificate checks, are the Cucumber suite's.

use std::{
    net::{IpAddr, Ipv4Addr},
    num::NonZeroUsize,
    time::Duration,
};

use aws_sdk_sqs::{
    error::SdkError, operation::get_queue_url::GetQueueUrlError, types::error::QueueDoesNotExist,
};
use nervix_connector::{
    BrokerIngestorConnector as _, EmitterOpenError, EmitterRecord, EmitterRecordId,
    EmitterWriteError, IngestorBatch, IngestorBatchRequest, IngestorConnector as _, IngestorError,
    IngestorReadCancellation, IngestorResume, RecordEmitter as _,
};
use nervix_dns::{DnsConfiguration, DnsLookupError, DnsLookupFailure, DnsResolver, NameServers};
use nervix_models::{ClientConfigEntry, SqsQueueName, Timestamp};
use nervix_primitives::{
    net::{TcpListener, TcpSocket, TcpStream},
    task::JoinHandle,
};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::queue::{
    SQS, SqsQueueEmitter, SqsQueueIngestor, SqsQueueIngestorError, SqsQueueIngestorPlan,
    connection::FailedRequest,
};

/// The fixture name every check resolves.
const SERVICE: &str = "sqs.nervix.test";
/// The access key every check signs with.
const ACCESS_KEY_ID: &str = "nervix-access-key";
/// The queue every check looks up.
pub(crate) const QUEUE: &str = "orders";

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
pub(crate) fn client_config(endpoint: &str) -> Vec<ClientConfigEntry> {
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

/// What a stand-in service answers each `GetQueueUrl` request with.
#[derive(Clone, Copy, Debug)]
enum QueueAnswer {
    /// The URL of the queue.
    Url,
    /// AWS's refusal for a queue it does not have: the JSON protocol's error type, and the
    /// query-compatible code the SDK classifies the answer by.
    DoesNotExist,
    /// The same refusal from an SQS-compatible service that sends only the JSON protocol's error
    /// type, as ElasticMQ does.
    DoesNotExistByType,
}

impl QueueAnswer {
    /// The body of every refusal for a queue the service does not have.
    const DOES_NOT_EXIST_BODY: &str = concat!(
        r#"{"__type":"com.amazonaws.sqs#QueueDoesNotExist","#,
        r#""message":"The specified queue does not exist."}"#
    );

    /// The whole HTTP response, after which the stand-in closes the connection.
    fn response(self) -> String {
        match self {
            Self::Url => {
                let body = format!(r#"{{"QueueUrl":"http://{SERVICE}/000000000000/{QUEUE}"}}"#);
                Self::http_response("200 OK", "", &body)
            }
            Self::DoesNotExist => Self::http_response(
                "400 Bad Request",
                "x-amzn-query-error: AWS.SimpleQueueService.NonExistentQueue;Sender\r\n",
                Self::DOES_NOT_EXIST_BODY,
            ),
            Self::DoesNotExistByType => {
                Self::http_response("400 Bad Request", "", Self::DOES_NOT_EXIST_BODY)
            }
        }
    }

    fn http_response(status: &str, headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: \
             application/x-amz-json-1.0\r\n{headers}Content-Length: {}\r\nConnection: \
             close\r\n\r\n{body}",
            body.len()
        )
    }
}

/// A service on `listener` that answers `connections` connections, one request each, with
/// `answer`, and returns the requests as they arrived. It stops listening after the last one.
fn stand_in(
    listener: TcpListener,
    connections: usize,
    answer: QueueAnswer,
) -> JoinHandle<Vec<String>> {
    nervix_primitives::task::spawn(async move {
        let mut requests = Vec::with_capacity(connections);
        for _ in 0..connections {
            let (stream, _) = listener
                .accept()
                .await
                .expect("the client under test dials this listener");
            requests.push(answer_request(stream, answer).await);
        }
        requests
    })
}

async fn answer_request(mut stream: TcpStream, answer: QueueAnswer) -> String {
    let request = read_request(&mut stream).await;
    stream
        .write_all(answer.response().as_bytes())
        .await
        .expect("the client under test waits for the response");
    request
}

/// Reads one whole request from `stream`: its head and the body its `Content-Length` announces.
pub(crate) async fn read_request(stream: &mut TcpStream) -> String {
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
    let service = stand_in(listener, 2, QueueAnswer::Url);
    let config = client_config(&format!("http://{SERVICE}:{port}"));

    let client = SqsQueueEmitter::client_from_config(&config, fixture.dns())
        .await
        .expect("the emitter client configuration is valid");
    let queue_url = SqsQueueEmitter::queue_url(&client, QUEUE)
        .await
        .expect("the answer that accepts returns the queue URL");
    assert_eq!(queue_url, format!("http://{SERVICE}/000000000000/{QUEUE}"));
    let mut source = open_source(&config, fixture.dns()).await;
    assert_eq!(
        source
            .resume()
            .await
            .expect("the source reaches the same answer"),
        IngestorResume::Ready
    );

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
    let service = stand_in(listener, 1, QueueAnswer::Url);

    let client = SqsQueueEmitter::client_from_config(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await
    .expect("the emitter client configuration is valid");
    SqsQueueEmitter::queue_url(&client, QUEUE)
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
    let client = SqsQueueEmitter::client_from_config(
        &client_config(&format!("http://{SERVICE}:9324")),
        fixture.dns(),
    )
    .await
    .expect("the emitter client configuration is valid");

    let report = SqsQueueEmitter::queue_url(&client, QUEUE)
        .await
        .expect_err("a host that does not resolve cannot be reached");

    assert_eq!(
        report.current_context(),
        &EmitterOpenError::Initialize { connector: SQS }
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
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();
    let client = SqsQueueEmitter::client_from_config(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await
    .expect("the emitter client configuration is valid");

    let report = SqsQueueEmitter::queue_url(&client, QUEUE)
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

/// A bound endpoint that refuses connections and keeps its address reserved for this check.
pub(crate) struct RefusedEndpoint {
    socket: TcpSocket,
}

impl RefusedEndpoint {
    pub(crate) fn reserve() -> Self {
        let socket = TcpSocket::new_v4().expect("a loopback socket can be created");
        socket
            .bind((Ipv4Addr::LOCALHOST, 0).into())
            .expect("a loopback port is available");
        Self { socket }
    }

    pub(crate) fn port(&self) -> u16 {
        self.socket
            .local_addr()
            .expect("a bound socket has an address")
            .port()
    }
}

#[nervix_primitives::test]
async fn a_refused_endpoint_keeps_its_port_reserved() {
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();

    assert!(
        TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .await
            .is_err(),
        "another fixture must not listen on the refused endpoint"
    );
    let error = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect_err("a reserved endpoint that does not listen refuses connections");
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);
}

#[nervix_primitives::test]
async fn a_send_that_cannot_connect_fails_the_publish_without_answering_a_record() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();
    for (send, operation) in [
        (
            nervix_models::SqsSend::Single,
            "SQS SendMessage failed: dispatch failure: ",
        ),
        (
            nervix_models::SqsSend::Batch,
            "SQS SendMessageBatch failed: dispatch failure: ",
        ),
    ] {
        let client = SqsQueueEmitter::client_from_config(
            &client_config(&format!("http://127.0.0.1:{port}")),
            fixture.dns(),
        )
        .await
        .expect("the emitter client configuration is valid");
        let mut connector = SqsQueueEmitter {
            client,
            queue_url: format!("http://127.0.0.1:{port}/000000000000/{QUEUE}"),
            send,
        };
        let record = EmitterRecord::new(
            EmitterRecordId::new(0),
            None,
            b"{\"user_id\":42}".to_vec(),
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        );

        let outcome = connector.publish(vec![record]).await.into_parts();

        assert!(outcome.delivered.is_empty());
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("a refused connection fails the publish");
        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: SQS }
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

    let report = SqsQueueEmitter::client_from_config(&config, fixture.dns())
        .await
        .expect_err("a CA file that cannot be read fails the emitter configuration");

    assert_eq!(
        report.current_context(),
        &EmitterOpenError::InvalidConfiguration { connector: SQS }
    );
    let message = message_of(&report).expect("the failure names the unreadable file");
    assert!(message.contains("TLS CA certificate"), "{message}");
}

/// A ingestor instance of the client `config` declares, polling the queue every check looks up.
pub(crate) async fn open_source(
    config: &[ClientConfigEntry],
    dns: DnsResolver,
) -> SqsQueueIngestor {
    let queue = SqsQueueName::try_from(QUEUE).expect("a fixed queue name is valid");
    let plan = SqsQueueIngestorPlan::new(config, queue, dns)
        .await
        .expect("the source client configuration is valid");
    SqsQueueIngestor::open(&plan, 0)
        .await
        .expect("opening a source sends no request")
}

/// A request for the next batch, which an ingestor that has not looked its queue up never sends.
pub(crate) fn batch_request() -> IngestorBatchRequest {
    IngestorBatchRequest {
        max_messages: NonZeroUsize::MIN,
        batch_timeout: None,
    }
}

#[nervix_primitives::test]
async fn a_source_looks_its_queue_up_once_when_it_first_resumes() {
    let fixture = Fixture::start().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    fixture.answer(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
    let service = stand_in(listener, 1, QueueAnswer::Url);

    let mut source = open_source(
        &client_config(&format!("http://{SERVICE}:{port}")),
        fixture.dns(),
    )
    .await;

    // Opening built the client alone: it neither resolved the service's name nor polled.
    assert_eq!(fixture.authority.questions_for(SERVICE), 0);
    assert!(source.needs_resume());
    let batch = source
        .next_batch(batch_request(), &IngestorReadCancellation::new())
        .await
        .expect("a source without its queue's URL asks to resume");
    assert!(matches!(batch, IngestorBatch::ResumeRequired));

    assert_eq!(
        source
            .resume()
            .await
            .expect("the stand-in returns the queue URL"),
        IngestorResume::Ready
    );
    assert!(!source.needs_resume());
    let requests = service.await.expect("the stand-in does not panic");
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .to_ascii_lowercase()
            .contains("x-amz-target: amazonsqs.getqueueurl"),
        "{}",
        requests[0]
    );
    // The stand-in no longer listens, so only an ingestor that kept the URL is ready again.
    assert_eq!(
        source
            .resume()
            .await
            .expect("a source that has its queue's URL sends no request"),
        IngestorResume::Ready
    );
}

#[nervix_primitives::test]
async fn a_source_that_cannot_reach_the_service_opens_and_fails_its_resume() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    let port = endpoint.port();
    let mut source = open_source(
        &client_config(&format!("http://127.0.0.1:{port}")),
        fixture.dns(),
    )
    .await;

    let report = source
        .resume()
        .await
        .expect_err("a refused connection cannot return the queue URL");

    assert_eq!(
        report.current_context(),
        &IngestorError::Resume { connector: SQS }
    );
    assert!(matches!(
        report.downcast_ref::<SqsQueueIngestorError>(),
        Some(SqsQueueIngestorError::ResolveQueue { queue }) if queue == QUEUE
    ));
    assert!(report.downcast_ref::<DnsLookupError>().is_none());
    let message = message_of(&report).expect("the failure carries its description");
    assert!(
        message.starts_with("SQS GetQueueUrl failed: dispatch failure: "),
        "{message}"
    );
    assert!(
        source.needs_resume(),
        "a failed lookup leaves the source to resume again"
    );
}

#[nervix_primitives::test]
async fn a_queue_the_service_does_not_have_fails_the_resume_with_its_name() {
    let fixture = Fixture::start().await;
    for answer in [QueueAnswer::DoesNotExist, QueueAnswer::DoesNotExistByType] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback port is available");
        let port = listener
            .local_addr()
            .expect("a bound listener has an address")
            .port();
        let service = stand_in(listener, 1, answer);
        let mut source = open_source(
            &client_config(&format!("http://127.0.0.1:{port}")),
            fixture.dns(),
        )
        .await;

        let report = source
            .resume()
            .await
            .expect_err("the service does not have the queue");

        assert_eq!(
            report.current_context(),
            &IngestorError::Resume { connector: SQS },
            "{answer:?}"
        );
        let missing = report
            .downcast_ref::<SqsQueueIngestorError>()
            .expect("the lookup failure stays typed beneath the resume failure");
        assert!(
            matches!(missing, SqsQueueIngestorError::MissingQueue { queue } if queue == QUEUE),
            "{answer:?}: {report:?}"
        );
        assert_eq!(missing.to_string(), "SQS queue 'orders' does not exist");
        assert!(source.needs_resume(), "{answer:?}");
        let requests = service.await.expect("the stand-in does not panic");
        assert_eq!(
            requests.len(),
            1,
            "{answer:?}: the SDK does not retry a refusal the service answered"
        );
    }
}
