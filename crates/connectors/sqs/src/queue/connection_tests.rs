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
    BrokerIngestorConnector as _, DeadlineElapsed, EmitterOpenError, EmitterRecord,
    EmitterRecordId, EmitterWriteError, IngestorBatch, IngestorBatchRequest,
    IngestorConnector as _, IngestorError, IngestorReadCancellation, IngestorResume,
    RecordEmitter as _,
};
use nervix_dns::{DnsConfiguration, DnsLookupError, DnsLookupFailure, DnsResolver, NameServers};
use nervix_models::{ClientConfigEntry, DeadlineKey, SqsQueueName, Timestamp};
use nervix_primitives::{
    net::{TcpListener, TcpSocket, TcpStream},
    task::JoinHandle,
};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::queue::{
    EmptyCaFile, InvalidSqsClientKey, SQS, SqsQueueEmitter, SqsQueueIngestor,
    SqsQueueIngestorError, SqsQueueIngestorPlan,
    connection::{SqsClientConfig, SqsRequest},
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
        entry("region", "us-east-1"),
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

#[nervix_primitives::test]
async fn a_service_response_keeps_its_short_description() {
    let fixture = Fixture::start().await;
    let client = SqsClientConfig::parse(&client_config("http://127.0.0.1:9324"))
        .expect("the client configuration is valid")
        .client(fixture.dns())
        .await;
    let missing = QueueDoesNotExist::builder()
        .message("the queue orders does not exist")
        .build();
    let error = SdkError::<GetQueueUrlError, ()>::service_error(
        GetQueueUrlError::QueueDoesNotExist(missing),
        (),
    );

    let failure = client.failed(SqsRequest::QueueLookup, &error);

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
    assert_eq!(
        report
            .downcast_ref::<InvalidSqsClientKey>()
            .map(|invalid| invalid.key),
        Some("tls_ca_file")
    );
    assert!(
        matches!(
            report.downcast_ref::<nervix_connector::ClientConfigError>(),
            Some(nervix_connector::ClientConfigError::ReadTlsFile { label, .. })
                if label == "TLS CA certificate"
        ),
        "the failure names the unreadable file: {report:?}"
    );
    assert_eq!(
        message_of(&report),
        Some(
            "invalid SQS client config key 'tls_ca_file': failed to read TLS CA certificate \
             '/nonexistent/nervix-sqs-ca.pem'"
        ),
        "the emitter leads its report with the whole chain, which DESCRIBE EMITTER shows"
    );
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

/// The content of a CA file Smithy's TLS context cannot take as a trust store, and what the client
/// configuration reports for it.
const UNTRUSTABLE_CA_FILES: [(&str, &str); 3] = [
    (
        "a certificate section that is not base64",
        "-----BEGIN CERTIFICATE-----\n!this is not base64!\n-----END CERTIFICATE-----\n",
    ),
    (
        "base64 that is not a certificate",
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
    ),
    ("no certificate", "no certificate is written here\n"),
];

/// A client of `endpoint` that trusts the CA file holding `content` alone, with the directory
/// that keeps the file.
fn client_config_trusting(endpoint: &str, content: &str) -> (Vec<ClientConfigEntry>, TempDir) {
    let directory = tempfile::tempdir().expect("a temporary directory can be created");
    let ca_file = directory.path().join("ca.pem");
    std::fs::write(&ca_file, content).expect("the CA file can be written");
    let mut config = client_config(endpoint);
    config.push(ClientConfigEntry {
        key: "tls_ca_file".to_string(),
        value: ca_file.display().to_string(),
    });
    (config, directory)
}

#[nervix_primitives::test]
async fn a_ca_file_smithy_cannot_trust_fails_the_client_configuration_of_both_directions() {
    let fixture = Fixture::start().await;
    let endpoint = RefusedEndpoint::reserve();
    let address = format!("https://127.0.0.1:{}", endpoint.port());
    for (case, content) in UNTRUSTABLE_CA_FILES {
        let (config, _directory) = client_config_trusting(&address, content);

        let emitter = SqsQueueEmitter::client_from_config(&config, fixture.dns()).await;
        let report = match emitter {
            Ok(client) => {
                // A request builds the connector, and with it the trust store.
                let lookup = SqsQueueEmitter::queue_url(&client, QUEUE).await;
                panic!("a CA file with {case} configured an emitter, whose lookup gave {lookup:?}");
            }
            Err(report) => report,
        };
        assert_eq!(
            report.current_context(),
            &EmitterOpenError::InvalidConfiguration { connector: SQS },
            "{case}"
        );
        assert_untrusted_ca_file(&report, case);

        let queue = SqsQueueName::try_from(QUEUE).expect("a fixed queue name is valid");
        let plan = SqsQueueIngestorPlan::new(&config, queue, fixture.dns()).await;
        let report = match plan {
            Ok(plan) => {
                let mut source = SqsQueueIngestor::open(&plan, 0)
                    .await
                    .expect("opening a source sends no request");
                let resume = source.resume().await;
                panic!("a CA file with {case} configured a source, whose resume gave {resume:?}");
            }
            Err(report) => report,
        };
        assert!(
            matches!(
                report.current_context(),
                SqsQueueIngestorError::ClientConfig
            ),
            "{case}: {report:?}"
        );
        assert_untrusted_ca_file(&report, case);
    }
}

/// `report` names `tls_ca_file` as the key the client cannot use, above the parse, the root store
/// or the empty file that refused it.
fn assert_untrusted_ca_file<C>(report: &error_stack::Report<C>, case: &str) {
    assert_eq!(
        report
            .downcast_ref::<InvalidSqsClientKey>()
            .map(|invalid| invalid.key),
        Some("tls_ca_file"),
        "{case}: {report:?}"
    );
    let refused_by_tls = matches!(
        report.downcast_ref::<nervix_connector::TlsClientConfigError>(),
        Some(
            nervix_connector::TlsClientConfigError::ParseCaCertificate { .. }
                | nervix_connector::TlsClientConfigError::AddCaCertificate { .. }
        )
    );
    let empty = report.downcast_ref::<EmptyCaFile>().is_some();
    assert!(refused_by_tls || empty, "{case}: {report:?}");
}

/// How long a check waits for a request that a deadline must end. Generous, because the deadline
/// ends it much sooner.
pub(crate) const GENEROUS_BUDGET: Duration = Duration::from_secs(30);

/// A service on `listener` that accepts every connection, reads nothing and never answers, and
/// hands each connection it accepted to `accepted`, which keeps it open.
pub(crate) fn silent_service(
    listener: TcpListener,
    accepted: nervix_primitives::sync::mpsc::Sender<TcpStream>,
) -> JoinHandle<()> {
    nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            if accepted.send(stream).await.is_err() {
                return;
            }
        }
    })
}

#[nervix_primitives::test]
async fn a_queue_lookup_the_service_never_answers_ends_within_timeout_ms_after_one_attempt() {
    let fixture = Fixture::start().await;
    let budget = Duration::from_millis(300);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    let (accepted, mut connections) = nervix_primitives::sync::mpsc::channel(8);
    let _service = silent_service(listener, accepted);
    let mut config = client_config(&format!("http://127.0.0.1:{port}"));
    config.push(ClientConfigEntry {
        key: "timeout_ms".to_string(),
        value: budget.as_millis().to_string(),
    });

    let mut source = open_source(&config, fixture.dns()).await;
    let resumed = nervix_primitives::time::timeout(GENEROUS_BUDGET, source.resume())
        .await
        .expect("a lookup the service never answers ends within the client's timeout_ms");
    let report = resumed.expect_err("a service that never answers returns no queue URL");
    let elapsed = report.downcast_ref::<DeadlineElapsed>().copied();
    assert_eq!(
        elapsed.map(|elapsed| (elapsed.operation, elapsed.key, elapsed.bound)),
        Some(("the SQS queue lookup", DeadlineKey::Timeout, budget)),
        "the lookup ran out of the client's timeout_ms: {report:?}"
    );
    assert_eq!(
        format!(
            "{}",
            report
                .downcast_ref::<DeadlineElapsed>()
                .expect("checked above")
        ),
        "the SQS queue lookup did not finish within timeout_ms 300ms"
    );
    let client = SqsQueueEmitter::client_from_config(&config, fixture.dns())
        .await
        .expect("the emitter client configuration is valid");
    let looked_up = nervix_primitives::time::timeout(
        GENEROUS_BUDGET,
        SqsQueueEmitter::queue_url(&client, QUEUE),
    )
    .await
    .expect("a lookup the service never answers ends within the client's timeout_ms");
    let report = looked_up.expect_err("a service that never answers returns no queue URL");
    assert_eq!(
        report.current_context(),
        &EmitterOpenError::Initialize { connector: SQS }
    );
    assert_eq!(
        report
            .downcast_ref::<DeadlineElapsed>()
            .map(|elapsed| (elapsed.operation, elapsed.bound)),
        Some(("the SQS queue lookup", budget)),
        "{report:?}"
    );

    // The SDK made each lookup once: the host's RETRY POLICY owns every retry.
    let mut accepted = Vec::new();
    while let Ok(connection) = connections.try_recv() {
        accepted.push(connection);
    }
    assert_eq!(
        accepted.len(),
        2,
        "one connection for each side's single attempt"
    );
}

#[nervix_primitives::test]
async fn a_session_token_is_sent_with_the_requests_of_both_directions() {
    let fixture = Fixture::start().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    let service = stand_in(listener, 2, QueueAnswer::Url);
    let mut config = client_config(&format!("http://127.0.0.1:{port}"));
    config.push(ClientConfigEntry {
        key: "session_token".to_string(),
        value: "nervix-session-token".to_string(),
    });

    let client = SqsQueueEmitter::client_from_config(&config, fixture.dns())
        .await
        .expect("a client with a session token is valid");
    SqsQueueEmitter::queue_url(&client, QUEUE)
        .await
        .expect("the stand-in returns the queue URL");
    let mut source = open_source(&config, fixture.dns()).await;
    source
        .resume()
        .await
        .expect("the stand-in returns the queue URL to the source too");

    let requests = service.await.expect("the stand-in does not panic");
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            request
                .to_ascii_lowercase()
                .contains("\r\nx-amz-security-token: nervix-session-token\r\n"),
            "the request carries the temporary credentials' token: {request}"
        );
        assert!(
            signed_headers(&request).contains(&"x-amz-security-token".to_string()),
            "the signature covers the token: {request}"
        );
    }
}

#[nervix_primitives::test]
async fn a_client_must_name_its_region_and_credentials_in_both_directions() {
    let fixture = Fixture::start().await;
    for missing in ["region", "access_key_id", "secret_access_key"] {
        let mut config = client_config("http://127.0.0.1:9324");
        config.retain(|entry| entry.key != missing);

        let emitter = SqsQueueEmitter::client_from_config(&config, fixture.dns()).await;
        let Err(report) = emitter else {
            panic!("an emitter client without {missing} was configured");
        };
        assert_eq!(
            report.current_context(),
            &EmitterOpenError::InvalidConfiguration { connector: SQS },
            "{missing}"
        );
        let queue = SqsQueueName::try_from(QUEUE).expect("a fixed queue name is valid");
        let plan = SqsQueueIngestorPlan::new(&config, queue, fixture.dns()).await;
        let Err(report) = plan else {
            panic!("a source client without {missing} was configured");
        };
        assert!(
            matches!(
                report.current_context(),
                SqsQueueIngestorError::ClientConfig
            ),
            "{missing}: {report:?}"
        );
    }
}

/// A CA certificate rustls takes as a trust anchor, in PEM.
pub(crate) fn ca_certificate_pem() -> String {
    let key = rcgen::KeyPair::generate().expect("a CA key generates");
    let mut params = rcgen::CertificateParams::new(Vec::new()).expect("an empty SAN list is valid");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "nervix sqs test ca");
    params
        .self_signed(&key)
        .expect("a CA certificate generates")
        .pem()
}

#[nervix_primitives::test]
async fn a_ca_file_holding_a_certificate_configures_both_directions() {
    let fixture = Fixture::start().await;
    let (config, _directory) =
        client_config_trusting("https://sqs.nervix.test:9325", &ca_certificate_pem());

    SqsQueueEmitter::client_from_config(&config, fixture.dns())
        .await
        .expect("an emitter trusting a CA certificate is configured");
    let queue = SqsQueueName::try_from(QUEUE).expect("a fixed queue name is valid");
    SqsQueueIngestorPlan::new(&config, queue, fixture.dns())
        .await
        .expect("a source trusting a CA certificate is configured");
}

#[nervix_primitives::test]
async fn a_receive_extends_only_its_operation_timeouts_by_its_long_poll() {
    let fixture = Fixture::start().await;
    let mut config = client_config("http://127.0.0.1:9324");
    config.push(ClientConfigEntry {
        key: "timeout_ms".to_string(),
        value: "250".to_string(),
    });
    let client = SqsClientConfig::parse(&config)
        .expect("the client configuration is valid")
        .client(fixture.dns())
        .await;
    let own = client
        .sdk()
        .config()
        .timeout_config()
        .cloned()
        .expect("the client is built with its timeouts");
    assert_eq!(own.operation_timeout(), Some(Duration::from_millis(250)));
    assert_eq!(
        own.operation_attempt_timeout(),
        Some(Duration::from_millis(250))
    );
    assert!(
        own.connect_timeout().is_some(),
        "the SDK's connect timeout bounds every connection"
    );
    assert_eq!(
        client
            .sdk()
            .config()
            .retry_config()
            .map(|retry| retry.max_attempts()),
        Some(1),
        "the SDK makes each request once"
    );

    let receive = client.receive_override(Duration::from_secs(1)).build();
    let receive = receive
        .timeout_config()
        .expect("a client with timeout_ms overrides a receive's timeouts");
    assert_eq!(
        receive.operation_timeout(),
        Some(Duration::from_millis(1250))
    );
    assert_eq!(
        receive.operation_attempt_timeout(),
        Some(Duration::from_millis(1250))
    );
    assert_eq!(receive.connect_timeout(), own.connect_timeout());

    let unbounded = SqsClientConfig::parse(&client_config("http://127.0.0.1:9324"))
        .expect("the client configuration is valid")
        .client(fixture.dns())
        .await;
    assert!(
        unbounded
            .receive_override(Duration::from_secs(1))
            .build()
            .timeout_config()
            .is_none(),
        "a client without timeout_ms overrides nothing"
    );
}

#[nervix_primitives::test]
async fn a_send_the_service_never_answers_ends_within_timeout_ms_in_both_modes() {
    let fixture = Fixture::start().await;
    let budget = Duration::from_millis(300);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a loopback port is available");
    let port = listener
        .local_addr()
        .expect("a bound listener has an address")
        .port();
    let (accepted, _connections) = nervix_primitives::sync::mpsc::channel(8);
    let _service = silent_service(listener, accepted);
    let mut config = client_config(&format!("http://127.0.0.1:{port}"));
    config.push(ClientConfigEntry {
        key: "timeout_ms".to_string(),
        value: budget.as_millis().to_string(),
    });
    for (send, request, message) in [
        (
            nervix_models::SqsSend::Single,
            "an SQS send",
            "SQS SendMessage failed: an SQS send did not finish within timeout_ms 300ms",
        ),
        (
            nervix_models::SqsSend::Batch,
            "an SQS batch send",
            "SQS SendMessageBatch failed: an SQS batch send did not finish within timeout_ms 300ms",
        ),
    ] {
        let client = SqsQueueEmitter::client_from_config(&config, fixture.dns())
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

        let outcome =
            nervix_primitives::time::timeout(GENEROUS_BUDGET, connector.publish(vec![record]))
                .await
                .expect("a send the service never answers ends within the client's timeout_ms")
                .into_parts();

        assert!(outcome.delivered.is_empty(), "{send:?}");
        assert!(outcome.rejected.is_empty(), "{send:?}");
        let failure = outcome
            .infrastructure_error
            .expect("a send the service never answers fails the publish");
        assert_eq!(
            failure.current_context(),
            &EmitterWriteError::Publish { connector: SQS }
        );
        assert_eq!(
            failure.downcast_ref::<DeadlineElapsed>().map(|elapsed| (
                elapsed.operation,
                elapsed.key,
                elapsed.bound
            )),
            Some((request, DeadlineKey::Timeout, budget)),
            "{send:?}: {failure:?}"
        );
        assert_eq!(message_of(&failure), Some(message), "{send:?}");
    }
}
