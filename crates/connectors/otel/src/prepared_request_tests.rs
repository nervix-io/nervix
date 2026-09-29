//! Preparing Export requests from mapped rows, and sending prepared requests unchanged over both
//! OTLP transports.

use std::collections::VecDeque;

use super::*;

fn config(entries: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    entries
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

/// Host services the sink under test never calls.
struct SilentHost;

impl nervix_connector::SinkTransientErrorStatus for SilentHost {
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}

    fn clear_transient_error(&self) {}
}

impl nervix_connector::SinkEventReporter for SilentHost {
    fn report_error(&self, _message: String) {}
}

impl nervix_connector::SinkStagingDirectory for SilentHost {
    fn staging_directory(&self) -> std::path::PathBuf {
        std::env::temp_dir()
    }
}

impl nervix_connector::SinkGeneralErrorHandler for SilentHost {
    fn handle_general_error(
        &self,
        _acks: &nervix_connector::SinkAcknowledgements,
        _reason: String,
    ) {
    }
}

const MAPPED_COLUMNS: [&str; 2] = ["time", "body"];

/// A logs sink mapping `time` and `body`, with one resource attribute and a scope, that sends to
/// `endpoint` over `protocol`.
async fn logs_sink(
    endpoint: &str,
    protocol: &str,
    compression: Option<&str>,
    batch: Option<EmitterBatchPolicy>,
) -> OtelSink {
    let mut entries = vec![("endpoint", endpoint), ("protocol", protocol)];
    if let Some(compression) = compression {
        entries.push(("compression", compression));
    }
    entries.push(("timeout_ms", "5000"));
    let dns = nervix_dns::DnsResolver::load(nervix_dns::DnsConfiguration::system())
        .await
        .assured("the host resolver configuration loads in tests");
    let mapped_schema = StdArc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        arrow_schema::Field::new("body", DataType::Utf8, true),
    ]));
    OtelSink::new(
        OtelSinkConfig {
            config: config(&entries),
            dns,
            signal: OtelSignal::Logs,
            batch,
            values: MAPPED_COLUMNS.map(String::from).to_vec(),
            attributes: Vec::new(),
            resource: vec![OtelResourceAttribute {
                key: "service.name".to_string(),
                value: OtelLiteral::String("checkout".to_string()),
            }],
            scope: Some(OtelScope {
                name: "nervix/test".to_string(),
                version: Some("1.0".to_string()),
            }),
            mapped_schema,
        },
        SinkHost::new(SilentHost),
    )
    .unwrap_or_else(|error| panic!("the test logs sink configuration is valid: {error:?}"))
}

/// The mapped columns of one batch: a time for every row and the given bodies, where `None` is a
/// null body.
fn mapped_logs(bodies: &[Option<&str>]) -> RecordBatch {
    let times = (1_i64..).take(bodies.len()).collect::<Vec<_>>();
    RecordBatch::try_new(
        StdArc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            arrow_schema::Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            StdArc::new(TimestampNanosecondArray::from(times)),
            StdArc::new(StringArray::from(bodies.to_vec())),
        ],
    )
    .assured("both test columns carry one value per row")
}

fn batch_limits(max_messages: u32, max_size: u64) -> EmitterBatchPolicy {
    EmitterBatchPolicy {
        max_messages: nervix_models::BatchMessageLimit::try_from(max_messages)
            .assured("every test limit is within the declared range"),
        max_size: format!("{max_size}B")
            .parse()
            .assured("every test size is a positive byte limit"),
    }
}

/// Prepares every row of `batch` as batch 3 of the host's buffer, the one carrier the host hands a
/// preparation.
async fn prepare_all(sink: &mut OtelSink, batch: &RecordBatch) -> RowRequestPreparation {
    let target_columns = MAPPED_COLUMNS.map(String::from);
    let selected_rows = (0..batch.num_rows()).collect::<Vec<_>>();
    sink.prepare(MappedSinkRows {
        target_columns: &target_columns,
        carriers: vec![MappedSinkCarrier {
            batch_index: 3,
            batch,
            selected_rows: &selected_rows,
            occurred_at: Timestamp::from_unix_nanos(42),
            acknowledgements: None,
        }],
    })
    .await
    .unwrap_or_else(|error| panic!("preparing mapped rows succeeds: {error:?}"))
}

fn position(row_index: usize) -> SinkRecordPosition {
    SinkRecordPosition {
        batch_index: 3,
        row_index,
    }
}

fn members(request: &PreparedRowRequest) -> Vec<usize> {
    request
        .members
        .iter()
        .map(|member| member.row_index)
        .collect()
}

/// The log records a prepared or sent Export request carries, after checking that it carries
/// the sink's one resource and one scope.
fn log_records(body: &[u8]) -> Vec<LogRecord> {
    let request = ExportLogsServiceRequest::decode(body)
        .assured("the sink prepares Export requests prost can decode");
    let [resource_logs] = request.resource_logs.as_slice() else {
        panic!("an Export request carries one resource");
    };
    let resource = resource_logs
        .resource
        .as_ref()
        .assured("the sink installs its resource in every request");
    assert_eq!(resource.attributes.len(), 1);
    assert_eq!(resource.attributes[0].key, "service.name");
    let [scope_logs] = resource_logs.scope_logs.as_slice() else {
        panic!("an Export request carries one scope");
    };
    let scope = scope_logs
        .scope
        .as_ref()
        .assured("the sink installs its scope in every request");
    assert_eq!(
        (scope.name.as_str(), scope.version.as_str()),
        ("nervix/test", "1.0")
    );
    scope_logs.log_records.clone()
}

fn bodies(records: &[LogRecord]) -> Vec<String> {
    let mut bodies = Vec::with_capacity(records.len());
    for record in records {
        match record.body.as_ref().and_then(|body| body.value.as_ref()) {
            Some(any_value::Value::StringValue(body)) => bodies.push(body.clone()),
            other => panic!("a mapped log body is a string, found {other:?}"),
        }
    }
    bodies
}

#[nervix_primitives::test]
async fn prepared_requests_divide_records_by_count_and_share_one_observed_time() {
    let mut sink = logs_sink(
        "http://127.0.0.1:9",
        "http/protobuf",
        None,
        Some(batch_limits(2, 1024 * 1024)),
    )
    .await;
    let batch = mapped_logs(&[Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]);

    let preparation = prepare_all(&mut sink, &batch).await;

    assert!(preparation.rejected.is_empty());
    let requests = &preparation.requests;
    assert_eq!(
        requests.iter().map(members).collect::<Vec<_>>(),
        vec![vec![0, 1], vec![2, 3], vec![4]]
    );
    let mut observed_times = Vec::new();
    let mut carried = Vec::new();
    for request in requests {
        let records = log_records(&request.body);
        observed_times.extend(records.iter().map(|record| record.observed_time_unix_nano));
        carried.push(bodies(&records));
    }
    assert_eq!(
        carried,
        vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["c".to_string(), "d".to_string()],
            vec!["e".to_string()],
        ]
    );
    let first_observed = observed_times[0];
    assert!(first_observed > 0);
    assert!(
        observed_times.iter().all(|time| *time == first_observed),
        "every record prepared together carries the one observed time sampled for them: \
         {observed_times:?}"
    );
}

#[nervix_primitives::test]
async fn a_request_prepared_without_batch_carries_every_record_of_the_batch() {
    let mut sink = logs_sink("http://127.0.0.1:9", "http/protobuf", None, None).await;
    let batch = mapped_logs(&[Some("a"), Some("b"), Some("c")]);

    let preparation = prepare_all(&mut sink, &batch).await;

    assert!(preparation.rejected.is_empty());
    let [request] = preparation.requests.as_slice() else {
        panic!("without BATCH one Arrow batch is one Export request");
    };
    assert_eq!(members(request), vec![0, 1, 2]);
    assert_eq!(bodies(&log_records(&request.body)), vec!["a", "b", "c"]);
}

#[nervix_primitives::test]
async fn preparation_halves_a_candidate_above_max_size_and_refuses_an_oversized_record() {
    let mut unbounded = logs_sink(
        "http://127.0.0.1:9",
        "http/protobuf",
        None,
        Some(batch_limits(3, 1024 * 1024)),
    )
    .await;
    let short = mapped_logs(&[Some("aaaa"), Some("bbbb")]);
    let [pair] = prepare_all(&mut unbounded, &short)
        .await
        .requests
        .try_into()
        .unwrap_or_else(|requests: Vec<PreparedRowRequest>| {
            panic!(
                "two short records fit one request, found {}",
                requests.len()
            )
        });
    let pair_size = u64::try_from(pair.body.len()).assured("a test request fits in u64");
    // A limit one byte below the pair's request admits each record alone, but not both.
    let mut sink = logs_sink(
        "http://127.0.0.1:9",
        "http/protobuf",
        None,
        Some(batch_limits(3, pair_size - 1)),
    )
    .await;
    let oversized = "x".repeat(usize::try_from(pair_size).assured("small"));
    let batch = mapped_logs(&[Some("aaaa"), Some("bbbb"), Some(oversized.as_str())]);

    let preparation = prepare_all(&mut sink, &batch).await;

    assert_eq!(
        preparation.requests.iter().map(members).collect::<Vec<_>>(),
        vec![vec![0], vec![1]],
        "the three-record candidate is halved until each request fits"
    );
    for request in &preparation.requests {
        assert!(u64::try_from(request.body.len()).assured("small") < pair_size);
    }
    let [refused] = preparation.rejected.as_slice() else {
        panic!("only the oversized record is refused");
    };
    assert_eq!(refused.id, position(2));
    assert_eq!(
        refused.error.code,
        nervix_models::MessageErrorCode::Validation
    );
    assert_eq!(
        refused.error.operation,
        nervix_models::MessageErrorOperation::Encode
    );
    let limit = pair_size - 1;
    assert!(
        refused
            .error
            .message
            .contains(&format!("above MAX SIZE {limit}B")),
        "the refusal names the limit: {}",
        refused.error.message
    );
}

#[nervix_primitives::test]
async fn preparation_refuses_a_record_whose_values_are_invalid_and_carries_the_rest() {
    let mut sink = logs_sink(
        "http://127.0.0.1:9",
        "http/protobuf",
        None,
        Some(batch_limits(10, 1024 * 1024)),
    )
    .await;
    let batch = mapped_logs(&[Some("a"), None, Some("c")]);

    let preparation = prepare_all(&mut sink, &batch).await;

    let [request] = preparation.requests.as_slice() else {
        panic!("the valid records share one request");
    };
    assert_eq!(members(request), vec![0, 2]);
    let [refused] = preparation.rejected.as_slice() else {
        panic!("the record without a body is refused");
    };
    assert_eq!(refused.id, position(1));
    assert_eq!(
        refused.error.code,
        nervix_models::MessageErrorCode::Validation
    );
}

/// One HTTP request the test receiver read.
#[derive(Debug)]
struct ReceivedHttpExport {
    target: String,
    content_encoding: Option<String>,
    body: Vec<u8>,
}

/// An HTTP/1.1 receiver on one loopback connection that captures every request and answers each
/// with the next of `answers`: a status and a body.
async fn http_receiver(
    answers: Vec<(u16, Vec<u8>)>,
) -> (
    String,
    StdArc<nervix_primitives::sync::blocking::Mutex<Vec<ReceivedHttpExport>>>,
) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .assured("the loopback interface accepts a test listener");
    let endpoint = format!(
        "http://{}",
        listener
            .local_addr()
            .assured("a bound listener has an address")
    );
    let received = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
    let captured = received.clone();
    nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .assured("the sink under test connects once");
        let mut buffer = Vec::new();
        for (status, answer) in answers {
            let head_end = loop {
                if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                    break end + 4;
                }
                let mut chunk = [0_u8; 4096];
                let read = stream
                    .read(&mut chunk)
                    .await
                    .assured("the test client writes");
                assert!(read > 0, "the client closed before its request ended");
                buffer.extend_from_slice(&chunk[..read]);
            };
            let head = String::from_utf8(buffer[..head_end].to_vec()).assured("ASCII head");
            let mut lines = head.split("\r\n");
            let target = lines
                .next()
                .and_then(|line| line.split(' ').nth(1))
                .assured("a request line names its target")
                .to_string();
            let mut length = 0;
            let mut content_encoding = None;
            for line in lines {
                let Some((name, value)) = line.split_once(':') else {
                    continue;
                };
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse::<usize>().assured("a length");
                }
                if name.eq_ignore_ascii_case("content-encoding") {
                    content_encoding = Some(value.trim().to_string());
                }
            }
            buffer.drain(..head_end);
            while buffer.len() < length {
                let mut chunk = [0_u8; 4096];
                let read = stream
                    .read(&mut chunk)
                    .await
                    .assured("the test client writes");
                assert!(read > 0, "the client closed before its body ended");
                buffer.extend_from_slice(&chunk[..read]);
            }
            let body = buffer.drain(..length).collect::<Vec<_>>();
            captured.lock().push(ReceivedHttpExport {
                target,
                content_encoding,
                body,
            });
            let mut response = format!(
                "HTTP/1.1 {status} Test\r\ncontent-type: \
                 application/x-protobuf\r\ncontent-length: {}\r\n\r\n",
                answer.len()
            )
            .into_bytes();
            response.extend_from_slice(&answer);
            stream
                .write_all(&response)
                .await
                .assured("the test client reads its answer");
        }
    });
    (endpoint, received)
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Read as _;

    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut decoded)
        .assured("the sink gzips what it sends with a valid stream");
    decoded
}

/// The requests prepared from `batch`, as the host hands them to the sink.
fn handed_over(preparation: &RowRequestPreparation) -> Vec<SinkRowRequest> {
    preparation
        .requests
        .iter()
        .enumerate()
        .map(|(index, request)| SinkRowRequest {
            id: SinkRecordId::new(index),
            body: request.body.clone(),
            occurred_at: Timestamp::from_unix_nanos(7),
        })
        .collect()
}

#[nervix_primitives::test]
async fn publish_sends_each_prepared_request_unchanged_over_http_and_answers_for_it() {
    let partial_success = ExportLogsServiceResponse {
        partial_success: Some(ExportLogsPartialSuccess {
            rejected_log_records: 1,
            error_message: "one record was dropped".to_string(),
        }),
    }
    .encode_to_vec();
    let (endpoint, received) = http_receiver(vec![
        (200, partial_success),
        (400, Vec::new()),
        (503, Vec::new()),
    ])
    .await;
    let mut sink = logs_sink(
        &endpoint,
        "http/protobuf",
        Some("gzip"),
        Some(batch_limits(1, 1024 * 1024)),
    )
    .await;
    let batch = mapped_logs(&[Some("a"), Some("b"), Some("c"), Some("d")]);
    let preparation = prepare_all(&mut sink, &batch).await;

    let outcome = sink.publish(handed_over(&preparation)).await.into_parts();

    assert_eq!(
        outcome.delivered,
        vec![SinkRecordId::new(0)],
        "a partial_success acknowledges the whole request"
    );
    let [refused] = outcome.rejected.as_slice() else {
        panic!("the request the receiver refused with 400 is rejected");
    };
    assert_eq!(refused.id, SinkRecordId::new(1));
    assert_eq!(refused.error.occurred_at, Timestamp::from_unix_nanos(7));
    let failure = outcome
        .infrastructure_error
        .expect("a 503 leaves the request and the ones after it unanswered");
    assert_eq!(
        failure.current_context(),
        &SinkPublishError::Publish { sink: OTEL }
    );
    let received = received.lock();
    assert_eq!(received.len(), 3, "the request after the 503 is never sent");
    for (index, export) in received.iter().enumerate() {
        assert_eq!(export.target, "/v1/logs");
        assert_eq!(export.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(
            gunzip(&export.body),
            preparation.requests[index].body,
            "request {index} is sent exactly as it was prepared"
        );
    }
}

/// Captures every logs Export request it serves and answers each from a script, then accepts.
struct ScriptedLogsService {
    received: StdArc<nervix_primitives::sync::blocking::Mutex<Vec<ExportLogsServiceRequest>>>,
    answers: nervix_primitives::sync::blocking::Mutex<
        VecDeque<Result<ExportLogsServiceResponse, GrpcStatus>>,
    >,
}

#[async_trait]
impl opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService
    for ScriptedLogsService
{
    async fn export(
        &self,
        request: GrpcRequest<ExportLogsServiceRequest>,
    ) -> Result<otel_tonic::Response<ExportLogsServiceResponse>, GrpcStatus> {
        self.received.lock().push(request.into_inner());
        let answer = self.answers.lock().pop_front();
        match answer {
            Some(Ok(response)) => Ok(otel_tonic::Response::new(response)),
            Some(Err(status)) => Err(status),
            None => Ok(otel_tonic::Response::new(
                ExportLogsServiceResponse::default(),
            )),
        }
    }
}

/// An OTLP/gRPC logs receiver on a loopback port that accepts gzip-compressed requests.
async fn grpc_receiver(
    answers: Vec<Result<ExportLogsServiceResponse, GrpcStatus>>,
) -> (
    String,
    StdArc<nervix_primitives::sync::blocking::Mutex<Vec<ExportLogsServiceRequest>>>,
) {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .assured("the loopback interface accepts a test listener");
    let endpoint = format!(
        "http://{}",
        listener
            .local_addr()
            .assured("a bound listener has an address")
    );
    let received = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Vec::new()));
    let service = ScriptedLogsService {
        received: received.clone(),
        answers: nervix_primitives::sync::blocking::Mutex::new(answers.into_iter().collect()),
    };
    nervix_primitives::task::spawn(
        otel_tonic::transport::Server::builder()
            .add_service(
                LogsServiceServer::new(service).accept_compressed(CompressionEncoding::Gzip),
            )
            .serve_with_incoming(nervix_primitives::stream::wrappers::TcpListenerStream::new(
                listener,
            )),
    );
    (endpoint, received)
}

#[nervix_primitives::test]
async fn publish_sends_each_prepared_request_unchanged_over_grpc_and_answers_for_it() {
    for compression in [None, Some("gzip")] {
        let (endpoint, received) = grpc_receiver(vec![
            Ok(ExportLogsServiceResponse {
                partial_success: Some(ExportLogsPartialSuccess {
                    rejected_log_records: 1,
                    error_message: String::new(),
                }),
            }),
            Err(GrpcStatus::invalid_argument("refused")),
            Err(GrpcStatus::unavailable("try again")),
        ])
        .await;
        let mut sink = logs_sink(
            &endpoint,
            "grpc",
            compression,
            Some(batch_limits(1, 1024 * 1024)),
        )
        .await;
        let batch = mapped_logs(&[Some("a"), Some("b"), Some("c"), Some("d")]);
        let preparation = prepare_all(&mut sink, &batch).await;

        let outcome = sink.publish(handed_over(&preparation)).await.into_parts();

        assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
        let [refused] = outcome.rejected.as_slice() else {
            panic!("the request refused with INVALID_ARGUMENT is rejected");
        };
        assert_eq!(refused.id, SinkRecordId::new(1));
        let failure = outcome
            .infrastructure_error
            .expect("UNAVAILABLE leaves the request and the ones after it unanswered");
        assert_eq!(
            failure.current_context(),
            &SinkPublishError::Publish { sink: OTEL }
        );
        let received = received.lock();
        assert_eq!(
            received.len(),
            3,
            "the request after UNAVAILABLE is never sent"
        );
        for (index, request) in received.iter().enumerate() {
            assert_eq!(
                request.encode_to_vec(),
                preparation.requests[index].body,
                "request {index} reaches the receiver exactly as it was prepared"
            );
        }
    }
}

#[test]
fn a_grpc_export_without_an_answer_is_retried_whatever_code_tonic_reports() {
    let lost_connection =
        GrpcStatus::from_error(Box::new(std::io::Error::other("connection closed")));
    assert_eq!(lost_connection.code(), GrpcCode::Unknown);
    let timed_out = GrpcStatus::from_error(Box::new(otel_tonic::TimeoutExpired(())));
    assert_eq!(timed_out.code(), GrpcCode::Cancelled);
    let unreadable_answer = ExportResponseDecoder {
        service: OtelExportService::Logs,
    }
    .service
    .decode_response(&b"\xff"[..])
    .map(|_| ())
    .map_err(|error| GrpcStatus::from_error(Box::new(error)))
    .expect_err("one 0xff byte is not a protobuf message");
    for status in [lost_connection, timed_out, unreadable_answer] {
        let OtelTransportOutcome::Failed(error) = OtelTransport::grpc_failure(status) else {
            panic!("an export without an answer is a failure the host retries");
        };
        assert_eq!(
            error.current_context(),
            &SinkPublishError::Publish { sink: OTEL }
        );
    }
}

#[test]
fn a_grpc_answer_is_classified_by_the_otlp_retryable_codes() {
    for code in [
        GrpcCode::Cancelled,
        GrpcCode::DeadlineExceeded,
        GrpcCode::ResourceExhausted,
        GrpcCode::Aborted,
        GrpcCode::OutOfRange,
        GrpcCode::Unavailable,
        GrpcCode::DataLoss,
    ] {
        let OtelTransportOutcome::Failed(error) =
            OtelTransport::grpc_failure(GrpcStatus::new(code, "answered"))
        else {
            panic!("{code:?} answered by the receiver is retried");
        };
        assert_eq!(
            error.current_context(),
            &SinkPublishError::Publish { sink: OTEL },
            "{code:?} answered by the receiver is retried"
        );
    }
    for code in [
        GrpcCode::Unknown,
        GrpcCode::Internal,
        GrpcCode::Unauthenticated,
        GrpcCode::PermissionDenied,
        GrpcCode::NotFound,
        GrpcCode::Unimplemented,
        GrpcCode::FailedPrecondition,
        GrpcCode::AlreadyExists,
    ] {
        let OtelTransportOutcome::Failed(error) =
            OtelTransport::grpc_failure(GrpcStatus::new(code, "answered"))
        else {
            panic!("{code:?} answered by the receiver fails the attempt");
        };
        assert_eq!(
            error.current_context(),
            &SinkPublishError::Misconfigured { sink: OTEL },
            "{code:?} answered by the receiver is not retried"
        );
    }
    assert!(matches!(
        OtelTransport::grpc_failure(GrpcStatus::new(GrpcCode::InvalidArgument, "answered")),
        OtelTransportOutcome::Rejected(_)
    ));
}
