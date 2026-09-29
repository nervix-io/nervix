//! Publishing an HTTP emitter's rows as the requests its HTTP sink sends.
//!
//! Layer: data plane.
//! - **Owns.** Preparing every row a flush released as one request — the fields the row was
//!   admitted with and its body, the exact bytes the codec produced or none — rejecting a row whose
//!   body cannot be encoded, retaining every prepared request until the sink answers for it, and
//!   handing the retained requests to the sink in the order they were prepared.
//! - **Depends on.** The emitter's buffered batches and the request fields each row carries, the
//!   codec's per-row encoding, the retained payloads and the answers they apply, and the connector
//!   contract's HTTP request sink.
//! - **Must not know.** How the request fields were evaluated, which endpoint the sink sends to, or
//!   when the emitter retries.

use async_trait::async_trait;
use nervix_connector::{HttpRequestSink, SinkLifecycle, SinkRecordPosition};

use super::{
    emitter_encoding::{PendingRowPayload, encode_pending_broker_payloads},
    *,
};

/// The body every request of an HTTP emitter carries.
pub(super) enum HttpRequestBody {
    /// Exactly the bytes the codec produces for the record.
    Encoded(Arc<CompiledCodec>),
    /// No content, for an emitter declared `WITHOUT BODY`.
    Absent,
}

/// An HTTP sink and the body the host prepares each of its requests with.
pub(super) struct PreparedRequestSink {
    sink: Box<dyn HttpRequestSink>,
    body: HttpRequestBody,
}

impl PreparedRequestSink {
    pub(super) fn new(sink: Box<dyn HttpRequestSink>, body: HttpRequestBody) -> Self {
        Self { sink, body }
    }
}

impl HttpRequestBody {
    /// One request for every row the batches still hold that no retained request carries yet, in
    /// row order. A row whose body cannot be encoded is rejected here and prepares no request.
    async fn prepare_pending(
        &self,
        context: &EmitterSinkContext,
        batches: &mut [EmitterPublishBatch],
    ) -> EmitterRuntimeResult<Vec<PreparedPayload<PreparedHttpRequest>>> {
        let mut prepared = Vec::new();
        let mut rejected = Vec::new();
        for (batch_index, batch) in batches.iter().enumerate() {
            tokio::task::consume_budget().await;
            let pending_rows = batch.pending_record_rows();
            if pending_rows.is_empty() {
                continue;
            }
            let bodies = self.bodies(context, batch, pending_rows).await?;
            for PendingRowBody { row_index, body } in bodies {
                let position = SinkRecordPosition {
                    batch_index,
                    row_index,
                };
                let body = match body {
                    Ok(body) => body,
                    Err(error) => {
                        rejected.push(RejectedEmitterRecord {
                            position,
                            reason: format!(
                                "emitter '{}' failed to encode record: {error}",
                                context.emitter.as_str()
                            ),
                            structured_error: None,
                        });
                        continue;
                    }
                };
                prepared.push(PreparedPayload {
                    members: vec![position],
                    occurred_at: batch.execution_now(),
                    content: PreparedHttpRequest {
                        fields: batch.http_request(row_index)?.clone(),
                        body,
                    },
                });
            }
        }
        finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
        Ok(prepared)
    }

    /// The body of each of `pending_rows` of `batch`: the codec's bytes, or no content.
    async fn bodies(
        &self,
        context: &EmitterSinkContext,
        batch: &EmitterPublishBatch,
        pending_rows: Vec<usize>,
    ) -> EmitterRuntimeResult<Vec<PendingRowBody>> {
        let codec = match self {
            Self::Encoded(codec) => codec,
            Self::Absent => {
                let mut bodies = Vec::with_capacity(pending_rows.len());
                for row_index in pending_rows {
                    bodies.push(PendingRowBody {
                        row_index,
                        body: Ok(None),
                    });
                }
                return Ok(bodies);
            }
        };
        let acks = batch.merged_acks();
        let payloads = await_emitter_confirmation(
            &acks,
            encode_pending_broker_payloads(codec.clone(), context, batch, pending_rows),
        )
        .await?;
        let mut bodies = Vec::with_capacity(payloads.len());
        for PendingRowPayload { row_index, payload } in payloads {
            let body = match payload {
                Ok(payload) => Ok(Some(payload)),
                Err(error) => Err(error),
            };
            bodies.push(PendingRowBody { row_index, body });
        }
        Ok(bodies)
    }
}

/// The body one pending row's request carries, or why the codec could not encode it.
struct PendingRowBody {
    row_index: usize,
    body: Result<Option<Vec<u8>>, Report<CodecError>>,
}

#[async_trait]
impl EmitterSink for PreparedRequestSink {
    fn lifecycle(&self) -> &dyn SinkLifecycle {
        &*self.sink
    }

    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
        &mut *self.sink
    }

    /// Prepares a request for every row no retained request carries yet, and sends every retained
    /// request in one write: the ones earlier attempts prepared and the sink left unanswered,
    /// exactly as they were first sent, followed by the ones prepared now.
    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        let EmitterPublication {
            batches, requests, ..
        } = publication;
        let prepared = self.body.prepare_pending(context, batches).await?;
        for request in prepared {
            requests.retain(request, batches)?;
        }
        if requests.is_empty() {
            return Ok(());
        }
        let PreparedWrite { records, payloads } = requests.next_write();
        let request_count = records.len();
        let outcome = self.sink.publish(records).await;
        let outcome = context.received_outcome(request_count, outcome);
        requests
            .answers(batches, payloads, outcome)?
            .apply(context, batches, DeliveredAcknowledgements::Host)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use nervix_connector::{PerRecordOutcome, SinkHttpRequest, SinkPublishError, SinkRecordId};
    use nervix_models::{
        CodecWireFormat, CreateCodec, CreateWireSchema, HttpApplicationHeaders, HttpBodyMode,
        HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin, JsonType, ResolvedCodecWireFormat,
        WireSchemaField,
    };
    use parking_lot::Mutex;

    use super::*;
    use crate::{
        runtime::test_fixtures::{input_schema, named, sink_context},
        runtime_schema::compile_codec,
    };

    /// How the scripted sink answers one write.
    enum Answer {
        /// The sink fails before it answers for any request, as an endpoint that lost the
        /// response does.
        Lose,
        /// The sink delivers every request of the write.
        DeliverAll,
        /// The sink rejects the first request and delivers the rest.
        RejectFirst,
    }

    /// One request the scripted sink was handed, with everything it would send.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SentRequest {
        method: String,
        target: String,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    }

    /// An HTTP sink that keeps every request it was handed and answers each write from a script.
    struct ScriptedHttpSink {
        writes: Arc<Mutex<Vec<Vec<SentRequest>>>>,
        answers: VecDeque<Answer>,
    }

    impl SinkLifecycle for ScriptedHttpSink {}

    #[async_trait]
    impl HttpRequestSink for ScriptedHttpSink {
        async fn publish(
            &mut self,
            requests: Vec<SinkHttpRequest>,
        ) -> PerRecordOutcome<SinkRecordId> {
            let mut sent = Vec::with_capacity(requests.len());
            for request in &requests {
                sent.push(SentRequest {
                    method: request.method.as_str().to_string(),
                    target: request.target.as_str().to_string(),
                    headers: request
                        .headers
                        .iter()
                        .map(|(name, value)| {
                            (name.as_str().to_string(), value.as_str().to_string())
                        })
                        .collect(),
                    body: request.body.clone(),
                });
            }
            self.writes.lock().push(sent);
            let mut outcome = PerRecordOutcome::with_capacity(requests.len());
            let answer = self
                .answers
                .pop_front()
                .expect("the test scripts an answer for every write it makes");
            match answer {
                Answer::Lose => {
                    outcome.fail(Report::new(SinkPublishError::Publish { sink: "scripted" }));
                }
                Answer::DeliverAll => {
                    for request in &requests {
                        outcome.deliver(request.id);
                    }
                }
                Answer::RejectFirst => {
                    for (index, request) in requests.iter().enumerate() {
                        if index == 0 {
                            outcome.reject(request.rejected("refused".to_string()));
                        } else {
                            outcome.deliver(request.id);
                        }
                    }
                }
            }
            outcome
        }
    }

    fn json_codec() -> Arc<CompiledCodec> {
        let wire = CreateWireSchema {
            name: named("input_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("value"),
                ty: JsonType::Integer,
                optional: false,
            }],
        };
        let model = CreateCodec {
            name: named("input_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: wire.name.clone(),
            },
            schema: named("emitter_input"),
            encoding_rules: Vec::new(),
        };
        compile_codec(&model, input_schema(), ResolvedCodecWireFormat::Json(&wire))
            .expect("the test codec and Arrow schema both define one required integer field")
    }

    fn request_fields(method: &str, path: &str, key: &str) -> HttpRequestFields {
        let origin = HttpOrigin::parse("https://api.example.com")
            .expect("the test origin has an HTTPS scheme and a host");
        let mut headers = HttpApplicationHeaders::default();
        headers
            .insert(
                HttpHeaderName::parse("Idempotency-Key").expect("a valid field name"),
                HttpHeaderValue::parse(key).expect("a valid field value"),
            )
            .expect("one short header is within the envelope");
        HttpRequestFields {
            method: HttpMethod::parse(method, HttpBodyMode::WithoutBody)
                .expect("the test method is a valid token"),
            target: origin
                .target(path)
                .expect("the test target is origin-relative"),
            headers,
        }
    }

    /// One buffered batch whose rows carry `values`, each with an acknowledgement root and the
    /// request fields `/events/<value>` with an idempotency key of its own.
    fn batch(values: &[i64]) -> (EmitterPublishBatch, Vec<AckCompletion>) {
        let mut messages = Vec::with_capacity(values.len());
        let mut completions = Vec::with_capacity(values.len());
        let mut requests = Vec::with_capacity(values.len());
        for value in values {
            let (acks, completion) = AckSet::root();
            messages.push(RelayMessage {
                key: None,
                record: test_runtime_row([("value".to_string(), RuntimeValue::I64(*value))]),
                acks,
            });
            completions.push(completion);
            requests.push(request_fields(
                "POST",
                &format!("/events/{value}"),
                &format!("key-{value}"),
            ));
        }
        let batch = RelayRecordBatch::from_messages(input_schema(), messages)
            .expect("the test rows match the emitter input schema");
        let batch = EmitterPublishBatch::from_batch(batch, Timestamp::from_unix_nanos(100))
            .with_http_requests(AdmittedHttpRequests::published(requests))
            .expect("one request for each row");
        (batch, completions)
    }

    fn publication<'a>(
        batches: &'a mut [EmitterPublishBatch],
        requests: &'a mut PreparedPayloads<PreparedHttpRequest>,
        payloads: &'a mut PreparedPayloads<EncodedPayload>,
        row_requests: &'a mut PreparedPayloads<RowRequestBody>,
        client_payloads: &'a mut PreparedPayloads<super::emitter_client::ClientPayload>,
    ) -> EmitterPublication<'a> {
        EmitterPublication {
            batches,
            payloads,
            client_payloads,
            requests,
            row_requests,
        }
    }

    #[tokio::test]
    async fn a_retry_resends_the_prepared_requests_unchanged_before_preparing_new_rows() {
        let context = sink_context();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut sink = PreparedRequestSink::new(
            Box::new(ScriptedHttpSink {
                writes: writes.clone(),
                answers: VecDeque::from([Answer::Lose, Answer::DeliverAll]),
            }),
            HttpRequestBody::Encoded(json_codec()),
        );
        let (first, first_completions) = batch(&[1, 2]);
        let mut batches = vec![first];
        let mut requests = PreparedPayloads::default();
        let mut payloads = PreparedPayloads::default();

        let lost = sink
            .publish_batches(
                &context,
                publication(
                    &mut batches,
                    &mut requests,
                    &mut payloads,
                    &mut PreparedPayloads::default(),
                    &mut PreparedPayloads::default(),
                ),
            )
            .await
            .expect_err("a lost response leaves its requests unresolved");
        assert!(emitter_publish_error_is_retryable(&lost));
        assert!(batches[0].pending_record_rows().is_empty());

        // A batch buffered after the lost write is prepared after the requests it retained.
        let (second, second_completions) = batch(&[3]);
        batches.push(second);
        sink.publish_batches(
            &context,
            publication(
                &mut batches,
                &mut requests,
                &mut payloads,
                &mut PreparedPayloads::default(),
                &mut PreparedPayloads::default(),
            ),
        )
        .await
        .expect("the retry is delivered");

        let sent = |value: i64| SentRequest {
            method: "POST".to_string(),
            target: format!("/events/{value}"),
            headers: vec![("idempotency-key".to_string(), format!("key-{value}"))],
            body: Some(format!(r#"{{"value":{value}}}"#).into_bytes()),
        };
        assert_eq!(
            *writes.lock(),
            vec![vec![sent(1), sent(2)], vec![sent(1), sent(2), sent(3)]]
        );
        assert!(requests.is_empty());
        assert!(payloads.is_empty(), "an HTTP sink retains no batch payload");
        for completion in first_completions.into_iter().chain(second_completions) {
            assert_eq!(completion.wait().await, AckOutcome::Ack);
        }
    }

    #[tokio::test]
    async fn a_bodyless_request_carries_no_content_and_a_rejection_resolves_only_its_row() {
        let context = sink_context();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut sink = PreparedRequestSink::new(
            Box::new(ScriptedHttpSink {
                writes: writes.clone(),
                answers: VecDeque::from([Answer::RejectFirst]),
            }),
            HttpRequestBody::Absent,
        );
        let (only, completions) = batch(&[7, 8]);
        let mut batches = vec![only];
        let mut requests = PreparedPayloads::default();
        let mut payloads = PreparedPayloads::default();

        sink.publish_batches(
            &context,
            publication(
                &mut batches,
                &mut requests,
                &mut payloads,
                &mut PreparedPayloads::default(),
                &mut PreparedPayloads::default(),
            ),
        )
        .await
        .expect("a rejection resolves its row and the other row is delivered");

        let written = writes.lock().clone();
        let [write] = written.as_slice() else {
            panic!("one write carries both requests");
        };
        assert!(write.iter().all(|request| request.body.is_none()));
        assert_eq!(batches[0].resolved_rows(), vec![true, true]);
        assert!(requests.is_empty());
        let [rejected, delivered] = completions.try_into().expect("the batch has two rows");
        assert_eq!(delivered.wait().await, AckOutcome::Ack);
        // The rejected row's message error is logged, which does not acknowledge its source.
        assert!(matches!(rejected.wait().await, AckOutcome::NoAck(_)));
    }

    #[test]
    fn a_row_without_request_fields_cannot_be_published() {
        let (with_fields, _completions) = batch(&[1]);
        assert!(with_fields.http_request(0).is_ok());
        let error = with_fields
            .http_request(1)
            .expect_err("row 1 is outside the batch");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::MissingHttpRequest { row: 1 }
        );

        let without_fields = EmitterPublishBatch::from_batch(
            crate::runtime::test_fixtures::input_batch(),
            Timestamp::from_unix_nanos(100),
        );
        let error = without_fields
            .http_request(0)
            .expect_err("a batch of another sink carries no request fields");
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::MissingHttpRequest { row: 0 }
        );
        let Err(error) =
            without_fields.with_http_requests(AdmittedHttpRequests::published(Vec::new()))
        else {
            panic!("every row needs its request fields");
        };
        assert_eq!(
            *error.current_context(),
            EmitterRuntimeError::HttpRequestCountMismatch {
                request_count: 0,
                row_count: 1,
            }
        );
    }
}
