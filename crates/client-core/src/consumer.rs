//! Application consumers of native client emitter output.
//!
//! Layer: edges.
//! - **Owns.** Opening an emitter consumer on one session exchange, reading Arrow deliveries,
//!   settling each current attempt, and releasing the attachment.
//! - **Depends on.** The session request exchange and typed consumer wire contract.
//! - **Must not know.** Which node executes the emitter or how its output is produced.

use bytes::Bytes;
use error_stack::Report;
use nervix_client_wire::{
    ClientRequest, CloseEmitterRequest, ConsumerId, EmitterBatchDecision, EmitterCloseDisposition,
    EmitterOpened, EmitterSettlement, OpenEmitterDisposition, OpenEmitterRequest,
    ReadEmitterBatchRequest, ReadEmitterDisposition, ReplyBody, SettleEmitterBatchRequest,
};
use nervix_models::{
    ClientConsumerLimits, DomainName, EmitterName, RelayName, SchemaField, Timestamp,
};
use nervix_primitives::sync::atomic::{AtomicBool, Ordering};
use nervix_recovery::Discarded as _;
use tokio::sync::oneshot;
use triomphe::Arc;
use uuid::Uuid;

use crate::{
    client::{Client, RecoveryMode, SessionRecovery},
    error::{ClientError, RequestKind},
    exchange::ExchangeRequests,
    producer::request_on_exchange,
};

struct ConsumerInner {
    id: ConsumerId,
    description: EmitterOpened,
    exchange: Arc<ExchangeRequests>,
    closed: AtomicBool,
}

/// One competing application consumer attached to an emitter on a single session exchange.
pub struct EmitterConsumer {
    inner: Arc<ConsumerInner>,
}

/// One Arrow batch attempt. A retry or timeout may deliver the same identity again with a fresh
/// reference; only the current reference can settle it.
pub struct EmitterDelivery {
    inner: Arc<ConsumerInner>,
    pub identity: Uuid,
    pub reference: Uuid,
    pub source_relay: RelayName,
    pub branch_fingerprint: Option<[u8; 32]>,
    pub batch: Bytes,
    pub members: u32,
    pub execution_now: Timestamp,
}

impl Client {
    /// Opens a competing consumer with an exact output schema. A lost exchange is restored before
    /// retrying the open; the returned consumer remains bound to the exchange that opened it.
    pub async fn subscribe_emitter(
        &self,
        domain: DomainName,
        emitter: EmitterName,
        expected_fields: Vec<SchemaField>,
        limits: ClientConsumerLimits,
    ) -> error_stack::Result<EmitterConsumer, ClientError> {
        let request = ClientRequest::OpenEmitter(OpenEmitterRequest {
            domain,
            emitter,
            expected_fields,
            limits,
        });
        let opened = tokio::time::timeout(self.inner.connector.retry_timeout(), async {
            for _ in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
                tokio::task::consume_budget().await;
                let exchange = self.inner.exchange.lock().await.requests();
                let (answer, answered) = oneshot::channel();
                let request = request.clone();
                tokio::spawn(async move {
                    let result = async {
                        let reply =
                            request_on_exchange(&exchange, request, RequestKind::OpenEmitter)
                                .await?;
                        let ReplyBody::OpenEmitter(outcome) = reply.body else {
                            return Err(Report::new(ClientError::unexpected_reply(
                                RequestKind::OpenEmitter,
                                reply.body,
                            )));
                        };
                        let description = match outcome.disposition {
                            OpenEmitterDisposition::Opened(opened) => *opened,
                            OpenEmitterDisposition::Refused(refusal) => {
                                return Err(Report::new(ClientError::ConsumerRefused {
                                    refusal,
                                    message: outcome.message,
                                }));
                            }
                        };
                        Ok(EmitterConsumer {
                            inner: Arc::new(ConsumerInner {
                                id: ConsumerId::opened_by(reply.request_id),
                                description,
                                exchange,
                                closed: AtomicBool::new(false),
                            }),
                        })
                    }
                    .await;
                    if let Err(Ok(consumer)) = answer.send(result) {
                        drop(consumer);
                    }
                });
                let attempt = answered
                    .await
                    .unwrap_or_else(|_| Err(Report::new(ClientError::SessionClosed)));
                let report = match attempt {
                    Ok(consumer) => return Ok(consumer),
                    Err(report) => report,
                };
                if !report.current_context().retryable_session_failure() {
                    return Err(report);
                }
                match self.recover_session(RecoveryMode::IfClosed).await? {
                    SessionRecovery::Ready => {}
                    SessionRecovery::Unavailable => return Err(report),
                }
            }
            Err(Report::new(ClientError::SessionClosed))
        })
        .await;
        opened.unwrap_or_else(|_| Err(Report::new(ClientError::RetryDeadline)))
    }
}

impl EmitterConsumer {
    pub fn description(&self) -> &EmitterOpened {
        &self.inner.description
    }

    pub fn id(&self) -> ConsumerId {
        self.inner.id
    }

    /// Waits for the next attempt. `None` means the endpoint or this attachment ended.
    pub async fn next_batch(&self) -> error_stack::Result<Option<EmitterDelivery>, ClientError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let request = ClientRequest::ReadEmitterBatch(ReadEmitterBatchRequest {
            consumer: self.inner.id,
        });
        let reply =
            request_on_exchange(&self.inner.exchange, request, RequestKind::ReadEmitterBatch)
                .await?;
        let ReplyBody::ReadEmitterBatch(outcome) = reply.body else {
            return Err(Report::new(ClientError::unexpected_reply(
                RequestKind::ReadEmitterBatch,
                reply.body,
            )));
        };
        match outcome.disposition {
            ReadEmitterDisposition::Ended => {
                self.inner.closed.store(true, Ordering::Release);
                Ok(None)
            }
            ReadEmitterDisposition::Batch(batch) => Ok(Some(EmitterDelivery {
                inner: self.inner.clone(),
                identity: batch.identity,
                reference: batch.reference,
                source_relay: batch.source_relay,
                branch_fingerprint: batch.branch_fingerprint,
                batch: batch.batch,
                members: batch.members,
                execution_now: batch.execution_now,
            })),
        }
    }

    /// Closes this attachment. Unsettled deliveries are immediately eligible for reassignment.
    pub async fn close(self) -> error_stack::Result<EmitterCloseDisposition, ClientError> {
        let request = ClientRequest::CloseEmitter(CloseEmitterRequest {
            consumer: self.inner.id,
        });
        let reply =
            request_on_exchange(&self.inner.exchange, request, RequestKind::CloseEmitter).await?;
        let ReplyBody::CloseEmitter(outcome) = reply.body else {
            return Err(Report::new(ClientError::unexpected_reply(
                RequestKind::CloseEmitter,
                reply.body,
            )));
        };
        self.inner.closed.store(true, Ordering::Release);
        Ok(outcome.disposition)
    }
}

impl Drop for EmitterConsumer {
    fn drop(&mut self) {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let exchange = self.inner.exchange.clone();
        let id = self.inner.id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                request_on_exchange(
                    &exchange,
                    ClientRequest::CloseEmitter(CloseEmitterRequest { consumer: id }),
                    RequestKind::CloseEmitter,
                )
                .await
                .discarded("consumer Drop cannot report a close error to its former caller");
            });
        }
    }
}

impl EmitterDelivery {
    pub async fn ack(&self) -> error_stack::Result<EmitterSettlement, ClientError> {
        self.settle(EmitterBatchDecision::Ack).await
    }

    pub async fn retry(&self) -> error_stack::Result<EmitterSettlement, ClientError> {
        self.settle(EmitterBatchDecision::Retry).await
    }

    pub async fn reject(
        &self,
        reason: impl Into<String>,
    ) -> error_stack::Result<EmitterSettlement, ClientError> {
        self.settle(EmitterBatchDecision::Reject(reason.into()))
            .await
    }

    async fn settle(
        &self,
        decision: EmitterBatchDecision,
    ) -> error_stack::Result<EmitterSettlement, ClientError> {
        let request = ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
            consumer: self.inner.id,
            reference: self.reference,
            decision,
        });
        let reply = request_on_exchange(
            &self.inner.exchange,
            request,
            RequestKind::SettleEmitterBatch,
        )
        .await?;
        let ReplyBody::SettleEmitterBatch(outcome) = reply.body else {
            return Err(Report::new(ClientError::unexpected_reply(
                RequestKind::SettleEmitterBatch,
                reply.body,
            )));
        };
        Ok(outcome.disposition)
    }
}

#[cfg(feature = "arrow")]
impl EmitterDelivery {
    /// Decodes the canonical Arrow stream and checks every field against the emitter's declared
    /// output schema before yielding it to the application.
    pub fn record_batch(&self) -> error_stack::Result<arrow_array::RecordBatch, ClientError> {
        use arrow_ipc::reader::StreamReader;
        let expected = SchemaField::arrow_schema(&self.inner.description.fields);
        let mut reader =
            StreamReader::try_new(std::io::Cursor::new(&self.batch), None).map_err(|_| {
                Report::new(ClientError::UnexpectedReply {
                    request: RequestKind::ReadEmitterBatch,
                })
            })?;
        if reader.schema().as_ref() != &expected {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            }));
        }
        let Some(batch) = reader.next() else {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            }));
        };
        let batch = batch.map_err(|_| {
            Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            })
        })?;
        if reader.next().is_some()
            || batch.num_rows()
                != meticulous::ResultExt::assured(
                    usize::try_from(self.members),
                    "a u32 row count fits in supported client targets",
                )
        {
            return Err(Report::new(ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch,
            }));
        }
        Ok(batch)
    }
}
