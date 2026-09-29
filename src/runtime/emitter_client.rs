//! Arrow output preparation for native client emitters.
//!
//! Layer: data plane.
//! - **Owns.** Packing one source relay and concrete branch at a time into exact-schema Arrow IPC,
//!   retaining prepared bytes and source members, and applying application ACKs or rejections.
//! - **Depends on.** The emitter host's prepared-payload contract and client delivery owner.
//! - **Must not know.** Session transport, cluster routing, NSPL text, or an external connector.

use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::writer::StreamWriter;
use async_trait::async_trait;
use bytes::Bytes;
use error_stack::Report;
use futures_util::future::join_all;
use nervix_connector::{
    PerRecordOutcome, RejectedSinkRecord, SinkLifecycle, SinkPublishError, SinkRecordId,
    SinkRecordPosition,
};
use nervix_models::{EmitterBatchPolicy, MessageErrorOperation, Timestamp};
use uuid::Uuid;

use super::{
    emitter_record_writes::{PreparedContent, PreparedPayload},
    *,
};

/// What one native delivery retains across infrastructure retries.
#[derive(Debug)]
pub(super) struct ClientPayload {
    pub(super) payload: ClientEmitterPayload,
}

impl PreparedContent for ClientPayload {
    type Written = ClientEmitterPayload;

    fn written(&self, _record: SinkRecordId, _occurred_at: Timestamp) -> Self::Written {
        self.payload.clone()
    }
}

pub(super) struct ClientEmitterSink {
    endpoint: Arc<ClientEmitterEndpoint>,
    runtime: Runtime,
    key: DomainNodeRef,
    output_schema: Arc<CompiledSchema>,
    batch: EmitterBatchPolicy,
}

impl ClientEmitterSink {
    pub(super) fn new(
        context: &EmitterSinkContext,
        plan: &ClientSinkPlan,
        output_schema: Arc<CompiledSchema>,
        retry: nervix_connector::ParsedRetryPolicy,
    ) -> Self {
        let key = DomainNodeRef::node_in(
            context.domain.clone(),
            ModelKind::Emitter,
            context.emitter.clone(),
        );
        let fields = output_schema.declared_fields();
        let max_bytes = plan.batch.max_size.bytes().get();
        let max_rows = plan.batch.max_messages.get().get();
        let endpoint = Arc::new(ClientEmitterEndpoint::new(
            ClientEmitterDescription {
                fields,
                maximum_payload_bytes: max_bytes,
                maximum_payload_rows: max_rows,
                window: plan.window,
                ack_timeout: plan.ack_timeout,
                retry_backoff: retry.backoff,
                retry_max_backoff: retry.max_backoff,
            },
            context.runtime.inner.client_emitter_budget.clone(),
            context
                .runtime
                .inner
                .metrics
                .client_emitter_series(&context.domain, &context.emitter),
        ));
        if let Some(previous) = context
            .runtime
            .inner
            .client_emitters
            .insert(key.clone(), endpoint.clone())
        {
            previous.replace();
        }
        Self {
            endpoint,
            runtime: context.runtime.clone(),
            key,
            output_schema,
            batch: plan.batch,
        }
    }

    fn encode_rows(
        &self,
        batch: &EmitterPublishBatch,
        rows: &[usize],
    ) -> EmitterRuntimeResult<Bytes> {
        let indices = UInt64Array::from(
            rows.iter()
                .map(|row| u64::try_from(*row).assured("an in-memory row index fits in u64"))
                .collect::<Vec<_>>(),
        );
        let source = batch.relay_batch().batch.batch();
        let columns = source
            .columns()
            .iter()
            .map(|column| take_arrow_array(column.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| emitter_report(EmitterRuntimeError::EncodeBatch, error))?;
        let selected = RecordBatch::try_new(self.output_schema.arrow_schema(), columns)
            .map_err(|error| emitter_report(EmitterRuntimeError::EncodeBatch, error))?;
        let mut body = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut body, &self.output_schema.arrow_schema())
                    .map_err(|error| emitter_report(EmitterRuntimeError::EncodeBatch, error))?;
            StreamWriter::write(&mut writer, &selected)
                .map_err(|error| emitter_report(EmitterRuntimeError::EncodeBatch, error))?;
            writer
                .finish()
                .map_err(|error| emitter_report(EmitterRuntimeError::EncodeBatch, error))?;
        }
        Ok(Bytes::from(body))
    }

    /// Packs each carrier without crossing its source relay or branch boundary. A candidate that
    /// exceeds the IPC byte bound is halved; one oversized row follows the message-error policy.
    fn prepare(
        &self,
        batches: &[EmitterPublishBatch],
    ) -> EmitterRuntimeResult<(
        Vec<PreparedPayload<ClientPayload>>,
        Vec<RejectedEmitterRecord>,
    )> {
        let mut prepared = Vec::new();
        let mut rejected = Vec::new();
        let max_rows = usize::try_from(self.batch.max_messages.get().get())
            .assured("client batch maximum is at most 65536");
        let max_bytes = self.batch.max_size.bytes().get();
        for (batch_index, batch) in batches.iter().enumerate() {
            let pending = batch.pending_record_rows();
            let mut next = 0;
            while next < pending.len() {
                let first_row = pending[next];
                let branch = batch.relay_batch().keys.get(first_row).ok_or_else(|| {
                    emitter_report(
                        EmitterRuntimeError::EncodeBatch,
                        "client emitter row has no branch identity",
                    )
                })?;
                let mut end = next + 1;
                while end - next < max_rows
                    && end < pending.len()
                    && batch.relay_batch().keys.get(pending[end]) == Some(branch)
                {
                    end += 1;
                }
                let (rows, body) = loop {
                    let rows = &pending[next..end];
                    let body = self.encode_rows(batch, rows)?;
                    if u64::try_from(body.len())
                        .assured("an in-memory IPC payload length fits in u64")
                        <= max_bytes
                    {
                        break (rows, body);
                    }
                    if rows.len() == 1 {
                        let position = SinkRecordPosition {
                            batch_index,
                            row_index: first_row,
                        };
                        rejected.push(RejectedEmitterRecord {
                            position,
                            reason: String::new(),
                            structured_error: Some(
                                RejectedSinkRecord::oversize(
                                    position,
                                    batch.execution_now(),
                                    format!("client output row exceeds {} IPC bytes", max_bytes),
                                )
                                .error,
                            ),
                        });
                        next += 1;
                        break (&pending[0..0], Bytes::new());
                    }
                    end = next + rows.len().div_ceil(2);
                };
                if rows.is_empty() {
                    continue;
                }
                let payload = ClientEmitterPayload {
                    identity: Uuid::now_v7(),
                    source: batch.source_relay().clone(),
                    branch: branch.clone(),
                    body,
                    members: rows.len(),
                    execution_now: batch.execution_now(),
                };
                prepared.push(PreparedPayload {
                    members: rows
                        .iter()
                        .map(|row_index| SinkRecordPosition {
                            batch_index,
                            row_index: *row_index,
                        })
                        .collect(),
                    occurred_at: batch.execution_now(),
                    content: ClientPayload { payload },
                });
                next = end;
            }
        }
        Ok((prepared, rejected))
    }
}

impl Drop for ClientEmitterSink {
    fn drop(&mut self) {
        if self
            .runtime
            .inner
            .client_emitters
            .remove_if(&self.key, |_, current| Arc::ptr_eq(current, &self.endpoint))
            .is_some()
        {
            self.endpoint.end();
        }
    }
}

#[async_trait]
impl SinkLifecycle for ClientEmitterSink {
    fn keeps_client_on_publish_failure(&self) -> bool {
        false
    }
}

#[async_trait]
impl EmitterSink for ClientEmitterSink {
    fn lifecycle(&self) -> &dyn SinkLifecycle {
        self
    }
    fn lifecycle_mut(&mut self) -> &mut dyn SinkLifecycle {
        self
    }

    async fn publish_batches(
        &mut self,
        context: &EmitterSinkContext,
        publication: EmitterPublication<'_>,
    ) -> EmitterRuntimeResult<()> {
        let EmitterPublication {
            batches,
            client_payloads,
            ..
        } = publication;
        let (prepared, rejected) = self.prepare(batches)?;
        for payload in prepared {
            client_payloads.retain(payload, batches)?;
        }
        finish_rejected_records(context, batches, rejected, MessageErrorOperation::Encode).await?;
        if client_payloads.is_empty() {
            return Ok(());
        }
        let write = client_payloads.next_write();
        let endpoint = self.endpoint.clone();
        let results = join_all(
            write
                .records
                .into_iter()
                .enumerate()
                .map(|(index, payload)| {
                    let endpoint = endpoint.clone();
                    async move {
                        (
                            SinkRecordId::new(index),
                            payload.execution_now,
                            endpoint.publish(payload).await,
                        )
                    }
                }),
        )
        .await;
        let mut outcome = PerRecordOutcome::with_capacity(results.len());
        for (id, occurred_at, result) in results {
            match result {
                Ok(ClientEmitterResult::Acknowledged) => outcome.deliver(id),
                Ok(ClientEmitterResult::Rejected(reason)) => {
                    outcome.reject(RejectedSinkRecord::external(id, occurred_at, reason));
                }
                Err(refusal) => outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: "client" })
                        .attach_printable(format!("client delivery unavailable: {refusal:?}")),
                ),
            }
        }
        client_payloads
            .answers(batches, write.payloads, outcome)?
            .apply(context, batches, DeliveredAcknowledgements::Host)
            .await
    }
}
