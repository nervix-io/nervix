//! The emitter Model and how it publishes.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The emitter Model with its body, batching contract and attachment, its `ALTER`
//!   operations and their typed failures, and the publishing modes a sink accepts.
//! - **Depends on.** The sink union, the batching policy, processor inputs and route
//!   construction, and the acknowledgement and retry vocabulary.
//! - **Must not know.** How an emitter is validated, planned, hosted or driven.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString, IntoStaticStr};
use thiserror::Error;

use crate::{
    AckWindow, ClientName, CodecName, EmitSink, EmitterName, ErrorPolicies, FlushPolicy,
    IcebergSink, InputCollectPolicy, ProcessorInputWhere, ProcessorInputs, RelayName, RetryPolicy,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateEmitter {
    pub name: EmitterName,
    pub from: ProcessorInputs,
    pub body: EmitterBody,
    pub sink: Box<EmitSink>,
    /// `BATCH MAX MESSAGES <n> MAX SIZE <bytes>`, absent when the emitter publishes one record per
    /// message. A sink whose [`EmitSink::batch_requirement`] is required never omits it.
    pub batch: Option<crate::EmitterBatchPolicy>,
    pub flush_policy: FlushPolicy,
    pub error_policies: ErrorPolicies,
    pub publishing_mode: EmitterPublishingMode,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

/// The complete payload selection of an emitter. HTTP distinguishes an encoded body from an
/// explicit absence of content; direct-value sinks construct their own destination values.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EmitterBody {
    Codec {
        codec: CodecName,
    },
    WithoutBody,
    Values,
    /// Native Arrow columns constructed against the declared client output schema.
    Client,
}

impl EmitterBody {
    pub fn codec(&self) -> Option<&CodecName> {
        match self {
            Self::Codec { codec } => Some(codec),
            Self::WithoutBody | Self::Values | Self::Client => None,
        }
    }
}

impl CreateEmitter {
    pub fn apply_alter(
        &mut self,
        alter: &AlterEmitter,
    ) -> error_stack::Result<(), AlterEmitterError> {
        if self.name != alter.emitter {
            return Err(Report::new(AlterEmitterError::EmitterNameMismatch {
                stored: self.name.clone(),
                requested: alter.emitter.clone(),
            }));
        }

        let mut candidate = self.clone();
        for operation in &alter.operations {
            candidate.apply_alter_operation(operation)?;
        }
        if let Some(violation) = candidate.batch_contract_violation() {
            return Err(Report::new(AlterEmitterError::Batch(violation)));
        }
        *self = candidate;
        Ok(())
    }

    /// Checks the batching clause against what this emitter's sink requires of it: present where
    /// the sink has no unbounded write, and no larger than a payload the destination can carry.
    pub fn validate_batch(&self) -> error_stack::Result<(), EmitterBatchContractError> {
        match self.batch_contract_violation() {
            Some(violation) => Err(Report::new(violation)),
            None => Ok(()),
        }
    }

    /// The requirement of its sink's batching contract that this emitter's clause breaks, if any.
    ///
    /// Both the standalone check and an alteration report a violation from here, each in its own
    /// error, so neither has to rebuild the other's report.
    fn batch_contract_violation(&self) -> Option<EmitterBatchContractError> {
        let sink = self.sink.transport_label();
        if let EmitSink::Http(_) = self.sink.as_ref()
            && self.batch.is_some()
        {
            return Some(EmitterBatchContractError::HttpUnsupported);
        }
        let Some(batch) = &self.batch else {
            return match self.sink.batch_requirement() {
                crate::EmitterBatchRequirement::Optional => None,
                crate::EmitterBatchRequirement::Required => {
                    Some(EmitterBatchContractError::Required { sink })
                }
            };
        };
        if let Some(maximum) = self.sink.batch_size_maximum()
            && batch.max_size.bytes() > maximum.bytes()
        {
            return Some(EmitterBatchContractError::SizeAboveSinkMaximum {
                sink,
                declared: batch.max_size,
                maximum,
            });
        }
        None
    }

    fn apply_alter_operation(
        &mut self,
        operation: &AlterEmitterOperation,
    ) -> error_stack::Result<(), AlterEmitterError> {
        match operation {
            AlterEmitterOperation::AddFrom {
                relay,
                where_clause,
            } => {
                self.ensure_input_absent(relay)?;
                self.from.from.push(relay.clone());
                if let Some(where_clause) = where_clause {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterEmitterOperation::DropFrom { relay } => {
                let index = self.input_index(relay)?;
                if self.from.from.len() == 1 {
                    return Err(Report::new(AlterEmitterError::CannotDropLastInput));
                }
                self.from.from.remove(index);
                self.from
                    .r#where
                    .retain(|input_where| input_where.relay != *relay);
            }
            AlterEmitterOperation::AlterFromSetWhere {
                relay,
                where_clause,
            } => {
                self.input_index(relay)?;
                if let Some(input_where) = self
                    .from
                    .r#where
                    .iter_mut()
                    .find(|input_where| input_where.relay == *relay)
                {
                    input_where.where_clause = where_clause.clone();
                } else {
                    self.from.r#where.push(ProcessorInputWhere {
                        relay: relay.clone(),
                        where_clause: where_clause.clone(),
                    });
                }
            }
            AlterEmitterOperation::AlterFromDropWhere { relay } => {
                self.input_index(relay)?;
                let Some(index) = self
                    .from
                    .r#where
                    .iter()
                    .position(|input_where| input_where.relay == *relay)
                else {
                    return Err(Report::new(AlterEmitterError::InputWhereNotConfigured {
                        relay: relay.clone(),
                    }));
                };
                self.from.r#where.remove(index);
            }
            AlterEmitterOperation::SetSink {
                sink,
                publishing_mode,
                body,
            } => {
                if !sink.accepts_publishing_mode(publishing_mode) {
                    return Err(Report::new(AlterEmitterError::PublishingModeUnsupported {
                        sink: sink.transport_label().to_string(),
                        mode: publishing_mode.kind_label().to_string(),
                    }));
                }
                self.sink = sink.clone();
                self.publishing_mode = publishing_mode.clone();
                if let Some(body) = body {
                    self.body = body.clone();
                }
            }
            AlterEmitterOperation::SetClient { client } => {
                let Some(current) = self.sink.client_mut() else {
                    return Err(Report::new(AlterEmitterError::ClientUnsupported));
                };
                *current = client.clone();
            }
            AlterEmitterOperation::SetEncodeUsing { codec } => {
                self.body = EmitterBody::Codec {
                    codec: codec.clone(),
                };
            }
            AlterEmitterOperation::DropEncode => {
                if let EmitSink::Http(_) = self.sink.as_ref() {
                    return Err(Report::new(AlterEmitterError::HttpDropEncode));
                }
                if self.body.codec().is_none() {
                    return Err(Report::new(AlterEmitterError::EncodeNotConfigured));
                }
                self.body = EmitterBody::Values;
            }
            AlterEmitterOperation::SetCollect { policy } => {
                self.from.collect_policy = Some(policy.clone());
            }
            AlterEmitterOperation::DropCollect => {
                self.from.collect_policy = None;
            }
            AlterEmitterOperation::SetAttachment { mode } => {
                self.mode = *mode;
            }
            AlterEmitterOperation::SetPublishingMode { mode } => {
                let selected = match (self.sink.as_ref(), mode) {
                    (
                        EmitSink::Client(_),
                        EmitterPublishingMode::BrokerAck {
                            window,
                            ack_timeout,
                            retry_policy,
                        },
                    ) => EmitterPublishingMode::ClientAck {
                        window: *window,
                        ack_timeout: ack_timeout.clone(),
                        retry_policy: retry_policy.clone(),
                    },
                    _ => mode.clone(),
                };
                if !self.sink.accepts_publishing_mode(&selected) {
                    return Err(Report::new(AlterEmitterError::PublishingModeUnsupported {
                        sink: self.sink.transport_label().to_string(),
                        mode: mode.kind_label().to_string(),
                    }));
                }
                self.publishing_mode = selected;
            }
            AlterEmitterOperation::SetBatch { policy } => {
                self.batch = Some(*policy);
            }
            AlterEmitterOperation::DropBatch => {
                if self.batch.take().is_none() {
                    return Err(Report::new(AlterEmitterError::BatchNotConfigured));
                }
            }
            AlterEmitterOperation::SetFlush { flush_policy } => {
                self.flush_policy = flush_policy.clone();
            }
            AlterEmitterOperation::SetCommit {
                commit_each,
                max_commit_size,
            } => {
                let EmitSink::Iceberg(IcebergSink::Table(table)) = self.sink.as_mut() else {
                    return Err(Report::new(AlterEmitterError::CommitPolicyUnsupported));
                };
                table.commit_each = commit_each.clone();
                table.max_commit_size = max_commit_size.clone();
            }
        }
        Ok(())
    }

    fn input_index(&self, relay: &RelayName) -> error_stack::Result<usize, AlterEmitterError> {
        self.from
            .from
            .iter()
            .position(|candidate| candidate == relay)
            .ok_or_else(|| {
                Report::new(AlterEmitterError::InputNotFound {
                    relay: relay.clone(),
                })
            })
    }

    fn ensure_input_absent(&self, relay: &RelayName) -> error_stack::Result<(), AlterEmitterError> {
        if self.from.from.iter().any(|candidate| candidate == relay) {
            Err(Report::new(AlterEmitterError::InputAlreadyExists {
                relay: relay.clone(),
            }))
        } else {
            Ok(())
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterEmitter {
    pub emitter: EmitterName,
    pub operations: Vec<AlterEmitterOperation>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum AlterEmitterOperation {
    AddFrom {
        relay: RelayName,
        where_clause: Option<crate::Expression>,
    },
    DropFrom {
        relay: RelayName,
    },
    AlterFromSetWhere {
        relay: RelayName,
        where_clause: crate::Expression,
    },
    AlterFromDropWhere {
        relay: RelayName,
    },
    SetSink {
        sink: Box<EmitSink>,
        publishing_mode: EmitterPublishingMode,
        body: Option<EmitterBody>,
    },
    SetClient {
        client: ClientName,
    },
    SetEncodeUsing {
        codec: CodecName,
    },
    DropEncode,
    SetCollect {
        policy: InputCollectPolicy,
    },
    DropCollect,
    SetAttachment {
        mode: AckMode,
    },
    SetPublishingMode {
        mode: EmitterPublishingMode,
    },
    SetBatch {
        policy: crate::EmitterBatchPolicy,
    },
    DropBatch,
    SetFlush {
        flush_policy: FlushPolicy,
    },
    SetCommit {
        commit_each: String,
        max_commit_size: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterEmitterError {
    #[error("ALTER targets emitter `{requested}`, but the stored emitter is `{stored}`")]
    EmitterNameMismatch {
        stored: EmitterName,
        requested: EmitterName,
    },
    #[error("input relay `{relay}` is already configured")]
    InputAlreadyExists { relay: RelayName },
    #[error("input relay `{relay}` is not configured")]
    InputNotFound { relay: RelayName },
    #[error("input relay `{relay}` has no WHERE clause")]
    InputWhereNotConfigured { relay: RelayName },
    #[error("an emitter must retain at least one input")]
    CannotDropLastInput,
    #[error("emitter encoding is not configured")]
    EncodeNotConfigured,
    #[error("CLIENT emitters do not name an external client")]
    ClientUnsupported,
    #[error("HTTP emitters select an absent body with SET TO HTTP ... WITHOUT BODY")]
    HttpDropEncode,
    #[error("COMMIT policy is only supported by Iceberg emitters")]
    CommitPolicyUnsupported,
    #[error("{sink} emitters do not support publishing mode {mode}")]
    PublishingModeUnsupported { sink: String, mode: String },
    #[error("emitter batching is not configured")]
    BatchNotConfigured,
    #[error(transparent)]
    Batch(EmitterBatchContractError),
}

/// Why an emitter's batching clause does not fit the sink it publishes to.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EmitterBatchContractError {
    #[error("HTTP emitters send one request per record and do not support BATCH")]
    HttpUnsupported,
    #[error(
        "{sink} emitters require BATCH MAX MESSAGES <n> MAX SIZE <bytes>, because every write \
         carries several rows"
    )]
    Required { sink: &'static str },
    #[error(
        "{sink} emitters accept BATCH MAX SIZE up to {maximum}, the largest message the \
         destination carries, found {declared}"
    )]
    SizeAboveSinkMaximum {
        sink: &'static str,
        declared: crate::PayloadSizeLimit,
        maximum: crate::PayloadSizeLimit,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EmitterPublishingMode {
    ClientAck {
        window: AckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    NoAck {
        retry_policy: RetryPolicy,
    },
    BrokerAck {
        window: AckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    MqttQos0 {
        retry_policy: RetryPolicy,
    },
    MqttQos1 {
        window: AckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    MqttQos2 {
        window: AckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    NatsJetStream {
        window: AckWindow,
        ack_timeout: String,
        retry_policy: RetryPolicy,
    },
    SqsSingle {
        retry_policy: RetryPolicy,
    },
    SqsBatch {
        retry_policy: RetryPolicy,
    },
    RequestAck {
        retry_policy: RetryPolicy,
    },
}

impl EmitterPublishingMode {
    pub fn retry_policy(&self) -> &RetryPolicy {
        match self {
            Self::ClientAck { retry_policy, .. }
            | Self::NoAck { retry_policy }
            | Self::BrokerAck { retry_policy, .. }
            | Self::MqttQos0 { retry_policy }
            | Self::MqttQos1 { retry_policy, .. }
            | Self::MqttQos2 { retry_policy, .. }
            | Self::NatsJetStream { retry_policy, .. }
            | Self::SqsSingle { retry_policy }
            | Self::SqsBatch { retry_policy }
            | Self::RequestAck { retry_policy } => retry_policy,
        }
    }

    pub fn ack_timeout(&self) -> Option<&str> {
        match self {
            Self::ClientAck { ack_timeout, .. }
            | Self::BrokerAck { ack_timeout, .. }
            | Self::MqttQos1 { ack_timeout, .. }
            | Self::MqttQos2 { ack_timeout, .. }
            | Self::NatsJetStream { ack_timeout, .. } => Some(ack_timeout),
            Self::NoAck { .. }
            | Self::MqttQos0 { .. }
            | Self::SqsSingle { .. }
            | Self::SqsBatch { .. }
            | Self::RequestAck { .. } => None,
        }
    }

    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::ClientAck { .. } => "ACK",
            Self::NoAck { .. } => "NO_ACK",
            Self::BrokerAck { .. } => "ACK",
            Self::MqttQos0 { .. } => "QOS 0",
            Self::MqttQos1 { .. } => "QOS 1 ACK",
            Self::MqttQos2 { .. } => "QOS 2 ACK",
            Self::NatsJetStream { .. } => "JETSTREAM ACK",
            Self::SqsSingle { .. } => "SINGLE",
            Self::SqsBatch { .. } => "BATCH",
            Self::RequestAck { .. } => "ACK",
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Default,
    AsRefStr,
    EnumString,
    IntoStaticStr,
)]
pub enum AckMode {
    #[default]
    #[strum(serialize = "ATTACHED")]
    Attached,
    #[strum(serialize = "DETACHED")]
    Detached,
}

#[cfg(test)]
#[path = "emitter_tests.rs"]
mod tests;
