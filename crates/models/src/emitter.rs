//! The emitter Model and how it publishes.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The emitter Model with its body, batching contract and attachment, its `ALTER`
//!   operations and their typed failures, and the publishing modes an emitter accepts.
//! - **Depends on.** The emitter union, the batching policy, processor inputs and route
//!   construction, and the acknowledgement and retry vocabulary.
//! - **Must not know.** How an emitter is validated, planned, hosted or driven.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use error_stack::{Report, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumIter, EnumString, IntoStaticStr};
use thiserror::Error;

use crate::{
    AckWindow, BatchUnavailability, ClientName, CodecName, EmitterBatchLegality, EmitterEntity,
    EmitterName, ErrorPolicies, FlushPolicy, InputCollectPolicy, ProcessorInputWhere,
    ProcessorInputs, RelayName, RetryPolicy,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CreateEmitter {
    pub labels: crate::EntityLabels,
    pub name: EmitterName,
    pub from: ProcessorInputs,
    pub body: EmitterBody,
    /// The envelope clauses and properties the statement writes after the payload, which set what
    /// the transport carries beside each payload.
    pub envelope: Box<crate::EmitterEnvelope>,
    pub connector: Box<EmitterEntity>,
    /// `BATCH MAX MESSAGES <n> MAX SIZE <bytes>`, absent when the emitter publishes one record per
    /// message. A emitter whose [`EmitterEntity::batch_legality`] requires it never omits it.
    pub batch: Option<crate::EmitterBatchPolicy>,
    pub flush_policy: FlushPolicy,
    pub error_policies: ErrorPolicies,
    #[serde(default)]
    pub mode: AckMode,
    #[serde(default)]
    pub construction: crate::RouteConstruction,
    pub materialized_state: Vec<crate::MaterializedStateDependency>,
}

/// The complete payload selection of an emitter. HTTP distinguishes an encoded body from an
/// explicit absence of content; direct-value emitters construct their own destination values.
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
        if self.connector.kind() != alter.kind {
            return Err(Report::new(AlterEmitterError::EmitterKindMismatch {
                stored: self.connector.kind(),
                requested: alter.kind,
            }));
        }
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

    /// Checks the batching clause against what this emitter's emitter requires of it: present where
    /// the emitter has no unbounded write, and no larger than a payload the destination can carry.
    pub fn validate_batch(&self) -> error_stack::Result<(), EmitterBatchContractError> {
        match self.batch_contract_violation() {
            Some(violation) => Err(Report::new(violation)),
            None => Ok(()),
        }
    }

    /// The requirement of its emitter's batching contract that this emitter's clause breaks, if any.
    ///
    /// Both the standalone check and an alteration report a violation from here, each in its own
    /// error, so neither has to rebuild the other's report.
    fn batch_contract_violation(&self) -> Option<EmitterBatchContractError> {
        let connector = self.connector.kind().phrase();
        let legality = self.connector.batch_legality();
        let Some(batch) = &self.batch else {
            return match legality {
                EmitterBatchLegality::Required { .. } => {
                    Some(EmitterBatchContractError::Required { connector })
                }
                EmitterBatchLegality::Optional { .. }
                | EmitterBatchLegality::Unavailable { .. } => None,
            };
        };
        let maximum = match legality {
            EmitterBatchLegality::Unavailable { reason } => {
                return Some(EmitterBatchContractError::Unavailable { connector, reason });
            }
            EmitterBatchLegality::Required { maximum }
            | EmitterBatchLegality::Optional { maximum } => maximum,
        };
        let maximum = maximum?;
        if batch.max_size.bytes() > maximum.bytes() {
            return Some(EmitterBatchContractError::SizeAboveEmitterMaximum {
                connector,
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
            } => self.add_input(relay, where_clause.as_ref()),
            AlterEmitterOperation::DropFrom { relay } => self.drop_input(relay),
            AlterEmitterOperation::AlterFromSetWhere {
                relay,
                where_clause,
            } => self.set_input_where(relay, where_clause),
            AlterEmitterOperation::AlterFromDropWhere { relay } => self.drop_input_where(relay),
            AlterEmitterOperation::SetTo {
                destination,
                body,
                envelope,
                construction,
            } => self.set_destination(destination, body, envelope, construction.as_ref()),
            AlterEmitterOperation::Change { change } => self.connector.apply_change(change),
            AlterEmitterOperation::SetClient { client } => self.set_client(client),
            AlterEmitterOperation::SetEncodeUsing { codec } => {
                self.body = EmitterBody::Codec {
                    codec: codec.clone(),
                };
                Ok(())
            }
            AlterEmitterOperation::DropEncode => self.drop_encode(),
            AlterEmitterOperation::AlterEnvelope { alteration } => self
                .envelope
                .apply_alteration(self.connector.kind(), alteration)
                .change_context(AlterEmitterError::Envelope),
            AlterEmitterOperation::Construction { alteration } => self
                .construction
                .apply_alteration(alteration)
                .change_context(AlterEmitterError::Construction),
            AlterEmitterOperation::SetCollect { policy } => {
                self.from.collect_policy = Some(policy.clone());
                Ok(())
            }
            AlterEmitterOperation::DropCollect => {
                self.from.collect_policy = None;
                Ok(())
            }
            AlterEmitterOperation::SetAttachment { mode } => {
                self.mode = *mode;
                Ok(())
            }
            AlterEmitterOperation::SetBatch { policy } => {
                self.batch = Some(*policy);
                Ok(())
            }
            AlterEmitterOperation::DropBatch => self.drop_batch(),
            AlterEmitterOperation::SetFlush { flush_policy } => {
                self.flush_policy = flush_policy.clone();
                Ok(())
            }
            AlterEmitterOperation::SetGeneralError { policy } => {
                self.error_policies.general = policy.clone();
                Ok(())
            }
        }
    }

    /// `ADD FROM <relay> [WHERE <expression>]`.
    fn add_input(
        &mut self,
        relay: &RelayName,
        where_clause: Option<&crate::Expression>,
    ) -> error_stack::Result<(), AlterEmitterError> {
        self.ensure_input_absent(relay)?;
        self.from.from.push(relay.clone());
        if let Some(where_clause) = where_clause {
            self.from.r#where.push(ProcessorInputWhere {
                relay: relay.clone(),
                where_clause: where_clause.clone(),
            });
        }
        Ok(())
    }

    /// `DROP FROM <relay>`, which leaves at least one input.
    fn drop_input(&mut self, relay: &RelayName) -> error_stack::Result<(), AlterEmitterError> {
        let index = self.input_index(relay)?;
        if self.from.from.len() == 1 {
            return Err(Report::new(AlterEmitterError::CannotDropLastInput));
        }
        self.from.from.remove(index);
        self.from
            .r#where
            .retain(|input_where| input_where.relay != *relay);
        Ok(())
    }

    /// `ALTER FROM <relay> SET WHERE <expression>`.
    fn set_input_where(
        &mut self,
        relay: &RelayName,
        where_clause: &crate::Expression,
    ) -> error_stack::Result<(), AlterEmitterError> {
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
        Ok(())
    }

    /// `ALTER FROM <relay> DROP WHERE`.
    fn drop_input_where(
        &mut self,
        relay: &RelayName,
    ) -> error_stack::Result<(), AlterEmitterError> {
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
        Ok(())
    }

    /// `SET TO <complete body>`: the entity, payload and envelope, and the construction where the
    /// body writes one.
    fn set_destination(
        &mut self,
        destination: &EmitterEntity,
        body: &EmitterBody,
        envelope: &crate::EmitterEnvelope,
        construction: Option<&crate::RouteConstruction>,
    ) -> error_stack::Result<(), AlterEmitterError> {
        if self.connector.kind() != destination.kind() {
            return Err(Report::new(AlterEmitterError::DestinationKindMismatch {
                stored: self.connector.kind(),
                requested: destination.kind(),
            }));
        }
        *self.connector = destination.clone();
        self.body = body.clone();
        *self.envelope = envelope.clone();
        if let Some(construction) = construction {
            self.construction = construction.clone();
        }
        Ok(())
    }

    /// `SET CLIENT <client>`.
    fn set_client(&mut self, client: &ClientName) -> error_stack::Result<(), AlterEmitterError> {
        let Some(current) = self.connector.client_mut() else {
            return Err(Report::new(AlterEmitterError::ClientUnsupported));
        };
        *current = client.clone();
        Ok(())
    }

    /// `DROP ENCODE`: the emitter writes its mapped values directly.
    fn drop_encode(&mut self) -> error_stack::Result<(), AlterEmitterError> {
        if self.connector.capabilities().body == crate::EmitterBodyForm::CodecOrWithoutBody {
            return Err(Report::new(AlterEmitterError::BodySelectionRequired {
                entity: self.connector.kind(),
            }));
        }
        if self.body.codec().is_none() {
            return Err(Report::new(AlterEmitterError::EncodeNotConfigured));
        }
        self.body = EmitterBody::Values;
        Ok(())
    }

    /// `DROP BATCH`.
    fn drop_batch(&mut self) -> error_stack::Result<(), AlterEmitterError> {
        if self.batch.take().is_none() {
            return Err(Report::new(AlterEmitterError::BatchNotConfigured));
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
    pub kind: crate::EmitterEntityKind,
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
    /// The complete destination: its entity, payload and envelope, and, where written, its
    /// construction.
    SetTo {
        destination: Box<EmitterEntity>,
        body: EmitterBody,
        envelope: Box<crate::EmitterEnvelope>,
        construction: Option<crate::RouteConstruction>,
    },
    /// `SET MODE`, `SET <locator>`, `SET <option>` or `DROP <option>`, as the entity's own grammar
    /// writes it, changing that part of the body and leaving the rest as it was.
    Change {
        change: crate::EmitterChange,
    },
    SetClient {
        client: ClientName,
    },
    SetEncodeUsing {
        codec: CodecName,
    },
    DropEncode,
    /// `SET <envelope clause>`, `DROP <envelope clause>`, `SET PROPERTIES { … }` or
    /// `DROP PROPERTIES`: one change to the emitter's envelope where it stands.
    AlterEnvelope {
        alteration: Box<crate::EmitterEnvelopeAlteration>,
    },
    /// `SET INHERIT …`, `ADD SET …`, `DROP INVOKE <n>`, `SET WHERE …` and the rest: one change to
    /// the emitter's construction where it stands.
    Construction {
        alteration: crate::ConstructionAlteration,
    },
    SetCollect {
        policy: InputCollectPolicy,
    },
    DropCollect,
    SetAttachment {
        mode: AckMode,
    },
    SetBatch {
        policy: crate::EmitterBatchPolicy,
    },
    DropBatch,
    SetFlush {
        flush_policy: FlushPolicy,
    },
    SetGeneralError {
        policy: crate::GeneralErrorPolicy,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlterEmitterError {
    #[error("ALTER {requested} EMITTER targets an emitter declared as {stored}")]
    EmitterKindMismatch {
        stored: crate::EmitterEntityKind,
        requested: crate::EmitterEntityKind,
    },
    #[error("{stored} EMITTER cannot change its destination to {requested}")]
    DestinationKindMismatch {
        stored: crate::EmitterEntityKind,
        requested: crate::EmitterEntityKind,
    },
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
    #[error("CLIENT CONSUMER emitters do not name an external client")]
    ClientUnsupported,
    #[error("{entity} emitters select an absent body with SET TO ... WITHOUT BODY")]
    BodySelectionRequired { entity: crate::EmitterEntityKind },
    #[error("emitter batching is not configured")]
    BatchNotConfigured,
    #[error("the stored {entity} emitter writes no {option}, so there is no {option} to drop")]
    OptionNotWritten {
        entity: crate::EmitterEntityKind,
        option: &'static str,
    },
    #[error("the {entity} emitter's ON CONFLICT cannot be written for its VALUES")]
    ConflictAction { entity: crate::EmitterEntityKind },
    #[error("the emitter's construction cannot take this alteration")]
    Construction,
    #[error(transparent)]
    Batch(EmitterBatchContractError),
    #[error("the emitter's envelope cannot take this alteration")]
    Envelope,
}

/// Why an emitter's batching clause does not fit the emitter it publishes to.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EmitterBatchContractError {
    #[error("{connector} emitters {reason} and do not support BATCH")]
    Unavailable {
        connector: &'static str,
        reason: BatchUnavailability,
    },
    #[error(
        "{connector} emitters require BATCH MAX MESSAGES <n> MAX SIZE <bytes>, because every \
         write carries several rows"
    )]
    Required { connector: &'static str },
    #[error(
        "{connector} emitters accept BATCH MAX SIZE up to {maximum}, the largest message the \
         destination carries, found {declared}"
    )]
    SizeAboveEmitterMaximum {
        connector: &'static str,
        declared: crate::PayloadSizeLimit,
        maximum: crate::PayloadSizeLimit,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum EmitterPublishingModeKind {
    ClientAck,
    NoAck,
    BrokerAck,
    RequestAck,
}

/// A borrowed observation of an entity-owned publication policy. It is never stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterPublishing<'a> {
    ClientAck {
        window: &'a AckWindow,
        ack_timeout: &'a str,
        retry_policy: &'a RetryPolicy,
    },
    NoAck {
        retry_policy: &'a RetryPolicy,
    },
    BrokerAck {
        window: &'a AckWindow,
        ack_timeout: &'a str,
        retry_policy: &'a RetryPolicy,
    },
    RequestAck {
        retry_policy: &'a RetryPolicy,
    },
}

impl<'a> EmitterPublishing<'a> {
    pub fn retry_policy(self) -> &'a RetryPolicy {
        match self {
            Self::ClientAck { retry_policy, .. }
            | Self::NoAck { retry_policy }
            | Self::BrokerAck { retry_policy, .. }
            | Self::RequestAck { retry_policy } => retry_policy,
        }
    }
    pub fn ack_timeout(self) -> Option<&'a str> {
        match self {
            Self::ClientAck { ack_timeout, .. } | Self::BrokerAck { ack_timeout, .. } => {
                Some(ack_timeout)
            }
            Self::NoAck { .. } | Self::RequestAck { .. } => None,
        }
    }
    pub fn kind_label(self) -> &'static str {
        self.kind().label()
    }
    pub fn kind(self) -> EmitterPublishingModeKind {
        match self {
            Self::ClientAck { .. } => EmitterPublishingModeKind::ClientAck,
            Self::NoAck { .. } => EmitterPublishingModeKind::NoAck,
            Self::BrokerAck { .. } => EmitterPublishingModeKind::BrokerAck,
            Self::RequestAck { .. } => EmitterPublishingModeKind::RequestAck,
        }
    }
}

impl EmitterPublishingModeKind {
    /// How a `MODE` clause names the mode, as `DESCRIBE` and diagnostics name it.
    pub const fn label(self) -> &'static str {
        match self {
            Self::ClientAck | Self::BrokerAck | Self::RequestAck => "ACK",
            Self::NoAck => "NO ACK",
        }
    }

    /// The mode written in full after `MODE`, as documentation and generated statements write it.
    pub const fn example(self) -> &'static str {
        match self {
            Self::ClientAck | Self::BrokerAck => {
                "ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s"
            }
            Self::NoAck => "NO ACK RETRY POLICY BACKOFF 1s MAX 30s",
            Self::RequestAck => "ACK RETRY POLICY BACKOFF 1s MAX 30s",
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
