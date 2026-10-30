//! The vocabulary of application sessions as a source and an emitter.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The producer source Model with its acknowledgement mode, the alterations of the
//!   ingestors that read it, the consumer emitter Model, and what each entity declares.
//! - **Depends on.** The shared source and emitter declarations, the acknowledgement and retry
//!   vocabulary, and the schema name a body references.
//! - **Must not know.** How a session attaches, how a batch is submitted, admitted or delivered,
//!   or which node serves a producer or a consumer.
//!
//! No client Model configures this connector: an application reaches it through its own session.
//! A producer's batches already carry the ingestor's input schema, so its source is held by
//! [`IngestorInput`](crate::IngestorInput) beside the transport contract and never pairs with a
//! codec, and an alteration of its ingestor holds no codec to set either.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    entity_union::{ALTERED_PAIR_CHECKED, connector_emitters, connector_ingestor_kinds},
    ingestor::AlteredIngestorEntity,
};
use crate::{
    AckWindow, AcknowledgementConfirmation, AlterIngestorError, AlterIngestorOperation,
    CanonicalPlexusError, ClientName, CreateIngestor, DeclaredDelivery, DeclaredQuiesce,
    EmitterBatchLegality, EmitterBodyForm, EmitterDeclaration, EmitterEntityBody,
    EmitterEntityCapabilities, EmitterReference, EmitterReferenceKind, IngestDeliveryKind,
    IngestQuiesceMode, IngestorAlteration, IngestorDeclaration, IngestorEntityCapabilities,
    IngestorEntityModel, IngestorInput, IngestorMetadataDeclaration, IngestorReferenceKind,
    PositionClass, RetryPolicy, SchemaName,
};

/// Application producers submitting native Arrow batches of one internal schema.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientProducerIngestor {
    /// The internal schema every submitted batch matches exactly, before route construction.
    pub schema: SchemaName,
    pub mode: ClientIngestMode,
}

/// How many submitted batches may await their acknowledgement across every producer of one
/// client ingestor, how long one waits without progress, and how a producer retries a batch that
/// was refused before admission.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientIngestMode {
    pub window: AckWindow,
    pub ack_timeout: String,
    pub retry_policy: RetryPolicy,
}

impl ClientIngestMode {
    /// Which delivery mode this is: `ACK SEQUENTIAL` or `ACK PARALLEL`.
    pub const fn kind(&self) -> IngestDeliveryKind {
        match self.window {
            AckWindow::Sequential => IngestDeliveryKind::AckSequential,
            AckWindow::Parallel { .. } => IngestDeliveryKind::AckParallel,
        }
    }

    /// The acknowledgement window as a count: one batch for `ACK SEQUENTIAL`, `max` for
    /// `ACK PARALLEL MAX <max>`.
    pub fn window_size(&self) -> NonZeroU64 {
        match self.window {
            AckWindow::Sequential => NonZeroU64::MIN,
            AckWindow::Parallel { max } => max,
        }
    }
}

/// A producer's parallel window holds every admitted batch until its acknowledgement, so it
/// collects acknowledgements without a batch timeout.
const CLIENT_ACK_PARALLEL: DeclaredDelivery = DeclaredDelivery {
    kind: IngestDeliveryKind::AckParallel,
    example: "ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s",
    requires: &[],
};

/// What the connector of a CLIENT PRODUCER holds and how long it waits.
const PRODUCER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "batches one producer submits ahead of their outcomes",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Items {
                count: 1024,
                unit: "batches, within 32 MiB, per producer grant",
            },
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[DeclaredWait {
        wait: "the acknowledgement of an admitted batch",
        bound: WaitBound::Clause("ACK TIMEOUT"),
    }],
    driver_limits: &[],
};

/// What the connector of a CLIENT CONSUMER holds and how long it waits.
const CONSUMER_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "batches retained for consumers on a node",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Bytes(134217728),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[DeclaredWait {
        wait: "a delivery attempt",
        bound: WaitBound::Clause("ACK TIMEOUT"),
    }],
    driver_limits: &[],
};

impl IngestorDeclaration for ClientProducerIngestor {
    /// A submitted batch carries no transport headers, so its rows expose an empty header list
    /// that no program may read.
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        headers: None,
        metadata: IngestorMetadataDeclaration::NONE,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Schema,
        position_class: PositionClass::None,
        acknowledgement_confirmation: AcknowledgementConfirmation::None,
        delivery: &[DeclaredDelivery::ACK_SEQUENTIAL, CLIENT_ACK_PARALLEL],
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: None,
        options: &[],
        bounds: PRODUCER_BOUNDS,
    };
}

connector_ingestor_kinds! {
    connector Client = "CLIENT";
    /// The entities a client source reads.
    pub enum ClientIngestorEntity {
        Producer(ClientProducerIngestor) = "PRODUCER", "Client batches";
    }
}

/// The alterations of the ingestors that read a client entity. Their batches need no codec, so no
/// operation of theirs sets one.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ClientIngestorAlteration {
    Producer(Vec<AlterIngestorOperation<ClientProducerIngestor>>),
}

impl ClientIngestorAlteration {
    /// The entity whose ingestors this alteration changes.
    pub const fn entity(&self) -> ClientIngestorEntity {
        match self {
            Self::Producer(_) => ClientIngestorEntity::Producer,
        }
    }

    /// Applies the operations in order to an ingestor that reads this entity.
    pub(crate) fn apply_to(
        &self,
        ingestor: &mut CreateIngestor,
    ) -> error_stack::Result<(), AlterIngestorError> {
        match self {
            Self::Producer(operations) => ingestor.apply_operations(operations),
        }
    }

    /// The canonical Plexus of each operation, in order.
    pub(crate) fn operations_to_plexus(
        &self,
    ) -> error_stack::Result<Vec<String>, CanonicalPlexusError> {
        match self {
            Self::Producer(operations) => {
                crate::canonical::alter_ingestor_operations_to_plexus(operations)
            }
        }
    }
}

impl From<Vec<AlterIngestorOperation<ClientProducerIngestor>>> for ClientIngestorAlteration {
    fn from(operations: Vec<AlterIngestorOperation<ClientProducerIngestor>>) -> Self {
        Self::Producer(operations)
    }
}

impl From<Vec<AlterIngestorOperation<ClientProducerIngestor>>> for IngestorAlteration {
    fn from(operations: Vec<AlterIngestorOperation<ClientProducerIngestor>>) -> Self {
        Self::Client(ClientIngestorAlteration::Producer(operations))
    }
}

impl AlteredIngestorEntity for ClientProducerIngestor {
    fn set_into(&self, input: &mut IngestorInput) {
        let source = input.client_entity_mut().verified(ALTERED_PAIR_CHECKED);
        *source = self.clone();
    }

    /// A client source writes no quiesce mode: it honors `SUSPEND` alone, which setting changes
    /// nothing.
    fn set_quiesce_of(
        input: &mut IngestorInput,
        quiesce: IngestQuiesceMode,
    ) -> error_stack::Result<(), AlterIngestorError> {
        if !input.supports_quiesce(&quiesce) {
            return Err(Report::new(AlterIngestorError::UnsupportedQuiesceMode {
                kind: Self::KIND,
                mode: quiesce.kind_label(),
            }));
        }
        Ok(())
    }
}

/// Application consumers receiving this emitter's native Arrow output.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientConsumerEmitter {
    pub mode: ClientConsumerPublishingMode,
    pub schema: SchemaName,
}

impl EmitterDeclaration for ClientConsumerEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Schema,
        body: EmitterBodyForm::Native,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: ClientConsumerPublishingMode::KINDS,
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: CONSUMER_BOUNDS,
    };
}

impl EmitterEntityBody for ClientConsumerEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Schema(&self.schema)
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        None
    }
}

connector_emitters! {
    connector Client = "CLIENT";
    /// What a client emitter writes.
    pub enum ClientEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum ClientEmitterMode;
    /// The entities a client emitter writes.
    pub enum ClientEmitterEntity {
        Consumer(ClientConsumerEmitter, ClientConsumerPublishingMode) = "CONSUMER";
    }
}

super::publishing::emitter_modes! {
    pub enum ClientConsumerPublishingMode {
        Ack { window: crate::AckWindow, ack_timeout: String, retry_policy: crate::RetryPolicy } => ClientAck,
    }
}
