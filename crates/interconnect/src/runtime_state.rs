//! The runtime state one node persists, and the envelopes that name and carry it between nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The runtime state kinds and their storage-key tags, the placement and snapshot
//!   envelopes, and the synchronization, acknowledgement and checkpoint messages built from them.
//! - **Depends on.** The vocabulary a placement names and the typed request contract.
//! - **Must not know.** How a runtime encodes, stores, fences or restores the state it names.

use std::time::Duration;

use nervix_models::{
    DomainName, ModelKind, ModelName, RemoteRuntimeField, SchemaFingerprint, WasmStateGeneration,
};
use rkyv::{Archive, Deserialize, Serialize};
use strum::{FromRepr, IntoStaticStr};

use crate::{InterconnectRequest, PoolClass, RemoteOperationFailure};

macro_rules! declare_runtime_state_kinds {
    ($($Kind:ident = $tag:literal,)+) => {
        /// The kinds of runtime state a node persists, and the byte each one occupies in a
        /// storage key.
        #[derive(
            Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, Hash, FromRepr,
            IntoStaticStr,
        )]
        #[repr(u8)]
        #[strum(serialize_all = "snake_case")]
        pub enum RuntimeStateKind {
            $($Kind = $tag,)+
        }

        impl RuntimeStateKind {
            /// This kind's name, as diagnostics and remote operation subjects spell it.
            pub fn as_str(self) -> &'static str {
                self.into()
            }
        }

        impl From<RuntimeStateKind> for u8 {
            fn from(value: RuntimeStateKind) -> Self {
                match value {
                    $(RuntimeStateKind::$Kind => $tag,)+
                }
            }
        }
    };
}

declare_runtime_state_kinds! {
    BranchAggregated = 0,
    Correlator = 1,
    Deduplicator = 2,
    KafkaOffset = 3,
    MaterializedRelay = 4,
    WasmProcessor = 5,
    WindowProcessor = 6,
    BranchLru = 7,
}

/// One runtime state a placement names, together with the lifetime that state belongs to.
///
/// Branch-aggregated metrics and Kafka offsets depend on no schema: they name nothing beyond their
/// kind and live as long as their entity does. Every other kind is laid out by the schemas its
/// entity depends on and names the fingerprint of those schemas, so a placement written under a
/// replaced schema never addresses the current state. WASM guest state also names the generation it
/// was saved in, so a placement of an earlier lifetime never addresses the current state.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RuntimeState {
    BranchAggregated,
    Correlator {
        schema: SchemaFingerprint,
    },
    Deduplicator {
        schema: SchemaFingerprint,
    },
    KafkaOffset,
    MaterializedRelay {
        schema: SchemaFingerprint,
    },
    WasmProcessor {
        schema: SchemaFingerprint,
        generation: WasmStateGeneration,
    },
    WindowProcessor {
        schema: SchemaFingerprint,
    },
    BranchLru {
        schema: SchemaFingerprint,
    },
}

/// Whether one runtime state's encoding depends on the schemas of its entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateSchema {
    /// The state names no schema, so it outlives every schema change of its entity.
    Independent,
    /// The state is laid out by the schemas this fingerprint covers, and is current only under it.
    Fingerprinted(SchemaFingerprint),
}

impl RuntimeState {
    /// The kind of runtime state this names.
    pub const fn kind(self) -> RuntimeStateKind {
        match self {
            Self::BranchAggregated => RuntimeStateKind::BranchAggregated,
            Self::Correlator { .. } => RuntimeStateKind::Correlator,
            Self::Deduplicator { .. } => RuntimeStateKind::Deduplicator,
            Self::KafkaOffset => RuntimeStateKind::KafkaOffset,
            Self::MaterializedRelay { .. } => RuntimeStateKind::MaterializedRelay,
            Self::WasmProcessor { .. } => RuntimeStateKind::WasmProcessor,
            Self::WindowProcessor { .. } => RuntimeStateKind::WindowProcessor,
            Self::BranchLru { .. } => RuntimeStateKind::BranchLru,
        }
    }

    /// The schemas this state's encoding depends on.
    pub const fn schema(self) -> StateSchema {
        match self {
            Self::BranchAggregated | Self::KafkaOffset => StateSchema::Independent,
            Self::Correlator { schema }
            | Self::Deduplicator { schema }
            | Self::MaterializedRelay { schema }
            | Self::WasmProcessor { schema, .. }
            | Self::WindowProcessor { schema }
            | Self::BranchLru { schema } => StateSchema::Fingerprinted(schema),
        }
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StatePlacementEnvelope {
    pub domain: DomainName,
    pub state: RuntimeState,
    pub kind: ModelKind,
    pub identifier: ModelName,
    pub branch_key: Option<Vec<RemoteRuntimeField>>,
}

/// One checkpoint of the state a placement names. It travels only beside that placement, which
/// alone says what the payload is laid out by.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSnapshotEnvelope {
    pub lsm: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateSyncRequest {
    pub placement: StatePlacementEnvelope,
    pub after_lsm: Option<u64>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSyncResponse {
    pub result: Result<Option<StateSnapshotEnvelope>, RemoteOperationFailure>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateReplicationAck {
    pub placement: StatePlacementEnvelope,
    pub lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StateCheckpointAvailable {
    pub placement: StatePlacementEnvelope,
    pub lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct OwnershipHandoffCheckpoint {
    pub placement: StatePlacementEnvelope,
    pub snapshot: StateSnapshotEnvelope,
}
impl InterconnectRequest for StateSyncRequest {
    type Response = StateSyncResponse;

    const NAME: &'static str = "state_sync";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}
