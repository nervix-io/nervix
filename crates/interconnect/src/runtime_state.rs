//! The runtime state one node persists, and the envelopes that name and carry it between nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The runtime state kinds and their storage-key tags, the placement and snapshot
//!   envelopes, and the synchronization, acknowledgement and checkpoint messages built from them.
//! - **Depends on.** The vocabulary a placement names and the typed request contract.
//! - **Must not know.** How a runtime encodes, stores, fences or restores the state it names.

use std::time::Duration;

use nervix_models::{DomainName, ModelKind, ModelName, RemoteRuntimeField};
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

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct StatePlacementEnvelope {
    pub domain: DomainName,
    pub state: RuntimeStateKind,
    pub kind: ModelKind,
    pub identifier: ModelName,
    pub schema_fingerprint: [u8; 32],
    pub branch_key: Option<Vec<RemoteRuntimeField>>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSnapshotEnvelope {
    pub lsm: u64,
    pub schema_fingerprint: [u8; 32],
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
