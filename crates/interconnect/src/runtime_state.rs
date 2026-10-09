//! The runtime state one node persists, and the envelopes that name and carry it between nodes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The runtime state kinds and their storage-key tags, the placement and snapshot
//!   envelopes, the synchronization, acknowledgement and checkpoint messages built from them, and
//!   the listing through which a replica learns which branch checkpoints of an entity changed.
//! - **Depends on.** The vocabulary a placement names and the typed request contract.
//! - **Must not know.** How a runtime encodes, stores, fences or restores the state it names.

use std::time::Duration;

use nervix_models::{
    DomainName, ModelKind, ModelName, RemoteRuntimeField, SchemaFingerprint, WasmStateGeneration,
};
use rkyv::{Archive, Deserialize, Serialize};
use strum::{FromRepr, IntoStaticStr};

use crate::{
    InterconnectRequest, InterconnectStreamRequest, PoolClass, RemoteOperationFailure,
    RequestSubquota,
};

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
    WindowProcessor = 8,
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

/// The exact checkpoint of the state a placement names. Its bytes travel through the bulk pool;
/// the receiving node verifies their length and digest before installing them.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSnapshotEnvelope {
    pub lsm: u64,
    pub length: u64,
    pub digest: [u8; 32],
}

/// Fetch the checkpoint selected by a preceding state-sync or handoff description. An owner
/// refuses a revision it no longer holds rather than substituting newer state.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateCheckpointRead {
    /// Use the state selected at request admission, or storage when it has no live state.
    Published,
    /// Use the exact checkpoint the source persisted while capturing a planned handoff.
    HandoffCapture,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct FetchStateCheckpoint {
    pub placement: StatePlacementEnvelope,
    pub lsm: u64,
    pub read: StateCheckpointRead,
}

/// Kafka checkpoints may exceed either the replication message or bulk memory ceiling. The
/// request selects a retained state handle once, and its response streams the current native
/// checkpoint rather than carrying that checkpoint inside a control response.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct SyncKafkaOffsets {
    pub placement: StatePlacementEnvelope,
    pub after_lsm: Option<u64>,
}

/// A typed admission and revision check precedes the bulk transfer. An unchanged checkpoint
/// requires neither encoding nor a Snapshot stream, and ordinary absence remains distinct from a
/// failed checkpoint read.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct DescribeKafkaOffsets {
    pub placement: StatePlacementEnvelope,
    pub after_lsm: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum KafkaOffsetRevision {
    Current,
    Advanced(u64),
}

impl InterconnectRequest for DescribeKafkaOffsets {
    type Response = Result<KafkaOffsetRevision, RemoteOperationFailure>;
    const NAME: &'static str = "describe_kafka_offsets";
    const CLASS: PoolClass = PoolClass::Commands;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectStreamRequest for SyncKafkaOffsets {
    const NAME: &'static str = "sync_kafka_offsets";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);
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
/// Where a replica stands in the catalog of branch checkpoints an owner keeps for one branch-keyed
/// entity: the catalog it read, and how many of that catalog's changes it has learned.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchCheckpointCursor {
    /// Chosen when the owner created the catalog, so a cursor of another catalog is never taken
    /// for one of its own.
    pub epoch: u64,
    /// How many of the catalog's changes the replica has learned.
    pub sequence: u64,
}

/// A replica's request for what changed in the owner's catalog of one entity's branch checkpoints
/// after `after`, or for the catalog from its beginning when it has no cursor yet.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct BranchCheckpointListingRequest {
    /// The placement of the entity's branch lifecycle, which names the entity and the schemas its
    /// branch states are laid out by.
    pub lifecycle: StatePlacementEnvelope,
    pub after: Option<BranchCheckpointCursor>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct BranchCheckpointListingResponse {
    pub result: Result<BranchCheckpointListing, RemoteOperationFailure>,
}

/// What an owner answers a listing request with.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum BranchCheckpointListing {
    /// The owner holds no branch state of the entity.
    Absent,
    /// The replica's cursor could not be served, so it forgets what it knew of the catalog and
    /// learns it from its beginning, starting with this page.
    Restarted(BranchCheckpointPage),
    /// The next changes after the replica's cursor.
    Continued(BranchCheckpointPage),
}

/// Changes of one catalog in the order they happened, up to a bounded number of them.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct BranchCheckpointPage {
    /// Where the replica stands once it applied this page.
    pub cursor: BranchCheckpointCursor,
    /// The branches whose state went away.
    pub removed: Vec<Option<Vec<RemoteRuntimeField>>>,
    /// The branch checkpoints that changed, each at its newest revision. A branch whose state went
    /// away and was registered again appears in both lists, and its checkpoint is the newer fact.
    pub revised: Vec<BranchCheckpointRevision>,
    /// Whether further changes follow the cursor.
    pub more: bool,
}

/// The newest checkpoint revision of one branch state.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct BranchCheckpointRevision {
    pub branch_key: Option<Vec<RemoteRuntimeField>>,
    pub state: RuntimeState,
    pub lsm: u64,
}

impl InterconnectRequest for BranchCheckpointListingRequest {
    type Response = BranchCheckpointListingResponse;

    const NAME: &'static str = "branch_checkpoint_listing";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectRequest for StateSyncRequest {
    type Response = StateSyncResponse;

    const NAME: &'static str = "state_sync";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

impl InterconnectStreamRequest for FetchStateCheckpoint {
    const NAME: &'static str = "fetch_state_checkpoint";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod wire_properties {
    use meticulous::ResultExt as _;
    use nervix_execution::Executor;

    use super::*;
    use crate::request::RkyvMessage;

    #[derive(Debug, bolero::TypeGenerator)]
    struct CheckpointDescriptionCase {
        lsm: u64,
        length: u32,
        digest: [u8; 32],
        present: bool,
    }

    #[test]
    fn bolero_checkpoint_descriptions_round_trip_through_the_replication_wire() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the ordinary test runtime builds");
        bolero::check!()
            .with_iterations(256)
            .with_max_len(64)
            .with_type::<CheckpointDescriptionCase>()
            .for_each(|case| {
                runtime.block_on(async {
                    let executor = Executor::default();
                    let response = StateSyncResponse {
                        result: Ok(case.present.then_some(StateSnapshotEnvelope {
                            lsm: case.lsm,
                            length: u64::from(case.length),
                            digest: case.digest,
                        })),
                    };
                    let class = StateSyncRequest::CLASS;
                    let (encoded, _reservation) = response
                        .clone()
                        .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                        .await
                        .assured("a bounded checkpoint description encodes");
                    let (decoded, _reservation) =
                        StateSyncResponse::decode_rkyv(executor, class, encoded)
                            .await
                            .assured("the encoded checkpoint description decodes");
                    assert_eq!(decoded, response);
                });
            });
    }
}
