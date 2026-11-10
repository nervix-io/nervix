//! The bounded owner-page operation for WASM processor guest-state inspection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Typed WASM state page requests, their replies and the archived bound of one branch
//!   entry that keeps a reply within its request class.
//! - **Depends on.** State and WASM inspection vocabulary, the execution limits and the
//!   interconnect request contract.
//! - **Must not know.** Plexus, session envelopes, rendering, checkpoint storage or owner selection.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use std::time::Duration;

use nervix_execution::Executor;
use nervix_models::{
    ClusterNodeIdentity, DomainName, StateInspectionBranchSelection, StateInspectionOrigin,
    StateInspectionRejection, Unit, WasmProcessorName, WasmStateEntryKey, WasmUnitStatePage,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectRequest, PoolClass};

/// The archived size one branch entry cannot exceed: its optional 32-byte fingerprint, checkpoint
/// and recovery with the longest command reference the vocabulary admits, and alignment padding.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the archived layout of WasmBranchStateInspection with a 128-byte command \
                  reference fits 512 bytes; the reply bound test pins it"
    )
)]
const MAX_ARCHIVED_ENTRY_BYTES: u64 = 512;

/// Reads one execution unit's WASM guest-state facts at its owner, with at most `max_entries` of
/// the selected branch entries that follow `after`.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct ReadWasmStatePage {
    pub domain: DomainName,
    pub processor: WasmProcessorName,
    pub unit: Unit,
    /// The committed control revision the coordinator read the processor's schedule at.
    pub control_revision: u64,
    pub selection: StateInspectionBranchSelection,
    pub after: Option<WasmStateEntryKey>,
    pub max_entries: u64,
    /// The exact owner process the coordinator resolved for the unit.
    pub target: ClusterNodeIdentity,
}

impl ReadWasmStatePage {
    /// The most branch entries one reply carries within the request class's payload limit. The
    /// unit's own facts take less room than two entries, which this capacity leaves for them.
    /// The subtraction saturates because a limit too small for even one entry beside those facts
    /// leaves no capacity, which a coordinator refuses rather than paging without progress.
    pub fn entry_capacity(executor: &Executor) -> u64 {
        let limit = <Self as InterconnectRequest>::CLASS.payload_limit(executor);
        let entries = limit / MAX_ARCHIVED_ENTRY_BYTES;
        nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "a payload no larger than the unit's own facts has room for zero branch entries",
            entries.saturating_sub(2)
        )
    }
}

/// One unit's facts as its owner read them, and the origin of that read.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct WasmStatePage {
    pub origin: StateInspectionOrigin,
    pub page: WasmUnitStatePage,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct ReadWasmStatePageResponse {
    pub result: Result<WasmStatePage, StateInspectionRejection>,
}

impl InterconnectRequest for ReadWasmStatePage {
    type Response = ReadWasmStatePageResponse;

    const NAME: &'static str = "read_wasm_state_page";
    const CLASS: PoolClass = PoolClass::Commands;
    const TIMEOUT: Duration = Duration::from_secs(10);
}

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "turmoil", feature = "loom"))
))]
mod tests {
    use std::num::NonZeroU64;

    use meticulous::ResultExt as _;
    use nervix_arbitrary::Arbitrary;
    use nervix_models::{
        BranchKeyFingerprint, ClusterNodeIncarnation, CommandExecutionReference,
        StateInspectionStage, WasmBranchStateInspection, WasmCheckpointCounts, WasmCheckpointStage,
        WasmCheckpointState, WasmRecoveryCounts, WasmRecoveryInspection, WasmSavedStateRejection,
        WasmStateGeneration, WasmStateRecoveryOutcome, WasmStateReset, WasmStateResetInspection,
        WasmStateResetReadiness, WasmStateResetReason, WasmStateResetScope,
        WasmUnitStateInspection,
    };

    use super::*;
    use crate::RkyvMessage;

    fn generation(arbitrary: &mut Arbitrary<'_>) -> WasmStateGeneration {
        WasmStateGeneration::try_from(arbitrary.positive_u64().get())
            .assured("a positive value is a generation")
    }

    fn optional_revision(arbitrary: &mut Arbitrary<'_>) -> Option<NonZeroU64> {
        if arbitrary.entropy().flag() {
            Some(arbitrary.positive_u64())
        } else {
            None
        }
    }

    fn optional_replicas(arbitrary: &mut Arbitrary<'_>) -> Option<u32> {
        if arbitrary.entropy().flag() {
            Some(u32::from(arbitrary.entropy().byte()))
        } else {
            None
        }
    }

    fn checkpoint(arbitrary: &mut Arbitrary<'_>) -> WasmCheckpointState {
        WasmCheckpointState {
            generation: generation(arbitrary),
            committed_revision: optional_revision(arbitrary),
            latest_revision: optional_revision(arbitrary),
            stage: arbitrary.entropy().pick([
                WasmCheckpointStage::Empty,
                WasmCheckpointStage::Captured,
                WasmCheckpointStage::LocallyDurable,
                WasmCheckpointStage::ReplicaConfirmed,
                WasmCheckpointStage::Failed,
            ]),
            required_replicas: optional_replicas(arbitrary),
            confirmed_replicas: optional_replicas(arbitrary),
        }
    }

    fn recovery(arbitrary: &mut Arbitrary<'_>) -> WasmRecoveryInspection {
        WasmRecoveryInspection {
            generation: generation(arbitrary),
            rejection: arbitrary.entropy().pick([
                WasmSavedStateRejection::SnapshotEnvelope,
                WasmSavedStateRejection::ApplicationState,
            ]),
            request: arbitrary.execution_reference(),
            outcome: arbitrary.entropy().pick([
                WasmStateRecoveryOutcome::Attempted,
                WasmStateRecoveryOutcome::Recovered,
                WasmStateRecoveryOutcome::Failed,
            ]),
        }
    }

    fn entries(arbitrary: &mut Arbitrary<'_>) -> Vec<WasmBranchStateInspection> {
        let mut keys = std::collections::BTreeSet::new();
        if arbitrary.entropy().flag() {
            keys.insert(None);
        }
        for _ in 0..arbitrary.entropy().byte() % 4 {
            keys.insert(Some(arbitrary.branch_fingerprint()));
        }
        let mut entries = Vec::new();
        for branch in keys {
            let (checkpoint, recovery) = match arbitrary.entropy().byte() % 3 {
                0 => (Some(checkpoint(arbitrary)), None),
                1 => (None, Some(recovery(arbitrary))),
                _ => (Some(checkpoint(arbitrary)), Some(recovery(arbitrary))),
            };
            entries.push(WasmBranchStateInspection {
                branch,
                checkpoint,
                recovery,
            });
        }
        entries
    }

    fn reset(arbitrary: &mut Arbitrary<'_>) -> WasmStateResetInspection {
        let branch = arbitrary.branch_fingerprint();
        let scope = arbitrary.entropy().pick([
            WasmStateResetScope::Unbranched,
            WasmStateResetScope::Branch(branch),
            WasmStateResetScope::AllBranches,
        ]);
        let reason = arbitrary.entropy().pick([
            WasmStateResetReason::Operator,
            WasmStateResetReason::Transaction,
            WasmStateResetReason::Guest,
            WasmStateResetReason::RejectedSnapshot,
        ]);
        let mut reset = WasmStateReset::publishing(arbitrary.execution_reference(), scope, reason);
        if arbitrary.entropy().flag() {
            reset.mark_ready();
        }
        WasmStateResetInspection {
            reset,
            generation: generation(arbitrary),
            readiness: arbitrary.entropy().pick([
                WasmStateResetReadiness::Resetting,
                WasmStateResetReadiness::AwaitingUsableExecution,
                WasmStateResetReadiness::Ready,
            ]),
        }
    }

    fn count(arbitrary: &mut Arbitrary<'_>) -> usize {
        usize::from(arbitrary.entropy().byte())
    }

    fn page(arbitrary: &mut Arbitrary<'_>) -> WasmUnitStatePage {
        let branches = entries(arbitrary);
        let omitted = count(arbitrary);
        WasmUnitStatePage {
            state: WasmUnitStateInspection {
                default_generation: generation(arbitrary),
                reset: if arbitrary.entropy().flag() {
                    Some(reset(arbitrary))
                } else {
                    None
                },
                entries: branches.len() + omitted,
                checkpoints: WasmCheckpointCounts {
                    total: count(arbitrary),
                    empty: count(arbitrary),
                    captured: count(arbitrary),
                    locally_durable: count(arbitrary),
                    awaiting_replicas: count(arbitrary),
                    replica_confirmed: count(arbitrary),
                    failed: count(arbitrary),
                },
                recoveries: WasmRecoveryCounts {
                    attempted: count(arbitrary),
                    recovered: count(arbitrary),
                    failed: count(arbitrary),
                },
                branches,
            },
            more: omitted != 0,
        }
    }

    /// Whatever the property's seeds draw, one page with every kind of fact crosses the owner's
    /// codec intact: a ready branch-scoped reset, an unbranched checkpoint entry and a branch's
    /// recovery entry, with entries beyond the page.
    #[test]
    fn a_page_with_its_reset_and_both_entry_kinds_round_trips() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("ordinary fixture runtime");
        let generation = |value: u64| {
            WasmStateGeneration::try_from(value).assured("a positive value is a generation")
        };
        let branch = BranchKeyFingerprint::new([5; 32]);
        let mut reset = WasmStateReset::publishing(
            CommandExecutionReference::parse("reset-0004").assured("a literal reference"),
            WasmStateResetScope::Branch(branch),
            WasmStateResetReason::Guest,
        );
        reset.mark_ready();
        let checkpoint = WasmCheckpointState {
            generation: generation(2),
            committed_revision: NonZeroU64::new(4),
            latest_revision: NonZeroU64::new(5),
            stage: WasmCheckpointStage::LocallyDurable,
            required_replicas: Some(1),
            confirmed_replicas: Some(0),
        };
        let recovery = WasmRecoveryInspection {
            generation: generation(1),
            rejection: WasmSavedStateRejection::SnapshotEnvelope,
            request: CommandExecutionReference::parse("recovery-0004")
                .assured("a literal reference"),
            outcome: WasmStateRecoveryOutcome::Failed,
        };
        let page = WasmUnitStatePage {
            state: WasmUnitStateInspection {
                default_generation: generation(1),
                reset: Some(WasmStateResetInspection {
                    reset,
                    generation: generation(2),
                    readiness: WasmStateResetReadiness::AwaitingUsableExecution,
                }),
                entries: 3,
                checkpoints: WasmCheckpointCounts {
                    total: 1,
                    empty: 0,
                    captured: 0,
                    locally_durable: 1,
                    awaiting_replicas: 1,
                    replica_confirmed: 0,
                    failed: 0,
                },
                recoveries: WasmRecoveryCounts {
                    attempted: 0,
                    recovered: 0,
                    failed: 1,
                },
                branches: vec![
                    WasmBranchStateInspection {
                        branch: None,
                        checkpoint: Some(checkpoint),
                        recovery: None,
                    },
                    WasmBranchStateInspection {
                        branch: Some(branch),
                        checkpoint: None,
                        recovery: Some(recovery),
                    },
                ],
            },
            more: true,
        };
        let reply = ReadWasmStatePageResponse {
            result: Ok(WasmStatePage {
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(
                        nervix_models::ClusterNodeName::parse("node-a")
                            .assured("a literal node name"),
                        ClusterNodeIncarnation::new(7),
                    ),
                    fingerprint: [9; 32],
                    generation: 2,
                    revision: 11,
                    stage: StateInspectionStage::Live,
                },
                page,
            }),
        };
        runtime.block_on(async {
            let executor = Executor::default();
            let class = <ReadWasmStatePage as InterconnectRequest>::CLASS;
            let (encoded, _charge) = reply
                .clone()
                .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                .await
                .assured("a bounded reply encodes");
            let (decoded, _charge) =
                ReadWasmStatePageResponse::decode_rkyv(executor, class, encoded)
                    .await
                    .assured("the reply decodes");
            assert_eq!(decoded, reply);
        });
    }

    #[test]
    fn bolero_wasm_state_page_messages_round_trip() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("ordinary fixture runtime");
        bolero::check!()
            .with_iterations(128)
            .with_max_len(512)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary = Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
                let target = arbitrary.node_identity();
                let request = ReadWasmStatePage {
                    domain: arbitrary.name(),
                    processor: arbitrary.name(),
                    unit: arbitrary.unit(),
                    control_revision: arbitrary.positive_u64().get(),
                    selection: match arbitrary.entropy().byte() % 3 {
                        0 => StateInspectionBranchSelection::All,
                        1 => StateInspectionBranchSelection::Unbranched,
                        _ => StateInspectionBranchSelection::Branch(arbitrary.branch_fingerprint()),
                    },
                    after: match arbitrary.entropy().byte() % 3 {
                        0 => None,
                        1 => Some(WasmStateEntryKey(None)),
                        _ => Some(WasmStateEntryKey(Some(arbitrary.branch_fingerprint()))),
                    },
                    max_entries: arbitrary.positive_u64().get(),
                    target: target.clone(),
                };
                let reply = ReadWasmStatePageResponse {
                    result: if arbitrary.entropy().flag() {
                        Err(arbitrary.entropy().pick([
                            StateInspectionRejection::UnknownEntity,
                            StateInspectionRejection::InvalidSelection,
                            StateInspectionRejection::StaleContinuation,
                            StateInspectionRejection::OwnerUnavailable,
                            StateInspectionRejection::NotReady,
                            StateInspectionRejection::Deadline,
                        ]))
                    } else {
                        Ok(WasmStatePage {
                            origin: StateInspectionOrigin {
                                node: target,
                                fingerprint: arbitrary.digest(),
                                generation: arbitrary.positive_u64().get(),
                                revision: arbitrary.positive_u64().get(),
                                stage: StateInspectionStage::Live,
                            },
                            page: page(&mut arbitrary),
                        })
                    },
                };
                runtime.block_on(async {
                    let executor = Executor::default();
                    let class = <ReadWasmStatePage as InterconnectRequest>::CLASS;
                    let (encoded, _charge) = request
                        .clone()
                        .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                        .await
                        .assured("a bounded request encodes");
                    let (decoded, _charge) =
                        ReadWasmStatePage::decode_rkyv(executor.clone(), class, encoded)
                            .await
                            .assured("the request decodes");
                    assert_eq!(decoded, request);
                    let (encoded, _charge) = reply
                        .clone()
                        .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                        .await
                        .assured("a bounded reply encodes");
                    let (decoded, _charge) =
                        ReadWasmStatePageResponse::decode_rkyv(executor, class, encoded)
                            .await
                            .assured("the reply decodes");
                    assert_eq!(decoded, reply);
                });
            });
    }

    #[test]
    fn a_reply_of_entry_capacity_largest_entries_fits_its_request_class() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("ordinary fixture runtime");
        let executor = Executor::default();
        let capacity = usize::try_from(ReadWasmStatePage::entry_capacity(&executor))
            .assured("the capacity of one bounded reply fits usize");
        let longest = CommandExecutionReference::parse("r".repeat(128))
            .assured("the reference rule admits 128 bytes");
        let generation = WasmStateGeneration::try_from(u64::MAX).assured("a positive generation");
        let mut entries = Vec::with_capacity(capacity);
        for index in 0..capacity {
            let mut fingerprint = [0xff; 32];
            let index = u64::try_from(index).assured("a reply index fits u64");
            fingerprint[..8].copy_from_slice(&index.to_be_bytes());
            entries.push(WasmBranchStateInspection {
                branch: Some(BranchKeyFingerprint::new(fingerprint)),
                checkpoint: Some(WasmCheckpointState {
                    generation,
                    committed_revision: Some(NonZeroU64::MAX),
                    latest_revision: Some(NonZeroU64::MAX),
                    stage: WasmCheckpointStage::LocallyDurable,
                    required_replicas: Some(u32::MAX),
                    confirmed_replicas: Some(u32::MAX),
                }),
                recovery: Some(WasmRecoveryInspection {
                    generation,
                    rejection: WasmSavedStateRejection::ApplicationState,
                    request: longest.clone(),
                    outcome: WasmStateRecoveryOutcome::Recovered,
                }),
            });
        }
        let mut reset = WasmStateReset::publishing(
            longest.clone(),
            WasmStateResetScope::Branch(BranchKeyFingerprint::new([0xff; 32])),
            WasmStateResetReason::RejectedSnapshot,
        );
        reset.mark_ready();
        let reply = ReadWasmStatePageResponse {
            result: Ok(WasmStatePage {
                origin: StateInspectionOrigin {
                    node: ClusterNodeIdentity::new(
                        nervix_models::ClusterNodeName::parse(&"n".repeat(32))
                            .assured("a long node name"),
                        ClusterNodeIncarnation::new(u64::MAX),
                    ),
                    fingerprint: [0xff; 32],
                    generation: u64::MAX,
                    revision: u64::MAX,
                    stage: StateInspectionStage::Live,
                },
                page: WasmUnitStatePage {
                    state: WasmUnitStateInspection {
                        default_generation: generation,
                        reset: Some(WasmStateResetInspection {
                            reset,
                            generation,
                            readiness: WasmStateResetReadiness::AwaitingUsableExecution,
                        }),
                        entries: usize::MAX,
                        checkpoints: WasmCheckpointCounts::default(),
                        recoveries: WasmRecoveryCounts::default(),
                        branches: entries,
                    },
                    more: true,
                },
            }),
        };
        let class = <ReadWasmStatePage as InterconnectRequest>::CLASS;
        runtime.block_on(async {
            reply
                .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                .await
                .assured("a reply of the entry capacity fits the class payload limit");
        });
    }
}
