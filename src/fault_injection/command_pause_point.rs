//! Typed identities of the command boundaries controlled by the server test harness.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The private key identifying each command pause in the shared fault owner.
//! - **Depends on.** Vocabulary identities and the harness's typed boundary selectors.
//! - **Must not know.** Command execution, expected outcomes or graph configuration.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns private keys of testing-only command pauses")
)]

use nervix_models::{
    BranchKeyFingerprint, ClusterNodeName, CommandExecutionReference, DomainName, DomainNodeRef,
    RestoreStep,
};

use super::{DomainResumeBoundary, StandaloneTransactionBoundary, StartPlacementBoundary};

/// The command boundary a test controls without racing an election against a request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum CommandPausePoint {
    ExplicitCommitFailure {
        node: ClusterNodeName,
        transaction_id: String,
    },
    TransactionCleanup {
        node: ClusterNodeName,
        domain: DomainName,
    },
    StandaloneTransaction {
        node: ClusterNodeName,
        domain: DomainName,
        boundary: StandaloneTransactionBoundary,
    },
    DomainResume {
        node: ClusterNodeName,
        domain: DomainName,
        boundary: DomainResumeBoundary,
    },
    StartPlacement {
        node: ClusterNodeName,
        domain: DomainName,
        boundary: StartPlacementBoundary,
    },
    Admission(ClusterNodeName),
    ClientInspectionAdmission(DomainName),
    RuntimeInstallation(ClusterNodeName),
    BackgroundReconciliation(ClusterNodeName),
    DomainTeardown(DomainName),
    InitialClusterParameters(ClusterNodeName),
    ForcedRecoveryMissingSchedule(ClusterNodeName),
    RuntimePreparation(ClusterNodeName),
    RuntimeReadiness {
        node: ClusterNodeName,
        domain: DomainName,
    },
    ApplicationRevisionResponse(ClusterNodeName),
    ApplicationProgressReport(ClusterNodeName),
    ReferenceLookup(ClusterNodeName),
    DurableAdmission(ClusterNodeName),
    Reconciliation {
        node: ClusterNodeName,
        reference: CommandExecutionReference,
    },
    Finalization(ClusterNodeName),
    RelocationPublication(DomainName),
    ResponseDelivery(ClusterNodeName),
    SubscriptionResponseDelivery(ClusterNodeName),
    ResourceInstallation(ClusterNodeName),
    ResourceSourceDiscovery(ClusterNodeName),
    RestoreStep {
        node_id: ClusterNodeName,
        step: RestoreStep,
    },
    BackupDownload(ClusterNodeName),
    MaterializedSnapshotOpening(DomainNodeRef),
    RestoreStatePublication {
        domain: DomainName,
        coordinator: ClusterNodeName,
    },
    RestoreBranchStateConversion(DomainName),
    IngestorRoutePoll(DomainNodeRef),
    ProcessorInputBeforePoll(DomainNodeRef),
    ProcessorInputAfterPoll(DomainNodeRef),
    IngestorBranchBuffer(DomainNodeRef, BranchKeyFingerprint),
    OwnershipHandoffFlush(DomainName),
    ProcessorFlushCompletion(DomainNodeRef),
    ProcessorPartitionInstall(DomainNodeRef),
    ProcessorPartitionForget(DomainNodeRef),
    ProcessorBranchRestore(DomainNodeRef),
    MaterializedRowRelease(DomainNodeRef),
    MaterializedRowTake(DomainNodeRef),
    MaterializedCheckpointSeal(DomainNodeRef),
    OwnershipHandoffActivation(DomainName),
    GracefulDrain(ClusterNodeName),
    TransactionCommit {
        node_id: ClusterNodeName,
        domain: String,
        completed_statements: usize,
    },
}
