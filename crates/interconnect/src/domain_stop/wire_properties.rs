//! A domain stop's drain and report messages through the bounded production wire codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current drain requests of every action, node views with entries of every
//!   kind, report requests and every report answer, and their full-value oracles.
//! - **Depends on.** The stop wire contracts, typed identities and the bounded executor.
//! - **Must not know.** How a node classifies its work or how the stop's owner decides.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_execution::Executor;
use nervix_models::{ClusterNodeName, ModelKind, ModelName, NodeRef, StopCutKind};

use super::*;
use crate::{
    RemoteOperationSubject,
    backup::capture_properties::{assert_round_trip, property_runtime},
};

const WAIT_KINDS: [StopWaitKind; 12] = [
    StopWaitKind::AdmittingIngestor,
    StopWaitKind::ActiveGenerator,
    StopWaitKind::NodeWork,
    StopWaitKind::RelayBatches,
    StopWaitKind::AdmittingRelay,
    StopWaitKind::EmitterBuffer,
    StopWaitKind::PublishOnTheWire,
    StopWaitKind::RequiredWait,
    StopWaitKind::EmitterRetry,
    StopWaitKind::RelayBatchesHeld,
    StopWaitKind::NodeWorkHeld,
    StopWaitKind::ProcessorHeld,
];

const CUT_KINDS: [StopCutKind; 10] = [
    StopCutKind::Enforced,
    StopCutKind::WindowAcknowledgements,
    StopCutKind::ReordererRows,
    StopCutKind::CorrelatorRecords,
    StopCutKind::RequiredWaitMessages,
    StopCutKind::EmitterUndelivered,
    StopCutKind::BranchLimitEntry,
    StopCutKind::RelayBatches,
    StopCutKind::NodeBatches,
    StopCutKind::IngestorPauseBuffer,
];

const ENTITY_KINDS: [ModelKind; 4] = [
    ModelKind::Emitter,
    ModelKind::Reorderer,
    ModelKind::Relay,
    ModelKind::Ingestor,
];

#[derive(Debug, bolero::TypeGenerator)]
struct StopWireCase {
    node: u16,
    epoch: u64,
    fence: u64,
    start_version: u64,
    counters: [u64; 3],
    items: [u64; 12],
    entity: u8,
}

impl StopWireCase {
    fn coordination(&self) -> CoordinationIdentity {
        let node = ClusterNodeName::parse(&format!("node-{}", self.node))
            .assured("a generated decimal suffix makes a valid node name");
        CoordinationIdentity::new(node, self.epoch.max(1), self.fence)
    }

    fn domain(&self) -> DomainName {
        DomainName::parse(&format!("stopping_{}", self.node))
            .assured("a generated decimal suffix makes a valid domain name")
    }

    fn entity(&self, index: usize) -> NodeRef {
        let position = usize::from(self.entity)
            .checked_add(index)
            .assured("a byte and an index below twelve fit usize");
        let kind = ENTITY_KINDS[position % ENTITY_KINDS.len()];
        let name = ModelName::parse(&format!("entity_{}_{index}", self.entity))
            .assured("a generated decimal suffix makes a valid model name");
        NodeRef::new(kind, name)
    }

    /// A view with one entry of every kind, each for an entity or, for the first, the whole
    /// domain, with the case's counts.
    fn view(&self, items: [u64; 12], counters: [u64; 3]) -> StopDrainStatus {
        let mut entries = Vec::new();
        for (index, kind) in WAIT_KINDS.into_iter().enumerate() {
            entries.push(StopWaitEntry {
                entity: (index != 0).then(|| self.entity(index)),
                kind,
                items: items[index],
            });
        }
        StopDrainStatus {
            entries,
            flush_obligations: counters[0],
            motion: counters[1],
        }
    }

    fn cuts(&self) -> Vec<StopCut> {
        let mut cuts = Vec::new();
        for (index, kind) in CUT_KINDS.into_iter().enumerate() {
            let entity = self.entity(index);
            cuts.push(StopCut {
                entity_kind: entity.kind,
                entity: entity.identifier,
                kind,
                items: self.items[index],
            });
        }
        cuts
    }

    fn failures(&self) -> [RemoteOperationFailure; 4] {
        let subject = RemoteOperationSubject::domain(&self.domain());
        [
            RemoteOperationFailure::rejected(subject.clone()),
            RemoteOperationFailure::unavailable(subject.clone()),
            RemoteOperationFailure::not_ready(subject.clone()),
            RemoteOperationFailure::failed(subject, "the stop's owner is not the leader".to_string()),
        ]
    }
}

#[test]
fn bolero_domain_stop_messages_round_trip() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .with_type::<StopWireCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                let drain_class = StopDrainRequest::CLASS;
                for action in [
                    StopDrainAction::FlushMovingWork,
                    StopDrainAction::Confirm,
                    StopDrainAction::Observe,
                ] {
                    nervix_primitives::task::consume_budget().await;
                    let request = StopDrainRequest {
                        coordination: case.coordination(),
                        domain: case.domain(),
                        action,
                    };
                    assert_round_trip(&executor, drain_class, request).await;
                }
                assert_round_trip(
                    &executor,
                    drain_class,
                    Ok::<_, RemoteOperationFailure>(StopDrainStatus::default()),
                )
                .await;
                for (items, counters) in [
                    ([0; 12], [0; 3]),
                    ([u64::MAX; 12], [u64::MAX; 3]),
                    (case.items, case.counters),
                ] {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(
                        &executor,
                        drain_class,
                        Ok::<_, RemoteOperationFailure>(case.view(items, counters)),
                    )
                    .await;
                }
                let report_class = StopReportRequest::CLASS;
                for start_version in [0, u64::MAX, case.start_version] {
                    nervix_primitives::task::consume_budget().await;
                    let request = StopReportRequest {
                        coordination: case.coordination(),
                        domain: case.domain(),
                        start_version,
                    };
                    assert_round_trip(&executor, report_class, request).await;
                }
                for answer in [
                    StopReportAnswer::Pending,
                    StopReportAnswer::Unknown,
                    StopReportAnswer::Reported(Vec::new()),
                    StopReportAnswer::Reported(case.cuts()),
                ] {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(
                        &executor,
                        report_class,
                        Ok::<_, RemoteOperationFailure>(answer),
                    )
                    .await;
                }
                for failure in case.failures() {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(
                        &executor,
                        drain_class,
                        Err::<StopDrainStatus, _>(failure.clone()),
                    )
                    .await;
                    assert_round_trip(
                        &executor,
                        report_class,
                        Err::<StopReportAnswer, _>(failure),
                    )
                    .await;
                }
            });
        });
}
