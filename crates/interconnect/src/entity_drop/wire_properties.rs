//! The drop report's request and answers through the bounded production wire codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current report requests, every report answer with discarded work of every
//!   kind for entities of several kinds, every refusal, and their full-value oracles.
//! - **Depends on.** The drop report wire contract, typed identities and the bounded executor.
//! - **Must not know.** How a node destroys an entity or what the coordinator answers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::Executor;
use nervix_models::{ClusterNodeName, DiscardedWorkKind, ModelKind, ModelName};

use super::*;
use crate::{
    RemoteOperationSubject,
    backup::capture_properties::{assert_round_trip, property_runtime},
};

const DISCARDED_KINDS: [DiscardedWorkKind; 8] = [
    DiscardedWorkKind::UndeliveredRecords,
    DiscardedWorkKind::HeldRows,
    DiscardedWorkKind::WindowRows,
    DiscardedWorkKind::WaitingRecords,
    DiscardedWorkKind::WaitingMessages,
    DiscardedWorkKind::RetainedPayloads,
    DiscardedWorkKind::RelayMessages,
    DiscardedWorkKind::UnprocessedMessages,
];

const ENTITY_KINDS: [ModelKind; 6] = [
    ModelKind::Emitter,
    ModelKind::Reorderer,
    ModelKind::WindowProcessor,
    ModelKind::Correlator,
    ModelKind::Relay,
    ModelKind::Ingestor,
];

#[derive(Debug, bolero::TypeGenerator)]
struct DropReportWireCase {
    node: u16,
    epoch: u64,
    fence: u64,
    revision: u64,
    items: [u64; 8],
    entity: u8,
}

impl DropReportWireCase {
    fn coordination(&self) -> CoordinationIdentity {
        let node = ClusterNodeName::parse(&format!("node-{}", self.node))
            .assured("a generated decimal suffix makes a valid node name");
        CoordinationIdentity::new(node, self.epoch.max(1), self.fence)
    }

    fn domain(&self) -> DomainName {
        DomainName::parse(&format!("dropping_{}", self.node))
            .assured("a generated decimal suffix makes a valid domain name")
    }

    /// One discarded count of every kind, each for an entity of a kind the case rotates through.
    fn discarded(&self) -> Vec<DiscardedWork> {
        let mut discarded = Vec::new();
        for (index, kind) in DISCARDED_KINDS.into_iter().enumerate() {
            let position = usize::from(self.entity)
                .checked_add(index)
                .assured("a byte and an index below eight fit usize");
            let entity_kind = ENTITY_KINDS[position % ENTITY_KINDS.len()];
            let entity = ModelName::parse(&format!("entity_{}_{index}", self.entity))
                .assured("a generated decimal suffix makes a valid model name");
            discarded.push(DiscardedWork {
                entity_kind,
                entity,
                kind,
                items: self.items[index],
            });
        }
        discarded
    }

    fn failures(&self) -> [RemoteOperationFailure; 4] {
        let subject = RemoteOperationSubject::domain(&self.domain());
        [
            RemoteOperationFailure::rejected(subject.clone()),
            RemoteOperationFailure::unavailable(subject.clone()),
            RemoteOperationFailure::not_ready(subject.clone()),
            RemoteOperationFailure::failed(
                subject,
                "the drop report was asked by a node that is not the leader".to_string(),
            ),
        ]
    }
}

#[test]
fn bolero_entity_drop_report_round_trip() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .with_type::<DropReportWireCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                let class = DropReportRequest::CLASS;
                for revision in [0, u64::MAX, case.revision] {
                    nervix_primitives::task::consume_budget().await;
                    let request = DropReportRequest {
                        coordination: case.coordination(),
                        domain: case.domain(),
                        revision,
                    };
                    assert_round_trip(&executor, class, request).await;
                }
                let mut full_width = case.discarded();
                for discarded in &mut full_width {
                    discarded.items = u64::MAX;
                }
                for answer in [
                    DropReportAnswer::Pending,
                    DropReportAnswer::Unknown,
                    DropReportAnswer::Reported(Vec::new()),
                    DropReportAnswer::Reported(case.discarded()),
                    DropReportAnswer::Reported(full_width.clone()),
                ] {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(&executor, class, Ok::<_, RemoteOperationFailure>(answer))
                        .await;
                }
                for failure in case.failures() {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(&executor, class, Err::<DropReportAnswer, _>(failure)).await;
                }
            });
        });
}
