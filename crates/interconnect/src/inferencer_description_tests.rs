//! Layer: harness.
//! Owns: complete round trips of current bounded inference observation requests and replies.
//! Depends on: the production bounded rkyv transport codec and typed observation vocabulary.
//! Must not know: runtime tensors, graph execution or historical wire shapes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks complete inference summary representations")
)]

use meticulous::ResultExt as _;
use nervix_execution::Executor;
use nervix_models::{InferencerModelFingerprint, InferencerReplicaProgress, SchemaFingerprint};

use super::*;
use crate::{RemoteOperationSubject, request::RkyvMessage};

#[test]
fn inferencer_description_uses_the_configured_request_budget() {
    let budget = std::time::Duration::from_secs(37);
    assert_eq!(
        DescribeInferencerRequest::DEADLINE.resolve(budget, crate::InterconnectLiveness::default()),
        budget,
    );
}

#[derive(Debug, bolero::TypeGenerator)]
struct DescriptionCase {
    identity: u8,
    partition: Option<u32>,
    schema: [u8; 32],
    model: [u8; 32],
    stateful: bool,
    branches: u8,
    width: u8,
    publication: bool,
    published: u64,
    persisted: u64,
    replica_count: u8,
    replica_offset: u8,
    confirmed: [bool; 3],
    replicated: [u64; 3],
    refusal: bool,
}

#[test]
fn bolero_inferencer_descriptions_round_trip_every_field_through_the_wire() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary codec test runtime builds");
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .with_type::<DescriptionCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                let unit = match case.partition {
                    Some(partition) => Unit::partition(partition),
                    None => Unit::SINGLE,
                };
                let request = DescribeInferencerRequest {
                    domain: format!("domain_{:02x}", case.identity)
                        .parse()
                        .assured("the generated domain is valid"),
                    name: format!("scoring_{:02x}", case.identity)
                        .parse()
                        .assured("the generated inferencer is valid"),
                    unit,
                    state: case.stateful.then_some(RuntimeState::Inferencer {
                        schema: SchemaFingerprint::from_digest(case.schema),
                        model: InferencerModelFingerprint::from_digest(case.model),
                    }),
                };
                let branches = if case.stateful {
                    u64::from(case.branches % 5)
                } else {
                    0
                };
                let published = if case.stateful && (case.publication || branches > 0) {
                    Some(case.published.max(u64::from(branches > 0)))
                } else {
                    None
                };
                let replica_count = if case.stateful {
                    usize::from(case.replica_count % 4)
                } else {
                    0
                };
                let mut replicas = Vec::with_capacity(replica_count);
                for index in 0..replica_count {
                    let node = (usize::from(case.replica_offset) + index) % 3;
                    let replicated_revision = if case.confirmed[node] {
                        published.map(|revision| case.replicated[node].min(revision))
                    } else {
                        None
                    };
                    replicas.push(InferencerReplicaProgress {
                        node: format!("node-{node}")
                            .parse()
                            .assured("the generated replica is valid"),
                        replicated_revision,
                    });
                }
                let summary = InferencerStateSummary {
                    branches,
                    state_bytes: branches * (u64::from(case.width % 8) + 1) * 4,
                    published_revision: published,
                    persisted_revision: published.map(|revision| case.persisted.min(revision)),
                    replicas,
                };
                let json = serde_json::to_vec(&summary)
                    .assured("the bounded typed summary encodes as JSON");
                let decoded: InferencerStateSummary =
                    serde_json::from_slice(&json).assured("the typed summary decodes from JSON");
                assert_eq!(decoded, summary);
                let response = if case.refusal {
                    Err(RemoteOperationFailure::Rejected {
                        subject: RemoteOperationSubject::entity(
                            &request.domain,
                            nervix_models::ModelKind::Inferencer,
                            request.name.clone(),
                        ),
                    })
                } else {
                    Ok(summary)
                };
                let class = DescribeInferencerRequest::CLASS;
                let (encoded, _charge) = request
                    .clone()
                    .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                    .await
                    .assured("a bounded current inference request encodes");
                let (decoded, _charge) =
                    DescribeInferencerRequest::decode_rkyv(executor.clone(), class, encoded)
                        .await
                        .assured("the inference request decodes");
                assert_eq!(decoded, request);
                let (encoded, _charge) = response
                    .clone()
                    .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                    .await
                    .assured("a bounded current inference response encodes");
                let (decoded, _charge) =
                    <Result<InferencerStateSummary, RemoteOperationFailure>>::decode_rkyv(
                        executor.clone(),
                        class,
                        encoded,
                    )
                    .await
                    .assured("the inference response decodes");
                assert_eq!(decoded, response);
            });
        });
}
