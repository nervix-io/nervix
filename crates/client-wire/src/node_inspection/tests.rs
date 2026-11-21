//! Whole-value properties and canonical fixtures for typed node inspections.
//!
//! Layer: test harness.
//! - **Owns.** Round trips comparing every bit of the observed nodes and committed records.
//! - **Depends on.** Existing wire fixtures, node record generators and the node codec.
//! - **Must not know.** Cluster observation, placement or VM evaluation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "verifies the node inspection boundary")
)]

use nervix_arbitrary::{Arbitrary, Domain};

use super::*;
use crate::{
    CommandDisposition, Reply, ReplyBody,
    tests::{
        fixtures::{name, request, round_trip_reply},
        samples::command_outcome,
    },
};

pub(crate) fn example() -> NodeInspection {
    let text =
        |value: &str| PropertyText::new(value).assured("the fixture holds valid property text");
    NodeInspection {
        nodes: vec![
            NodeInspectionRow {
                name: name("node-1"),
                live: true,
                voter: true,
                cordoned: false,
                terminating: false,
                record: Some(NodeInspectionRecord {
                    revision: u64::MAX,
                    cpu_count: NonZeroU64::new(24),
                    memory_limit: None,
                    properties: NodeProperties {
                        architecture: text("x86_64"),
                        cpu_model: Detected::Undetected(UndetectedReason::NotReported),
                        gpus: GpuInventory::Devices {
                            devices: GpuDevices::try_from(vec![GpuDevice {
                                model: Detected::Value(text("Example GPU")),
                                memory_bytes: Detected::Value(NonZeroU64::MAX),
                            }])
                            .assured("a single device is in bounds"),
                            driver: Detected::Value(text("550.54.15")),
                        },
                        onnx: OnnxRuntimeDetection::Loaded(OnnxRuntime {
                            version: Detected::Value(text("1.30.0")),
                            providers: Detected::Value(
                                ExecutionProviders::try_from(vec![
                                    text("CUDAExecutionProvider"),
                                    text("CPUExecutionProvider"),
                                ])
                                .assured("the example providers fit the list"),
                            ),
                            cuda: CudaProviderRegistration::Registered,
                        }),
                    },
                    tags: NodeTags::from_assignments(["owner=Data Δ=Ops", "zone=eu"])
                        .assured("the fixture tags are distinct and printable"),
                }),
            },
            NodeInspectionRow {
                name: name("node-2"),
                live: true,
                voter: false,
                cordoned: true,
                terminating: true,
                record: None,
            },
        ],
    }
}

fn round_trip(inspection: NodeInspection, message: String) {
    let mut outcome = command_outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    outcome.message = message;
    outcome.node_inspection = Some(Box::new(inspection));
    let reply = Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(outcome)),
    };
    assert_eq!(round_trip_reply(&reply), reply);
}

#[test]
fn node_inspection_formats_and_detection_states_keep_the_typed_result() {
    let example = example();
    round_trip(example.clone(), "readable node preview".to_string());
    round_trip(example.clone(), node_inspection_json(&example));

    let mut example = example;
    for &reason in NODE_UNDETECTED_REASONS {
        let record = example.nodes[0]
            .record
            .as_mut()
            .assured("the first row has its committed record");
        record.properties.gpus = GpuInventory::Undetected(reason);
        record.properties.onnx = OnnxRuntimeDetection::Undetected(reason);
        round_trip(example.clone(), "undetected".to_string());
    }
    for &cuda in NODE_CUDA_REGISTRATIONS {
        let record = example.nodes[0]
            .record
            .as_mut()
            .assured("the first row has its committed record");
        record.properties.gpus = GpuInventory::NoDevice;
        record.properties.onnx = OnnxRuntimeDetection::Loaded(OnnxRuntime {
            version: Detected::Undetected(UndetectedReason::QueryFailed),
            providers: Detected::Undetected(UndetectedReason::LibraryUnavailable),
            cuda,
        });
        round_trip(example.clone(), "loaded ONNX Runtime".to_string());
    }
}

#[test]
fn bolero_node_inspection_preserves_every_committed_and_observed_fact() {
    bolero::check!().with_type::<[u8; 512]>().for_each(|input| {
        let mut arbitrary = Arbitrary::new(&input[4..], Domain::Vocabulary);
        let count = usize::from(input[0] % 4);
        let mut nodes = Vec::with_capacity(count);
        for (index, flags) in input[1..].iter().copied().take(count).enumerate() {
            let record = if flags & 16 == 0 {
                Some(arbitrary.committed_node_record().into())
            } else {
                None
            };
            nodes.push(NodeInspectionRow {
                name: name(&format!("node-{index}")),
                live: flags & 1 != 0,
                voter: flags & 2 != 0,
                cordoned: flags & 4 != 0,
                terminating: flags & 8 != 0,
                record,
            });
        }
        round_trip(NodeInspection { nodes }, "node inspection".to_string());
    });
}
