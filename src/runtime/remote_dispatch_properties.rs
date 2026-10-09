//! Generated relay batches from the sending runtime's payload to the receiving runtime's rows.
//!
//! Layer: test harness.
//!
//! - **Owns.** The property that a relay batch a node routes to another arrives as the same rows,
//!   branch and watermarks, and the property that a payload whose parts disagree is refused with
//!   the first defect it has before any of its rows becomes a batch.
//! - **Depends on.** The generated batches and their logical oracle, the routed payload the sender
//!   builds, and the receiver's decoding of a payload into rows.
//! - **Must not know.** The transport that carries a payload, which the interconnect's own wire
//!   properties cover, or the relay the rows are dispatched into.

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_interconnect::{RelayDelivery, RelayPayload};
use nervix_models::{
    AckMode, ClusterNodeName, DomainName, RelayName, RemoteAckRegistration, RemoteRuntimeField,
    RemoteRuntimeValue,
};
use nervix_primitives::sync::Arc;

use super::{RemoteRelayDecodeError, Runtime};
use crate::{
    runtime::{
        BranchKey, RecordMetadataColumns,
        relay_batch::RelayRecordBatch,
        relay_boundary::{RemoteRuntimeConsumer, RoutedDelivery, routed_payload},
    },
    runtime_ack::AckSet,
    runtime_schema::{
        RuntimeRecordMetadata,
        generated_batches::{GeneratedDomain, GeneratedSchema, assert_same_batch},
    },
};

/// The bytes one case reads its batch, branch, watermarks and registrations from.
const CASE_BYTES: usize = 2048;

/// One relay batch a node routes to a consumer on another node.
struct RoutedCase {
    schema: GeneratedSchema,
    batch: RelayRecordBatch,
    acks: Vec<Option<RemoteAckRegistration>>,
    delivery: RelayDelivery,
    consumer: RemoteRuntimeConsumer,
    domain: DomainName,
}

impl RoutedCase {
    fn new(arbitrary: &mut Arbitrary<'_>) -> Self {
        // What shapes the case is read before its batch: an ordinary run's few bytes run out while
        // a batch is generated, and a choice read after that takes its first option, which would
        // leave nearly every case unbranched, attached and without a registration.
        let key = BranchKey::generated_scope(arbitrary);
        let mode = arbitrary
            .entropy()
            .pick([AckMode::Attached, AckMode::Detached]);
        let registered_rows = arbitrary.entropy().byte();
        let schema = GeneratedDomain::Arrow.schema(arbitrary);
        let rows = GeneratedDomain::Arrow.batch(arbitrary, &schema);
        let row_count = rows.num_rows();
        let mut low = Vec::with_capacity(row_count);
        let mut high = Vec::with_capacity(row_count);
        let mut acks = Vec::with_capacity(row_count);
        for row in 0..row_count {
            low.push(arbitrary.entropy().any_i64());
            high.push(arbitrary.entropy().any_i64());
            // A generated view holds fewer rows than the mask has bits.
            let registered = (registered_rows >> row) & 1 == 1;
            acks.push(if registered {
                Some(RemoteAckRegistration {
                    ack_id: arbitrary.entropy().any_u64(),
                    registrar: arbitrary.node_identity(),
                })
            } else {
                None
            });
        }
        let batch = RelayRecordBatch {
            key: key.clone(),
            keys: vec![key; row_count],
            batch: Arc::new(schema.runtime_batch(rows)),
            metadata: RecordMetadataColumns::from_nanos(low, high),
            acks: vec![AckSet::empty(); row_count],
        };
        let mut channel_incarnation = [0; 16];
        for byte in &mut channel_incarnation {
            *byte = arbitrary.entropy().byte();
        }
        Self {
            schema,
            batch,
            acks,
            delivery: RelayDelivery {
                channel_incarnation,
                sequence: arbitrary.entropy().any_u64(),
            },
            consumer: RemoteRuntimeConsumer {
                node_id: arbitrary.rule_name::<ClusterNodeName>(),
                relay: arbitrary.rule_name::<RelayName>(),
                mode,
            },
            domain: arbitrary.rule_name::<DomainName>(),
        }
    }

    /// The payload the sending runtime routes this batch to its consumer in.
    async fn payload(&self, runtime: &Runtime) -> RelayPayload {
        let batch_ipc = self
            .batch
            .batch
            .encode_arrow_ipc(runtime.executor())
            .await
            .assured("a bounded batch encodes");
        routed_payload(RoutedDelivery {
            delivery: self.delivery,
            domain: &self.domain,
            consumer: &self.consumer,
            batch: &self.batch,
            batch_ipc,
            acks: self.acks.clone(),
        })
    }
}

/// A branch key's remote form compared exactly: field names, and floats by their bits.
fn exact_key(key: &Option<Vec<RemoteRuntimeField>>) -> Option<Vec<(String, String)>> {
    let fields = key.as_ref()?;
    Some(
        fields
            .iter()
            .map(|field| (field.name.clone(), exact_value(&field.value)))
            .collect(),
    )
}

/// A remote value rendered with every float as its bits, which is how two values that compare
/// equal as floats, such as the two zeros, are told apart.
fn exact_value(value: &RemoteRuntimeValue) -> String {
    match value {
        RemoteRuntimeValue::F32(float) => format!("F32({:#010x})", float.to_bits()),
        RemoteRuntimeValue::F64(float) => format!("F64({:#018x})", float.to_bits()),
        RemoteRuntimeValue::Array(elements) | RemoteRuntimeValue::Vec(elements) => {
            let elements = elements
                .iter()
                .map(|element| format!("{element:?}|{}", exact_element_bits(element)))
                .collect::<Vec<_>>();
            format!("{elements:?}")
        }
        other => format!("{other:?}"),
    }
}

fn exact_element_bits(element: &nervix_models::RemoteRuntimeElementValue) -> String {
    use nervix_models::RemoteRuntimeElementValue as Element;
    match element {
        Element::F32(float) => format!("{:#010x}", float.to_bits()),
        Element::F64(float) => format!("{:#018x}", float.to_bits()),
        Element::Array(elements) | Element::Vec(elements) => elements
            .iter()
            .map(exact_element_bits)
            .collect::<Vec<_>>()
            .join(","),
        _ => String::new(),
    }
}

/// A relay batch a node routes to a consumer on another node arrives there as exactly that batch:
/// the receiver decodes the routed payload into the same rows of the relay's exact schema, every
/// value and null with floats by their bits, the same concrete branch or the absence of one, every
/// row's watermarks, and one acknowledgement registration per row exactly as the sender
/// registered them.
#[test]
fn bolero_routed_batches_arrive_with_their_rows_branch_and_watermarks() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens");
    bolero::check!()
        .with_iterations(128)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let receiver = Runtime::new();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let case = RoutedCase::new(&mut arbitrary);
            runtime.block_on(async {
                let mut payload = case.payload(&receiver).await;
                assert_eq!(payload.delivery, case.delivery);
                assert_eq!(payload.domain, case.domain);
                assert_eq!(payload.relay, case.consumer.relay);
                assert_eq!(payload.acks, case.acks, "every row keeps its registration");
                let sent_key = exact_key(&payload.key);
                let decoded = receiver
                    .decode_remote_rows(&case.schema.compiled, &mut payload)
                    .await
                    .assured("a routed payload decodes on the receiver");
                assert_same_batch(decoded.batch.batch(), case.batch.batch.batch());
                assert_eq!(
                    decoded.key.as_ref().map(BranchKey::as_str),
                    case.batch.key.as_ref().map(BranchKey::as_str),
                    "the concrete branch, or its absence, is kept"
                );
                assert_eq!(
                    exact_key(&BranchKey::to_remote_key(&decoded.key)),
                    sent_key,
                    "the branch key's values are kept to the bit"
                );
                let watermarks = RecordMetadataColumns::from_remote(payload.metadata.clone());
                assert_eq!(
                    watermarks.to_remote(),
                    case.batch.metadata.to_remote(),
                    "every row's watermarks are kept"
                );
                let rows = RelayRecordBatch::from_runtime_batch(
                    case.schema.compiled.clone(),
                    decoded.key,
                    decoded.batch,
                    watermarks,
                    vec![AckSet::empty(); case.acks.len()],
                )
                .assured("decoded rows and their sidecars form one relay batch");
                assert_eq!(rows.record_batch().num_rows(), case.acks.len());
            });
        });
}

/// One way a routed payload's parts can disagree with each other.
#[derive(Debug, Clone, Copy)]
enum PayloadDefect {
    /// The body carries another schema's batch.
    ForeignBody,
    /// One watermark entry too many or too few.
    MetadataCount,
    /// One registration too many or too few.
    AckCount,
    /// The branch key names no field.
    EmptyKey,
    /// A branch key field is not a field name.
    KeyFieldName,
    /// A branch key datetime is not RFC 3339 text.
    KeyDatetime,
    /// A branch key float is not finite.
    KeyFloat,
}

impl PayloadDefect {
    const ALL: [Self; 7] = [
        Self::ForeignBody,
        Self::MetadataCount,
        Self::AckCount,
        Self::EmptyKey,
        Self::KeyFieldName,
        Self::KeyDatetime,
        Self::KeyFloat,
    ];

    /// The defect the receiver names first for a payload with this one defect: it decodes the
    /// body, then counts the watermarks and the registrations, then reads the branch key.
    fn expected(self, metadata: usize, acks: usize, rows: usize) -> RemoteRelayDecodeError {
        match self {
            Self::ForeignBody => RemoteRelayDecodeError::Body,
            Self::MetadataCount => RemoteRelayDecodeError::MetadataCount { metadata, rows },
            Self::AckCount => RemoteRelayDecodeError::AckCount { acks, rows },
            Self::EmptyKey | Self::KeyFieldName | Self::KeyDatetime | Self::KeyFloat => {
                RemoteRelayDecodeError::BranchKey
            }
        }
    }
}

/// A routed payload whose body, watermarks, registrations and branch key do not describe one batch
/// of its relay is refused with the first of its defects, in the order the receiver checks them,
/// before any of its rows becomes a batch.
#[test]
fn bolero_routed_payloads_whose_parts_disagree_are_refused_with_their_defect() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens");
    bolero::check!()
        .with_iterations(128)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let receiver = Runtime::new();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // The defect is read before the cases: an ordinary run's few bytes run out while a
            // batch is generated, and a choice read after that takes its first option.
            let defect = arbitrary.entropy().pick(PayloadDefect::ALL);
            let grow = arbitrary.entropy().flag();
            let case = RoutedCase::new(&mut arbitrary);
            let foreign = RoutedCase::new(&mut arbitrary);
            runtime.block_on(async {
                let mut payload = case.payload(&receiver).await;
                let rows = case.acks.len();
                match defect {
                    PayloadDefect::ForeignBody => {
                        if foreign.schema.compiled.arrow_schema()
                            == case.schema.compiled.arrow_schema()
                        {
                            return;
                        }
                        payload.batch_ipc = foreign.payload(&receiver).await.batch_ipc;
                    }
                    PayloadDefect::MetadataCount => {
                        if grow || payload.metadata.is_empty() {
                            payload
                                .metadata
                                .push(RuntimeRecordMetadata::test().to_remote());
                        } else {
                            payload.metadata.pop();
                        }
                    }
                    PayloadDefect::AckCount => {
                        if grow || payload.acks.is_empty() {
                            payload.acks.push(None);
                        } else {
                            payload.acks.pop();
                        }
                    }
                    PayloadDefect::EmptyKey => payload.key = Some(Vec::new()),
                    PayloadDefect::KeyFieldName => {
                        payload.key = Some(vec![RemoteRuntimeField {
                            name: "not a field name".to_string(),
                            value: RemoteRuntimeValue::Bool(true),
                        }]);
                    }
                    PayloadDefect::KeyDatetime => {
                        payload.key = Some(vec![RemoteRuntimeField {
                            name: "at".to_string(),
                            value: RemoteRuntimeValue::Datetime("yesterday".to_string()),
                        }]);
                    }
                    PayloadDefect::KeyFloat => {
                        payload.key = Some(vec![RemoteRuntimeField {
                            name: "ratio".to_string(),
                            value: RemoteRuntimeValue::F64(f64::NAN),
                        }]);
                    }
                }
                check_refusal(&receiver, &case, payload, defect, rows).await;
            });
        });
}

async fn check_refusal(
    receiver: &Runtime,
    case: &RoutedCase,
    mut payload: RelayPayload,
    defect: PayloadDefect,
    rows: usize,
) {
    let metadata = payload.metadata.len();
    let acks = payload.acks.len();
    let refused = receiver
        .decode_remote_rows(&case.schema.compiled, &mut payload)
        .await;
    let Err(refused) = refused else {
        panic!("a payload with a {defect:?} defect is refused");
    };
    assert_eq!(
        refused.current_context(),
        &defect.expected(metadata, acks, rows),
        "the receiver names the payload's defect: {refused:?}"
    );
}
