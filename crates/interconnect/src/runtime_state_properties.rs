//! Complete runtime-state messages through the production bounded interconnect codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded request/reply generation, exact value checks and damaged-message checks.
//! - **Depends on.** Current message types, the codec, executor and vocabulary generators.
//! - **Must not know.** Runtime installation, checkpoint contents or connection scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests current runtime-state wire messages")
)]

use std::fmt::Debug;

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain, assert_remote_keys_eq};
use nervix_execution::Executor;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, CoordinationIdentity, ModelName, NodeRef,
    SchemaFingerprint, WasmStateGeneration,
};

use super::*;
use crate::{
    CaptureOwnershipHandoffStateRequest, OwnershipHandoffFailure, OwnershipHandoffResponse,
    PrepareOwnershipHandoffStateRequest, RemoteOperationSubject, TransportError,
    request::RkyvMessage,
};

const CASE_BYTES: usize = 2048;
const MESSAGE_KINDS: usize = <Message as strum::EnumCount>::COUNT;
const _: () = assert!(MESSAGE_KINDS > 0);

#[derive(strum::EnumCount)]
enum Message {
    ListingRequest(BranchCheckpointListingRequest),
    ListingResponse(BranchCheckpointListingResponse),
    Fetch(FetchStateCheckpoint),
    KafkaSync(SyncKafkaOffsets),
    KafkaDescribe(DescribeKafkaOffsets),
    KafkaResponse(Result<KafkaOffsetRevision, RemoteOperationFailure>),
    SyncRequest(StateSyncRequest),
    SyncResponse(StateSyncResponse),
    Ack(StateReplicationAck),
    Available(StateCheckpointAvailable),
    Prepare(PrepareOwnershipHandoffStateRequest),
    Capture(CaptureOwnershipHandoffStateRequest),
    CaptureResponse(OwnershipHandoffResponse<Vec<OwnershipHandoffCheckpoint>>),
}

struct Messages<'a> {
    arbitrary: Arbitrary<'a>,
}

impl<'a> Messages<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            arbitrary: Arbitrary::new(bytes, Domain::Vocabulary),
        }
    }

    fn number(&mut self) -> u64 {
        self.arbitrary.entropy().any_u64()
    }

    fn digest(&mut self) -> [u8; 32] {
        std::array::from_fn(|_| self.arbitrary.entropy().byte())
    }

    fn state(&mut self) -> RuntimeState {
        let variant = self.arbitrary.entropy().byte() % 8;
        let schema = SchemaFingerprint::from_digest(self.digest());
        match variant {
            0 => RuntimeState::BranchAggregated,
            1 => RuntimeState::Correlator { schema },
            2 => RuntimeState::Deduplicator { schema },
            3 => RuntimeState::KafkaOffset,
            4 => RuntimeState::MaterializedRelay { schema },
            5 => {
                let generation = self.arbitrary.entropy().boundary_biased(1..=u64::MAX);
                RuntimeState::WasmProcessor {
                    schema,
                    generation: WasmStateGeneration::try_from(generation)
                        .assured("the generated generation is nonzero"),
                }
            }
            6 => RuntimeState::WindowProcessor { schema },
            _ => RuntimeState::BranchLru { schema },
        }
    }

    fn unit(&mut self) -> nervix_models::Unit {
        match self.arbitrary.entropy().byte() % 3 {
            0 => nervix_models::Unit::Single,
            variant => {
                let ordinal = u32::try_from(
                    self.arbitrary
                        .entropy()
                        .boundary_biased(0..=u64::from(u32::MAX)),
                )
                .assured("the generated unit ordinal is bounded by u32::MAX");
                if variant == 1 {
                    nervix_models::Unit::Partition(ordinal)
                } else {
                    nervix_models::Unit::Instance(ordinal)
                }
            }
        }
    }

    fn placement(&mut self) -> StatePlacementEnvelope {
        let unit = self.unit();
        StatePlacementEnvelope {
            domain: self.arbitrary.rule_name(),
            state: self.state(),
            kind: self.arbitrary.model_kind(),
            identifier: self.arbitrary.rule_name::<ModelName>(),
            unit,
            branch_key: self.arbitrary.remote_branch_key(),
        }
    }

    fn snapshot(&mut self) -> StateSnapshotEnvelope {
        StateSnapshotEnvelope {
            lsm: self.number(),
            length: self.number(),
            digest: self.digest(),
        }
    }

    fn cursor(&mut self) -> BranchCheckpointCursor {
        BranchCheckpointCursor {
            epoch: self.number(),
            sequence: self.number(),
        }
    }

    fn failure(&mut self) -> RemoteOperationFailure {
        let class = self.arbitrary.entropy().byte() % 4;
        let subject = match self.arbitrary.entropy().byte() % 4 {
            0 => RemoteOperationSubject::domain(&self.arbitrary.rule_name()),
            1 => {
                let domain = self.arbitrary.rule_name();
                let kind = self.arbitrary.model_kind();
                let name = self.arbitrary.rule_name::<ModelName>();
                RemoteOperationSubject::entity(&domain, kind, name)
            }
            2 => RemoteOperationSubject::state(&self.placement()),
            _ => RemoteOperationSubject::subscription_interest(
                &self.arbitrary.rule_name(),
                &self.arbitrary.rule_name(),
                &ClusterNodeIdentity::new(
                    self.arbitrary.cluster_node(),
                    ClusterNodeIncarnation::new(self.number()),
                ),
            ),
        };
        match class {
            0 => RemoteOperationFailure::rejected(subject),
            1 => RemoteOperationFailure::unavailable(subject),
            2 => RemoteOperationFailure::not_ready(subject),
            _ => RemoteOperationFailure::failed(subject, self.arbitrary.string()),
        }
    }

    fn listing(&mut self) -> BranchCheckpointListing {
        let variant = self.arbitrary.entropy().byte() % 3;
        if variant == 0 {
            return BranchCheckpointListing::Absent;
        }
        let cursor = self.cursor();
        let count = self.arbitrary.entropy().count(4);
        let mut removed = Vec::with_capacity(count);
        for _ in 0..count {
            removed.push(self.arbitrary.remote_branch_key());
        }
        let count = self.arbitrary.entropy().count(4);
        let mut revised = Vec::with_capacity(count);
        for _ in 0..count {
            revised.push(BranchCheckpointRevision {
                branch_key: self.arbitrary.remote_branch_key(),
                state: self.state(),
                lsm: self.number(),
            });
        }
        let page = BranchCheckpointPage {
            cursor,
            removed,
            revised,
            more: self.arbitrary.entropy().flag(),
        };
        if variant == 1 {
            BranchCheckpointListing::Restarted(page)
        } else {
            BranchCheckpointListing::Continued(page)
        }
    }

    fn checkpoints(&mut self) -> Vec<OwnershipHandoffCheckpoint> {
        let count = self.arbitrary.entropy().count(4);
        let mut checkpoints = Vec::with_capacity(count);
        for _ in 0..count {
            checkpoints.push(OwnershipHandoffCheckpoint {
                placement: self.placement(),
                snapshot: self.snapshot(),
            });
        }
        checkpoints
    }

    fn draw(&mut self, kind: usize) -> Message {
        match kind {
            0 => {
                let after = if self.arbitrary.entropy().flag() {
                    Some(self.cursor())
                } else {
                    None
                };
                Message::ListingRequest(BranchCheckpointListingRequest {
                    lifecycle: self.placement(),
                    after,
                })
            }
            1 => {
                let result = if self.arbitrary.entropy().flag() {
                    Ok(self.listing())
                } else {
                    Err(self.failure())
                };
                Message::ListingResponse(BranchCheckpointListingResponse { result })
            }
            2 => {
                let read = self.arbitrary.entropy().pick([
                    StateCheckpointRead::Published,
                    StateCheckpointRead::HandoffCapture,
                ]);
                Message::Fetch(FetchStateCheckpoint {
                    placement: self.placement(),
                    lsm: self.number(),
                    read,
                })
            }
            3 => {
                let after_lsm = if self.arbitrary.entropy().flag() {
                    Some(self.number())
                } else {
                    None
                };
                Message::KafkaSync(SyncKafkaOffsets {
                    placement: self.placement(),
                    after_lsm,
                })
            }
            4 => Message::KafkaDescribe(DescribeKafkaOffsets {
                placement: self.placement(),
                after_lsm: self.number(),
            }),
            5 => {
                let result = match self.arbitrary.entropy().byte() % 3 {
                    0 => Ok(KafkaOffsetRevision::Current),
                    1 => Ok(KafkaOffsetRevision::Advanced(self.number())),
                    _ => Err(self.failure()),
                };
                Message::KafkaResponse(result)
            }
            6 => {
                let after_lsm = if self.arbitrary.entropy().flag() {
                    Some(self.number())
                } else {
                    None
                };
                Message::SyncRequest(StateSyncRequest {
                    placement: self.placement(),
                    after_lsm,
                })
            }
            7 => {
                let result = match self.arbitrary.entropy().byte() % 3 {
                    0 => Ok(None),
                    1 => Ok(Some(self.snapshot())),
                    _ => Err(self.failure()),
                };
                Message::SyncResponse(StateSyncResponse { result })
            }
            8 => Message::Ack(StateReplicationAck {
                placement: self.placement(),
                lsm: self.number(),
            }),
            9 => Message::Available(StateCheckpointAvailable {
                placement: self.placement(),
                lsm: self.number(),
            }),
            10 => {
                let coordination = CoordinationIdentity::new(
                    self.arbitrary.cluster_node(),
                    self.number(),
                    self.number(),
                );
                Message::Prepare(PrepareOwnershipHandoffStateRequest {
                    coordination,
                    operation_id: self.arbitrary.string(),
                    source: self.arbitrary.cluster_node(),
                    destination: self.arbitrary.cluster_node(),
                    source_incarnation: ClusterNodeIncarnation::new(self.number()),
                    destination_incarnation: ClusterNodeIncarnation::new(self.number()),
                    domain: self.arbitrary.rule_name(),
                    unit: self.unit(),
                    entity: NodeRef::new(
                        self.arbitrary.model_kind(),
                        self.arbitrary.rule_name::<ModelName>(),
                    ),
                    base_schedule_fingerprint: self.digest(),
                    target_schedule_fingerprint: self.digest(),
                    checkpoints: self.checkpoints(),
                })
            }
            11 => {
                let coordination = CoordinationIdentity::new(
                    self.arbitrary.cluster_node(),
                    self.number(),
                    self.number(),
                );
                Message::Capture(CaptureOwnershipHandoffStateRequest {
                    coordination,
                    operation_id: self.arbitrary.string(),
                    source: self.arbitrary.cluster_node(),
                    source_incarnation: ClusterNodeIncarnation::new(self.number()),
                    domain: self.arbitrary.rule_name(),
                    unit: self.unit(),
                    entity: NodeRef::new(
                        self.arbitrary.model_kind(),
                        self.arbitrary.rule_name::<ModelName>(),
                    ),
                    base_schedule_fingerprint: self.digest(),
                })
            }
            _ => {
                let response = if self.arbitrary.entropy().flag() {
                    Ok(self.checkpoints())
                } else {
                    Err(OwnershipHandoffFailure::rejected(self.arbitrary.string()))
                };
                Message::CaptureResponse(response)
            }
        }
    }
}

async fn round_trip<M: RkyvMessage + PartialEq + Clone + Debug>(
    executor: &Executor,
    class: PoolClass,
    expected: &M,
) -> M {
    let (encoded, _encoded_charge) = expected
        .clone()
        .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
        .await
        .assured("bounded runtime-state messages fit their owning class");
    let (decoded, _decoded_charge) = M::decode_rkyv(executor.clone(), class, encoded)
        .await
        .assured("a current encoded message decodes");
    assert_eq!(
        &decoded, expected,
        "every field and response variant survives the production wire"
    );
    decoded
}

fn assert_listing_keys(
    actual: &BranchCheckpointListingResponse,
    expected: &BranchCheckpointListingResponse,
) {
    if let (
        Ok(BranchCheckpointListing::Restarted(a) | BranchCheckpointListing::Continued(a)),
        Ok(BranchCheckpointListing::Restarted(b) | BranchCheckpointListing::Continued(b)),
    ) = (&actual.result, &expected.result)
    {
        for (actual, expected) in a.removed.iter().zip(&b.removed) {
            assert_remote_keys_eq(actual, expected);
        }
        for (actual, expected) in a.revised.iter().zip(&b.revised) {
            assert_remote_keys_eq(&actual.branch_key, &expected.branch_key);
        }
    }
}

fn assert_checkpoint_keys(
    actual: &[OwnershipHandoffCheckpoint],
    expected: &[OwnershipHandoffCheckpoint],
) {
    for (actual, expected) in actual.iter().zip(expected) {
        assert_remote_keys_eq(&actual.placement.branch_key, &expected.placement.branch_key);
    }
}

impl Message {
    async fn check(&self, executor: &Executor) {
        macro_rules! placement {
            ($message:expr, $class:expr, $field:ident) => {{
                let decoded = round_trip(executor, $class, $message).await;
                assert_remote_keys_eq(&decoded.$field.branch_key, &$message.$field.branch_key);
            }};
        }
        match self {
            Self::ListingRequest(m) => {
                placement!(m, BranchCheckpointListingRequest::CLASS, lifecycle)
            }
            Self::ListingResponse(m) => {
                let decoded = round_trip(executor, BranchCheckpointListingRequest::CLASS, m).await;
                assert_listing_keys(&decoded, m);
            }
            Self::Fetch(m) => placement!(m, FetchStateCheckpoint::CLASS, placement),
            Self::KafkaSync(m) => placement!(m, SyncKafkaOffsets::CLASS, placement),
            Self::KafkaDescribe(m) => placement!(m, DescribeKafkaOffsets::CLASS, placement),
            Self::KafkaResponse(m) => {
                drop(round_trip(executor, DescribeKafkaOffsets::CLASS, m).await);
            }
            Self::SyncRequest(m) => placement!(m, StateSyncRequest::CLASS, placement),
            Self::SyncResponse(m) => {
                round_trip(executor, StateSyncRequest::CLASS, m).await;
            }
            Self::Ack(m) => placement!(m, PoolClass::Replication, placement),
            Self::Available(m) => placement!(m, PoolClass::Replication, placement),
            Self::Prepare(m) => {
                let decoded =
                    round_trip(executor, PrepareOwnershipHandoffStateRequest::CLASS, m).await;
                assert_checkpoint_keys(&decoded.checkpoints, &m.checkpoints);
                let reply: OwnershipHandoffResponse<()> = match &m.checkpoints.first() {
                    Some(_) => Ok(()),
                    None => Err(OwnershipHandoffFailure::rejected(m.operation_id.clone())),
                };
                drop(
                    round_trip(executor, PrepareOwnershipHandoffStateRequest::CLASS, &reply).await,
                );
            }
            Self::Capture(m) => {
                round_trip(executor, CaptureOwnershipHandoffStateRequest::CLASS, m).await;
            }
            Self::CaptureResponse(m) => {
                let decoded =
                    round_trip(executor, CaptureOwnershipHandoffStateRequest::CLASS, m).await;
                if let (Ok(actual), Ok(expected)) = (&decoded, m) {
                    assert_checkpoint_keys(actual, expected);
                }
            }
        }
    }

    async fn check_damage(&self, executor: &Executor, bytes: &[u8]) {
        macro_rules! damaged {
            ($message:expr, $class:expr) => {
                check_damage(executor, $class, $message, bytes).await
            };
        }
        match self {
            Self::ListingRequest(m) => damaged!(m, BranchCheckpointListingRequest::CLASS),
            Self::ListingResponse(m) => damaged!(m, BranchCheckpointListingRequest::CLASS),
            Self::Fetch(m) => damaged!(m, FetchStateCheckpoint::CLASS),
            Self::KafkaSync(m) => damaged!(m, SyncKafkaOffsets::CLASS),
            Self::KafkaDescribe(m) => damaged!(m, DescribeKafkaOffsets::CLASS),
            Self::KafkaResponse(m) => damaged!(m, DescribeKafkaOffsets::CLASS),
            Self::SyncRequest(m) => damaged!(m, StateSyncRequest::CLASS),
            Self::SyncResponse(m) => damaged!(m, StateSyncRequest::CLASS),
            Self::Ack(m) => damaged!(m, PoolClass::Replication),
            Self::Available(m) => damaged!(m, PoolClass::Replication),
            Self::Prepare(m) => damaged!(m, PrepareOwnershipHandoffStateRequest::CLASS),
            Self::Capture(m) => damaged!(m, CaptureOwnershipHandoffStateRequest::CLASS),
            Self::CaptureResponse(m) => damaged!(m, CaptureOwnershipHandoffStateRequest::CLASS),
        }
    }
}

async fn check_damage<M: RkyvMessage + Clone>(
    executor: &Executor,
    class: PoolClass,
    message: &M,
    damage: &[u8],
) {
    let (mut encoded, encoded_charge) = message
        .clone()
        .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
        .await
        .assured("the bounded seed message encodes");
    let mut entropy = nervix_arbitrary::Entropy::new(damage);
    match entropy.byte() % 3 {
        0 => encoded.truncate(entropy.count(encoded.len())),
        1 => {
            if !encoded.is_empty() {
                let index = entropy.count(encoded.len() - 1);
                encoded[index] ^= entropy.byte() | 1;
            }
        }
        _ => encoded = damage.iter().take(256).copied().collect(),
    }
    match M::decode_rkyv(executor.clone(), class, encoded).await {
        Ok((decoded, charge)) => {
            drop(decoded);
            drop(charge);
        }
        Err(error) => assert!(
            matches!(error.current_context(), TransportError::Decode(_)),
            "damaged bytes have a typed decode outcome: {error:?}"
        ),
    }
    drop(encoded_charge);
    match M::decode_rkyv(executor.clone(), class, Vec::new()).await {
        Err(error) => assert!(matches!(error.current_context(), TransportError::Decode(_))),
        Ok(_) => panic!("an empty frame cannot hold a runtime-state message"),
    }
    match message
        .clone()
        .encode_rkyv(executor.clone(), class, 0)
        .await
    {
        Err(error) => assert!(
            matches!(error.current_context(), TransportError::Encode(_)),
            "a zero-byte bound refuses the message: {error:?}"
        ),
        Ok(_) => panic!("a runtime-state message cannot fit a zero-byte limit"),
    }
}

fn runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary property runtime builds")
}

fn assert_no_charge(executor: &Executor) {
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.commands_memory.reserved_bytes, 0);
    assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
}

#[test]
fn bolero_runtime_state_replication_messages_round_trip() {
    let runtime = runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let mut messages = Messages::new(bytes);
            let kind = messages.arbitrary.entropy().count(MESSAGE_KINDS - 1);
            let message = messages.draw(kind);
            let executor = Executor::default();
            runtime.block_on(message.check(&executor));
            assert_no_charge(&executor);
        });
}

#[test]
fn bolero_damaged_runtime_state_replication_messages_fail_typed() {
    let runtime = runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let mut messages = Messages::new(bytes);
            let kind = messages.arbitrary.entropy().count(MESSAGE_KINDS - 1);
            let message = messages.draw(kind);
            let executor = Executor::default();
            runtime.block_on(message.check_damage(&executor, bytes));
            assert_no_charge(&executor);
        });
}

/// Deliberate current-shape boundaries supplement generated cases: every state/model kind,
/// every field value kind, both branch presences, full-width descriptors and all refusal arms.
#[test]
fn runtime_state_wire_variant_boundaries() {
    use nervix_models::{RemoteRuntimeElementValue as Element, RemoteRuntimeValue as Value};
    use strum::IntoEnumIterator as _;

    let runtime = runtime();
    let executor = Executor::default();
    let schema = SchemaFingerprint::from_digest([u8::MAX; 32]);
    let states = [
        RuntimeState::BranchAggregated,
        RuntimeState::Correlator { schema },
        RuntimeState::Deduplicator { schema },
        RuntimeState::KafkaOffset,
        RuntimeState::MaterializedRelay { schema },
        RuntimeState::WasmProcessor {
            schema,
            generation: WasmStateGeneration::try_from(u64::MAX)
                .assured("the maximum generation is nonzero"),
        },
        RuntimeState::WindowProcessor { schema },
        RuntimeState::BranchLru { schema },
    ];
    let values = [
        Value::U8(u8::MAX),
        Value::I8(i8::MIN),
        Value::U16(u16::MAX),
        Value::I16(i16::MIN),
        Value::U32(u32::MAX),
        Value::I32(i32::MIN),
        Value::U64(u64::MAX),
        Value::I64(i64::MIN),
        Value::Bool(true),
        Value::String("\0ordered-é-🦀".to_string()),
        Value::Datetime("2026-10-13T12:34:56.123456789-03:30".to_string()),
        Value::F32(-0.0),
        Value::F64(f64::from_bits(1)),
        Value::Array(vec![Element::Vec(vec![
            Element::F32(0.0),
            Element::F32(-0.0),
            Element::F64(-0.0),
        ])]),
        Value::Vec(Vec::new()),
    ];
    let fields: Vec<_> = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| RemoteRuntimeField {
            name: format!("key_{index:02}"),
            value,
        })
        .collect();
    runtime.block_on(async {
        for state in states {
            nervix_primitives::task::consume_budget().await;
            for kind in ModelKind::iter() {
                nervix_primitives::task::consume_budget().await;
                for branch_key in [None, Some(fields.clone())] {
                    nervix_primitives::task::consume_budget().await;
                    let placement = StatePlacementEnvelope {
                        domain: DomainName::parse("boundaries").assured("a fixed valid domain"),
                        state,
                        kind,
                        identifier: ModelName::parse("checkpoint")
                            .assured("a fixed valid model name"),
                        unit: nervix_models::Unit::Single,
                        branch_key,
                    };
                    Message::SyncRequest(StateSyncRequest {
                        placement: placement.clone(),
                        after_lsm: Some(u64::MAX),
                    })
                    .check(&executor)
                    .await;
                    for read in [
                        StateCheckpointRead::Published,
                        StateCheckpointRead::HandoffCapture,
                    ] {
                        nervix_primitives::task::consume_budget().await;
                        Message::Fetch(FetchStateCheckpoint {
                            placement: placement.clone(),
                            lsm: u64::MAX,
                            read,
                        })
                        .check(&executor)
                        .await;
                    }
                }
            }
        }
        for class in 0..4 {
            nervix_primitives::task::consume_budget().await;
            for subject in 0..4 {
                nervix_primitives::task::consume_budget().await;
                let input = [class, subject];
                let failure = Messages::new(&input).failure();
                Message::ListingResponse(BranchCheckpointListingResponse {
                    result: Err(failure.clone()),
                })
                .check(&executor)
                .await;
                Message::SyncResponse(StateSyncResponse {
                    result: Err(failure.clone()),
                })
                .check(&executor)
                .await;
                Message::KafkaResponse(Err(failure)).check(&executor).await;
            }
        }
        for length in [0, 1, u64::MAX] {
            nervix_primitives::task::consume_budget().await;
            Message::SyncResponse(StateSyncResponse {
                result: Ok(Some(StateSnapshotEnvelope {
                    lsm: u64::MAX,
                    length,
                    digest: [u8::MAX; 32],
                })),
            })
            .check(&executor)
            .await;
        }
        let page = BranchCheckpointPage {
            cursor: BranchCheckpointCursor {
                epoch: u64::MAX,
                sequence: u64::MAX,
            },
            removed: vec![None, Some(fields.clone())],
            revised: states
                .into_iter()
                .map(|state| BranchCheckpointRevision {
                    branch_key: Some(fields.clone()),
                    state,
                    lsm: u64::MAX,
                })
                .collect(),
            more: true,
        };
        for listing in [
            BranchCheckpointListing::Absent,
            BranchCheckpointListing::Restarted(page.clone()),
            BranchCheckpointListing::Continued(page),
        ] {
            nervix_primitives::task::consume_budget().await;
            Message::ListingResponse(BranchCheckpointListingResponse {
                result: Ok(listing),
            })
            .check(&executor)
            .await;
        }
    });
    assert_no_charge(&executor);
}
