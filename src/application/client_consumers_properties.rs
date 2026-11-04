//! Generated client consumer stream frames through the bounded codec of the stream.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated opening messages, serving-node frames and owning-node answers of a client
//!   consumer stream, the boundary frames every ordinary run walks, and the properties that the
//!   codec of the stream hands each frame to the other end whole, bounds a frame by its payload
//!   limit, and refuses damaged bytes with its typed decode failure.
//! - **Depends on.** The stream's frames, the checks every forwarded client stream is held to, the
//!   interconnect's duplex codec, the executor and the vocabulary generators.
//! - **Must not know.** The transport that carries a frame, sessions, how a delivery's chunks are
//!   put back together, or what a frame makes either end of a stream do.
//!
//! A stream carries a delivery's Arrow bytes as opaque chunks, so no chunk here is an Arrow
//! stream: the client emitter delivery property owns that form.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::BTreeSet,
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, fields_of_every_type};
use nervix_execution::Executor;
use nervix_interconnect::{DuplexCodec, TransportError};
use nervix_models::{
    AckWindow, ClientConsumerLimits, ClusterNodeName, DEFAULT_CLIENT_CONSUMER_SESSION_BYTES,
    DomainName, EmitterName, MAX_CLIENT_CONSUMER_BATCHES, RelayName, Timestamp,
};
use strum::{EnumIter, IntoEnumIterator as _};

use super::{CHUNK_BYTES, ConsumerEvent, ConsumerItem, OpenConsumerStream};
use crate::{
    application::test_fixtures::duplex_frame_checks::{
        StreamFrame, assert_damaged_bytes_fail_typed_or_decode_to_a_frame,
        assert_nothing_stays_charged, assert_the_payload_limit_bounds_a_frame,
        executor_with_relay_payload_limit, property_runtime, through_the_stream,
    },
    runtime::{ClientEmitterAnswer, ClientEmitterDescription, ClientEmitterRefusal},
    runtime_schema::generated_batches::Damage,
};

/// The bytes one case reads its frame and its damage from.
const CASE_BYTES: usize = 2048;

/// The most bytes of one generated delivery chunk that is not the largest chunk a stream carries.
const SMALL_CHUNK_BYTES: usize = 96;

/// The relay payload limit the limit check narrows the codec to, so that it meets the limit with
/// frames of a few kibibytes.
const NARROW_PAYLOAD_LIMIT: u64 = 4096;

/// Which end of a stream a frame travels from, and so the type its receiver decodes it as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
enum ConsumerDirection {
    /// The message that opens the stream.
    Opening,
    /// A frame of the serving node.
    Serving,
    /// An answer of the owning node.
    Owning,
}

/// Which frame of a stream a case holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, EnumIter)]
enum ConsumerFrameKind {
    Opening,
    Settle,
    ServingHeartbeat,
    Close,
    Opened,
    Refused,
    DeliveryStart,
    DeliveryChunk,
    Settled,
    Ended,
    OwningHeartbeat,
}

/// One frame of a stream, as the type its direction carries.
#[derive(Debug, PartialEq, Eq)]
enum ConsumerFrame {
    Opening(OpenConsumerStream),
    Item(ConsumerItem),
    Event(ConsumerEvent),
}

/// Every reason an emitter endpoint refuses an open or a settlement.
const EVERY_REFUSAL: [ClientEmitterRefusal; 7] = [
    ClientEmitterRefusal::Closed,
    ClientEmitterRefusal::SchemaMismatch,
    ClientEmitterRefusal::InvalidWindow,
    ClientEmitterRefusal::NodeBudgetFull,
    ClientEmitterRefusal::StaleReference,
    ClientEmitterRefusal::WrongConsumer,
    ClientEmitterRefusal::InvalidReason,
];

/// The place of `refusal` in [`EVERY_REFUSAL`]. The match is exhaustive, so a new refusal fails to
/// compile here until the list holds it.
fn refusal_position(refusal: ClientEmitterRefusal) -> usize {
    match refusal {
        ClientEmitterRefusal::Closed => 0,
        ClientEmitterRefusal::SchemaMismatch => 1,
        ClientEmitterRefusal::InvalidWindow => 2,
        ClientEmitterRefusal::NodeBudgetFull => 3,
        ClientEmitterRefusal::StaleReference => 4,
        ClientEmitterRefusal::WrongConsumer => 5,
        ClientEmitterRefusal::InvalidReason => 6,
    }
}

/// A 16-byte identity: all zeros, all ones, or any sixteen bytes. An identity is accepted
/// whatever its bytes, so the constant ones are as valid as any.
fn identity(arbitrary: &mut Arbitrary<'_>) -> [u8; 16] {
    match arbitrary.entropy().byte() % 3 {
        0 => [0; 16],
        1 => [u8::MAX; 16],
        _ => std::array::from_fn(|_| arbitrary.entropy().byte()),
    }
}

/// Any count a `u32` holds, landing on its ends as often as anywhere else.
fn any_u32(arbitrary: &mut Arbitrary<'_>) -> u32 {
    let value = arbitrary.entropy().boundary_biased(0..=u64::from(u32::MAX));
    u32::try_from(value).verified("the range above ends at u32::MAX")
}

fn answer(arbitrary: &mut Arbitrary<'_>) -> ClientEmitterAnswer {
    match arbitrary.entropy().byte() % 3 {
        0 => ClientEmitterAnswer::Ack,
        1 => ClientEmitterAnswer::Retry,
        _ => ClientEmitterAnswer::Reject(arbitrary.string()),
    }
}

fn description(arbitrary: &mut Arbitrary<'_>) -> ClientEmitterDescription {
    ClientEmitterDescription {
        fields: arbitrary.schema_fields(0),
        maximum_payload_bytes: arbitrary.entropy().any_u64(),
        maximum_payload_rows: any_u32(arbitrary),
        window: arbitrary.ack_window(),
        ack_timeout: arbitrary.physical_duration(),
        retry_backoff: arbitrary.physical_duration(),
        retry_max_backoff: arbitrary.physical_duration(),
    }
}

/// The bytes of one delivery chunk: none, a few of any value, or one byte repeated to the most
/// bytes one chunk carries.
fn chunk(arbitrary: &mut Arbitrary<'_>) -> Vec<u8> {
    if arbitrary.entropy().byte() % 8 == 7 {
        return vec![arbitrary.entropy().byte(); CHUNK_BYTES];
    }
    let length = arbitrary.entropy().count(SMALL_CHUNK_BYTES);
    let mut bytes = Vec::with_capacity(length);
    for _ in 0..length {
        bytes.push(arbitrary.entropy().byte());
    }
    bytes
}

impl ConsumerFrameKind {
    fn direction(self) -> ConsumerDirection {
        match self {
            Self::Opening => ConsumerDirection::Opening,
            Self::Settle | Self::ServingHeartbeat | Self::Close => ConsumerDirection::Serving,
            Self::Opened
            | Self::Refused
            | Self::DeliveryStart
            | Self::DeliveryChunk
            | Self::Settled
            | Self::Ended
            | Self::OwningHeartbeat => ConsumerDirection::Owning,
        }
    }

    /// A frame of this kind, its fields drawn from `arbitrary`.
    fn frame(self, arbitrary: &mut Arbitrary<'_>) -> ConsumerFrame {
        match self {
            Self::Opening => ConsumerFrame::Opening(OpenConsumerStream {
                serving_node: arbitrary.rule_name::<ClusterNodeName>(),
                domain: arbitrary.rule_name::<DomainName>(),
                emitter: arbitrary.rule_name::<EmitterName>(),
                expected_fields: arbitrary.schema_fields(0),
                limits: arbitrary.client_consumer_limits(),
            }),
            Self::Settle => ConsumerFrame::Item(ConsumerItem::Settle {
                key: arbitrary.entropy().any_u64(),
                reference: identity(arbitrary),
                answer: answer(arbitrary),
            }),
            Self::ServingHeartbeat => ConsumerFrame::Item(ConsumerItem::Heartbeat),
            Self::Close => ConsumerFrame::Item(ConsumerItem::Close),
            Self::Opened => ConsumerFrame::Event(ConsumerEvent::Opened(description(arbitrary))),
            Self::Refused => ConsumerFrame::Event(ConsumerEvent::Refused(
                arbitrary.entropy().pick(EVERY_REFUSAL),
            )),
            Self::DeliveryStart => {
                let branch_fingerprint = if arbitrary.entropy().flag() {
                    Some(arbitrary.digest())
                } else {
                    None
                };
                ConsumerFrame::Event(ConsumerEvent::DeliveryStart {
                    identity: identity(arbitrary),
                    reference: identity(arbitrary),
                    source: arbitrary.rule_name::<RelayName>(),
                    branch_fingerprint,
                    members: any_u32(arbitrary),
                    execution_now: arbitrary.timestamp(),
                    bytes: any_u32(arbitrary),
                })
            }
            Self::DeliveryChunk => {
                let last = arbitrary.entropy().flag();
                ConsumerFrame::Event(ConsumerEvent::DeliveryChunk {
                    bytes: chunk(arbitrary),
                    last,
                })
            }
            Self::Settled => {
                let key = arbitrary.entropy().any_u64();
                let outcome = if arbitrary.entropy().flag() {
                    Err(arbitrary.entropy().pick(EVERY_REFUSAL))
                } else {
                    Ok(())
                };
                ConsumerFrame::Event(ConsumerEvent::Settled { key, outcome })
            }
            Self::Ended => ConsumerFrame::Event(ConsumerEvent::Ended),
            Self::OwningHeartbeat => ConsumerFrame::Event(ConsumerEvent::Heartbeat),
        }
    }
}

impl ConsumerFrame {
    /// Which frame this is. The matches are exhaustive, so a frame added to the stream fails to
    /// compile here until the generator draws it.
    fn kind(&self) -> ConsumerFrameKind {
        match self {
            Self::Opening(OpenConsumerStream { .. }) => ConsumerFrameKind::Opening,
            Self::Item(item) => match item {
                ConsumerItem::Settle { .. } => ConsumerFrameKind::Settle,
                ConsumerItem::Heartbeat => ConsumerFrameKind::ServingHeartbeat,
                ConsumerItem::Close => ConsumerFrameKind::Close,
            },
            Self::Event(event) => match event {
                ConsumerEvent::Opened(_) => ConsumerFrameKind::Opened,
                ConsumerEvent::Refused(_) => ConsumerFrameKind::Refused,
                ConsumerEvent::DeliveryStart { .. } => ConsumerFrameKind::DeliveryStart,
                ConsumerEvent::DeliveryChunk { .. } => ConsumerFrameKind::DeliveryChunk,
                ConsumerEvent::Settled { .. } => ConsumerFrameKind::Settled,
                ConsumerEvent::Ended => ConsumerFrameKind::Ended,
                ConsumerEvent::Heartbeat => ConsumerFrameKind::OwningHeartbeat,
            },
        }
    }
}

impl StreamFrame for ConsumerFrame {
    type Direction = ConsumerDirection;

    fn direction(&self) -> ConsumerDirection {
        self.kind().direction()
    }

    async fn encoded(self, codec: &DuplexCodec) -> Result<Vec<u8>, Report<TransportError>> {
        let encoded = match self {
            Self::Opening(opening) => codec.encode(opening).await,
            Self::Item(item) => codec.encode(item).await,
            Self::Event(event) => codec.encode(event).await,
        }?;
        Ok(encoded.bytes)
    }

    async fn decoded(
        direction: ConsumerDirection,
        codec: &DuplexCodec,
        bytes: Vec<u8>,
    ) -> Result<Self, Report<TransportError>> {
        match direction {
            ConsumerDirection::Opening => {
                let decoded = codec.decode::<OpenConsumerStream>(bytes).await?;
                Ok(Self::Opening(decoded.item))
            }
            ConsumerDirection::Serving => {
                let decoded = codec.decode::<ConsumerItem>(bytes).await?;
                Ok(Self::Item(decoded.item))
            }
            ConsumerDirection::Owning => {
                let decoded = codec.decode::<ConsumerEvent>(bytes).await?;
                Ok(Self::Event(decoded.item))
            }
        }
    }
}

/// The frame `input` describes: its kind first, so that every kind has the case's bytes to itself.
fn generated_frame(input: &[u8]) -> ConsumerFrame {
    let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
    let kind: ConsumerFrameKind = arbitrary.variant();
    kind.frame(&mut arbitrary)
}

/// Every frame of a client consumer stream reaches the other end whole through the bounded codec
/// of the stream, under the relay class and payload limit the transport encodes it with: the
/// message that opens the stream with its schema and limits, a settlement with its reference and
/// answer, and each answer of the owning node, an emitter's whole description, every refusal, a
/// delivery's identity, source, branch fingerprint, row count, instant and length, its chunks and
/// each settlement outcome among them. Nothing stays charged once a frame is dropped.
#[test]
fn bolero_consumer_stream_frames_reach_the_other_end_whole() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let codec = DuplexCodec::of::<OpenConsumerStream>(&executor);
            // The frames derive no `Clone`, and the same bytes always build the same frame.
            let sent = generated_frame(input);
            let expected = generated_frame(input);
            let received = runtime.block_on(through_the_stream(&codec, sent));
            assert_eq!(received, expected);
            assert_nothing_stays_charged(&executor);
        });
}

/// What an opened consumer is told, with a schema of every field type and every duration of its
/// policy `duration`.
fn description_at(
    window: AckWindow,
    duration: Duration,
    maximum_payload_bytes: u64,
    maximum_payload_rows: u32,
) -> ClientEmitterDescription {
    ClientEmitterDescription {
        fields: fields_of_every_type(),
        maximum_payload_bytes,
        maximum_payload_rows,
        window,
        ack_timeout: duration,
        retry_backoff: duration,
        retry_max_backoff: duration,
    }
}

/// A frame that carries a delivery chunk of `length` bytes.
fn chunk_of(length: usize) -> ConsumerFrame {
    ConsumerFrame::Event(ConsumerEvent::DeliveryChunk {
        bytes: vec![0xa5; length],
        last: true,
    })
}

/// The frames an ordinary run must not leave to its random cases: every answer, refusal and
/// settlement outcome, the ends of every count, instant and duration a frame carries, a delivery
/// with and without a branch fingerprint, and a chunk of no bytes and of the most bytes a stream
/// sends in one.
fn boundary_frames() -> Vec<ConsumerFrame> {
    let mut frames = vec![
        ConsumerFrame::Item(ConsumerItem::Heartbeat),
        ConsumerFrame::Item(ConsumerItem::Close),
        ConsumerFrame::Event(ConsumerEvent::Ended),
        ConsumerFrame::Event(ConsumerEvent::Heartbeat),
    ];
    let declared_most = ClientConsumerLimits {
        batches: NonZeroU32::new(MAX_CLIENT_CONSUMER_BATCHES)
            .assured("a consumer may hold a batch outstanding"),
        bytes: NonZeroU64::new(DEFAULT_CLIENT_CONSUMER_SESSION_BYTES)
            .assured("a consumer may hold bytes outstanding"),
    };
    let least = ClientConsumerLimits {
        batches: NonZeroU32::MIN,
        bytes: NonZeroU64::MIN,
    };
    let largest = ClientConsumerLimits {
        batches: NonZeroU32::MAX,
        bytes: NonZeroU64::MAX,
    };
    for limits in [least, declared_most, largest] {
        for expected_fields in [Vec::new(), fields_of_every_type()] {
            frames.push(ConsumerFrame::Opening(OpenConsumerStream {
                serving_node: ClusterNodeName::parse("serving-node")
                    .assured("a literal cluster node name"),
                domain: DomainName::parse("orders").assured("a literal domain name"),
                emitter: EmitterName::parse("outlet").assured("a literal emitter name"),
                expected_fields,
                limits,
            }));
        }
    }
    for key in [0, 1, u64::MAX] {
        for reference in [[0; 16], [u8::MAX; 16]] {
            for answer in [
                ClientEmitterAnswer::Ack,
                ClientEmitterAnswer::Retry,
                ClientEmitterAnswer::Reject(String::new()),
                ClientEmitterAnswer::Reject("the row failed validation \u{2014} \u{1F980}".into()),
            ] {
                frames.push(ConsumerFrame::Item(ConsumerItem::Settle {
                    key,
                    reference,
                    answer,
                }));
            }
        }
        frames.push(ConsumerFrame::Event(ConsumerEvent::Settled {
            key,
            outcome: Ok(()),
        }));
        for refusal in EVERY_REFUSAL {
            frames.push(ConsumerFrame::Event(ConsumerEvent::Settled {
                key,
                outcome: Err(refusal),
            }));
        }
    }
    for refusal in EVERY_REFUSAL {
        frames.push(ConsumerFrame::Event(ConsumerEvent::Refused(refusal)));
    }
    for duration in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
        for window in [
            AckWindow::Sequential,
            AckWindow::Parallel {
                max: NonZeroU64::MIN,
            },
            AckWindow::Parallel {
                max: NonZeroU64::MAX,
            },
        ] {
            frames.push(ConsumerFrame::Event(ConsumerEvent::Opened(description_at(
                window, duration, 0, 0,
            ))));
            frames.push(ConsumerFrame::Event(ConsumerEvent::Opened(description_at(
                window,
                duration,
                u64::MAX,
                u32::MAX,
            ))));
        }
    }
    for branch_fingerprint in [None, Some([0; 32]), Some([u8::MAX; 32])] {
        for execution_now in [i64::MIN, -1, 0, 1, i64::MAX] {
            for count in [0, 1, u32::MAX] {
                for identity in [[0; 16], [u8::MAX; 16]] {
                    frames.push(ConsumerFrame::Event(ConsumerEvent::DeliveryStart {
                        identity,
                        reference: identity,
                        source: RelayName::parse("orders").assured("a literal relay name"),
                        branch_fingerprint,
                        members: count,
                        execution_now: Timestamp::from_unix_nanos(execution_now),
                        bytes: count,
                    }));
                }
            }
        }
    }
    for last in [false, true] {
        for length in [0, 1, CHUNK_BYTES] {
            frames.push(ConsumerFrame::Event(ConsumerEvent::DeliveryChunk {
                bytes: vec![0x5a; length],
                last,
            }));
        }
    }
    frames
}

/// The frames a random case may leave undrawn reach the other end whole on every run: each
/// answer, refusal and settlement outcome, counts and instants at both ends of their range, policy
/// durations of zero, one nanosecond and the longest duration, a schema of every field type, and a
/// chunk of the most bytes the stream sends in one, which the default relay payload limit carries.
#[nervix_primitives::test]
async fn every_boundary_frame_reaches_the_other_end_whole() {
    let executor = Executor::default();
    let codec = DuplexCodec::of::<OpenConsumerStream>(&executor);
    let mut kinds = BTreeSet::new();
    // The frames derive no `Clone`, so the list is built once to send and once to compare with.
    for (sent, expected) in boundary_frames().into_iter().zip(boundary_frames()) {
        kinds.insert(expected.kind());
        let received = through_the_stream(&codec, sent).await;
        assert_eq!(received, expected);
    }
    let every_kind: BTreeSet<ConsumerFrameKind> = ConsumerFrameKind::iter().collect();
    assert_eq!(kinds, every_kind, "the boundary frames hold every kind");
    assert_nothing_stays_charged(&executor);
}

/// The generator builds the kind it is asked for, so a case whose first byte names a kind holds a
/// frame of it, and every kind is drawn.
#[test]
fn every_frame_kind_is_generated_as_asked() {
    let mut drawn = BTreeSet::new();
    for first in 0..=u8::MAX {
        // The first byte names the kind; the rest only has to differ from case to case.
        let bytes = [first, !first, first ^ 0x5a, 0xff];
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let kind: ConsumerFrameKind = arbitrary.variant();
        let frame = kind.frame(&mut arbitrary);
        assert_eq!(frame.kind(), kind);
        drawn.insert(kind);
    }
    let every_kind: BTreeSet<ConsumerFrameKind> = ConsumerFrameKind::iter().collect();
    assert_eq!(drawn, every_kind);
}

/// The list of refusals names each refusal once, in the order the exhaustive match gives them.
#[test]
fn the_refusals_a_frame_carries_are_every_refusal() {
    let positions: Vec<usize> = EVERY_REFUSAL.into_iter().map(refusal_position).collect();
    assert_eq!(positions, (0..EVERY_REFUSAL.len()).collect::<Vec<_>>());
}

/// The codec of a stream carries a delivery chunk up to its class's payload limit and refuses the
/// first one past it with its typed encode failure, before anything is sent and with nothing left
/// charged.
#[nervix_primitives::test]
async fn a_chunk_past_the_payload_limit_is_refused_with_the_typed_encode_failure() {
    let executor = executor_with_relay_payload_limit(NARROW_PAYLOAD_LIMIT);
    let codec = DuplexCodec::of::<OpenConsumerStream>(&executor);
    assert_the_payload_limit_bounds_a_frame(&codec, NARROW_PAYLOAD_LIMIT, chunk_of).await;
    assert_nothing_stays_charged(&executor);
}

/// A damaged frame of a stream, a frame of one direction read as another's, and arbitrary bytes
/// are refused with the codec's typed decode failure, as bytes that do not decode and never as
/// work the node could not admit, or decode to a frame the codec keeps whole. Either way nothing
/// stays charged.
#[test]
fn bolero_damaged_consumer_stream_frames_fail_typed_or_decode_to_a_frame() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let codec = DuplexCodec::of::<OpenConsumerStream>(&executor);
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // What shapes the case is read before the frame: an ordinary run's few bytes run out
            // while a frame is generated, and a damage read after that would truncate nearly every
            // frame to nothing.
            let damage = Damage::draw(arbitrary.entropy());
            let misdirected = arbitrary.entropy().byte().is_multiple_of(8);
            let read_as: ConsumerDirection = arbitrary.variant();
            let kind: ConsumerFrameKind = arbitrary.variant();
            let frame = kind.frame(&mut arbitrary);
            runtime.block_on(async {
                let valid = frame
                    .encoded(&codec)
                    .await
                    .assured("a generated frame encodes within the relay payload limit");
                // A misdirected frame arrives intact and is read as another direction's type; any
                // other is damaged and read as its own.
                let (direction, bytes) = if misdirected {
                    (read_as, valid)
                } else {
                    (kind.direction(), damage.apply(valid))
                };
                assert_damaged_bytes_fail_typed_or_decode_to_a_frame::<ConsumerFrame>(
                    &codec, direction, bytes,
                )
                .await;
            });
            assert_nothing_stays_charged(&executor);
        });
}
