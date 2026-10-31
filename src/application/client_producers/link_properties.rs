//! Generated client producer link frames through the bounded codec of the link's stream.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated opening messages, serving-node frames and owning-node answers of a client
//!   producer link, the boundary frames every ordinary run walks, and the properties that the codec
//!   of the link's stream hands each frame to the other end whole, bounds a frame by its payload
//!   limit, and refuses damaged bytes with its typed decode failure.
//! - **Depends on.** The link's frames, the checks every forwarded client stream is held to, the
//!   interconnect's duplex codec, the executor and the vocabulary generators.
//! - **Must not know.** The transport that carries a frame, sessions, or what a frame makes either
//!   end of a link do.
//!
//! A link carries a forwarded batch as opaque bytes, so no batch here is an Arrow stream: the
//! producer batch properties own that form.

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
use nervix_client_wire::SessionLimits;
use nervix_execution::Executor;
use nervix_interconnect::{DuplexCodec, TransportError};
use nervix_models::{
    AckWindow, ClientAttachmentId, ClientBatchDefect, ClientEndpointContract,
    ClientOutcomeUncertainty, ClientProcessingFailure, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerEndReason, ClientProducerGrant, ClientProducerLimits,
    ClientProducerPolicy, ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal,
    ClusterNodeName, DEFAULT_CLIENT_PRODUCER_SESSION_BYTES, DomainName, IngestorName,
    MAX_CLIENT_PRODUCER_BATCHES,
};
use strum::{EnumIter, IntoEnumIterator as _};

use super::{ClientProducerLinkEvent, ClientProducerLinkItem, OpenClientProducerLink};
use crate::{
    application::test_fixtures::duplex_frame_checks::{
        StreamFrame, assert_damaged_bytes_fail_typed_or_decode_to_a_frame,
        assert_nothing_stays_charged, assert_the_payload_limit_bounds_a_frame,
        executor_with_relay_payload_limit, property_runtime, through_the_stream,
    },
    runtime_schema::generated_batches::Damage,
};

/// The bytes one case reads its frame and its damage from.
const CASE_BYTES: usize = 2048;

/// The most bytes of one generated forwarded batch.
const BATCH_BYTES: usize = 96;

/// The relay payload limit the limit check narrows the codec to, so that it meets the limit with
/// frames of a few kibibytes.
const NARROW_PAYLOAD_LIMIT: u64 = 4096;

/// Which end of a link a frame travels from, and so the type its receiver decodes it as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
enum LinkDirection {
    /// The message that opens the link.
    Opening,
    /// A frame of the serving node.
    Serving,
    /// An answer of the owning node.
    Owning,
}

/// Which frame of a link a case holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, EnumIter)]
enum LinkFrameKind {
    Opening,
    Open,
    Submit,
    Clear,
    Close,
    Detach,
    ServingHeartbeat,
    Opened,
    Refused,
    Admitting,
    Outcome,
    Admission,
    Ended,
    Closed,
    OwningHeartbeat,
}

/// One frame of a link, as the type its direction carries.
#[derive(Debug, PartialEq, Eq)]
enum LinkFrame {
    Opening(OpenClientProducerLink),
    Item(ClientProducerLinkItem),
    Event(ClientProducerLinkEvent),
}

impl LinkFrameKind {
    fn direction(self) -> LinkDirection {
        match self {
            Self::Opening => LinkDirection::Opening,
            Self::Open
            | Self::Submit
            | Self::Clear
            | Self::Close
            | Self::Detach
            | Self::ServingHeartbeat => LinkDirection::Serving,
            Self::Opened
            | Self::Refused
            | Self::Admitting
            | Self::Outcome
            | Self::Admission
            | Self::Ended
            | Self::Closed
            | Self::OwningHeartbeat => LinkDirection::Owning,
        }
    }

    /// A frame of this kind, its fields drawn from `arbitrary`.
    fn frame(self, arbitrary: &mut Arbitrary<'_>) -> LinkFrame {
        match self {
            Self::Opening => LinkFrame::Opening(OpenClientProducerLink {
                serving_node: arbitrary.rule_name::<ClusterNodeName>(),
            }),
            Self::Open => LinkFrame::Item(ClientProducerLinkItem::Open {
                key: arbitrary.entropy().any_u64(),
                domain: arbitrary.rule_name::<DomainName>(),
                ingestor: arbitrary.rule_name::<IngestorName>(),
                expected_fields: arbitrary.schema_fields(0),
                limits: arbitrary.client_producer_limits(),
                max_batch_bytes: arbitrary.positive_u64(),
            }),
            Self::Submit => LinkFrame::Item(ClientProducerLinkItem::Submit {
                key: arbitrary.entropy().any_u64(),
                submission: arbitrary.positive_u64(),
                batch: forwarded_batch(arbitrary),
            }),
            Self::Clear => LinkFrame::Item(ClientProducerLinkItem::Clear {
                key: arbitrary.entropy().any_u64(),
                submission: arbitrary.positive_u64(),
            }),
            Self::Close => LinkFrame::Item(ClientProducerLinkItem::Close {
                key: arbitrary.entropy().any_u64(),
            }),
            Self::Detach => LinkFrame::Item(ClientProducerLinkItem::Detach {
                key: arbitrary.entropy().any_u64(),
            }),
            Self::ServingHeartbeat => LinkFrame::Item(ClientProducerLinkItem::Heartbeat),
            Self::Opened => LinkFrame::Event(ClientProducerLinkEvent::Opened {
                key: arbitrary.entropy().any_u64(),
                description: arbitrary.client_producer_description(),
            }),
            Self::Refused => LinkFrame::Event(ClientProducerLinkEvent::Refused {
                key: arbitrary.entropy().any_u64(),
                refusal: arbitrary.variant(),
            }),
            Self::Admitting => LinkFrame::Event(ClientProducerLinkEvent::Admitting {
                key: arbitrary.entropy().any_u64(),
                submission: arbitrary.positive_u64(),
            }),
            Self::Outcome => {
                let key = arbitrary.entropy().any_u64();
                let submission = arbitrary.positive_u64();
                let outcome = arbitrary.client_submission_outcome();
                let detail = if arbitrary.entropy().flag() {
                    Some(arbitrary.string())
                } else {
                    None
                };
                LinkFrame::Event(ClientProducerLinkEvent::Outcome {
                    key,
                    submission,
                    outcome,
                    detail,
                })
            }
            Self::Admission => LinkFrame::Event(ClientProducerLinkEvent::Admission {
                key: arbitrary.entropy().any_u64(),
                admission: arbitrary.variant(),
            }),
            Self::Ended => LinkFrame::Event(ClientProducerLinkEvent::Ended {
                key: arbitrary.entropy().any_u64(),
                reason: arbitrary.variant(),
            }),
            Self::Closed => LinkFrame::Event(ClientProducerLinkEvent::Closed {
                key: arbitrary.entropy().any_u64(),
            }),
            Self::OwningHeartbeat => LinkFrame::Event(ClientProducerLinkEvent::Heartbeat),
        }
    }
}

/// The bytes of one forwarded batch: none, or up to [`BATCH_BYTES`] of any value.
fn forwarded_batch(arbitrary: &mut Arbitrary<'_>) -> Vec<u8> {
    let length = arbitrary.entropy().count(BATCH_BYTES);
    let mut batch = Vec::with_capacity(length);
    for _ in 0..length {
        batch.push(arbitrary.entropy().byte());
    }
    batch
}

impl LinkFrame {
    /// Which frame this is. The matches are exhaustive, so a frame added to the link fails to
    /// compile here until the generator draws it.
    fn kind(&self) -> LinkFrameKind {
        match self {
            Self::Opening(OpenClientProducerLink { .. }) => LinkFrameKind::Opening,
            Self::Item(item) => match item {
                ClientProducerLinkItem::Open { .. } => LinkFrameKind::Open,
                ClientProducerLinkItem::Submit { .. } => LinkFrameKind::Submit,
                ClientProducerLinkItem::Clear { .. } => LinkFrameKind::Clear,
                ClientProducerLinkItem::Close { .. } => LinkFrameKind::Close,
                ClientProducerLinkItem::Detach { .. } => LinkFrameKind::Detach,
                ClientProducerLinkItem::Heartbeat => LinkFrameKind::ServingHeartbeat,
            },
            Self::Event(event) => match event {
                ClientProducerLinkEvent::Opened { .. } => LinkFrameKind::Opened,
                ClientProducerLinkEvent::Refused { .. } => LinkFrameKind::Refused,
                ClientProducerLinkEvent::Admitting { .. } => LinkFrameKind::Admitting,
                ClientProducerLinkEvent::Outcome { .. } => LinkFrameKind::Outcome,
                ClientProducerLinkEvent::Admission { .. } => LinkFrameKind::Admission,
                ClientProducerLinkEvent::Ended { .. } => LinkFrameKind::Ended,
                ClientProducerLinkEvent::Closed { .. } => LinkFrameKind::Closed,
                ClientProducerLinkEvent::Heartbeat => LinkFrameKind::OwningHeartbeat,
            },
        }
    }
}

impl StreamFrame for LinkFrame {
    type Direction = LinkDirection;

    fn direction(&self) -> LinkDirection {
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
        direction: LinkDirection,
        codec: &DuplexCodec,
        bytes: Vec<u8>,
    ) -> Result<Self, Report<TransportError>> {
        match direction {
            LinkDirection::Opening => {
                let decoded = codec.decode::<OpenClientProducerLink>(bytes).await?;
                Ok(Self::Opening(decoded.item))
            }
            LinkDirection::Serving => {
                let decoded = codec.decode::<ClientProducerLinkItem>(bytes).await?;
                Ok(Self::Item(decoded.item))
            }
            LinkDirection::Owning => {
                let decoded = codec.decode::<ClientProducerLinkEvent>(bytes).await?;
                Ok(Self::Event(decoded.item))
            }
        }
    }
}

/// The frame `input` describes: its kind first, so that every kind has the case's bytes to itself.
fn generated_frame(input: &[u8]) -> LinkFrame {
    let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
    let kind: LinkFrameKind = arbitrary.variant();
    kind.frame(&mut arbitrary)
}

/// Every frame of a client producer link reaches the other end whole through the bounded codec of
/// the link's stream, under the relay class and payload limit the transport encodes it with: the
/// message that opens the link, each frame the serving node sends, an open with its schema and
/// limits, a batch with its bytes and its clearance among them, and each answer of the owning
/// node, with a producer's whole description, every refusal, outcome, admission state and end
/// reason. Nothing stays charged once a frame is dropped.
#[test]
fn bolero_producer_link_frames_reach_the_other_end_whole() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let codec = DuplexCodec::of::<OpenClientProducerLink>(&executor);
            // The frames derive no `Clone`, and the same bytes always build the same frame.
            let sent = generated_frame(input);
            let expected = generated_frame(input);
            let received = runtime.block_on(through_the_stream(&codec, sent));
            assert_eq!(received, expected);
            assert_nothing_stays_charged(&executor);
        });
}

/// Every outcome a submitted batch has, with every cause each one names. The matches below are
/// exhaustive, so a new outcome or refusal fails to compile until this list holds it.
fn every_submission_outcome() -> Vec<ClientSubmissionOutcome> {
    let mut outcomes = vec![ClientSubmissionOutcome::Completed];
    for defect in ClientBatchDefect::iter() {
        outcomes.push(ClientSubmissionOutcome::NotAdmitted(
            ClientSubmissionRefusal::InvalidBatch(defect),
        ));
    }
    for refusal in [
        ClientSubmissionRefusal::Suspended,
        ClientSubmissionRefusal::Busy,
        ClientSubmissionRefusal::Draining,
        ClientSubmissionRefusal::ProducerEnded,
        ClientSubmissionRefusal::CreditExceeded,
    ] {
        outcomes.push(ClientSubmissionOutcome::NotAdmitted(refusal));
    }
    for failure in ClientProcessingFailure::iter() {
        outcomes.push(ClientSubmissionOutcome::ProcessingFailed(failure));
    }
    for uncertainty in ClientOutcomeUncertainty::iter() {
        outcomes.push(ClientSubmissionOutcome::OutcomeUnknown(uncertainty));
    }
    for outcome in &outcomes {
        match outcome {
            ClientSubmissionOutcome::Completed
            | ClientSubmissionOutcome::ProcessingFailed(_)
            | ClientSubmissionOutcome::OutcomeUnknown(_) => {}
            ClientSubmissionOutcome::NotAdmitted(refusal) => match refusal {
                ClientSubmissionRefusal::InvalidBatch(_)
                | ClientSubmissionRefusal::Suspended
                | ClientSubmissionRefusal::Busy
                | ClientSubmissionRefusal::Draining
                | ClientSubmissionRefusal::ProducerEnded
                | ClientSubmissionRefusal::CreditExceeded => {}
            },
        }
    }
    outcomes
}

/// What an opened producer is told, with every count at the top of its range, a schema of every
/// field type and every duration of its policy `duration`.
fn description_at(
    window: AckWindow,
    duration: Duration,
    admission: ClientProducerAdmission,
) -> ClientProducerDescription {
    ClientProducerDescription {
        attachment: ClientAttachmentId::from_u128(u128::MAX),
        fields: fields_of_every_type(),
        generation: u64::MAX,
        contract: ClientEndpointContract::from_digest([u8::MAX; 32]),
        policy: ClientProducerPolicy {
            window,
            ack_timeout: duration,
            retry_backoff: duration,
            retry_max_backoff: duration,
        },
        grant: ClientProducerGrant {
            batches: NonZeroU32::MAX,
            bytes: NonZeroU64::MAX,
            max_batch_bytes: NonZeroU64::MAX,
            max_batch_rows: NonZeroU32::MAX,
        },
        admission,
    }
}

/// The frames an ordinary run must not leave to its random cases: every answer, outcome, refusal,
/// admission state and end reason, and the ends of every count and duration a frame carries.
fn boundary_frames() -> Vec<LinkFrame> {
    let mut frames = vec![
        LinkFrame::Opening(OpenClientProducerLink {
            serving_node: ClusterNodeName::parse("serving-node")
                .assured("a literal cluster node name"),
        }),
        LinkFrame::Item(ClientProducerLinkItem::Heartbeat),
        LinkFrame::Event(ClientProducerLinkEvent::Heartbeat),
    ];
    for key in [0, 1, u64::MAX] {
        frames.push(LinkFrame::Item(ClientProducerLinkItem::Close { key }));
        frames.push(LinkFrame::Item(ClientProducerLinkItem::Detach { key }));
        frames.push(LinkFrame::Event(ClientProducerLinkEvent::Closed { key }));
        for submission in [NonZeroU64::MIN, NonZeroU64::MAX] {
            frames.push(LinkFrame::Item(ClientProducerLinkItem::Clear {
                key,
                submission,
            }));
            frames.push(LinkFrame::Event(ClientProducerLinkEvent::Admitting {
                key,
                submission,
            }));
            for batch in [Vec::new(), vec![0], vec![u8::MAX; BATCH_BYTES]] {
                frames.push(LinkFrame::Item(ClientProducerLinkItem::Submit {
                    key,
                    submission,
                    batch,
                }));
            }
        }
        for refusal in ClientProducerRefusal::iter() {
            frames.push(LinkFrame::Event(ClientProducerLinkEvent::Refused {
                key,
                refusal,
            }));
        }
        for reason in ClientProducerEndReason::iter() {
            frames.push(LinkFrame::Event(ClientProducerLinkEvent::Ended {
                key,
                reason,
            }));
        }
        for admission in ClientProducerAdmission::iter() {
            frames.push(LinkFrame::Event(ClientProducerLinkEvent::Admission {
                key,
                admission,
            }));
        }
    }
    for outcome in every_submission_outcome() {
        for detail in [
            None,
            Some(String::new()),
            Some("the node that executes the ingestor was lost \u{2014} \u{1F980}".to_string()),
        ] {
            frames.push(LinkFrame::Event(ClientProducerLinkEvent::Outcome {
                key: u64::MAX,
                submission: NonZeroU64::MAX,
                outcome,
                detail,
            }));
        }
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
            for admission in ClientProducerAdmission::iter() {
                frames.push(LinkFrame::Event(ClientProducerLinkEvent::Opened {
                    key: u64::MAX,
                    description: description_at(window, duration, admission),
                }));
            }
        }
    }
    let declared_most = ClientProducerLimits {
        batches: NonZeroU32::new(MAX_CLIENT_PRODUCER_BATCHES)
            .assured("a producer may hold a batch outstanding"),
        bytes: NonZeroU64::new(DEFAULT_CLIENT_PRODUCER_SESSION_BYTES)
            .assured("a producer may hold bytes outstanding"),
    };
    let least = ClientProducerLimits {
        batches: NonZeroU32::MIN,
        bytes: NonZeroU64::MIN,
    };
    let largest = ClientProducerLimits {
        batches: NonZeroU32::MAX,
        bytes: NonZeroU64::MAX,
    };
    for limits in [least, declared_most, largest] {
        for expected_fields in [Vec::new(), fields_of_every_type()] {
            frames.push(LinkFrame::Item(ClientProducerLinkItem::Open {
                key: u64::MAX,
                domain: DomainName::parse("orders").assured("a literal domain name"),
                ingestor: IngestorName::parse("intake").assured("a literal ingestor name"),
                expected_fields,
                limits,
                max_batch_bytes: limits.bytes,
            }));
        }
    }
    frames
}

/// The frames a random case may leave undrawn reach the other end whole on every run: each
/// refusal, outcome, admission state and end reason, counts at both ends of their range, policy
/// durations of zero, one nanosecond and the longest duration, and a schema of every field type.
#[nervix_primitives::test]
async fn every_boundary_frame_reaches_the_other_end_whole() {
    let executor = Executor::default();
    let codec = DuplexCodec::of::<OpenClientProducerLink>(&executor);
    let mut kinds = BTreeSet::new();
    // The frames derive no `Clone`, so the list is built once to send and once to compare with.
    for (sent, expected) in boundary_frames().into_iter().zip(boundary_frames()) {
        kinds.insert(expected.kind());
        let received = through_the_stream(&codec, sent).await;
        assert_eq!(received, expected);
    }
    let every_kind: BTreeSet<LinkFrameKind> = LinkFrameKind::iter().collect();
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
        let kind: LinkFrameKind = arbitrary.variant();
        let frame = kind.frame(&mut arbitrary);
        assert_eq!(frame.kind(), kind);
        drawn.insert(kind);
    }
    let every_kind: BTreeSet<LinkFrameKind> = LinkFrameKind::iter().collect();
    assert_eq!(drawn, every_kind);
}

/// A frame that forwards a batch of `length` bytes.
fn submit_of(length: usize) -> LinkFrame {
    LinkFrame::Item(ClientProducerLinkItem::Submit {
        key: u64::MAX,
        submission: NonZeroU64::MAX,
        batch: vec![0xa5; length],
    })
}

/// The largest batch the default session limits let a producer submit fits a frame of a link
/// under the default relay payload limit, so a batch such a session admitted is never refused for
/// its size on its way to the owning node.
#[nervix_primitives::test]
async fn the_largest_batch_a_default_session_submits_fits_a_frame() {
    let executor = Executor::default();
    let codec = DuplexCodec::of::<OpenClientProducerLink>(&executor);
    let largest = SessionLimits::DEFAULT.max_submitted_batch_bytes();
    let received = through_the_stream(&codec, submit_of(largest)).await;
    assert_eq!(received, submit_of(largest));
    assert_nothing_stays_charged(&executor);
}

/// The codec of a link carries a forwarded batch up to its class's payload limit and refuses the
/// first one past it with its typed encode failure, before anything is sent and with nothing left
/// charged.
#[nervix_primitives::test]
async fn a_batch_past_the_payload_limit_is_refused_with_the_typed_encode_failure() {
    let executor = executor_with_relay_payload_limit(NARROW_PAYLOAD_LIMIT);
    let codec = DuplexCodec::of::<OpenClientProducerLink>(&executor);
    assert_the_payload_limit_bounds_a_frame(&codec, NARROW_PAYLOAD_LIMIT, submit_of).await;
    assert_nothing_stays_charged(&executor);
}

/// A damaged frame of a link, a frame of one direction read as another's, and arbitrary bytes are
/// refused with the codec's typed decode failure, as bytes that do not decode and never as work
/// the node could not admit, or decode to a frame the codec keeps whole. Either way nothing stays
/// charged.
#[test]
fn bolero_damaged_producer_link_frames_fail_typed_or_decode_to_a_frame() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let codec = DuplexCodec::of::<OpenClientProducerLink>(&executor);
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // What shapes the case is read before the frame: an ordinary run's few bytes run out
            // while a frame is generated, and a damage read after that would truncate nearly every
            // frame to nothing.
            let damage = Damage::draw(arbitrary.entropy());
            let misdirected = arbitrary.entropy().byte().is_multiple_of(8);
            let read_as: LinkDirection = arbitrary.variant();
            let kind: LinkFrameKind = arbitrary.variant();
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
                assert_damaged_bytes_fail_typed_or_decode_to_a_frame::<LinkFrame>(
                    &codec, direction, bytes,
                )
                .await;
            });
            assert_nothing_stays_charged(&executor);
        });
}
