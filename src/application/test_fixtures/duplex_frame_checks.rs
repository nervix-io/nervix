//! What the frames of a forwarded client stream are held to through the stream's bounded codec.
//!
//! Layer: test harness.
//!
//! - **Owns.** What it means for a frame to reach the other end of its duplex stream whole, the
//!   verdict on damaged bytes read as a frame, the scan that finds the largest frame a payload
//!   limit carries, and the runtime and executors the frame properties run under.
//! - **Depends on.** The interconnect's duplex codec and the executor.
//! - **Must not know.** Which frames a stream carries or what they make either end do: the client
//!   producer link and the client consumer stream each describe their own.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::fmt;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{ExecutionConfig, ExecutionError, Executor, OperationLimits};
use nervix_interconnect::{DuplexCodec, TransportError};
use ubyte::ByteUnit;

/// One frame of a duplex stream, in whichever direction it travels. The opening message, the
/// initiator's frames and the responder's answers are different types on the wire, and the
/// receiver of each direction decodes only its own.
pub(in crate::application) trait StreamFrame:
    Sized + PartialEq + fmt::Debug
{
    /// Which end of the stream a frame travels from, and so the type its receiver decodes.
    type Direction: Copy;

    fn direction(&self) -> Self::Direction;

    /// The frame's bytes as its stream carries them, under the class and the payload limit the
    /// codec of the stream applies.
    async fn encoded(self, codec: &DuplexCodec) -> Result<Vec<u8>, Report<TransportError>>;

    /// What the receiver of `direction` decodes `bytes` to.
    async fn decoded(
        direction: Self::Direction,
        codec: &DuplexCodec,
        bytes: Vec<u8>,
    ) -> Result<Self, Report<TransportError>>;
}

pub(in crate::application) fn property_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens")
}

/// An executor whose relay payload limit is `payload_limit` bytes, with the default everything
/// else, so that a check meets the limit with frames of a few kibibytes.
pub(in crate::application) fn executor_with_relay_payload_limit(payload_limit: u64) -> Executor {
    Executor::new(ExecutionConfig {
        limits: OperationLimits {
            relay_encoded_bytes: ByteUnit::Byte(payload_limit),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("a narrower relay payload limit keeps the default budgets valid")
}

pub(in crate::application) fn assert_nothing_stays_charged(executor: &Executor) {
    assert_eq!(
        executor.snapshot().relay_memory.reserved_bytes,
        0,
        "no relay charge outlives its frame"
    );
}

/// `frame` as the other end of its stream receives it: encoded as the stream carries it and decoded
/// as the receiver of its direction decodes it.
pub(in crate::application) async fn through_the_stream<F: StreamFrame>(
    codec: &DuplexCodec,
    frame: F,
) -> F {
    let direction = frame.direction();
    let bytes = frame
        .encoded(codec)
        .await
        .assured("a frame within the payload limit of its stream encodes");
    F::decoded(direction, codec, bytes)
        .await
        .assured("a frame the codec of its stream encoded decodes")
}

/// Holds `damaged` bytes, read as a frame of `direction`, to the codec's contract: they are refused
/// with the typed decode failure, as bytes that do not decode and never as work the node could not
/// admit or execute, or they decode to a frame the codec keeps whole.
pub(in crate::application) async fn assert_damaged_bytes_fail_typed_or_decode_to_a_frame<
    F: StreamFrame,
>(
    codec: &DuplexCodec,
    direction: F::Direction,
    damaged: Vec<u8>,
) {
    let first = match F::decoded(direction, codec, damaged.clone()).await {
        Ok(frame) => frame,
        Err(refused) => {
            assert!(
                matches!(refused.current_context(), TransportError::Decode(_)),
                "damaged bytes are refused as bytes that do not decode: {refused:?}"
            );
            assert!(
                refused.downcast_ref::<ExecutionError>().is_none(),
                "damaged bytes are refused for what they hold, never as work the node could not \
                 admit or execute: {refused:?}"
            );
            return;
        }
    };
    // A frame derives no `Clone`, and the same bytes always decode to the same frame.
    let again = F::decoded(direction, codec, damaged)
        .await
        .assured("bytes that decoded once decode again");
    let rewritten = again
        .encoded(codec)
        .await
        .assured("a frame the codec decoded encodes within the limit it arrived under");
    let restored = F::decoded(direction, codec, rewritten)
        .await
        .assured("a frame the codec encoded decodes");
    assert_eq!(
        restored, first,
        "an accepted frame is one the codec keeps whole"
    );
}

/// Holds the codec to its payload limit of `payload_limit` bytes over the frames
/// `frame_holding` builds around a payload of each length up to the limit: every frame up to the
/// largest one the limit carries stays within the limit and reaches the other end whole, and every
/// longer one is refused with the typed encode failure. A frame's own fields take part of the
/// limit, so the largest payload is a little shorter than it.
pub(in crate::application) async fn assert_the_payload_limit_bounds_a_frame<F: StreamFrame>(
    codec: &DuplexCodec,
    payload_limit: u64,
    frame_holding: impl Fn(usize) -> F,
) {
    let longest = usize::try_from(payload_limit).assured("a narrowed limit fits in usize");
    let mut largest_carried = None;
    let mut first_refused = None;
    for length in 0..=longest {
        let frame = frame_holding(length);
        let direction = frame.direction();
        match frame.encoded(codec).await {
            Ok(bytes) => {
                assert!(
                    first_refused.is_none(),
                    "a payload of {length} bytes encoded after a shorter one was refused"
                );
                let encoded = u64::try_from(bytes.len()).assured("a bounded frame length");
                assert!(
                    encoded <= payload_limit,
                    "an encoded frame of {encoded} bytes is past the limit of {payload_limit}"
                );
                let received = F::decoded(direction, codec, bytes)
                    .await
                    .assured("a frame within the limit decodes");
                assert_eq!(received, frame_holding(length));
                largest_carried = Some(length);
            }
            Err(refused) => {
                assert!(
                    matches!(refused.current_context(), TransportError::Encode(_)),
                    "a frame past the limit is refused as one that does not encode: {refused:?}"
                );
                if first_refused.is_none() {
                    first_refused = Some(length);
                }
            }
        }
    }
    let largest_carried = largest_carried.assured("a frame without a payload fits the limit");
    let first_refused = first_refused
        .assured("a payload as long as the whole limit leaves no room for the frame around it");
    assert_eq!(
        largest_carried.checked_add(1),
        Some(first_refused),
        "every payload up to the largest one is carried and every longer one refused"
    );
}
