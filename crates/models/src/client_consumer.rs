//! Capacity requested by application consumers of native client emitters.
//!
//! Layer: vocabulary.
//! - **Owns.** Consumer count and byte ceilings and the typed limits a consumer asks for.
//! - **Depends on.** Serialization.
//! - **Must not know.** Sessions, the interconnect, Arrow encoding, or delivery ownership.

use std::num::{NonZeroU32, NonZeroU64};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

pub const MAX_CLIENT_CONSUMERS_PER_SESSION: usize = 32;
pub const CLIENT_CONSUMER_SESSION_BYTES: u64 = 32 * 1024 * 1024;
pub const CLIENT_CONSUMER_NODE_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_CLIENT_CONSUMER_BATCHES: u32 = 1024;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ClientConsumerLimits {
    pub batches: NonZeroU32,
    pub bytes: NonZeroU64,
}

impl ClientConsumerLimits {
    pub fn is_within_bounds(self) -> bool {
        self.batches.get() <= MAX_CLIENT_CONSUMER_BATCHES
            && self.bytes.get() <= CLIENT_CONSUMER_SESSION_BYTES
    }
}

const _: () = {
    assert!(CLIENT_CONSUMER_SESSION_BYTES <= CLIENT_CONSUMER_NODE_BYTES);
    assert!(
        CLIENT_CONSUMER_SESSION_BYTES + crate::CLIENT_PRODUCER_SESSION_BYTES <= 64 * 1024 * 1024
    );
    assert!(CLIENT_CONSUMER_NODE_BYTES + crate::CLIENT_PRODUCER_NODE_BYTES <= 256 * 1024 * 1024);
};
