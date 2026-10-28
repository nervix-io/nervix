//! What one node must bring to reach the live cluster, and whether two nodes can run together.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The prerequisite record a node publishes in gossip and carries in its connection
//!   handshake, the rule that judges two records compatible, the typed mismatch that names the
//!   prerequisite and both values, and the record's gossip text.
//! - **Depends on.** Serialization crates and `humantime`'s duration rendering.
//! - **Must not know.** How a node detects or configures a value, how gossip or the handshake
//!   carries the record, or what a node does when a peer is refused.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{fmt, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use strum::{AsRefStr, Display, EnumIter, IntoEnumIterator as _};
use thiserror::Error;

/// How far apart two nodes' values of one timing prerequisite may be: the larger must stay below
/// this multiple of the smaller.
///
/// One rolling change may therefore move a timing by less than this factor. Raft derives its
/// minimum election timeout as six heartbeats, so the slowest heartbeat a compatible leader sends
/// is at most a third of the minimum election timeout of any follower it leads, and the rest of
/// that timeout is left for delivery.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the bounded-compatibility invariant every mixed pairing of timings must keep"
    )
)]
pub const TIMING_COMPATIBILITY_FACTOR: u32 = 2;

/// A digest over the trust anchors a node verifies its peers' certificates against.
///
/// Fingerprints are compared only for equality; no byte pattern means anything of its own.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct TrustRootFingerprint([u8; 32]);

impl TrustRootFingerprint {
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    pub const fn as_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The fingerprint of the one node-to-node wire contract a build speaks.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct WireContractFingerprint([u8; 32]);

impl WireContractFingerprint {
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    pub const fn as_digest(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Writes a digest as lowercase hexadecimal, the form a fingerprint reads as everywhere.
fn write_hex(formatter: &mut fmt::Formatter<'_>, digest: &[u8; 32]) -> fmt::Result {
    for byte in digest {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

/// Why text names no fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "text that spells no fingerprint is refused as a pure conversion outcome"
    )
)]
pub enum FingerprintTextError {
    #[error("a fingerprint is 64 hexadecimal digits, not {length} characters")]
    Length { length: usize },
    #[error("a fingerprint has a character that is not a hexadecimal digit")]
    NotHexadecimal,
}

/// Reads 64 hexadecimal digits, of either case, into the digest they spell.
fn read_hex(text: &str) -> Result<[u8; 32], FingerprintTextError> {
    let digits = text.as_bytes();
    if digits.len() != 64 {
        return Err(FingerprintTextError::Length {
            length: text.chars().count(),
        });
    }
    // Sixty-four digits split into thirty-two whole pairs, so nothing remains beside them.
    let (pairs, _) = digits.as_chunks::<2>();
    let mut digest = [0_u8; 32];
    for (byte, pair) in digest.iter_mut().zip(pairs) {
        let pair = std::str::from_utf8(pair.as_slice())
            .map_err(|_| FingerprintTextError::NotHexadecimal)?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| FingerprintTextError::NotHexadecimal)?;
    }
    Ok(digest)
}

macro_rules! fingerprint_text {
    ($fingerprint:ident) => {
        impl fmt::Display for $fingerprint {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(formatter, &self.0)
            }
        }

        impl fmt::Debug for $fingerprint {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($fingerprint), "("))?;
                write_hex(formatter, &self.0)?;
                formatter.write_str(")")
            }
        }

        impl std::str::FromStr for $fingerprint {
            type Err = FingerprintTextError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                read_hex(text).map(Self)
            }
        }

        impl Serialize for $fingerprint {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $fingerprint {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = String::deserialize(deserializer)?;
                let digest = read_hex(&text).map_err(de::Error::custom)?;
                Ok(Self(digest))
            }
        }
    };
}

fingerprint_text!(TrustRootFingerprint);
fingerprint_text!(WireContractFingerprint);

/// What a node promises about its Raft log before it acknowledges an append.
///
/// A build has exactly one promise; the record carries it so that a node built with another one is
/// refused instead of joining a cluster whose acknowledgements mean something else.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    AsRefStr,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum RaftLogDurability {
    /// Every append is synchronized to stable storage, data and metadata, before it is
    /// acknowledged.
    #[strum(serialize = "fsync every append")]
    #[serde(rename = "fsync-every-append")]
    FsyncEveryAppend,
}

/// The largest operations a node accepts on the paths consensus needs to join: one semantic
/// command and one management or discovery event.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ConsensusFloors {
    pub largest_command_bytes: u64,
    pub largest_management_event_bytes: u64,
}

/// One timing prerequisite. Its name is the option a node is given it with.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    EnumIter,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum TimingPrerequisite {
    RaftHeartbeatInterval,
    GossipInterval,
    ClockSkewTolerance,
}

impl TimingPrerequisite {
    /// The setting's name, which its option and its setting definition both use.
    pub const fn setting_name(self) -> &'static str {
        match self {
            Self::RaftHeartbeatInterval => "raft-heartbeat-interval",
            Self::GossipInterval => "gossip-interval",
            Self::ClockSkewTolerance => "clock-skew-tolerance",
        }
    }
}

impl fmt::Display for TimingPrerequisite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.setting_name())
    }
}

/// The timings a node brings to the cluster. Two nodes may hold different values of each while a
/// rolling change moves it, as long as every pairing stays within [`TIMING_COMPATIBILITY_FACTOR`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct PrerequisiteTimings {
    pub raft_heartbeat_interval: Duration,
    pub gossip_interval: Duration,
    pub clock_skew_tolerance: Duration,
}

impl PrerequisiteTimings {
    pub const fn get(&self, prerequisite: TimingPrerequisite) -> Duration {
        match prerequisite {
            TimingPrerequisite::RaftHeartbeatInterval => self.raft_heartbeat_interval,
            TimingPrerequisite::GossipInterval => self.gossip_interval,
            TimingPrerequisite::ClockSkewTolerance => self.clock_skew_tolerance,
        }
    }

    /// The first timing whose two values are too far apart for the nodes to run together.
    fn first_beyond_bound(&self, peer: &Self) -> Option<PrerequisiteMismatch> {
        for prerequisite in TimingPrerequisite::iter() {
            let local = self.get(prerequisite);
            let peer = peer.get(prerequisite);
            if !timings_coexist(local, peer) {
                return Some(PrerequisiteMismatch::Timing {
                    prerequisite,
                    local,
                    peer,
                });
            }
        }
        None
    }

    /// The timings whose values differ from `other`'s, compatible or not.
    pub fn differing(&self, other: &Self) -> Vec<TimingPrerequisite> {
        let mut differing = Vec::new();
        for prerequisite in TimingPrerequisite::iter() {
            if self.get(prerequisite) != other.get(prerequisite) {
                differing.push(prerequisite);
            }
        }
        differing
    }
}

/// Whether two values of one timing can run together: equal, or the larger below
/// [`TIMING_COMPATIBILITY_FACTOR`] times the smaller.
fn timings_coexist(first: Duration, second: Duration) -> bool {
    if first == second {
        return true;
    }
    let (smaller, larger) = if first < second {
        (first, second)
    } else {
        (second, first)
    };
    let bound = smaller
        .as_nanos()
        .checked_mul(u128::from(TIMING_COMPATIBILITY_FACTOR))
        .assured("a duration's nanoseconds fit 94 bits, so a small multiple of them fits 128");
    larger.as_nanos() < bound
}

/// Everything one node brings to reach the live cluster.
///
/// A node publishes it in its gossip record and carries it in every connection handshake. Two
/// nodes run together only when [`NodePrerequisites::verify_peer`] accepts their records.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct NodePrerequisites {
    pub cluster_identity: String,
    pub trust_root: TrustRootFingerprint,
    pub wire_contract: WireContractFingerprint,
    pub consensus_floors: ConsensusFloors,
    pub durability: RaftLogDurability,
    pub timings: PrerequisiteTimings,
}

impl NodePrerequisites {
    /// Whether a node with `peer`'s record can run beside this one, and if not, the first
    /// prerequisite that keeps them apart, seen from this node.
    ///
    /// Identity, trust, wire contract, consensus floors and durability must be equal. Each timing
    /// must stay within [`TIMING_COMPATIBILITY_FACTOR`] of the other node's. The judgment is
    /// symmetric: reversing the two records reverses the mismatch, never removes it.
    pub fn verify_peer(&self, peer: &Self) -> Result<(), PrerequisiteMismatch> {
        if self.cluster_identity != peer.cluster_identity {
            return Err(PrerequisiteMismatch::ClusterIdentity {
                local: self.cluster_identity.clone(),
                peer: peer.cluster_identity.clone(),
            });
        }
        if self.trust_root != peer.trust_root {
            return Err(PrerequisiteMismatch::TrustRoot {
                local: self.trust_root,
                peer: peer.trust_root,
            });
        }
        if self.wire_contract != peer.wire_contract {
            return Err(PrerequisiteMismatch::WireContract {
                local: self.wire_contract,
                peer: peer.wire_contract,
            });
        }
        if self.consensus_floors.largest_command_bytes
            != peer.consensus_floors.largest_command_bytes
        {
            return Err(PrerequisiteMismatch::LargestCommand {
                local: self.consensus_floors.largest_command_bytes,
                peer: peer.consensus_floors.largest_command_bytes,
            });
        }
        if self.consensus_floors.largest_management_event_bytes
            != peer.consensus_floors.largest_management_event_bytes
        {
            return Err(PrerequisiteMismatch::LargestManagementEvent {
                local: self.consensus_floors.largest_management_event_bytes,
                peer: peer.consensus_floors.largest_management_event_bytes,
            });
        }
        if self.durability != peer.durability {
            return Err(PrerequisiteMismatch::Durability {
                local: self.durability,
                peer: peer.durability,
            });
        }
        match self.timings.first_beyond_bound(&peer.timings) {
            Some(mismatch) => Err(mismatch),
            None => Ok(()),
        }
    }

    /// Every prerequisite with its value written the way an operator reads it, in the order
    /// [`NodePrerequisites::verify_peer`] compares them.
    pub fn described(&self) -> Vec<DescribedPrerequisite> {
        let mut described = vec![
            DescribedPrerequisite {
                prerequisite: Prerequisite::ClusterIdentity,
                value: self.cluster_identity.clone(),
            },
            DescribedPrerequisite {
                prerequisite: Prerequisite::TrustRoot,
                value: self.trust_root.to_string(),
            },
            DescribedPrerequisite {
                prerequisite: Prerequisite::WireContract,
                value: self.wire_contract.to_string(),
            },
            DescribedPrerequisite {
                prerequisite: Prerequisite::LargestCommand,
                value: ByteCount(self.consensus_floors.largest_command_bytes).to_string(),
            },
            DescribedPrerequisite {
                prerequisite: Prerequisite::LargestManagementEvent,
                value: ByteCount(self.consensus_floors.largest_management_event_bytes).to_string(),
            },
            DescribedPrerequisite {
                prerequisite: Prerequisite::Durability,
                value: self.durability.to_string(),
            },
        ];
        for timing in TimingPrerequisite::iter() {
            described.push(DescribedPrerequisite {
                prerequisite: Prerequisite::from(timing),
                value: DurationText(self.timings.get(timing)).to_string(),
            });
        }
        described
    }

    /// The record as its gossip value carries it.
    pub fn to_gossip_text(&self) -> String {
        serde_json::to_string(self)
            .assured("the record holds only strings, digests, counts and durations JSON can write")
    }

    /// Reads a record from its gossip value.
    pub fn from_gossip_text(text: &str) -> Result<Self, Report<PrerequisiteRecordError>> {
        serde_json::from_str(text)
            .map_err(|error| Report::new(error).change_context(PrerequisiteRecordError))
    }
}

/// One prerequisite and its value, written the way an operator reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedPrerequisite {
    pub prerequisite: Prerequisite,
    pub value: String,
}

/// Writes a timing the way an operator gives it, such as `250ms` or `1s 500ms`.
pub fn timing_text(timing: Duration) -> String {
    DurationText(timing).to_string()
}

/// A gossip value that names no prerequisite record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("a gossip value holds no prerequisite record")]
pub struct PrerequisiteRecordError;

/// One prerequisite, named the way an operator reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, AsRefStr, EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum Prerequisite {
    ClusterIdentity,
    TrustRoot,
    WireContract,
    LargestCommand,
    LargestManagementEvent,
    Durability,
    RaftHeartbeatInterval,
    GossipInterval,
    ClockSkewTolerance,
}

impl fmt::Display for Prerequisite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ClusterIdentity => "cluster identity",
            Self::TrustRoot => "trust root",
            Self::WireContract => "wire contract",
            Self::LargestCommand => "largest command",
            Self::LargestManagementEvent => "largest management event",
            Self::Durability => "durability promise",
            Self::RaftHeartbeatInterval => TimingPrerequisite::RaftHeartbeatInterval.setting_name(),
            Self::GossipInterval => TimingPrerequisite::GossipInterval.setting_name(),
            Self::ClockSkewTolerance => TimingPrerequisite::ClockSkewTolerance.setting_name(),
        };
        formatter.write_str(name)
    }
}

impl From<TimingPrerequisite> for Prerequisite {
    fn from(timing: TimingPrerequisite) -> Self {
        match timing {
            TimingPrerequisite::RaftHeartbeatInterval => Self::RaftHeartbeatInterval,
            TimingPrerequisite::GossipInterval => Self::GossipInterval,
            TimingPrerequisite::ClockSkewTolerance => Self::ClockSkewTolerance,
        }
    }
}

/// Writes a byte count in the largest binary unit that holds it exactly.
struct ByteCount(u64);

impl fmt::Display for ByteCount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        const UNITS: [(u64, &str); 3] = [(1 << 30, "GiB"), (1 << 20, "MiB"), (1 << 10, "KiB")];
        for (size, unit) in UNITS {
            if self.0 != 0 && self.0.is_multiple_of(size) {
                let count = self
                    .0
                    .checked_div(size)
                    .verified("the unit size is nonzero");
                return write!(formatter, "{count}{unit}");
            }
        }
        write!(formatter, "{}B", self.0)
    }
}

/// Writes a duration the way an operator gives it, such as `250ms` or `1s 500ms`.
struct DurationText(Duration);

impl fmt::Display for DurationText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.0).fmt(formatter)
    }
}

/// The first prerequisite two nodes disagree on, with both values, seen from the node that judged
/// the pairing: `local` is that node's own value and `peer` the other node's.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Error,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a judged incompatibility between two prerequisite records is the answer a \
                  handshake carries, not a failure of the judging node"
    )
)]
pub enum PrerequisiteMismatch {
    #[error("cluster identity differs: this node is in cluster '{local}', the peer in '{peer}'")]
    ClusterIdentity { local: String, peer: String },
    #[error("trust root differs: this node trusts {local}, the peer {peer}")]
    TrustRoot {
        local: TrustRootFingerprint,
        peer: TrustRootFingerprint,
    },
    #[error("wire contract differs: this node speaks {local}, the peer {peer}")]
    WireContract {
        local: WireContractFingerprint,
        peer: WireContractFingerprint,
    },
    #[error(
        "largest command differs: this node accepts {}, the peer {}",
        ByteCount(*local),
        ByteCount(*peer)
    )]
    LargestCommand { local: u64, peer: u64 },
    #[error(
        "largest management event differs: this node accepts {}, the peer {}",
        ByteCount(*local),
        ByteCount(*peer)
    )]
    LargestManagementEvent { local: u64, peer: u64 },
    #[error("durability promise differs: this node promises '{local}', the peer '{peer}'")]
    Durability {
        local: RaftLogDurability,
        peer: RaftLogDurability,
    },
    #[error(
        "{prerequisite} is {} on this node and {} on the peer, not within a factor of \
         {TIMING_COMPATIBILITY_FACTOR}",
        DurationText(*local),
        DurationText(*peer)
    )]
    Timing {
        prerequisite: TimingPrerequisite,
        local: Duration,
        peer: Duration,
    },
}

impl PrerequisiteMismatch {
    /// The prerequisite the two nodes disagree on.
    pub const fn prerequisite(&self) -> Prerequisite {
        match self {
            Self::ClusterIdentity { .. } => Prerequisite::ClusterIdentity,
            Self::TrustRoot { .. } => Prerequisite::TrustRoot,
            Self::WireContract { .. } => Prerequisite::WireContract,
            Self::LargestCommand { .. } => Prerequisite::LargestCommand,
            Self::LargestManagementEvent { .. } => Prerequisite::LargestManagementEvent,
            Self::Durability { .. } => Prerequisite::Durability,
            Self::Timing { prerequisite, .. } => match prerequisite {
                TimingPrerequisite::RaftHeartbeatInterval => Prerequisite::RaftHeartbeatInterval,
                TimingPrerequisite::GossipInterval => Prerequisite::GossipInterval,
                TimingPrerequisite::ClockSkewTolerance => Prerequisite::ClockSkewTolerance,
            },
        }
    }

    /// The same disagreement seen from the other node: its value becomes the local one.
    #[must_use]
    pub fn reversed(self) -> Self {
        match self {
            Self::ClusterIdentity { local, peer } => Self::ClusterIdentity {
                local: peer,
                peer: local,
            },
            Self::TrustRoot { local, peer } => Self::TrustRoot {
                local: peer,
                peer: local,
            },
            Self::WireContract { local, peer } => Self::WireContract {
                local: peer,
                peer: local,
            },
            Self::LargestCommand { local, peer } => Self::LargestCommand {
                local: peer,
                peer: local,
            },
            Self::LargestManagementEvent { local, peer } => Self::LargestManagementEvent {
                local: peer,
                peer: local,
            },
            Self::Durability { local, peer } => Self::Durability {
                local: peer,
                peer: local,
            },
            Self::Timing {
                prerequisite,
                local,
                peer,
            } => Self::Timing {
                prerequisite,
                local: peer,
                peer: local,
            },
        }
    }
}

#[cfg(test)]
#[path = "prerequisites_tests.rs"]
mod tests;
