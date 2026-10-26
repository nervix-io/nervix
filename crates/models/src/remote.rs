use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{ClusterNodeIdentity, Timestamp};

/// An acknowledgement or relay admission that one node process registered and waits to have
/// resolved by the node it sent the registration to.
///
/// Every process numbers its registrations from one, so the number alone repeats across the runs
/// of a node. The registration therefore also names the run that registered it, and every
/// resolution carries its registration back: a resolution addressed to an earlier run of a node
/// never resolves the entry a later run holds under the same number.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RemoteAckRegistration {
    pub ack_id: u64,
    /// The run of the node that registered `ack_id`, which every resolution is sent back to.
    pub registrar: ClusterNodeIdentity,
}

impl RemoteAckRegistration {
    /// The resolution of this registration with `outcome`, addressed to the run that registered it.
    pub fn resolution(&self, outcome: RemoteAckOutcome) -> RemoteAckResolution {
        RemoteAckResolution {
            registration: self.clone(),
            outcome,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RemoteAckResolution {
    /// The registration this resolves, which names the run it is addressed to.
    pub registration: RemoteAckRegistration,
    pub outcome: RemoteAckOutcome,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RemoteAckOutcome {
    Alive,
    /// Nonterminal progress of one remote ACK root. Its sequence fences a delayed status update
    /// from changing the upstream root after a newer park or resume was observed.
    Progress {
        sequence: u64,
        parked: bool,
    },
    Ack,
    NoAck(String),
}

impl RemoteAckOutcome {
    pub fn is_progress(&self) -> bool {
        matches!(self, Self::Alive | Self::Progress { .. })
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RemoteRuntimeRecordMetadata {
    pub ingested_at_low_watermark: Timestamp,
    pub ingested_at_high_watermark: Timestamp,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RemoteRuntimeField {
    pub name: String,
    pub value: RemoteRuntimeValue,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RemoteRuntimeValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    Datetime(String),
    F32(f32),
    F64(f64),
    Array(Vec<RemoteRuntimeElementValue>),
    Vec(Vec<RemoteRuntimeElementValue>),
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum RemoteRuntimeElementValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    Datetime(String),
    F32(f32),
    F64(f64),
    Array(#[rkyv(omit_bounds)] Vec<RemoteRuntimeElementValue>),
    Vec(#[rkyv(omit_bounds)] Vec<RemoteRuntimeElementValue>),
}
