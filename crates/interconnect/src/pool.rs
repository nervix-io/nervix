//! The independent connection pools that isolate internal traffic classes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The traffic classes a node separates its peer connections into, and the execution
//!   class, CPU class, and byte limits each one operates under.
//! - **Depends on.** The execution budget the limits are read from.
//! - **Must not know.** Connection lifetime, request dispatch, or what any message means.

use meticulous::OptionExt as _;
use nervix_execution::{CpuClass, Executor, MemoryClass};
use rkyv::{Archive, Deserialize, Serialize};
use strum::{AsRefStr, EnumCount};

use crate::RKYV_RECORD_OVERHEAD_BYTES;

/// The independent connection pools that isolate internal traffic classes.
#[derive(
    Debug,
    Clone,
    Copy,
    Archive,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    AsRefStr,
    EnumCount,
)]
#[strum(serialize_all = "snake_case")]
pub enum PoolClass {
    Management,
    Commands,
    Replication,
    Relay,
    Bulk,
}

impl PoolClass {
    pub const ALL: [Self; 5] = [
        Self::Management,
        Self::Commands,
        Self::Replication,
        Self::Relay,
        Self::Bulk,
    ];

    /// This class's position in the fixed observation arrays it indexes.
    pub const fn index(self) -> usize {
        match self {
            Self::Management => 0,
            Self::Commands => 1,
            Self::Replication => 2,
            Self::Relay => 3,
            Self::Bulk => 4,
        }
    }

    pub(crate) const PRECONNECTED: [Self; 4] = [
        Self::Management,
        Self::Commands,
        Self::Replication,
        Self::Relay,
    ];

    pub(crate) const fn is_preconnected(self) -> bool {
        matches!(
            self,
            Self::Management | Self::Commands | Self::Replication | Self::Relay
        )
    }

    pub(crate) fn preconnected_connections_per_peer() -> usize {
        let mut connections = 0usize;
        for class in Self::PRECONNECTED {
            connections = connections
                .checked_add(class.connections_per_peer())
                .assured("the fixed set of preconnected pool slots fits in usize");
        }
        connections
    }

    pub const fn connections_per_peer(self) -> usize {
        match self {
            Self::Relay => 2,
            Self::Management | Self::Commands | Self::Replication | Self::Bulk => 1,
        }
    }

    pub const fn stream_slots_per_connection(self) -> usize {
        match self {
            Self::Management | Self::Relay => 64,
            Self::Commands => 32,
            // One ordered append stream per follower, and room beside it for the ownership
            // handoff requests that share this pool.
            Self::Replication => 8,
            Self::Bulk => 4,
        }
    }

    pub(crate) const fn memory_class(self) -> MemoryClass {
        match self {
            Self::Management => MemoryClass::Management,
            Self::Commands | Self::Replication => MemoryClass::Commands,
            Self::Relay => MemoryClass::Relay,
            Self::Bulk => MemoryClass::Bulk,
        }
    }

    pub(crate) const fn cpu_class(self) -> CpuClass {
        match self {
            Self::Management | Self::Commands | Self::Replication => CpuClass::Control,
            Self::Relay => CpuClass::Data,
            Self::Bulk => CpuClass::Bulk,
        }
    }

    pub(crate) fn payload_limit(self, executor: &Executor) -> u64 {
        match self {
            Self::Management => executor.limits().management_event_bytes.as_u64(),
            Self::Commands => executor.limits().command_bytes.as_u64(),
            Self::Replication => executor.limits().replication_batch_bytes.as_u64(),
            Self::Relay => executor.limits().relay_encoded_bytes.as_u64(),
            Self::Bulk => executor
                .limits()
                .bulk_chunk_bytes
                .as_u64()
                .checked_add(RKYV_RECORD_OVERHEAD_BYTES)
                .assured("the bulk application limit leaves room inside a u64 for rkyv metadata"),
        }
    }

    pub(crate) fn control_body_limit(self, executor: &Executor) -> u64 {
        if self == Self::Bulk {
            return executor
                .limits()
                .bulk_chunk_bytes
                .as_u64()
                .checked_add(2 * RKYV_RECORD_OVERHEAD_BYTES)
                .assured(
                    "the bulk application limit leaves room inside a u64 for nested rkyv metadata",
                );
        }
        self.payload_limit(executor)
    }
}
