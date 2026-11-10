//! The resolved capacity claim of one concrete node run.
//!
//! Layer: vocabulary.
//! - **Owns.** CPU and memory capacity, transport, DNS, database, consensus, engine and connector host limits, peer receive bounds and the incarnation-bound claim.
//! - **Depends on.** Node identities and serialization primitives.
//! - **Must not know.** Detection, consensus, transport or placement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{num::NonZeroU64, time::Duration};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::ClusterNodeIdentity;

/// The engine limits one node has resolved for its compiled programs and guests.
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
pub struct EngineCapabilities {
    pub vm_inline_row_limit: u64,
    pub vm_regex_search_cache_bytes: NonZeroU64,
    pub vm_dynamic_pattern_cache_capacity: NonZeroU64,
    pub vm_pattern_sets_per_batch: NonZeroU64,
    pub wasm_stack_bytes: NonZeroU64,
    pub wasm_memory_reservation_bytes: u64,
    pub wasm_parallel_compilation: bool,
}

/// Local host cadence and intake limits, independent of external client configuration.
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
pub struct ConnectorHostCapabilities {
    pub retry_multiplier: std::num::NonZeroU32,
    pub retry_jitter_percent: u8,
    pub kafka_domain_assignment_retry: std::time::Duration,
    pub kafka_partition_watch_interval: std::time::Duration,
    pub syslog_intake_queue_capacity: NonZeroU64,
    pub syslog_read_chunk_bytes: NonZeroU64,
}

/// Positive limits the receiver enforces. They bound work; they never change cluster acceptance.
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
pub struct PeerReceiveBounds {
    pub relay_batch_bytes: NonZeroU64,
    pub relay_decoded_bytes: NonZeroU64,
    pub bulk_chunk_bytes: NonZeroU64,
    pub bulk_window_bytes: NonZeroU64,
}

impl PeerReceiveBounds {
    /// The common safe bound, retaining each receiver's independent dimensions.
    pub fn intersection(self, other: Self) -> Self {
        Self {
            relay_batch_bytes: self.relay_batch_bytes.min(other.relay_batch_bytes),
            relay_decoded_bytes: self.relay_decoded_bytes.min(other.relay_decoded_bytes),
            bulk_chunk_bytes: self.bulk_chunk_bytes.min(other.bulk_chunk_bytes),
            bulk_window_bytes: self.bulk_window_bytes.min(other.bulk_window_bytes),
        }
    }
}

/// The immutable transport capacity and deadline claim of one run.
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
pub struct InterconnectCapabilities {
    pub reconnect_jitter_permille: u16,
    pub max_peers: NonZeroU64,
    pub max_connections: NonZeroU64,
    pub concurrent_handshakes: NonZeroU64,
    pub incoming_queue_capacity: NonZeroU64,
    pub request_quota: NonZeroU64,
    pub completion_quota: NonZeroU64,
    pub health_probes: NonZeroU64,
    pub relay_watermarks: NonZeroU64,
    pub stream_window_bytes: u32,
    pub connection_window_bytes: u32,
    pub connection_setup_timeout: Duration,
    pub request_timeout: Duration,
    pub progress_timeout: Duration,
    pub reconnect_backoff: Duration,
    pub max_reconnect_backoff: Duration,
}

/// The selected resolver address families, carried as part of a node's local policy claim.
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
    strum::Display,
    strum::EnumString,
)]
#[strum(serialize_all = "kebab-case")]
pub enum DnsAddressFamilies {
    DualStack,
    Ipv4,
    Ipv6,
}
/// The resolved local DNS policy.
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
pub struct DnsCapabilities {
    pub cache_entries: u64,
    pub positive_max_ttl: Duration,
    pub negative_max_ttl: Duration,
    pub hook_lookup_budget: Duration,
    pub address_families: DnsAddressFamilies,
    pub trust_negative_answers: bool,
    pub concurrent_lookups: NonZeroU64,
}
/// Local consensus engine capacities and deadlines, as this run constructs them.
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
pub struct ConsensusCapabilities {
    pub event_capacity: u64,
    pub append_batch_entries: u64,
    pub outstanding_append_batches: u64,
    pub outstanding_append_bytes: u64,
    pub append_idle_bound: Duration,
    pub snapshot_transfer_timeout: Duration,
    pub membership_mutation_timeout: Duration,
    pub retention_interval: Duration,
    pub retention_admission_timeout: Duration,
    pub retention_admission_poll: Duration,
    pub purge_batch_entries: u64,
    pub api_channel_capacity: u64,
    pub notification_channel_capacity: u64,
    pub state_machine_channel_capacity: u64,
    pub api_batch_capacity: u64,
    pub snapshot_chunk_bytes: u64,
    pub replication_backoff: Duration,
    pub replication_backoff_maximum: Duration,
    pub membership_step_down_delay: Duration,
    pub replication_lag_threshold: u64,
}

/// What one node can carry. Unknown host measurements stay explicit.
///
/// This is also the sealed class of node-local capacity settings. Further sizing derivations
/// belong here and receive their policy inputs explicitly.
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
pub struct NodeCapabilities {
    pub engines: EngineCapabilities,
    pub connector_host: ConnectorHostCapabilities,
    pub cpu_count: Option<NonZeroU64>,
    pub memory_bytes: Option<NonZeroU64>,
    pub receive: PeerReceiveBounds,
    pub interconnect: InterconnectCapabilities,
    pub dns: DnsCapabilities,
    pub node_database: crate::DatabaseCapabilities,
    pub consensus_database: crate::DatabaseCapabilities,
    pub consensus: ConsensusCapabilities,
    pub listeners: crate::ListenerBounds,
}

impl NodeCapabilities {
    /// The limits a peer may use for this receiver after its claim commits.
    pub const fn peer_receive_bounds(&self) -> PeerReceiveBounds {
        self.receive
    }
}

/// One record per node name, replaced by a newer run and retained after departure until DROP NODE.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeCapabilityRecord {
    pub identity: ClusterNodeIdentity,
    pub capabilities: NodeCapabilities,
}

/// The peer-count input shared by transport capacity and DNS concurrency derivations.
pub struct MaxPeers;
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("interconnect-max-peers ({0}) must be positive")]
pub struct InvalidPeerCapacity(pub usize);
impl crate::SettingDefinition for MaxPeers {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = InvalidPeerCapacity;
    const NAME: &'static str = "interconnect-max-peers";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        64
    }
    fn validate(value: &usize, _: &()) -> Result<(), error_stack::Report<Self::Error>> {
        if *value == 0 {
            return Err(error_stack::Report::new(InvalidPeerCapacity(*value)));
        }
        Ok(())
    }
}
