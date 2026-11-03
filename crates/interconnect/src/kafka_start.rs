//! Coordinate one canonical explicit Kafka start across independently owned intake units.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Start admission metadata and references to bounded bulk position transfers.
//! - **Depends on.** Placement vocabulary and the typed interconnect request contract.
//! - **Must not know.** Brokers, native checkpoint encoding or runtime ownership decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns Kafka start wire contracts")
)]

use std::time::Duration;

use nervix_models::ClusterNodeIdentity;
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    InterconnectRequest, PoolClass, RemoteOperationFailure, StatePlacementEnvelope,
    StateSnapshotEnvelope,
};

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafkaStartAction {
    Read,
    Propose { snapshot: StateSnapshotEnvelope },
    Acknowledge { start_generation: u64 },
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct KafkaStartRequest {
    pub placement: StatePlacementEnvelope,
    pub source: ClusterNodeIdentity,
    pub generation: u64,
    pub ownership_fingerprint: [u8; 32],
    pub action: KafkaStartAction,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum KafkaStartResponse {
    Unresolved,
    Current {
        generation: u64,
    },
    Seed {
        generation: u64,
        snapshot: StateSnapshotEnvelope,
    },
}

impl InterconnectRequest for KafkaStartRequest {
    type Response = Result<KafkaStartResponse, RemoteOperationFailure>;
    const NAME: &'static str = "coordinate_kafka_start";
    const CLASS: PoolClass = PoolClass::Commands;
    const TIMEOUT: Duration = Duration::from_secs(60);
}
