//! Typed node-to-node requests of a domain stop: the drain the leader's stop owner runs while the
//! domain stops, and the report each node's teardown keeps of what it cut.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The stop drain and stop report wire contracts and their pool, quota, deadline and
//!   coordination identity declarations.
//! - **Depends on.** The authenticated request surface and typed vocabulary identities.
//! - **Must not know.** How a node classifies its work, how a teardown cuts it, or what the stop's
//!   owner decides from the views.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::{CoordinationIdentity, DomainName, NodeRef, StopCut};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectRequest, PoolClass, RemoteOperationFailure, RequestSubquota};

/// What a node does for a stop's drain before it reports its view.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum StopDrainAction {
    /// Release the work in motion the node still holds: while it holds any, the node starts a
    /// force-flush generation unless one is already running.
    FlushMovingWork,
    /// Start the force-flush generation that confirms nothing is still moving.
    Confirm,
    /// Report the view without starting a generation.
    Observe,
}

/// Why an entity's work holds a stop open, or what it holds that the stop will cut.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum StopWaitKind {
    /// An ingestor that has not stopped admitting.
    AdmittingIngestor,
    /// A generator still producing.
    ActiveGenerator,
    /// Work a node has in hand or queued, or collected and route-buffered work a flush releases.
    NodeWork,
    /// Batches a relay holds for consumers that take them.
    RelayBatches,
    /// A relay that admitted a batch while the node read its view.
    AdmittingRelay,
    /// Records an emitter holds that its next flush publishes.
    EmitterBuffer,
    /// A publish attempt of an emitter on the wire, which runs to its outcome.
    PublishOnTheWire,
    /// Messages a `REQUIRED WAIT` retains until their state is present.
    RequiredWait,
    /// Records an emitter holds while it waits for a retry.
    EmitterRetry,
    /// Batches a relay holds for a consumer that takes nothing now.
    RelayBatchesHeld,
    /// Work a node holds while it waits on a full queue or takes nothing now.
    NodeWorkHeld,
    /// Rows a reorderer holds until their release time, records a correlator holds until their
    /// partner arrives, or rows a window retains whose acknowledgements stay pending until
    /// further input fills it.
    ProcessorHeld,
}

impl StopWaitKind {
    /// Whether the work is in motion, which the stop waits for, rather than held, which the stop
    /// does not wait for and its teardown cuts.
    pub fn in_motion(self) -> bool {
        match self {
            Self::AdmittingIngestor
            | Self::ActiveGenerator
            | Self::NodeWork
            | Self::RelayBatches
            | Self::AdmittingRelay
            | Self::EmitterBuffer
            | Self::PublishOnTheWire => true,
            Self::RequiredWait
            | Self::EmitterRetry
            | Self::RelayBatchesHeld
            | Self::NodeWorkHeld
            | Self::ProcessorHeld => false,
        }
    }

    /// The kind as the status of a stopping domain names it.
    pub fn describe(self) -> &'static str {
        match self {
            Self::AdmittingIngestor => "an ingestor still admitting",
            Self::ActiveGenerator => "a generator still producing",
            Self::NodeWork => "work a node has in hand or still releases",
            Self::RelayBatches => "batches a relay still delivers",
            Self::AdmittingRelay => "a relay admitting a batch",
            Self::EmitterBuffer => "records an emitter still publishes",
            Self::PublishOnTheWire => "a publish on the wire",
            Self::RequiredWait => "messages a REQUIRED WAIT retains",
            Self::EmitterRetry => "records an emitter holds for a retry",
            Self::RelayBatchesHeld => "batches a relay holds for a consumer that takes nothing",
            Self::NodeWorkHeld => "work a node holds that cannot move on",
            Self::ProcessorHeld => {
                "rows or records a processor holds until their time, their partner or further input"
            }
        }
    }
}

/// One entity's work of one kind on one node.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopWaitEntry {
    /// The entity whose work this counts, or `None` for work the node counts for the whole
    /// domain, as it counts the generators still producing.
    pub entity: Option<NodeRef>,
    pub kind: StopWaitKind,
    pub items: u64,
}

/// A node's view of a stopping domain.
#[derive(Debug, Clone, Default, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopDrainStatus {
    /// Every entity's work, in motion or held, that the view found.
    pub entries: Vec<StopWaitEntry>,
    /// Force-flush obligations the domain's participants that still take work have not
    /// completed.
    pub flush_obligations: u64,
    /// Every relay admission and hand-on the node has counted for the domain: a confirming round
    /// whose two views differ here saw work move.
    pub motion: u64,
}

impl StopDrainStatus {
    /// Whether the node holds work in motion.
    pub fn moving(&self) -> bool {
        self.entries.iter().any(|entry| entry.kind.in_motion())
    }
}

/// The view of a stopping domain the leader's stop owner asks a node for, after the drain step it
/// asks the node to take.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopDrainRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub action: StopDrainAction,
}

impl InterconnectRequest for StopDrainRequest {
    type Response = Result<StopDrainStatus, RemoteOperationFailure>;
    const NAME: &'static str = "domain_stop_drain";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

/// What a node's teardown cut for the stop that ended `domain` at `start_version`.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum StopReportAnswer {
    /// The node's teardown has not ended the domain's tasks yet.
    Pending,
    /// The node holds no report for that stop: it restarted, or never ran the domain.
    Unknown,
    /// Every cut of the node's teardown; empty when it cut nothing.
    Reported(Vec<StopCut>),
}

/// The report the leader's stop owner collects from a node once the stop ended.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct StopReportRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub start_version: u64,
}

impl InterconnectRequest for StopReportRequest {
    type Response = Result<StopReportAnswer, RemoteOperationFailure>;
    const NAME: &'static str = "domain_stop_report";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod wire_properties;
