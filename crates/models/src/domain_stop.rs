//! A domain's stop: the requests a stopping domain serves, and how the stop that ended it ended.
//!
//! Layer: vocabulary.
//! - **Owns.** The cause a stop records, the requests a stopping domain serves (each operator's,
//!   by the execution reference it was requested under, and the licensing stop gate's), how the
//!   stop that ended a domain ended, what its teardown cut, and the transitions of the domain record
//!   that keep them.
//! - **Depends on.** The licensing stop reason, the wait limit clause, node and model names, and
//!   serialization.
//! - **Must not know.** Consensus application, lifecycle execution, draining or policy evaluation.

use std::{collections::BTreeMap, time::Duration};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;

use crate::{
    ClusterNodeName, CommandExecutionReference, DomainStatus, LicensingStopReason, ModelKind,
    ModelName, Timestamp, WaitLimit,
};

/// How long the licensing stop waits for the work in motion before it ends the domain's tasks by
/// force, counted in committed licensing time from the expiry that closed the stop gate.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "a licensing restriction, not a tunable: a temporary license must leave the \
                  cluster stopped one minute after its expiry, so no setting, flag, clause or \
                  test build shortens or lengthens it"
    )
)]
pub const LICENSING_STOP_ENFORCE_AFTER: Duration = Duration::from_secs(60);

/// Why a domain stopped, as the stop that ended it records it.
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
pub enum DomainStopCause {
    /// An operator's `STOP`, a transaction's `STOP` step, or the rollback of a `START` whose
    /// application failed.
    Operator,
    /// The licensing stop gate closed while the domain ran (specification §10.7).
    Licensing(LicensingStopReason),
}

/// The identity of one request a stop serves: what the status of a stopping domain names, and what
/// the ending of an enforced stop names as the request that enforced it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum StopRequestId {
    /// A `STOP` statement or a transaction's `STOP` step, by the execution reference it was
    /// requested under.
    Operator(CommandExecutionReference),
    /// The licensing stop gate's request.
    Licensing,
}

impl std::fmt::Display for StopRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operator(reference) => f.write_str(reference.as_str()),
            Self::Licensing => f.write_str("licensing"),
        }
    }
}

/// One operator's request to stop a domain.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperatorStopRequest {
    /// When the request was made, as the leader that admitted it read its clock.
    pub requested_at: Timestamp,
    /// The clause that forces or calls off the request once its time has passed, if it has one.
    pub limit: Option<WaitLimit>,
}

impl OperatorStopRequest {
    /// When the request's clause acts, or `None` without a clause or when no instant can hold it.
    pub fn limit_at(&self) -> Option<Timestamp> {
        let limit = self.limit?;
        self.requested_at.checked_add(limit.after()).ok()
    }
}

/// A request to stop a domain, as a `STOP` statement, a transaction's `STOP` step or the licensing
/// stop gate makes it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum StopRequest {
    Operator {
        reference: CommandExecutionReference,
        request: OperatorStopRequest,
    },
    Licensing(LicensingStopReason),
}

impl StopRequest {
    pub fn id(&self) -> StopRequestId {
        match self {
            Self::Operator { reference, .. } => StopRequestId::Operator(reference.clone()),
            Self::Licensing(_) => StopRequestId::Licensing,
        }
    }
}

/// The requests a stopping domain serves. A stopping domain always holds at least one.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainStopping {
    /// The consensus revision whose entry began the stop. A stop called off and begun again is
    /// another stop, which the owner that drained the first one may not end.
    pub began_at_revision: u64,
    /// Each operator's request, by the execution reference it was requested under.
    pub operators: BTreeMap<CommandExecutionReference, OperatorStopRequest>,
    /// The licensing stop gate's request, which is forced a fixed time after the expiry and which
    /// no `CANCEL` withdraws.
    pub licensing: Option<LicensingStopReason>,
}

impl DomainStopping {
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty() && self.licensing.is_none()
    }

    /// The identity of every request, the licensing request first.
    pub fn request_ids(&self) -> Vec<StopRequestId> {
        let mut ids = Vec::new();
        if self.licensing.is_some() {
            ids.push(StopRequestId::Licensing);
        }
        for reference in self.operators.keys() {
            ids.push(StopRequestId::Operator(reference.clone()));
        }
        ids
    }

    pub fn holds(&self, id: &StopRequestId) -> bool {
        match id {
            StopRequestId::Operator(reference) => self.operators.contains_key(reference),
            StopRequestId::Licensing => self.licensing.is_some(),
        }
    }

    /// Why the domain stops: the licensing cause while the licensing request takes part.
    pub fn cause(&self) -> DomainStopCause {
        match self.licensing {
            Some(reason) => DomainStopCause::Licensing(reason),
            None => DomainStopCause::Operator,
        }
    }

    /// The earliest operator `ENFORCE` and when it acts, in the clock its requests were read
    /// with; ties go to the request whose reference sorts first.
    pub fn earliest_operator_enforcement(&self) -> Option<(Timestamp, StopRequestId)> {
        let mut earliest: Option<(Timestamp, StopRequestId)> = None;
        for (reference, request) in &self.operators {
            let Some(WaitLimit::Enforce { .. }) = request.limit else {
                continue;
            };
            let Some(at) = request.limit_at() else {
                continue;
            };
            let earlier = match &earliest {
                Some((current, _)) => at < *current,
                None => true,
            };
            if earlier {
                earliest = Some((at, StopRequestId::Operator(reference.clone())));
            }
        }
        earliest
    }

    /// When the licensing request forces the stop, in committed licensing time, or `None` without
    /// a licensing request or when no instant can hold it.
    pub fn licensing_enforcement(&self) -> Option<Timestamp> {
        let LicensingStopReason::EphemeralExpired { expired_at } = self.licensing?;
        expired_at.checked_add(LICENSING_STOP_ENFORCE_AFTER).ok()
    }

    /// Every operator request whose `CANCEL` has acted by `now`, in the clock its requests were
    /// read with.
    pub fn cancelled_by(&self, now: Timestamp) -> Vec<CommandExecutionReference> {
        let mut cancelled = Vec::new();
        for (reference, request) in &self.operators {
            let Some(WaitLimit::Cancel { .. }) = request.limit else {
                continue;
            };
            if let Some(at) = request.limit_at()
                && at <= now
            {
                cancelled.push(reference.clone());
            }
        }
        cancelled
    }

    /// The earliest instant any operator clause acts, so its owner knows when to look again.
    pub fn next_operator_limit(&self) -> Option<Timestamp> {
        let mut next: Option<Timestamp> = None;
        for request in self.operators.values() {
            let Some(at) = request.limit_at() else {
                continue;
            };
            next = Some(match next {
                Some(current) => current.min(at),
                None => at,
            });
        }
        next
    }
}

/// How the stop that ended a domain ended.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum StopEnding {
    /// Every message in motion came to rest before the domain's tasks ended, and the final
    /// snapshot was written.
    Completed,
    /// A request's `ENFORCE`, or the licensing stop's fixed time, ended the domain's tasks while
    /// work was still in motion; the last snapshot was left as it was.
    Enforced { by: StopRequestId },
}

impl StopEnding {
    pub fn enforced(&self) -> bool {
        matches!(self, Self::Enforced { .. })
    }
}

/// What a stop's teardown cut, by kind. Every kind but [`StopCutKind::Enforced`] is held work
/// whose kind has no saved form yet, cut as a stop has always cut it and answered "not processed".
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum StopCutKind {
    /// Work still in motion that an enforcement cut.
    Enforced,
    /// The acknowledgements still pending for rows resting in a window.
    WindowAcknowledgements,
    /// Rows a reorderer holds until their release time.
    ReordererRows,
    /// Records a correlator holds until their partner arrives.
    CorrelatorRecords,
    /// Messages a `REQUIRED WAIT` retains until their state is present.
    RequiredWaitMessages,
    /// Records an emitter had not delivered, waiting for a retry or for a destination that did
    /// not take them.
    EmitterUndelivered,
    /// The message an entry point holds at the limit of branches.
    BranchLimitEntry,
    /// Batches a relay holds that its consumers cannot take.
    RelayBatches,
    /// Batches a node still holds that cannot move on.
    NodeBatches,
    /// Payloads an ingestor's quiesce buffer retained for a pause still open at the stop.
    IngestorPauseBuffer,
}

impl StopCutKind {
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// Why a stop's teardown cuts what an entity holds of this kind.
    pub fn reason(self) -> StopCutReason {
        match self {
            Self::Enforced => StopCutReason::Enforcement,
            Self::IngestorPauseBuffer => StopCutReason::SessionEnded,
            Self::WindowAcknowledgements
            | Self::ReordererRows
            | Self::CorrelatorRecords
            | Self::RequiredWaitMessages
            | Self::EmitterUndelivered
            | Self::BranchLimitEntry
            | Self::RelayBatches
            | Self::NodeBatches => StopCutReason::NotSavedYet,
        }
    }

    /// The kind as an answer and the domain's description name it.
    pub fn describe(self) -> &'static str {
        match self {
            Self::Enforced => "work in motion cut by enforcement",
            Self::WindowAcknowledgements => "acknowledgements pending for rows resting in a window",
            Self::ReordererRows => "rows a reorderer holds",
            Self::CorrelatorRecords => "records a correlator holds",
            Self::RequiredWaitMessages => "messages a REQUIRED WAIT retains",
            Self::EmitterUndelivered => "an emitter's undelivered records",
            Self::BranchLimitEntry => "the message an entry point holds at the limit of branches",
            Self::RelayBatches => "batches a relay holds that its consumers cannot take",
            Self::NodeBatches => "batches a node holds that cannot move on",
            Self::IngestorPauseBuffer => "payloads an ingestor's pause buffer retained",
        }
    }
}

/// Why a stop's teardown cut what an entity held, which its answer reports in a clause of its
/// own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCutReason {
    /// An enforcement ended work that was still in motion.
    Enforcement,
    /// The kind has no saved form yet, so a stop cuts what it holds rather than wait for it.
    NotSavedYet,
    /// A stop ends an ingestor's session whatever its `ON QUIESCE` declares, and with it the
    /// payloads a pause still open at the stop retained; no saved form is planned for them.
    SessionEnded,
}

/// One entity's cut of one kind on one node.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StopCut {
    pub entity_kind: ModelKind,
    pub entity: ModelName,
    pub kind: StopCutKind,
    /// Rows, records, messages, batches or payloads, as the kind counts them.
    pub items: u64,
}

/// What one node's teardown cut.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum NodeStopReport {
    /// Every cut of the node's teardown, by entity; empty when it cut nothing.
    Reported { cuts: Vec<StopCut> },
    /// The node left, or restarted, before its report was collected, so what it cut is unknown.
    Unreported,
}

/// What a stop's teardown cut, node by node.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DomainStopReport {
    pub nodes: BTreeMap<ClusterNodeName, NodeStopReport>,
}

impl DomainStopReport {
    /// Whether the teardown cut nothing on any node that reported, and every node reported.
    pub fn cut_nothing(&self) -> bool {
        self.nodes.values().all(|report| match report {
            NodeStopReport::Reported { cuts } => cuts.is_empty(),
            NodeStopReport::Unreported => false,
        })
    }
}

/// How the stop that ended a domain ended, kept while the domain stays stopped.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainStopEnding {
    pub cause: DomainStopCause,
    pub ending: StopEnding,
    /// Every request the ending resolved together.
    pub resolved: Vec<StopRequestId>,
    /// What the teardown cut, once every node reported it.
    pub report: Option<DomainStopReport>,
}

impl DomainStopEnding {
    /// The ending of a stop that came before the domain ran any work: a `START` rolled back, or a
    /// restore whose installation the licensing stop gate kept from running. It cut nothing.
    pub fn before_any_work(cause: DomainStopCause) -> Self {
        Self {
            cause,
            ending: StopEnding::Completed,
            resolved: Vec::new(),
            report: Some(DomainStopReport::default()),
        }
    }
}

/// The stop a domain is in while it stops, or the stop that ended it while it stays stopped.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum DomainStop {
    Stopping(DomainStopping),
    Ended(DomainStopEnding),
}

/// What a stop request did to a domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopRequestAdmission {
    /// The request began the stop of a running or paused domain.
    Began,
    /// The request joined the stop the domain is already in.
    Joined,
    /// The domain's stop already holds this request.
    AlreadyRequested,
    /// The domain is already stopped.
    AlreadyStopped,
}

/// What withdrawing an operator's request did to a stopping domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopWithdrawal {
    /// The request left, and the stop goes on for the requests that remain.
    Withdrawn,
    /// The request was the last one, so the stop is called off and the domain runs on.
    CalledOff,
    /// The domain's stop does not hold this request.
    NotHeld,
}

impl crate::DomainState {
    /// The requests of the stop the domain is in, if it is stopping.
    pub fn stopping(&self) -> Option<&DomainStopping> {
        match &self.stop {
            Some(DomainStop::Stopping(stopping)) => Some(stopping),
            Some(DomainStop::Ended(_)) | None => None,
        }
    }

    /// How the stop that ended the domain ended, if it is stopped by one.
    pub fn stop_ending(&self) -> Option<&DomainStopEnding> {
        match &self.stop {
            Some(DomainStop::Ended(ending)) => Some(ending),
            Some(DomainStop::Stopping(_)) | None => None,
        }
    }

    /// Why the domain's stop stops it, or stopped it: the licensing cause or an operator's.
    pub fn stop_cause(&self) -> Option<DomainStopCause> {
        match &self.stop {
            Some(DomainStop::Stopping(stopping)) => Some(stopping.cause()),
            Some(DomainStop::Ended(ending)) => Some(ending.cause),
            None => None,
        }
    }

    /// Applies a stop request, at the consensus revision `revision` whose entry carries it. A
    /// running or paused domain begins stopping, and a stopping one takes the request beside those
    /// it holds; a repeated request changes nothing. Whether the request may act at all, against
    /// the mutation that holds the domain, is its caller's decision.
    pub fn request_stop(&mut self, request: StopRequest, revision: u64) -> StopRequestAdmission {
        match self.status {
            DomainStatus::Stopped => return StopRequestAdmission::AlreadyStopped,
            DomainStatus::Running | DomainStatus::Paused => {
                self.status = DomainStatus::Stopping;
                self.stop = Some(DomainStop::Stopping(DomainStopping {
                    began_at_revision: revision,
                    operators: BTreeMap::new(),
                    licensing: None,
                }));
                self.add_stop_request(request);
                return StopRequestAdmission::Began;
            }
            DomainStatus::Stopping => {}
        }
        if self
            .stopping()
            .is_some_and(|stopping| stopping.holds(&request.id()))
        {
            return StopRequestAdmission::AlreadyRequested;
        }
        self.add_stop_request(request);
        StopRequestAdmission::Joined
    }

    fn add_stop_request(&mut self, request: StopRequest) {
        let Some(DomainStop::Stopping(stopping)) = &mut self.stop else {
            return;
        };
        match request {
            StopRequest::Operator { reference, request } => {
                stopping.operators.insert(reference, request);
            }
            StopRequest::Licensing(reason) => stopping.licensing = Some(reason),
        }
    }

    /// Withdraws an operator's request whose `CANCEL` acted. The last request's withdrawal calls
    /// the stop off: the domain runs on with nothing cut.
    pub fn withdraw_stop_request(
        &mut self,
        reference: &CommandExecutionReference,
    ) -> StopWithdrawal {
        let Some(DomainStop::Stopping(stopping)) = &mut self.stop else {
            return StopWithdrawal::NotHeld;
        };
        if stopping.operators.remove(reference).is_none() {
            return StopWithdrawal::NotHeld;
        }
        if !stopping.is_empty() {
            return StopWithdrawal::Withdrawn;
        }
        self.status = DomainStatus::Running;
        self.stop = None;
        StopWithdrawal::CalledOff
    }

    /// Ends the stop the domain is in: the domain stops and leaves its clock, keeping how the stop
    /// ended and every request the ending resolved. Returns `false` when the domain is not
    /// stopping.
    pub fn end_stop(&mut self, ending: StopEnding) -> bool {
        let Some(DomainStop::Stopping(stopping)) = &self.stop else {
            return false;
        };
        let ended = DomainStopEnding {
            cause: stopping.cause(),
            ending,
            resolved: stopping.request_ids(),
            report: None,
        };
        self.status = DomainStatus::Stopped;
        self.clock = None;
        self.stop = Some(DomainStop::Ended(ended));
        true
    }

    /// Records what the ended stop's teardown cut. Returns `false` when the domain is not stopped
    /// by a stop whose report is still missing.
    pub fn record_stop_report(&mut self, report: DomainStopReport) -> bool {
        let Some(DomainStop::Ended(ending)) = &mut self.stop else {
            return false;
        };
        if ending.report.is_some() {
            return false;
        }
        ending.report = Some(report);
        true
    }

    /// Stops a domain that has not run any work since it last stopped, as the rollback of a
    /// failed `START` does: nothing is in motion, so nothing drains and nothing is cut. A domain
    /// already stopped keeps the stop that ended it.
    pub fn record_stop_before_any_work(&mut self, cause: DomainStopCause) {
        if self.status != DomainStatus::Stopped {
            self.stop = Some(DomainStop::Ended(DomainStopEnding::before_any_work(cause)));
        }
        self.status = DomainStatus::Stopped;
        self.clock = None;
    }

    /// Clears the stop a start leaves behind.
    pub fn clear_stop(&mut self) {
        self.stop = None;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{DomainConfig, DomainName, DomainPace, DomainStartPoint, DomainState};

    fn reference(value: &str) -> CommandExecutionReference {
        CommandExecutionReference::parse(value).expect("the test reference is a valid literal")
    }

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * 1_000_000_000)
    }

    fn operator(name: &str, requested_at: i64, limit: Option<WaitLimit>) -> StopRequest {
        StopRequest::Operator {
            reference: reference(name),
            request: OperatorStopRequest {
                requested_at: at(requested_at),
                limit,
            },
        }
    }

    fn domain(status: DomainStatus) -> DomainState {
        DomainState {
            id: DomainName::parse("orders").expect("the test domain name is valid"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
            },
            status,
            start_version: 3,
            last_start: DomainStartPoint::Resume,
            clock: None,
            stop: None,
        }
    }

    fn expiry() -> LicensingStopReason {
        LicensingStopReason::EphemeralExpired {
            expired_at: at(100),
        }
    }

    #[test]
    fn a_request_begins_the_stop_of_a_running_domain_and_a_second_joins_it() {
        let mut state = domain(DomainStatus::Running);
        assert_eq!(
            state.request_stop(operator("first", 10, None), 7),
            StopRequestAdmission::Began
        );
        assert_eq!(state.status, DomainStatus::Stopping);
        let stopping = state.stopping().expect("the domain is stopping");
        assert_eq!(stopping.began_at_revision, 7);
        assert_eq!(
            state.request_stop(operator("second", 11, None), 9),
            StopRequestAdmission::Joined
        );
        assert_eq!(
            state.request_stop(operator("first", 10, None), 9),
            StopRequestAdmission::AlreadyRequested,
            "a repeated request changes nothing"
        );
        let stopping = state.stopping().expect("the domain is still stopping");
        assert_eq!(stopping.operators.len(), 2);
        assert_eq!(
            stopping.began_at_revision, 7,
            "joining keeps the stop that began"
        );
        assert_eq!(stopping.cause(), DomainStopCause::Operator);
    }

    #[test]
    fn a_stopped_domain_takes_no_request() {
        let mut state = domain(DomainStatus::Stopped);
        assert_eq!(
            state.request_stop(operator("late", 10, None), 7),
            StopRequestAdmission::AlreadyStopped
        );
        assert_eq!(state.status, DomainStatus::Stopped);
        assert_eq!(state.stop, None);
    }

    #[test]
    fn withdrawing_one_request_keeps_the_stop_and_the_last_calls_it_off() {
        let mut state = domain(DomainStatus::Running);
        state.request_stop(operator("plain", 10, None), 7);
        state.request_stop(
            operator(
                "cancelling",
                10,
                Some(WaitLimit::Cancel {
                    after: Duration::from_secs(2),
                }),
            ),
            8,
        );
        assert_eq!(
            state.withdraw_stop_request(&reference("cancelling")),
            StopWithdrawal::Withdrawn
        );
        assert_eq!(state.status, DomainStatus::Stopping);
        assert_eq!(
            state.withdraw_stop_request(&reference("cancelling")),
            StopWithdrawal::NotHeld
        );
        assert_eq!(
            state.withdraw_stop_request(&reference("plain")),
            StopWithdrawal::CalledOff
        );
        assert_eq!(state.status, DomainStatus::Running);
        assert_eq!(state.stop, None, "a called-off stop leaves nothing behind");
    }

    #[test]
    fn the_licensing_request_is_never_withdrawn() {
        let mut state = domain(DomainStatus::Running);
        state.request_stop(StopRequest::Licensing(expiry()), 7);
        assert_eq!(
            state.withdraw_stop_request(&reference("anything")),
            StopWithdrawal::NotHeld
        );
        let stopping = state.stopping().expect("the licensing stop goes on");
        assert_eq!(stopping.cause(), DomainStopCause::Licensing(expiry()));
        assert_eq!(
            stopping.licensing_enforcement(),
            Some(at(160)),
            "the licensing stop is forced a fixed minute after the expiry"
        );
    }

    #[test]
    fn ending_a_stop_resolves_every_request_and_keeps_the_ending_until_its_report() {
        let mut state = domain(DomainStatus::Running);
        state.request_stop(operator("plain", 10, None), 7);
        state.request_stop(StopRequest::Licensing(expiry()), 8);
        assert!(state.end_stop(StopEnding::Enforced {
            by: StopRequestId::Licensing,
        }));
        assert_eq!(state.status, DomainStatus::Stopped);
        let ending = state.stop_ending().expect("the domain keeps its ending");
        assert_eq!(ending.cause, DomainStopCause::Licensing(expiry()));
        assert_eq!(
            ending.resolved,
            vec![
                StopRequestId::Licensing,
                StopRequestId::Operator(reference("plain"))
            ]
        );
        assert_eq!(ending.report, None);
        assert!(
            !state.end_stop(StopEnding::Completed),
            "a stopped domain has no stop to end"
        );
        assert!(state.record_stop_report(DomainStopReport::default()));
        assert!(
            !state.record_stop_report(DomainStopReport::default()),
            "a report is recorded once"
        );
        state.clear_stop();
        assert_eq!(state.stop, None);
    }

    #[test]
    fn clauses_act_at_their_own_instants() {
        let mut state = domain(DomainStatus::Running);
        state.request_stop(
            operator(
                "late",
                10,
                Some(WaitLimit::Enforce {
                    after: Duration::from_secs(30),
                }),
            ),
            7,
        );
        state.request_stop(
            operator(
                "early",
                12,
                Some(WaitLimit::Enforce {
                    after: Duration::from_secs(5),
                }),
            ),
            8,
        );
        state.request_stop(
            operator(
                "giving_up",
                10,
                Some(WaitLimit::Cancel {
                    after: Duration::from_secs(3),
                }),
            ),
            9,
        );
        let stopping = state.stopping().expect("the domain is stopping");
        assert_eq!(
            stopping.earliest_operator_enforcement(),
            Some((at(17), StopRequestId::Operator(reference("early"))))
        );
        assert_eq!(stopping.next_operator_limit(), Some(at(13)));
        assert!(stopping.cancelled_by(at(12)).is_empty());
        assert_eq!(stopping.cancelled_by(at(13)), vec![reference("giving_up")]);
    }

    #[test]
    fn a_stop_before_any_work_cuts_nothing_and_keeps_an_earlier_ending() {
        let mut state = domain(DomainStatus::Running);
        state.record_stop_before_any_work(DomainStopCause::Operator);
        assert_eq!(state.status, DomainStatus::Stopped);
        let ending = state
            .stop_ending()
            .expect("the rollback records its ending");
        assert!(ending.resolved.is_empty());
        assert!(
            ending
                .report
                .as_ref()
                .is_some_and(DomainStopReport::cut_nothing)
        );
        let kept = state.stop.clone();
        state.record_stop_before_any_work(DomainStopCause::Licensing(expiry()));
        assert_eq!(
            state.stop, kept,
            "a stopped domain keeps the stop that ended it"
        );
    }
}
