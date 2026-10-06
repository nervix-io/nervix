//! Remote relay dispatch, admission, and acknowledgement coordination.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay wire encoding and decoding, epoch-scoped admission reconciliation,
//!   cancellation, progress handling, and remote acknowledgement correlation.
//! - **Depends on.** Branch-local relay boundaries, execution admission, cluster membership, and
//!   the authenticated interconnect.
//! - **Must not know.** NSPL text, transactions, scheduling policy, or connector internals.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "remote relay delivery, admission and acknowledgements run per payload attempt \
                  or frame"
    )
)]

use error_stack::ResultExt as _;

use super::*;

pub(super) const REMOTE_RELAY_INSTANTIATION_WAIT: Duration = Duration::from_secs(5);

pub(super) const REMOTE_RELAY_INSTANTIATION_POLL: Duration = Duration::from_millis(25);

pub(super) const REMOTE_ACK_ALIVE_INTERVAL: Duration = Duration::from_millis(100);

/// How long a node that forwarded a record acknowledgement with an admitted relay delivery waits
/// without a report about it from the receiver before it fails the acknowledgement.
///
/// The receiver reports an acknowledgement it holds `REMOTE_ACK_ALIVE_INTERVAL` after its previous
/// report was delivered or given up, and gives a report up after
/// `RemoteDispatcher::DISPATCH_TIMEOUT`. Two consecutive reports that each exhaust that deadline
/// stay inside this bound, so only a receiver that stopped reporting the acknowledgement altogether
/// is silent this long: its terminal outcome was lost on the way back, or its run ended.
pub(super) const REMOTE_ACK_SILENCE_TIMEOUT: Duration = Duration::from_secs(15);

/// How often a node looks for forwarded record acknowledgements whose receiver fell silent.
pub(super) const REMOTE_ACK_SILENCE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// The sweeps that may find a forwarded acknowledgement unreported before the next one fails it. A
/// report can land just after a sweep, so the acknowledgement fails between
/// `REMOTE_ACK_SILENCE_TIMEOUT` and one sweep interval later.
pub(super) const REMOTE_ACK_SILENT_SWEEPS: u64 =
    REMOTE_ACK_SILENCE_TIMEOUT.as_secs() / REMOTE_ACK_SILENCE_SWEEP_INTERVAL.as_secs();

const _: () = {
    assert!(REMOTE_ACK_SILENCE_TIMEOUT.subsec_nanos() == 0);
    assert!(REMOTE_ACK_SILENCE_SWEEP_INTERVAL.subsec_nanos() == 0);
    assert!(REMOTE_ACK_SILENCE_SWEEP_INTERVAL.as_secs() > 0);
    assert!(
        REMOTE_ACK_SILENCE_TIMEOUT
            .as_secs()
            .is_multiple_of(REMOTE_ACK_SILENCE_SWEEP_INTERVAL.as_secs())
    );
    assert!(REMOTE_ACK_SILENT_SWEEPS > 0);
    // Two consecutive reports that each exhaust their dispatch deadline stay inside the bound.
    assert!(
        2 * (RemoteDispatcher::DISPATCH_TIMEOUT.as_millis()
            + REMOTE_ACK_ALIVE_INTERVAL.as_millis())
            < REMOTE_ACK_SILENCE_TIMEOUT.as_millis()
    );
};

pub(super) const REMOTE_RELAY_TOTAL_TIMEOUT: Duration = Duration::from_secs(300);

enum FailedRelayAdmissionResolution {
    Admitted,
    Failed(Report<RemoteDispatchError>),
    Indeterminate(Report<RemoteDispatchError>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum RemoteDispatchError {
    #[error("relay branch delivery {sequence} to node '{target}' was evicted before admission")]
    BranchEvicted {
        target: ClusterNodeName,
        sequence: u64,
    },
    #[error("node '{target}' rejected relay admission")]
    AdmissionRejected { target: ClusterNodeName },
    #[error("relay admission response channel from node '{target}' closed")]
    AdmissionResponseClosed { target: ClusterNodeName },
    #[error("timed out after {timeout:?} waiting for node '{target}' to admit a relay batch")]
    AdmissionInactivityTimeout {
        target: ClusterNodeName,
        timeout: Duration,
    },
    #[error("timed out after {timeout:?} waiting for relay admission from node '{target}'")]
    AdmissionTotalTimeout {
        target: ClusterNodeName,
        timeout: Duration,
    },
    #[error("failed to cancel relay delivery {sequence} on node '{target}'")]
    AdmissionCancellation {
        target: ClusterNodeName,
        sequence: u64,
    },
    #[error("timed out cancelling relay delivery {sequence} on node '{target}'")]
    AdmissionCancellationTimeout {
        target: ClusterNodeName,
        sequence: u64,
    },
    #[error("the outcome of relay delivery {sequence} on node '{target}' is indeterminate")]
    AdmissionIndeterminate {
        target: ClusterNodeName,
        sequence: u64,
    },
    #[error("relay delivery {sequence} on node '{target}' returned a non-terminal admission state")]
    AdmissionNonTerminal {
        target: ClusterNodeName,
        sequence: u64,
    },
    #[error("failed to send a remote payload to node '{target}'")]
    Send { target: ClusterNodeName },
    #[error("timed out sending a remote payload to node '{target}'")]
    SendTimeout { target: ClusterNodeName },
}

/// Why a relay payload another node sent does not decode into a batch of its relay. A variant
/// that wraps a decoder keeps that decoder's own failure beneath it.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum RemoteRelayDecodeError {
    #[error("relay payload is missing its admission registration")]
    MissingAdmission,
    #[error("failed to decode the relay batch body")]
    Body,
    #[error("remote metadata count {metadata} does not match batch row count {rows}")]
    MetadataCount { metadata: usize, rows: usize },
    #[error("remote ack count {acks} does not match batch row count {rows}")]
    AckCount { acks: usize, rows: usize },
    #[error("subscription fanout payload must not carry remote ack registrations")]
    SubscriptionAcks,
    #[error("failed to decode the remote branch key")]
    BranchKey,
    #[error("failed to assemble the relay batch")]
    Batch,
}

/// The rows a remote relay payload carries, decoded, and the branch they belong to.
struct DecodedRemoteRows {
    batch: RuntimeRecordBatch,
    key: Option<BranchKey>,
}

#[derive(Debug, Clone)]
pub(super) enum RelayAdmissionUpdate {
    Pending,
    Alive,
    Admitted,
    Rejected(String),
}

struct RemoteRelayAdmissionContext<'a> {
    registration: &'a RemoteAckRegistration,
    transport: &'a RelayAdmission,
    admitted: &'a mut bool,
}

/// The remote acknowledgement correlation registry. The runtime and the `RemoteDispatcher` it
/// attaches must observe one instance of this: the dispatcher allocates the correlation ids that
/// the runtime resolves when acknowledgements come back over the interconnect.
///
/// Both maps hold only registrations this run of the node handed out, keyed by their number. A
/// resolution resolves an entry only once the dispatcher confirms that it names this run.
pub(super) struct RemoteDispatchRegistry {
    next_ack_id: AtomicU64,
    /// Record acknowledgements this node forwarded with relay deliveries, waiting for the nodes
    /// the deliveries went to.
    pending_acks: DashMap<u64, PendingRemoteAck, RandomState>,
    pub(super) pending_relay_admissions:
        DashMap<u64, watch::Sender<RelayAdmissionUpdate>, RandomState>,
}

/// A record acknowledgement this node forwarded with a relay delivery, waiting for the node the
/// delivery went to.
///
/// Its atomics change only while it is in the registry, through a borrowed lookup under the
/// shard's shared lock. Parked progress changes under the shard's exclusive lock. The sweep fails
/// it only through an exclusive removal that rechecks the count, so the shard lock, not the
/// atomics' ordering, orders that recheck after every report that reached the acknowledgement
/// first.
struct PendingRemoteAck {
    /// The node the delivery went to, which reports and resolves the acknowledgement.
    receiver: ClusterNodeName,
    acks: AckSet,
    /// Whether the receiver admitted the delivery. Until it has, the delivery's own admission wait
    /// decides its failure, and sweeps pass the acknowledgement by.
    admitted: AtomicBool,
    /// Sweeps that found no report since the receiver last reported the acknowledgement, or since
    /// its delivery was admitted.
    silent_sweeps: AtomicU64,
    /// A parked share remains unresolved while it releases the upstream ownership handoff.
    required_wait: Option<AckParkGuard>,
    /// Delayed progress cannot reverse a newer park or resume.
    progress_sequence: Option<u64>,
}

impl PendingRemoteAck {
    fn new(receiver: ClusterNodeName, acks: AckSet) -> Self {
        Self {
            receiver,
            acks,
            admitted: AtomicBool::new(false),
            silent_sweeps: AtomicU64::new(0),
            required_wait: None,
            progress_sequence: None,
        }
    }

    fn admit(&self) {
        self.admitted.store(true, Ordering::Relaxed);
    }

    /// The receiver reported that it still holds the acknowledgement.
    fn report(&self) {
        self.silent_sweeps.store(0, Ordering::Relaxed);
        self.acks.ack_alive();
    }

    fn progress(&mut self, sequence: u64, parked: bool) {
        if self.progress_sequence.is_some_and(|seen| sequence <= seen) {
            return;
        }
        self.progress_sequence = Some(sequence);
        if parked {
            if self.required_wait.is_none() {
                self.required_wait = Some(AckParkGuard::new([&self.acks]));
            }
        } else {
            self.required_wait = None;
        }
    }

    /// Counts one sweep that found no report, and answers whether the receiver has now been silent
    /// past the bound. An acknowledgement whose delivery was not admitted is never counted.
    #[allow(deprecated)] // until try_update is stabilized
    fn swept(&self) -> bool {
        if !self.admitted.load(Ordering::Relaxed) {
            return false;
        }
        #[allow(deprecated)] // until try_update is stabilized
        let previous = self
            .silent_sweeps
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sweeps| {
                sweeps.checked_add(1)
            })
            .assured(
                "each sweep counts once, and the first sweep past REMOTE_ACK_SILENT_SWEEPS \
                 removes the acknowledgement unless a report reset its count to zero",
            );
        previous >= REMOTE_ACK_SILENT_SWEEPS
    }

    /// Whether the receiver has been silent past the bound, as the sweep's exclusive removal
    /// rechecks it.
    fn is_silent(&self) -> bool {
        self.admitted.load(Ordering::Relaxed)
            && self.silent_sweeps.load(Ordering::Relaxed) > REMOTE_ACK_SILENT_SWEEPS
    }
}

impl RemoteDispatchRegistry {
    pub(super) fn new() -> Self {
        Self {
            next_ack_id: AtomicU64::new(1),
            pending_acks: DashMap::default(),
            pending_relay_admissions: DashMap::default(),
        }
    }

    /// The number of a new registration. Every run of the node numbers its registrations from one.
    pub(super) fn next_ack_id(&self) -> u64 {
        self.next_ack_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Holds `acks`, forwarded to `receiver` under `ack_id`, until the receiver resolves them.
    pub(super) fn register_ack(&self, ack_id: u64, receiver: ClusterNodeName, acks: AckSet) {
        nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.pending_acks
                .insert(ack_id, PendingRemoteAck::new(receiver, acks))
        );
    }

    /// Starts waiting on the receiver's reports about the acknowledgement registered under
    /// `ack_id`, now that the receiver admitted the delivery that carried it.
    pub(super) fn admit_ack(&self, ack_id: u64) {
        if let Some(pending) = nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.pending_acks.get(&ack_id)
        ) {
            pending.admit();
        }
    }

    /// Starts waiting on the receiver's reports about every acknowledgement `registrations` names,
    /// now that the receiver admitted the delivery that carried them.
    pub(super) fn admit_acks(&self, registrations: &[Option<RemoteAckRegistration>]) {
        for registration in registrations.iter().flatten() {
            self.admit_ack(registration.ack_id);
        }
    }

    /// Records the receiver's report that it still holds the acknowledgement registered under
    /// `ack_id`, and answers whether this node was still waiting for it.
    pub(super) fn report_ack(&self, ack_id: u64) -> bool {
        let Some(pending) = nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.pending_acks.get(&ack_id)
        ) else {
            return false;
        };
        pending.report();
        true
    }

    /// Applies a receiver's parked or resumed status in sequence order, while still counting even
    /// a stale status as a liveness report for the silence timeout.
    pub(super) fn progress_ack(&self, ack_id: u64, sequence: u64, parked: bool) -> bool {
        let Some(mut pending) = nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery, including ordered parked and \
             resumed progress",
            self.pending_acks.get_mut(&ack_id)
        ) else {
            return false;
        };
        pending.progress(sequence, parked);
        pending.report();
        true
    }

    /// Resolves the acknowledgement registered under `ack_id` with the receiver's terminal
    /// `outcome`, and answers whether this node was still waiting for it.
    pub(super) fn resolve_ack(&self, ack_id: u64, outcome: AckOutcome) -> bool {
        let Some((_, pending)) = nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.pending_acks.remove(&ack_id)
        ) else {
            return false;
        };
        match outcome {
            AckOutcome::Ack => pending.acks.ack_success(),
            AckOutcome::NoAck(error) => pending.acks.no_ack(error),
        }
        true
    }

    /// Stops waiting for the acknowledgement registered under `ack_id` because its delivery
    /// failed. The caller resolves the acknowledgement itself.
    pub(super) fn clear_ack(&self, ack_id: u64) {
        nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.pending_acks.remove(&ack_id)
        );
    }

    /// Whether this node still waits for the acknowledgement registered under `ack_id`.
    #[cfg(test)]
    pub(super) fn holds_ack(&self, ack_id: u64) -> bool {
        self.pending_acks.contains_key(&ack_id)
    }

    /// Counts one sweep against every forwarded acknowledgement of an admitted delivery, fails each
    /// one whose receiver reported nothing about it for `REMOTE_ACK_SILENCE_TIMEOUT`, and returns
    /// how many it failed for each receiver.
    pub(super) fn fail_silent_acks(&self) -> BTreeMap<ClusterNodeName, usize> {
        let mut silent = Vec::new();
        for pending in nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.pending_acks.iter()
        ) {
            if pending.swept() {
                silent.push(*pending.key());
            }
        }
        let mut failed = BTreeMap::new();
        for ack_id in silent {
            let removed = nervix_primitives::expect_lint!(
                nervix::sync_acquisition,
                "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts \
                 and acknowledgement owners through terminal delivery",
                self.pending_acks
                    .remove_if(&ack_id, |_, pending| pending.is_silent())
            );
            let Some((_, pending)) = removed else {
                continue;
            };
            pending.acks.no_ack(format!(
                "node '{}' reported nothing about the forwarded record for {}",
                pending.receiver.as_str(),
                humantime::format_duration(REMOTE_ACK_SILENCE_TIMEOUT)
            ));
            match failed.get_mut(&pending.receiver) {
                Some(count) => {
                    *count = usize::checked_add(*count, 1)
                        .assured("one sweep fails at most the acknowledgements held in memory");
                }
                None => {
                    failed.insert(pending.receiver, 1);
                }
            }
        }
        failed
    }

    /// Sweeps the forwarded acknowledgements once every `REMOTE_ACK_SILENCE_SWEEP_INTERVAL` for as
    /// long as it runs, failing each one its receiver stopped reporting. A sweep that runs late
    /// counts once, so a node whose own execution stalled does not fail acknowledgements whose
    /// reports it could not receive meanwhile.
    pub(super) async fn sweep_silent_acks(&self) {
        let mut sweeps = nervix_primitives::time::interval(REMOTE_ACK_SILENCE_SWEEP_INTERVAL);
        sweeps.set_missed_tick_behavior(nervix_primitives::time::MissedTickBehavior::Skip);
        loop {
            nervix_primitives::task::consume_budget().await;
            sweeps.tick().await;
            let failed = self.fail_silent_acks();
            for (receiver, acknowledgements) in failed {
                warn!(
                    target_node = %receiver,
                    acknowledgements,
                    silence = %humantime::format_duration(REMOTE_ACK_SILENCE_TIMEOUT),
                    "failed forwarded record acknowledgements the receiving node stopped reporting"
                );
            }
        }
    }
}

fn remote_ack_progress(completion: &AckCompletion) -> RemoteAckOutcome {
    let (sequence, parked) = completion.remote_progress();
    RemoteAckOutcome::Progress { sequence, parked }
}

/// How this node reaches the rest of its cluster. The node attaches it once, after joining the
/// cluster, and never replaces it.
///
/// It is also the node's identity. The interconnect authenticates the node under its name and the
/// cluster handle carries the incarnation gossip announced for this run, so a runtime holding a
/// dispatcher always knows which node it runs on, and a runtime holding none has not joined a
/// cluster.
pub(super) struct RemoteDispatcher {
    pub(super) cluster: Arc<cluster::ClusterHandle>,
    pub(super) interconnect: Transport,
    /// The same admission the attaching runtime holds, so a body this dispatcher encodes is
    /// charged against the same budgets the runtime's own work is.
    pub(super) executor: Executor,
    /// The same registry the attaching runtime holds, so an acknowledgement this dispatcher sent
    /// a correlation id for resolves against the entry the runtime is waiting on.
    pub(super) registry: Arc<RemoteDispatchRegistry>,
}

impl std::fmt::Debug for RemoteDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDispatcher").finish_non_exhaustive()
    }
}

impl RemoteDispatcher {
    pub(super) const DISPATCH_RETRY_INTERVAL: Duration = Duration::from_millis(25);
    pub(super) const DISPATCH_TIMEOUT: Duration = Duration::from_secs(5);

    /// The node's bounded execution and memory admission, which every body this dispatcher
    /// encodes or decodes is submitted through.
    pub(super) fn executor(&self) -> &Executor {
        &self.executor
    }

    /// The node this dispatcher speaks for.
    pub(super) fn local_node_id(&self) -> &ClusterNodeName {
        self.interconnect.node_id()
    }

    /// The incarnation that tells this run of the node apart from its earlier and later runs.
    pub(super) fn local_node_incarnation(&self) -> ClusterNodeIncarnation {
        self.cluster.local_incarnation()
    }

    /// A registration numbered in this process that names this run of the node, so its resolution
    /// can only ever resolve the entry registered under it here.
    fn next_registration(&self) -> RemoteAckRegistration {
        RemoteAckRegistration {
            ack_id: self.registry.next_ack_id(),
            registrar: ClusterNodeIdentity::new(
                self.local_node_id().clone(),
                self.local_node_incarnation(),
            ),
        }
    }

    /// Whether this run of the node handed out `registration`. Every run numbers its
    /// registrations from one, so a registration an earlier run handed out can carry the number of
    /// an entry this run holds.
    pub(super) fn registered(&self, registration: &RemoteAckRegistration) -> bool {
        registration.registrar.node_id() == self.local_node_id()
            && registration.registrar.incarnation() == self.local_node_incarnation()
    }

    /// Registers `acks` to be resolved by `receiver`, the node the returned registration is sent
    /// to.
    pub(super) fn register_pending_ack(
        &self,
        receiver: &ClusterNodeName,
        acks: AckSet,
    ) -> RemoteAckRegistration {
        let registration = self.next_registration();
        self.registry
            .register_ack(registration.ack_id, receiver.clone(), acks);
        registration
    }

    /// Starts waiting on the receiver's reports about the acknowledgements a delivery carried, now
    /// that the receiver admitted the delivery. From here on, an acknowledgement its receiver
    /// reports nothing about for `REMOTE_ACK_SILENCE_TIMEOUT` fails.
    pub(super) fn admit_pending_acks(&self, registrations: &[Option<RemoteAckRegistration>]) {
        self.registry.admit_acks(registrations);
    }

    pub(super) fn forwarded_ack(acks: &AckSet) -> AckSet {
        acks.attached()
    }

    pub(super) fn clear_pending_ack(&self, registration: &RemoteAckRegistration) {
        self.registry.clear_ack(registration.ack_id);
    }

    /// Registers a relay admission to be resolved by the node the returned registration is sent
    /// to, and returns the updates its resolutions publish.
    pub(super) fn register_pending_relay_admission(
        &self,
    ) -> (RemoteAckRegistration, watch::Receiver<RelayAdmissionUpdate>) {
        let registration = self.next_registration();
        let (sender, receiver) = watch::channel(RelayAdmissionUpdate::Pending);
        nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.registry
                .pending_relay_admissions
                .insert(registration.ack_id, sender)
        );
        (registration, receiver)
    }

    pub(super) fn clear_pending_relay_admission(&self, registration: &RemoteAckRegistration) {
        nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.registry
                .pending_relay_admissions
                .remove(&registration.ack_id)
        );
    }

    pub(super) async fn request_with_timeout<M>(
        &self,
        node_id: &ClusterNodeName,
        message: M,
        timeout: Duration,
    ) -> error_stack::Result<M::Response, nervix_interconnect::RequestError>
    where
        M: InterconnectRequest,
    {
        self.interconnect
            .request_with_timeout(node_id, message, timeout)
            .await
    }

    /// Open a bounded, flow-controlled stream of one bulk response's bytes.
    pub(super) async fn request_stream<M>(
        &self,
        node_id: &ClusterNodeName,
        message: M,
    ) -> error_stack::Result<
        nervix_interconnect::IncomingByteStream,
        nervix_interconnect::RequestError,
    >
    where
        M: nervix_interconnect::InterconnectStreamRequest,
    {
        self.interconnect.request_stream(node_id, message).await
    }

    pub(super) async fn dispatch_admitted_relay_payload(
        &self,
        node_id: &ClusterNodeName,
        mut payload: RelayPayload,
        branch_channel: &RelayOutboundSlot,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        if branch_channel.cancellation().is_cancelled() {
            return Err(Report::new(RemoteDispatchError::BranchEvicted {
                target: node_id.clone(),
                sequence: payload.delivery.sequence,
            }));
        }
        let delivery = payload.delivery;
        let mut cancellation_guard = self
            .interconnect
            .relay_cancellation_guard(node_id.clone(), delivery);
        let (registration, admission) = self.register_pending_relay_admission();
        payload.admission = Some(registration.clone());
        let dispatch = self.dispatch(node_id, Envelope::RelayPayload(payload));
        tokio::pin!(dispatch);
        let dispatch_result = nervix_primitives::select! {
            biased;
            result = &mut dispatch => result,
            () = branch_channel.cancellation().cancelled() => {
                Err(Report::new(RemoteDispatchError::BranchEvicted {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                }))
            },
        };
        if let Err(error) = dispatch_result {
            self.clear_pending_relay_admission(&registration);
            let resolution = self
                .resolve_failed_relay_admission(node_id, delivery, error, &mut cancellation_guard)
                .await;
            return match resolution {
                FailedRelayAdmissionResolution::Admitted => Ok(()),
                FailedRelayAdmissionResolution::Failed(error) => Err(error),
                FailedRelayAdmissionResolution::Indeterminate(error) => {
                    branch_channel.reopen_delivery_channel();
                    Err(error)
                }
            };
        }
        let admission = Self::await_relay_admission(node_id, admission, Self::DISPATCH_TIMEOUT);
        tokio::pin!(admission);
        let result = nervix_primitives::select! {
            biased;
            result = &mut admission => result,
            () = branch_channel.cancellation().cancelled() => {
                Err(Report::new(RemoteDispatchError::BranchEvicted {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                }))
            },
        };
        if let Err(error) = result {
            self.clear_pending_relay_admission(&registration);
            let resolution = self
                .resolve_failed_relay_admission(node_id, delivery, error, &mut cancellation_guard)
                .await;
            return match resolution {
                FailedRelayAdmissionResolution::Admitted => Ok(()),
                FailedRelayAdmissionResolution::Failed(error) => Err(error),
                FailedRelayAdmissionResolution::Indeterminate(error) => {
                    branch_channel.reopen_delivery_channel();
                    Err(error)
                }
            };
        }
        cancellation_guard.disarm();
        Ok(())
    }

    async fn resolve_failed_relay_admission(
        &self,
        node_id: &ClusterNodeName,
        delivery: RelayDelivery,
        original_error: Report<RemoteDispatchError>,
        cancellation_guard: &mut RelayCancellationGuard,
    ) -> FailedRelayAdmissionResolution {
        let deadline = Instant::now()
            .checked_add(Self::DISPATCH_TIMEOUT)
            .assured("the fixed relay cancellation deadline fits the monotonic clock");
        let status = loop {
            nervix_primitives::task::consume_budget().await;
            let cancellation = nervix_primitives::time::timeout_at(
                deadline,
                self.interconnect.cancel_relay(node_id, delivery),
            )
            .await;
            match cancellation {
                Ok(Ok(
                    status @ (RelayAdmissionStatus::Admitted
                    | RelayAdmissionStatus::Rejected(_)
                    | RelayAdmissionStatus::Cancelled
                    | RelayAdmissionStatus::Retired
                    | RelayAdmissionStatus::Indeterminate),
                )) => break status,
                Ok(Ok(
                    RelayAdmissionStatus::Reserved
                    | RelayAdmissionStatus::BodyReceived
                    | RelayAdmissionStatus::Unknown,
                )) => {}
                Ok(Err(error)) => {
                    let report = error
                        .change_context(RemoteDispatchError::AdmissionCancellation {
                            target: node_id.clone(),
                            sequence: delivery.sequence,
                        })
                        .attach_printable(original_error);
                    return FailedRelayAdmissionResolution::Indeterminate(report);
                }
                Err(_) => {
                    let report = Report::new(RemoteDispatchError::AdmissionCancellationTimeout {
                        target: node_id.clone(),
                        sequence: delivery.sequence,
                    })
                    .attach_printable(original_error);
                    return FailedRelayAdmissionResolution::Indeterminate(report);
                }
            }
            if Instant::now() >= deadline {
                let report = Report::new(RemoteDispatchError::AdmissionIndeterminate {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                })
                .attach_printable(original_error);
                return FailedRelayAdmissionResolution::Indeterminate(report);
            }
            sleep(Self::DISPATCH_RETRY_INTERVAL).await;
        };
        match status {
            RelayAdmissionStatus::Admitted => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Admitted
            }
            RelayAdmissionStatus::Rejected(reason) => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Failed(
                    Report::new(RemoteDispatchError::AdmissionRejected {
                        target: node_id.clone(),
                    })
                    .attach_printable(reason),
                )
            }
            RelayAdmissionStatus::Cancelled => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Failed(original_error)
            }
            RelayAdmissionStatus::Retired | RelayAdmissionStatus::Indeterminate => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Indeterminate(
                    Report::new(RemoteDispatchError::AdmissionIndeterminate {
                        target: node_id.clone(),
                        sequence: delivery.sequence,
                    })
                    .attach_printable(original_error),
                )
            }
            RelayAdmissionStatus::Reserved
            | RelayAdmissionStatus::BodyReceived
            | RelayAdmissionStatus::Unknown => FailedRelayAdmissionResolution::Indeterminate(
                Report::new(RemoteDispatchError::AdmissionNonTerminal {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                })
                .attach_printable(original_error),
            ),
        }
    }

    pub(super) async fn await_relay_admission(
        node_id: &ClusterNodeName,
        mut admission: watch::Receiver<RelayAdmissionUpdate>,
        inactivity_timeout: Duration,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        let total_deadline = Instant::now()
            .checked_add(REMOTE_RELAY_TOTAL_TIMEOUT)
            .assured("the fixed relay total timeout fits the monotonic clock");
        loop {
            nervix_primitives::task::consume_budget().await;
            let inactivity_deadline = Instant::now()
                .checked_add(inactivity_timeout)
                .assured("a bounded relay inactivity timeout fits the monotonic clock");
            let deadline = inactivity_deadline.min(total_deadline);
            match nervix_primitives::time::timeout_at(deadline, admission.changed()).await {
                Ok(Ok(())) => {
                    let update = admission.borrow_and_update().clone();
                    match update {
                        RelayAdmissionUpdate::Pending | RelayAdmissionUpdate::Alive => {}
                        RelayAdmissionUpdate::Admitted => return Ok(()),
                        RelayAdmissionUpdate::Rejected(reason) => {
                            return Err(Report::new(RemoteDispatchError::AdmissionRejected {
                                target: node_id.clone(),
                            })
                            .attach_printable(reason));
                        }
                    }
                }
                Ok(Err(_)) => {
                    return Err(Report::new(RemoteDispatchError::AdmissionResponseClosed {
                        target: node_id.clone(),
                    }));
                }
                Err(_) => {
                    if Instant::now() >= total_deadline {
                        return Err(Report::new(RemoteDispatchError::AdmissionTotalTimeout {
                            target: node_id.clone(),
                            timeout: REMOTE_RELAY_TOTAL_TIMEOUT,
                        }));
                    }
                    return Err(Report::new(
                        RemoteDispatchError::AdmissionInactivityTimeout {
                            target: node_id.clone(),
                            timeout: inactivity_timeout,
                        },
                    ));
                }
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "subscription delivery keeps its ordering state in its selected outbound \
                      channel",
            key = "subscription channel incarnation",
            bound = "one admitted subscription payload at a time per retained channel"
        )
    )]
    pub(super) async fn dispatch_subscription_fanout(
        &self,
        services: &RelayBoundaryServices,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
        excluded_nodes: &BTreeSet<ClusterNodeName>,
    ) {
        let local_node_id = self.local_node_id();
        let interest_index = self.cluster.subscription_interest_index();
        let channels = services.subscription_channels(
            &batch.key,
            StdArc::clone(&interest_index),
            domain,
            relay,
        );
        let Some(interested_nodes) = interest_index.nodes(domain.as_str(), relay.as_str()) else {
            return;
        };
        // One encode for the whole fanout. Every interested node carries this same allocation,
        // and the first one serializes it inside its own outbound slot: the slot is what orders
        // the batches a node receives, so an encode performed ahead of it lets a later batch
        // overtake an earlier one.
        let mut encoded_body: Option<ChargedBytes> = None;
        for node_id in interested_nodes.keys() {
            nervix_primitives::task::consume_budget().await;
            if node_id == local_node_id || excluded_nodes.contains(node_id) {
                continue;
            }
            let Some(outbound_slot) = channels.slot(node_id, relay) else {
                continue;
            };
            let Some(_slot) = outbound_slot.lock_for_delivery().await else {
                continue;
            };
            let batch_ipc = match encoded_body.clone() {
                Some(bytes) => bytes,
                None => match batch.batch.encode_arrow_ipc(self.executor()).await {
                    Ok(bytes) => {
                        encoded_body = Some(bytes.clone());
                        bytes
                    }
                    Err(error) => {
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %error,
                            "failed to serialize remote subscription batch"
                        );
                        return;
                    }
                },
            };
            let delivery = outbound_slot.next_delivery();
            if let Err(error) = self
                .dispatch_admitted_relay_payload(
                    node_id,
                    RelayPayload {
                        delivery,
                        kind: RelayPayloadKind::SubscriptionFanout,
                        domain: domain.clone(),
                        relay: relay.clone(),
                        key: BranchKey::to_remote_key(&batch.key),
                        batch_ipc: batch_ipc.clone(),
                        metadata: batch.metadata.to_remote(),
                        acks: vec![None; batch.acks.len()],
                        admission: None,
                    },
                    &outbound_slot,
                )
                .await
            {
                warn!(
                    target_node = %node_id,
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %error,
                    "failed to dispatch remote subscription payload"
                );
            }
        }
    }

    pub(super) async fn dispatch(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        let deadline = Instant::now()
            .checked_add(Self::DISPATCH_TIMEOUT)
            .assured("the fixed remote dispatch timeout fits the monotonic clock");
        loop {
            nervix_primitives::task::consume_budget().await;
            let result = nervix_primitives::time::timeout_at(
                deadline,
                self.interconnect.send(node_id, envelope.clone()),
            )
            .await;
            let error = match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => error,
                Err(_) => {
                    return Err(Report::new(RemoteDispatchError::SendTimeout {
                        target: node_id.clone(),
                    }));
                }
            };
            if Instant::now() >= deadline {
                return Err(error.change_context(RemoteDispatchError::Send {
                    target: node_id.clone(),
                }));
            }
            nervix_primitives::select! {
                _ = sleep_until(deadline) => {
                    return Err(
                        error.change_context(RemoteDispatchError::Send {
                            target: node_id.clone(),
                        }),
                    );
                }
                _ = sleep(Self::DISPATCH_RETRY_INTERVAL) => {}
            }
        }
    }
}

pub(super) fn push_remote_runtime_consumer(
    consumers: &mut Vec<RemoteRuntimeConsumer>,
    node_id: &ClusterNodeName,
    relay: &RelayName,
    mode: AckMode,
) {
    if let Some(existing) = consumers
        .iter_mut()
        .find(|consumer| consumer.node_id == node_id.clone() && consumer.relay == *relay)
    {
        if mode == AckMode::Attached {
            existing.mode = AckMode::Attached;
        }
        return;
    }

    consumers.push(RemoteRuntimeConsumer {
        node_id: node_id.clone(),
        relay: relay.clone(),
        mode,
    });
}

/// The instantiated relay a remote payload is delivered into: the boundary services that own it,
/// and the schema its Arrow batch must decode against.
pub(in crate::runtime) struct RemoteRelayTarget {
    pub(super) services: Arc<RelayBoundaryServices>,
    pub(super) schema: Arc<CompiledSchema>,
}

impl Runtime {
    pub(crate) async fn wait_for_domain_routing(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<SharedDomainRouting, Report<RuntimeError>> {
        let deadline = Instant::now()
            .checked_add(REMOTE_RELAY_INSTANTIATION_WAIT)
            .assured("the fixed relay-instantiation wait fits the monotonic clock");
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(routing) = self.domain_routing(domain) {
                return Ok(routing);
            }
            if Instant::now() >= deadline {
                return Err(Report::new(RuntimeError::relay_not_instantiated(
                    domain, relay,
                )));
            }
            sleep(REMOTE_RELAY_INSTANTIATION_POLL).await;
        }
    }

    /// Publishes how this node reaches its cluster, and with it the node's identity, once the node
    /// has joined the cluster. `interconnect` is the transport bound under this node's name.
    pub(crate) fn attach_remote_dispatcher(
        &self,
        cluster: Arc<cluster::ClusterHandle>,
        interconnect: Transport,
    ) {
        let dispatcher = RemoteDispatcher {
            cluster,
            interconnect,
            executor: self.inner.executor.clone(),
            registry: self.inner.remote_dispatch.clone(),
        };
        self.inner
            .remote_dispatcher
            .store(Some(StdArc::new(dispatcher)));
        let registry = self.inner.remote_dispatch.clone();
        self.spawn_remote_ack_watcher_task(async move {
            registry.sweep_silent_acks().await;
        });
    }

    /// Whether `node_id` names this node. A node that has not joined a cluster has no name, so no
    /// node is local to it.
    pub(in crate::runtime) fn is_local_node(&self, node_id: &ClusterNodeName) -> bool {
        let dispatcher = self.inner.remote_dispatcher.load();
        let Some(dispatcher) = dispatcher.as_deref() else {
            return false;
        };
        dispatcher.local_node_id() == node_id
    }

    pub(in crate::runtime) async fn inject_remote_stream_boundary_message(
        &self,
        services: &RelayBoundaryServices,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        services.inject_remote_message(batch).await
    }

    pub(crate) async fn handle_remote_stream(
        &self,
        payload: RelayPayload,
        transport_admission: RelayAdmission,
        routing: &mut DomainRoutingCache,
    ) -> Result<(), Report<RuntimeError>> {
        let Some(admission) = payload.admission.clone() else {
            let error = Report::new(RemoteRelayDecodeError::MissingAdmission).change_context(
                RuntimeError::decode_remote_relay(&payload.domain, &payload.relay),
            );
            return Err(error);
        };
        let branch = match BranchKey::from_remote_key(payload.key.clone()) {
            Ok(Some(branch)) => Some(branch.as_str().to_string()),
            Ok(None) | Err(_) => None,
        };
        self.inner
            .fault_injection
            .pause_remote_relay_admission_if_armed(&payload.domain, branch.as_deref())
            .await;
        let mut admitted = false;
        let result = match payload.kind {
            RelayPayloadKind::Routed => {
                self.handle_remote_stream_payload_with_admission(
                    payload,
                    false,
                    routing,
                    Some(RemoteRelayAdmissionContext {
                        registration: &admission,
                        transport: &transport_admission,
                        admitted: &mut admitted,
                    }),
                )
                .await
            }
            RelayPayloadKind::SubscriptionFanout => {
                self.handle_remote_subscription_payload_with_admission(
                    payload,
                    routing,
                    Some(RemoteRelayAdmissionContext {
                        registration: &admission,
                        transport: &transport_admission,
                        admitted: &mut admitted,
                    }),
                )
                .await
            }
            RelayPayloadKind::Ingress => {
                self.handle_remote_stream_payload_with_admission(
                    payload,
                    true,
                    routing,
                    Some(RemoteRelayAdmissionContext {
                        registration: &admission,
                        transport: &transport_admission,
                        admitted: &mut admitted,
                    }),
                )
                .await
            }
        };
        if !admitted && let Err(error) = &result {
            self.send_remote_relay_admission_outcome(
                &admission,
                RemoteAckOutcome::NoAck(format!("{error:#}")),
            )
            .await;
        }
        result?;
        Ok(())
    }

    pub(super) async fn send_remote_relay_admission_outcome(
        &self,
        admission: &RemoteAckRegistration,
        outcome: RemoteAckOutcome,
    ) {
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return;
        };
        if let Err(error) = dispatcher
            .dispatch(
                admission.registrar.node_id(),
                Envelope::Ack(admission.resolution(outcome)),
            )
            .await
        {
            warn!(
                target_node = %admission.registrar,
                admission_id = admission.ack_id,
                error = %error,
                "failed to return relay admission response"
            );
        }
    }

    pub(in crate::runtime) fn remote_stream_target(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<RemoteRelayTarget, RuntimeError> {
        let not_instantiated = || Report::new(RuntimeError::relay_not_instantiated(domain, relay));
        if routing.passive_only {
            return Err(not_instantiated());
        }
        let Some(services) = routing.relay_services.get(relay).cloned() else {
            return Err(not_instantiated());
        };
        let Some(schema) = routing.relay_schemas.get(relay).cloned() else {
            return Err(not_instantiated());
        };
        Ok(RemoteRelayTarget { services, schema })
    }

    pub(in crate::runtime) async fn wait_for_remote_stream_target(
        &self,
        routing: &mut DomainRoutingCache,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<RemoteRelayTarget, RuntimeError> {
        let deadline = Instant::now()
            .checked_add(REMOTE_RELAY_INSTANTIATION_WAIT)
            .assured("the fixed relay-instantiation wait fits the monotonic clock");
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.remote_stream_target(routing.load(), domain, relay) {
                Ok(target) => return Ok(target),
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                }
            }
            sleep(REMOTE_RELAY_INSTANTIATION_POLL).await;
        }
    }

    #[cfg(test)]
    pub(super) async fn handle_remote_stream_payload_with_owner_ingress(
        &self,
        remote: RelayPayload,
        owner_ingress: bool,
    ) -> error_stack::Result<(), RuntimeError> {
        let Some(mut routing) = self.domain_routing_cache(&remote.domain) else {
            return Err(Report::new(RuntimeError::relay_not_instantiated(
                &remote.domain,
                &remote.relay,
            )));
        };
        self.handle_remote_stream_payload_with_admission(remote, owner_ingress, &mut routing, None)
            .await
    }

    /// The rows `remote` carries, decoded against `schema` and checked against the metadata and
    /// acknowledgement sidecars that travel with them, and the branch they belong to.
    async fn decode_remote_rows(
        &self,
        schema: &CompiledSchema,
        remote: &mut RelayPayload,
    ) -> error_stack::Result<DecodedRemoteRows, RemoteRelayDecodeError> {
        let batch = schema
            .decode_arrow_body(self.executor(), remote.batch_ipc.clone())
            .await
            .change_context(RemoteRelayDecodeError::Body)?;
        let rows = batch.batch().num_rows();
        if remote.metadata.len() != rows {
            return Err(Report::new(RemoteRelayDecodeError::MetadataCount {
                metadata: remote.metadata.len(),
                rows,
            }));
        }
        if remote.acks.len() != rows {
            return Err(Report::new(RemoteRelayDecodeError::AckCount {
                acks: remote.acks.len(),
                rows,
            }));
        }
        let key = BranchKey::from_remote_key(remote.key.take())
            .change_context(RemoteRelayDecodeError::BranchKey)?;
        Ok(DecodedRemoteRows { batch, key })
    }

    async fn handle_remote_stream_payload_with_admission(
        &self,
        mut remote: RelayPayload,
        owner_ingress: bool,
        routing: &mut DomainRoutingCache,
        admission: Option<RemoteRelayAdmissionContext<'_>>,
    ) -> error_stack::Result<(), RuntimeError> {
        let RemoteRelayTarget { services, schema } = self
            .wait_for_remote_stream_target(routing, &remote.domain, &remote.relay)
            .await?;
        if owner_ingress && !self.owns_relay(&services) {
            return Err(Report::new(RuntimeError::relay_not_instantiated(
                &remote.domain,
                &remote.relay,
            )));
        }
        let decoded = match self.decode_remote_rows(&schema, &mut remote).await {
            Ok(decoded) => decoded,
            Err(report) => {
                return Err(report.change_context(RuntimeError::decode_remote_relay(
                    &remote.domain,
                    &remote.relay,
                )));
            }
        };
        let acks = remote
            .acks
            .into_iter()
            .map(|ack| {
                if let Some(ack) = ack {
                    let (acks, completion) = self.tracked_ack_root(&remote.domain);
                    self.spawn_remote_ack_watcher(remote.domain.clone(), completion, Some(ack));
                    acks
                } else {
                    AckSet::empty()
                }
            })
            .collect::<Vec<_>>();
        let batch = RelayRecordBatch::from_runtime_batch(
            schema,
            decoded.key,
            decoded.batch,
            RecordMetadataColumns::from_remote(remote.metadata),
            acks,
        )
        .map_err(|error| {
            error
                .change_context(RemoteRelayDecodeError::Batch)
                .change_context(RuntimeError::decode_remote_relay(
                    &remote.domain,
                    &remote.relay,
                ))
        })?;
        let dispatch = async {
            let dispatch = if owner_ingress {
                self.ingest_stream_boundary_message(
                    &remote.domain,
                    &remote.relay,
                    &services,
                    &batch,
                )
                .await
            } else {
                self.inject_remote_stream_boundary_message(&services, &batch)
                    .await
            };
            if dispatch.is_ok() {
                for ack in batch.acks.iter() {
                    ack.ack_success();
                }
                return Ok(());
            }
            for ack in batch.acks.iter() {
                ack.no_ack("failed to dispatch remote relay message through local runtime");
            }
            Err(Report::new(RuntimeError::DispatchRemoteRelay {
                domain: remote.domain.clone(),
                relay: remote.relay.clone(),
            }))
        };
        if let Some(admission) = admission {
            if admission.transport.admit() == RelayAdmissionDecision::Cancelled {
                return Ok(());
            }
            *admission.admitted = true;
            self.send_remote_relay_admission_outcome(admission.registration, RemoteAckOutcome::Ack)
                .await;
            self.inner
                .fault_injection
                .pause_remote_relay_dispatch_if_armed(&remote.domain)
                .await;
            return dispatch.await;
        }
        dispatch.await
    }

    async fn handle_remote_subscription_payload_with_admission(
        &self,
        mut remote: RelayPayload,
        routing: &mut DomainRoutingCache,
        admission: Option<RemoteRelayAdmissionContext<'_>>,
    ) -> error_stack::Result<(), RuntimeError> {
        let (services, schema) = {
            let snapshot = routing.load();
            let not_instantiated = || {
                Report::new(RuntimeError::relay_not_instantiated(
                    &remote.domain,
                    &remote.relay,
                ))
            };
            if snapshot.passive_only {
                return Err(not_instantiated());
            }
            let Some(services) = snapshot.relay_services.get(&remote.relay).cloned() else {
                return Err(not_instantiated());
            };
            let Some(schema) = snapshot.relay_schemas.get(&remote.relay).cloned() else {
                return Err(not_instantiated());
            };
            (services, schema)
        };
        if remote.acks.iter().any(Option::is_some) {
            let error = Report::new(RemoteRelayDecodeError::SubscriptionAcks).change_context(
                RuntimeError::decode_remote_relay(&remote.domain, &remote.relay),
            );
            return Err(error);
        }
        let decoded = match self.decode_remote_rows(&schema, &mut remote).await {
            Ok(decoded) => decoded,
            Err(report) => {
                return Err(report.change_context(RuntimeError::decode_remote_relay(
                    &remote.domain,
                    &remote.relay,
                )));
            }
        };
        let ack_count = remote.acks.len();
        let batch = RelayRecordBatch::from_runtime_batch(
            schema,
            decoded.key,
            decoded.batch,
            RecordMetadataColumns::from_remote(remote.metadata),
            vec![AckSet::empty(); ack_count],
        )
        .map_err(|error| {
            error
                .change_context(RemoteRelayDecodeError::Batch)
                .change_context(RuntimeError::decode_remote_relay(
                    &remote.domain,
                    &remote.relay,
                ))
        })?;
        let dispatch = services.fanout_local_subscriptions(&batch);
        if let Some(admission) = admission {
            if admission.transport.admit() == RelayAdmissionDecision::Cancelled {
                return Ok(());
            }
            *admission.admitted = true;
            self.send_remote_relay_admission_outcome(admission.registration, RemoteAckOutcome::Ack)
                .await;
            dispatch.await;
            return Ok(());
        }
        dispatch.await;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn handle_remote_subscription_payload(
        &self,
        remote: RelayPayload,
    ) -> Result<(), Report<RuntimeError>> {
        let Some(mut routing) = self.domain_routing_cache(&remote.domain) else {
            return Err(Report::new(RuntimeError::relay_not_instantiated(
                &remote.domain,
                &remote.relay,
            )));
        };
        self.handle_remote_subscription_payload_with_admission(remote, &mut routing, None)
            .await
    }

    /// Resolves the admission or record acknowledgement that `resolution` names.
    ///
    /// Only a registration this run of the node handed out is resolved. A receiver keeps resolving
    /// what an earlier run of the node registered after that run ended, and the earlier run
    /// numbered its registrations from one as this run does, so a resolution naming another run is
    /// rejected rather than resolving the entry this run holds under the same number.
    pub(crate) fn handle_remote_ack_resolution(&self, resolution: RemoteAckResolution) {
        let RemoteAckResolution {
            registration,
            outcome,
        } = resolution;
        let dispatcher = self.inner.remote_dispatcher.load();
        let Some(dispatcher) = dispatcher.as_deref() else {
            debug!(
                ack_id = registration.ack_id,
                registrar = %registration.registrar,
                "rejected a remote ack resolution before this node joined its cluster"
            );
            return;
        };
        if !dispatcher.registered(&registration) {
            debug!(
                ack_id = registration.ack_id,
                registrar = %registration.registrar,
                "rejected a remote ack resolution addressed to another run of this node"
            );
            return;
        }
        let ack_id = registration.ack_id;
        if outcome.is_progress() {
            if let Some(admission) = nervix_primitives::expect_lint!(
                nervix::sync_acquisition,
                "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts \
                 and acknowledgement owners through terminal delivery",
                self.inner
                    .remote_dispatch
                    .pending_relay_admissions
                    .get(&ack_id)
            ) {
                if admission.is_closed() {
                    drop(admission);
                    nervix_primitives::expect_lint!(
                        nervix::sync_acquisition,
                        "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload \
                         attempts and acknowledgement owners through terminal delivery",
                        self.inner
                            .remote_dispatch
                            .pending_relay_admissions
                            .remove(&ack_id)
                    );
                } else {
                    admission.send_if_modified(|update| {
                        if let RelayAdmissionUpdate::Admitted | RelayAdmissionUpdate::Rejected(_) =
                            update
                        {
                            return false;
                        }
                        *update = RelayAdmissionUpdate::Alive;
                        true
                    });
                }
                return;
            }
            // A report can follow the acknowledgement's failure: the receiver keeps reporting a
            // record whose reports this node stopped hearing for the whole silence bound.
            let reported = match outcome {
                RemoteAckOutcome::Alive => self.inner.remote_dispatch.report_ack(ack_id),
                RemoteAckOutcome::Progress { sequence, parked } => self
                    .inner
                    .remote_dispatch
                    .progress_ack(ack_id, sequence, parked),
                RemoteAckOutcome::Ack | RemoteAckOutcome::NoAck(_) => unreachable!(),
            };
            if !reported {
                debug!(ack_id, "received remote ack progress for unknown ack id");
                return;
            }
            trace!(ack_id, "received remote ack progress");
            return;
        }

        if let Some((_, admission)) = nervix_primitives::expect_lint!(
            nervix::sync_acquisition,
            "Typed Ratchet 05 https://app.clickup.com/t/86bc9erep: retain payload attempts and \
             acknowledgement owners through terminal delivery",
            self.inner
                .remote_dispatch
                .pending_relay_admissions
                .remove(&ack_id)
        ) {
            let terminal_update = match outcome {
                RemoteAckOutcome::Ack => RelayAdmissionUpdate::Admitted,
                RemoteAckOutcome::NoAck(error) => RelayAdmissionUpdate::Rejected(error),
                RemoteAckOutcome::Alive | RemoteAckOutcome::Progress { .. } => return,
            };
            admission.send_if_modified(|update| {
                if let RelayAdmissionUpdate::Admitted | RelayAdmissionUpdate::Rejected(_) = update {
                    return false;
                }
                *update = terminal_update;
                true
            });
            return;
        }

        let outcome = match outcome {
            RemoteAckOutcome::Ack => AckOutcome::Ack,
            RemoteAckOutcome::NoAck(error) => AckOutcome::NoAck(error),
            RemoteAckOutcome::Alive | RemoteAckOutcome::Progress { .. } => return,
        };
        trace!(ack_id, outcome = ?outcome, "resolving remote ack");
        // An outcome can follow the acknowledgement's failure, when the sweep failed it after the
        // receiver fell silent, or when its delivery failed after the receiver admitted it.
        if !self.inner.remote_dispatch.resolve_ack(ack_id, outcome) {
            debug!(ack_id, "received remote ack resolution for unknown ack id");
        }
    }

    pub(in crate::runtime) fn spawn_remote_ack_watcher(
        &self,
        domain: DomainName,
        completion: AckCompletion,
        ack: Option<RemoteAckRegistration>,
    ) {
        let Some(ack) = ack else {
            return;
        };
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return;
        };
        let fault_injection = self.inner.fault_injection.clone();
        self.spawn_remote_ack_watcher_task(async move {
            let mut completion = completion;
            loop {
                nervix_primitives::select! {
                    _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {
                        trace!(
                            domain = domain.as_str(),
                            ack_id = ack.ack_id,
                            target_node = %ack.registrar,
                            "sending remote ack alive"
                        );
                        if let Err(error) = dispatcher
                            .dispatch(
                                ack.registrar.node_id(),
                                Envelope::Ack(ack.resolution(remote_ack_progress(&completion))),
                            )
                            .await
                        {
                            warn!(
                                domain = domain.as_str(),
                                ack_id = ack.ack_id,
                                target_node = %ack.registrar,
                                error = %error,
                                "failed to return remote ack alive"
                            );
                        }
                    }
                    progress = completion.wait_for_progress() => {
                        match progress {
                            AckProgress::Alive => {
                                trace!(
                                    domain = domain.as_str(),
                                    ack_id = ack.ack_id,
                                    target_node = %ack.registrar,
                                    "forwarding remote ack alive"
                                );
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        ack.registrar.node_id(),
                                        Envelope::Ack(ack.resolution(remote_ack_progress(&completion))),
                                    )
                                    .await
                                {
                                    warn!(
                                        domain = domain.as_str(),
                                        ack_id = ack.ack_id,
                                        target_node = %ack.registrar,
                                        error = %error,
                                        "failed to forward remote ack alive"
                                    );
                                }
                            }
                            AckProgress::Complete(outcome) => {
                                trace!(
                                    domain = domain.as_str(),
                                    ack_id = ack.ack_id,
                                    target_node = %ack.registrar,
                                    outcome = ?outcome,
                                    "sending remote ack resolution"
                                );
                                let outcome = match outcome {
                                    AckOutcome::Ack => RemoteAckOutcome::Ack,
                                    AckOutcome::NoAck(error) => RemoteAckOutcome::NoAck(error),
                                };
                                if fault_injection.loses_remote_acknowledgement(
                                    dispatcher.local_node_id(),
                                    ack.registrar.node_id(),
                                ) {
                                    break;
                                }
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        ack.registrar.node_id(),
                                        Envelope::Ack(ack.resolution(outcome)),
                                    )
                                    .await
                                {
                                    warn!(
                                        domain = domain.as_str(),
                                        ack_id = ack.ack_id,
                                        target_node = %ack.registrar,
                                        error = %error,
                                        "failed to return remote ack resolution"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn spawn_remote_ack_watcher_task(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let shutdown = self.inner.remote_ack_watcher_shutdown.clone();
        if shutdown.is_cancelled() {
            return;
        }
        self.inner.remote_ack_watcher_tasks.spawn(async move {
            nervix_primitives::select! {
                biased;
                _ = shutdown.cancelled() => {}
                _ = task => {}
            }
        });
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the typed revision supplies bounded iteration over its \
                                   installed emitter and entrypoint plans")
    )]
    pub(in crate::runtime) fn remote_runtime_consumers_for_revision(
        revision: &ExecutionRevision,
        local_node_id: &ClusterNodeName,
    ) -> HashMap<RelayName, Vec<RemoteRuntimeConsumer>> {
        let mut consumers = HashMap::<RelayName, Vec<RemoteRuntimeConsumer>>::new();
        let owned_relays = revision
            .nodes
            .values()
            .filter(|node| node.execution_node() == Some(local_node_id))
            .filter(|node| node.kind() == ModelKind::Relay)
            .map(|node| RelayName::from(&node.identifier))
            .collect::<HashSet<_>>();
        for spec in &revision.processors.processors {
            let Some(node) = revision
                .nodes
                .get(&NodeRef::new(spec.spec.kind, spec.spec.processor.clone()))
            else {
                continue;
            };
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            for relay in &spec.spec.input_relays {
                if !owned_relays.contains(relay) || node.executes_on(local_node_id) {
                    continue;
                }
                push_remote_runtime_consumer(
                    consumers.entry(relay.clone()).or_default(),
                    target_node,
                    relay,
                    spec.spec.mode,
                );
            }
        }
        for emitter in revision.emitters.emitters() {
            let node = revision
                .nodes
                .get(&NodeRef::new(
                    ModelKind::Emitter,
                    ModelName::from(&emitter.name),
                ))
                .assured("the emitter plans were decided from this same schedule");
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            for input in &emitter.inputs {
                if !owned_relays.contains(&input.relay) || node.executes_on(local_node_id) {
                    continue;
                }
                push_remote_runtime_consumer(
                    consumers.entry(input.relay.clone()).or_default(),
                    target_node,
                    &input.relay,
                    emitter.mode,
                );
            }
        }
        for plan in revision.entrypoints.reingestors() {
            let identity = NodeRef::new(ModelKind::Reingestor, ModelName::from(&plan.name));
            let node = revision
                .nodes
                .get(&identity)
                .assured("every caller passes the entrypoint plans decided from this schedule");
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            if node.executes_on(local_node_id) {
                continue;
            }
            for input in &plan.inputs {
                if !owned_relays.contains(&input.relay) {
                    continue;
                }
                push_remote_runtime_consumer(
                    consumers.entry(input.relay.clone()).or_default(),
                    target_node,
                    &input.relay,
                    plan.mode,
                );
            }
        }
        consumers
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::FutureExt as _;
    use nervix_models::{AckMode, ClusterNodeName, RemoteAckOutcome};
    use nervix_primitives::{
        sync::{oneshot, watch},
        time::{Instant, sleep, timeout},
    };

    use super::*;
    use crate::runtime_ack::{AckCompletion, AckOutcome, AckSet};

    /// Scheduler-independent hang guard for event-driven unit-test observations.
    const ASYNC_EVENT_FAILSAFE: Duration = Duration::from_secs(30);

    struct DropNotice(Option<oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                sender
                    .send(())
                    .means_peer_left("remote ACK watcher cancellation test");
            }
        }
    }

    #[test]
    fn a_reingestor_on_another_node_consumes_the_relays_this_node_owns() {
        let domain = domain("default");
        let fixture = EntrypointTestDomain {
            relays: &["incoming", "outgoing"],
            fields: &[("value", nervix_models::ParseAsType::I64)],
            branch_fields: &[],
        };
        let reingestor = nervix_models::CreateReingestor {
            name: named("repartition"),
            from: nervix_models::ProcessorInputs::single(named("incoming")),
            output_routes: with_inherit_all(nervix_models::ProcessorOutputs::single(named(
                "outgoing",
            )))
            .with_flush_policy(FlushPolicy::Immediate)
            .with_branch(nervix_models::OutputBranch::Unbranched),
            mode: AckMode::Detached,
            materialized_state: Vec::new(),
            filter_where: None,
        };
        let plans = fixture.plans(
            &domain,
            vec![nervix_models::Model::Reingestor(reingestor.clone())],
        );
        let owner = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
        let executor = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
        let mut nodes = plans.nodes.into_values().collect::<Vec<_>>();
        for node in &mut nodes {
            let placement = if node.kind() == ModelKind::Reingestor {
                &executor
            } else {
                &owner
            };
            node.primary_node = Some(placement.clone());
            node.assigned_nodes = vec![placement.clone()];
        }
        let schedule = DomainSchedule::new(domain.clone(), nodes, Vec::new());
        let revision = ExecutionRevision::from_schedule(&schedule)
            .assured("the fixture schedule resolves its complete execution plans");

        let owned = Runtime::remote_runtime_consumers_for_revision(&revision, &owner);
        let consumers = owned
            .get(&named::<RelayName>("incoming"))
            .assured("the remote reingestor reads the relay this node owns");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].node_id, executor);
        assert_eq!(consumers[0].mode, AckMode::Detached);
        assert!(!owned.contains_key(&named::<RelayName>("outgoing")));

        let executing = Runtime::remote_runtime_consumers_for_revision(&revision, &executor);
        assert!(executing.is_empty());
    }

    #[nervix_primitives::test]
    async fn runtime_shutdown_cancels_remote_ack_watcher_tasks() {
        let runtime = Runtime::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        runtime.spawn_remote_ack_watcher_task(async move {
            let _notice = DropNotice(Some(dropped_tx));
            started_tx
                .send(())
                .means_peer_left("remote ACK watcher cancellation test starter");
            std::future::pending::<()>().await;
        });
        started_rx
            .await
            .assured("the tracked watcher sends before entering its pending state");

        runtime.shutdown().await;

        let dropped = timeout(ASYNC_EVENT_FAILSAFE, dropped_rx).await;
        assert!(
            dropped.is_ok(),
            "runtime shutdown should cancel a pending remote ACK watcher"
        );
        dropped
            .verified("the watcher cancellation deadline was checked by the assertion above")
            .assured("dropping the watcher always sends its drop notice");
        assert!(runtime.inner.remote_ack_watcher_tasks.is_empty());
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn ack_alive_resets_ingestor_ack_timeout() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (acks, completion) = AckSet::root();
        let ack_task = acks.clone();

        nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            ack_task.ack_alive();
            sleep(Duration::from_millis(150)).await;
            ack_task.ack_success();
        });

        assert_eq!(
            Runtime::await_ack_completion(
                &mut shutdown_rx,
                completion,
                Duration::from_millis(200),
            )
            .await,
            Some(AckOutcome::Ack)
        );
        drop(shutdown_tx);
    }

    /// A runtime attached to a loopback cluster of one node, and the dispatcher that hands out its
    /// registrations.
    async fn joined_runtime() -> (Runtime, StdArc<RemoteDispatcher>) {
        let runtime = Runtime::default();
        let node = ClusterNodeName::parse("node-1").expect("the fixture node name is valid");
        attach_loopback_cluster(&runtime, &node).await;
        let dispatcher = runtime
            .inner
            .remote_dispatcher
            .load_full()
            .expect("attaching the loopback cluster attaches the dispatcher");
        (runtime, dispatcher)
    }

    /// The node the tests' deliveries go to, which resolves the acknowledgements they forward.
    fn receiving_node() -> ClusterNodeName {
        ClusterNodeName::parse("node-2").expect("the fixture node name is valid")
    }

    /// The registration an earlier run of the same node handed out under the same number.
    fn from_an_earlier_run(registration: &RemoteAckRegistration) -> RemoteAckRegistration {
        let incarnation = registration
            .registrar
            .incarnation()
            .get()
            .checked_sub(1)
            .expect("a loopback cluster's incarnation is its start time in nanoseconds");
        RemoteAckRegistration {
            ack_id: registration.ack_id,
            registrar: ClusterNodeIdentity::new(
                registration.registrar.node_id().clone(),
                ClusterNodeIncarnation::new(incarnation),
            ),
        }
    }

    #[nervix_primitives::test]
    async fn remote_ack_alive_packet_resets_ingestor_ack_timeout() {
        let (runtime, dispatcher) = joined_runtime().await;
        nervix_primitives::time::pause();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (acks, completion) = AckSet::root();
        let registration = dispatcher.register_pending_ack(&receiving_node(), acks);
        let runtime_task = runtime.clone();
        let resolved = registration.clone();

        nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Alive));
            sleep(Duration::from_millis(150)).await;
            runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Ack));
        });

        assert_eq!(
            Runtime::await_ack_completion(
                &mut shutdown_rx,
                completion,
                Duration::from_millis(200),
            )
            .await,
            Some(AckOutcome::Ack)
        );
        assert!(
            !runtime.inner.remote_dispatch.holds_ack(registration.ack_id),
            "terminal ack must clear the pending remote ack"
        );
        drop(shutdown_tx);
    }

    #[nervix_primitives::test]
    async fn remote_parked_progress_releases_and_restores_upstream_handoff_ownership() {
        let (runtime, dispatcher) = joined_runtime().await;
        let tracker = Arc::new(AckRootTracker::default());
        let (acks, completion) = AckSet::tracked_root(tracker.clone());
        let registration = dispatcher.register_pending_ack(&receiving_node(), acks);
        dispatcher.registry.admit_ack(registration.ack_id);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);

        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Progress {
            sequence: 1,
            parked: true,
        }));
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
        assert!(completion.remote_progress().1);

        // A delayed status cannot undo the newer park. An actual resume can.
        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Progress {
            sequence: 0,
            parked: false,
        }));
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Progress {
            sequence: 2,
            parked: false,
        }));
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);
        assert!(!completion.remote_progress().1);

        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
        assert!(!runtime.inner.remote_dispatch.holds_ack(registration.ack_id));
    }

    #[nervix_primitives::test]
    async fn remote_relay_admission_alive_resets_dispatch_timeout() {
        let (runtime, dispatcher) = joined_runtime().await;
        nervix_primitives::time::pause();
        let (registration, admission_rx) = dispatcher.register_pending_relay_admission();
        let runtime_task = runtime.clone();
        let resolved = registration.clone();

        nervix_primitives::task::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Alive));
            sleep(Duration::from_millis(150)).await;
            runtime_task.handle_remote_ack_resolution(resolved.resolution(RemoteAckOutcome::Ack));
        });

        RemoteDispatcher::await_relay_admission(
            &ClusterNodeName::parse("relay-owner").expect("valid name"),
            admission_rx,
            Duration::from_millis(200),
        )
        .await
        .expect("alive progress should preserve the relay admission wait");
        assert!(
            runtime
                .inner
                .remote_dispatch
                .pending_relay_admissions
                .get(&registration.ack_id)
                .is_none(),
            "terminal admission ack must clear pending admission state"
        );
    }

    #[nervix_primitives::test]
    async fn a_resolution_addressed_to_an_earlier_run_leaves_the_pending_ack_unresolved() {
        let (runtime, dispatcher) = joined_runtime().await;
        let (acks, completion) = AckSet::root();
        let registration = dispatcher.register_pending_ack(&receiving_node(), acks);
        let completion = completion.wait();
        tokio::pin!(completion);

        let earlier = from_an_earlier_run(&registration);
        runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Alive));
        runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Ack));

        assert!(
            (&mut completion).now_or_never().is_none(),
            "an acknowledgement addressed to an earlier run must not complete this run's record"
        );
        assert!(
            runtime.inner.remote_dispatch.holds_ack(registration.ack_id),
            "this run's registration stays pending for its own resolution"
        );

        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));
        assert_eq!(
            timeout(ASYNC_EVENT_FAILSAFE, completion)
                .await
                .expect("this run's own resolution completes the record"),
            AckOutcome::Ack
        );
    }

    #[nervix_primitives::test]
    async fn a_resolution_addressed_to_an_earlier_run_leaves_the_relay_admission_pending() {
        let (runtime, dispatcher) = joined_runtime().await;
        let (registration, mut admission_rx) = dispatcher.register_pending_relay_admission();

        let earlier = from_an_earlier_run(&registration);
        runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Alive));
        runtime.handle_remote_ack_resolution(earlier.resolution(RemoteAckOutcome::Ack));

        assert!(
            !admission_rx
                .has_changed()
                .expect("the pending admission keeps its sender"),
            "an admission outcome addressed to an earlier run must not reach this run's admission"
        );

        runtime.handle_remote_ack_resolution(
            registration.resolution(RemoteAckOutcome::NoAck("branch is draining".to_string())),
        );
        admission_rx
            .changed()
            .await
            .expect("this run's own admission outcome remains observable");
        assert!(matches!(
            &*admission_rx.borrow_and_update(),
            RelayAdmissionUpdate::Rejected(reason) if reason == "branch is draining"
        ));
    }

    #[nervix_primitives::test]
    async fn a_cleared_registration_ignores_a_late_resolution() {
        let (runtime, dispatcher) = joined_runtime().await;
        let (acks, completion) = AckSet::root();
        let acknowledgement = dispatcher.register_pending_ack(&receiving_node(), acks.clone());
        let (admission, admission_rx) = dispatcher.register_pending_relay_admission();

        dispatcher.clear_pending_ack(&acknowledgement);
        dispatcher.clear_pending_relay_admission(&admission);
        runtime.handle_remote_ack_resolution(acknowledgement.resolution(RemoteAckOutcome::Alive));
        runtime.handle_remote_ack_resolution(acknowledgement.resolution(RemoteAckOutcome::Ack));
        runtime.handle_remote_ack_resolution(admission.resolution(RemoteAckOutcome::Ack));

        let completion = completion.wait();
        tokio::pin!(completion);
        assert!(
            (&mut completion).now_or_never().is_none(),
            "the dispatch that cleared the registration resolves the record, not a late outcome"
        );
        assert!(
            admission_rx.has_changed().is_err(),
            "clearing the admission ends its updates"
        );
        drop(acks);
    }

    #[nervix_primitives::test]
    async fn progress_for_an_abandoned_admission_retires_its_registration() {
        let (runtime, dispatcher) = joined_runtime().await;
        let (admission, admission_rx) = dispatcher.register_pending_relay_admission();
        drop(admission_rx);

        runtime.handle_remote_ack_resolution(admission.resolution(RemoteAckOutcome::Alive));

        assert!(
            !runtime
                .inner
                .remote_dispatch
                .pending_relay_admissions
                .contains_key(&admission.ack_id),
            "an admission nobody waits for is retired by its next progress report"
        );
    }

    #[nervix_primitives::test]
    async fn a_node_outside_a_cluster_resolves_nothing() {
        let runtime = Runtime::default();
        let (acks, completion) = AckSet::root();
        runtime
            .inner
            .remote_dispatch
            .register_ack(1, receiving_node(), acks);
        let completion = completion.wait();
        tokio::pin!(completion);
        let registration = RemoteAckRegistration {
            ack_id: 1,
            registrar: ClusterNodeIdentity::new(
                ClusterNodeName::parse("node-1").expect("the fixture node name is valid"),
                ClusterNodeIncarnation::new(1),
            ),
        };

        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));

        assert!(
            (&mut completion).now_or_never().is_none(),
            "a node that has not joined a cluster has handed out no registration to resolve"
        );
    }

    #[nervix_primitives::test]
    async fn remote_relay_admission_rejection_preserves_the_typed_reason() {
        let target = ClusterNodeName::parse("relay-owner").expect("valid name");
        let (admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);
        admission_tx.send_replace(RelayAdmissionUpdate::Rejected(
            "branch is draining".to_string(),
        ));

        let error =
            RemoteDispatcher::await_relay_admission(&target, admission_rx, Duration::from_secs(1))
                .await
                .expect_err("a rejected relay admission must fail dispatch");

        assert!(matches!(
            error.current_context(),
            RemoteDispatchError::AdmissionRejected {
                target: error_target,
            } if error_target == &target
        ));
    }

    #[nervix_primitives::test]
    async fn remote_relay_admission_reports_a_closed_response_channel() {
        let target = ClusterNodeName::parse("relay-owner").expect("valid name");
        let (admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);
        drop(admission_tx);

        let error =
            RemoteDispatcher::await_relay_admission(&target, admission_rx, Duration::from_secs(1))
                .await
                .expect_err("a closed relay admission response must fail dispatch");

        assert!(matches!(
            error.current_context(),
            RemoteDispatchError::AdmissionResponseClosed {
                target: error_target,
            } if error_target == &target
        ));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn remote_relay_admission_bounds_the_total_wait() {
        let target = ClusterNodeName::parse("relay-owner").expect("valid name");
        let (_admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);

        let error = RemoteDispatcher::await_relay_admission(
            &target,
            admission_rx,
            REMOTE_RELAY_TOTAL_TIMEOUT + Duration::from_secs(1),
        )
        .await
        .expect_err("relay admission must stop at its total deadline");

        assert!(matches!(
            error.current_context(),
            RemoteDispatchError::AdmissionTotalTimeout {
                target: error_target,
                timeout: REMOTE_RELAY_TOTAL_TIMEOUT,
            } if error_target == &target
        ));
    }

    #[nervix_primitives::test]
    async fn remote_relay_admission_progress_is_coalesced() {
        let (runtime, dispatcher) = joined_runtime().await;
        let (registration, mut admission_rx) = dispatcher.register_pending_relay_admission();

        for _ in 0..100 {
            runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Alive));
        }
        assert!(
            admission_rx
                .has_changed()
                .expect("sender should remain open")
        );
        assert!(matches!(
            &*admission_rx.borrow_and_update(),
            RelayAdmissionUpdate::Alive
        ));
        assert!(
            !admission_rx
                .has_changed()
                .expect("sender should remain open"),
            "replaceable progress must occupy one pending update"
        );

        runtime.handle_remote_ack_resolution(registration.resolution(RemoteAckOutcome::Ack));
        admission_rx
            .changed()
            .await
            .expect("the terminal admission outcome should remain observable");
        assert!(matches!(
            &*admission_rx.borrow_and_update(),
            RelayAdmissionUpdate::Admitted
        ));
    }

    #[nervix_primitives::test]
    async fn forwarding_to_a_remote_relay_owner_extends_the_ack_chain() {
        let (acks, completion) = AckSet::root();
        let forwarded = RemoteDispatcher::forwarded_ack(&acks);
        let completion = completion.wait();
        tokio::pin!(completion);

        acks.ack_success();
        assert!(
            timeout(Duration::from_millis(50), &mut completion)
                .await
                .is_err(),
            "local producer completion must wait for the remote relay path"
        );
        forwarded.ack_success();
        assert_eq!(
            timeout(Duration::from_secs(1), &mut completion)
                .await
                .expect("remote relay completion should resolve the producer ACK"),
            AckOutcome::Ack
        );
    }

    #[nervix_primitives::test]
    async fn relay_gate_holds_nonowner_dispatch_before_the_remote_slot() {
        let services = test_relay_boundary_services();
        services.replace_owner_node(Some(ClusterNodeName::parse("node-2").expect("valid name")));
        let gate = services.fanout.dispatch_gate();
        let lease = RelayDispatchGateLease::engage(
            gate,
            Instant::now() + Duration::from_secs(1),
            "relay owner is moving",
        );
        let task_services = services.clone();
        let dispatch = nervix_primitives::task::spawn(async move {
            task_services
                .dispatch_to_owner(&domain("default"), &named("orders"), &quiesce_test_batch())
                .await
        });

        nervix_primitives::task::yield_now().await;
        assert!(
            !dispatch.is_finished(),
            "a gated nonowner must not enter its remote dispatch slot"
        );

        drop(lease);
        timeout(Duration::from_secs(1), dispatch)
            .await
            .expect("releasing the gate should wake the nonowner dispatch")
            .expect("dispatch task should join")
            .expect_err("the isolated test has no remote dispatcher");
    }

    #[nervix_primitives::test]
    async fn relay_gate_fails_buffered_owner_batch_when_its_attached_consumer_moves() {
        let domain = domain("default");
        let relay = named("orders");
        let services = test_relay_boundary_services();
        let consumer = services.add_local_runtime_consumer(AckMode::Attached);
        let (acks, completion) = AckSet::root();
        let mut batch = quiesce_test_batch();
        batch.acks = vec![acks.clone()];
        let gate = services.fanout.dispatch_gate();
        let mut lease = RelayDispatchGateLease::engage(
            gate,
            Instant::now() + Duration::from_secs(1),
            "attached consumer is moving",
        );
        assert!(lease.wait_quiescent().await);
        drop(consumer);
        services.remove_local_runtime_consumer(AckMode::Attached);
        let result = services
            .fanout_owner_batch(
                &domain,
                &relay,
                &batch,
                &ConfiguredFaultInjection::default(),
            )
            .await;
        assert!(
            result.is_err(),
            "a closed routing gate must fail the attached batch before fan-out"
        );
        let outcome = timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("the failed owner fan-out must resolve its root ACK");
        assert!(matches!(outcome, AckOutcome::NoAck(_)));
    }

    /// One acknowledgement forwarded to `receiving_node()`, held by a registry of its own.
    struct ForwardedAck {
        registry: RemoteDispatchRegistry,
        ack_id: u64,
        completion: AckCompletion,
    }

    impl ForwardedAck {
        fn registered() -> Self {
            let registry = RemoteDispatchRegistry::new();
            let (acks, completion) = AckSet::root();
            let ack_id = registry.next_ack_id();
            registry.register_ack(ack_id, receiving_node(), acks);
            Self {
                registry,
                ack_id,
                completion,
            }
        }

        /// Runs `sweeps` sweeps and requires that none of them fails anything.
        fn sweep_without_failing(&self, sweeps: u64) {
            for _ in 0..sweeps {
                assert!(
                    self.registry.fail_silent_acks().is_empty(),
                    "no sweep before the silence bound passes may fail the acknowledgement"
                );
            }
        }

        /// Requires the next sweep to fail the acknowledgement.
        fn sweep_failing_it(&self) {
            assert_eq!(
                self.registry.fail_silent_acks().get(&receiving_node()),
                Some(&1),
                "the first sweep past the silence bound fails the acknowledgement"
            );
            assert!(!self.registry.holds_ack(self.ack_id));
        }
    }

    #[nervix_primitives::test]
    async fn an_admitted_acknowledgement_its_receiver_stops_reporting_fails_at_the_silence_bound() {
        let forwarded = ForwardedAck::registered();
        forwarded.registry.admit_ack(forwarded.ack_id);

        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
        forwarded.sweep_failing_it();

        let outcome = timeout(ASYNC_EVENT_FAILSAFE, forwarded.completion.wait())
            .await
            .expect("the failed acknowledgement resolves its root");
        assert_eq!(
            outcome,
            AckOutcome::NoAck(
                "node 'node-2' reported nothing about the forwarded record for 15s".to_string()
            )
        );
        assert!(
            !forwarded.registry.report_ack(forwarded.ack_id),
            "a report after the failure finds nothing to keep alive"
        );
        assert!(
            !forwarded
                .registry
                .resolve_ack(forwarded.ack_id, AckOutcome::Ack),
            "an outcome after the failure finds nothing to resolve"
        );
    }

    #[nervix_primitives::test]
    async fn one_sweep_counts_every_acknowledgement_it_fails_against_its_receiver() {
        let registry = RemoteDispatchRegistry::new();
        let mut completions = Vec::new();
        for _ in 0..2 {
            let (acks, completion) = AckSet::root();
            let ack_id = registry.next_ack_id();
            registry.register_ack(ack_id, receiving_node(), acks);
            registry.admit_ack(ack_id);
            completions.push(completion);
        }
        for _ in 0..REMOTE_ACK_SILENT_SWEEPS {
            assert!(registry.fail_silent_acks().is_empty());
        }

        let failed = registry.fail_silent_acks();

        assert_eq!(failed.get(&receiving_node()), Some(&2));
        for completion in completions {
            let outcome = timeout(ASYNC_EVENT_FAILSAFE, completion.wait())
                .await
                .expect("every failed acknowledgement resolves its root");
            assert!(matches!(outcome, AckOutcome::NoAck(_)));
        }
    }

    #[nervix_primitives::test]
    async fn a_report_restarts_the_silence_of_an_admitted_acknowledgement() {
        let forwarded = ForwardedAck::registered();
        forwarded.registry.admit_ack(forwarded.ack_id);
        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);

        assert!(forwarded.registry.report_ack(forwarded.ack_id));

        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
        forwarded.sweep_failing_it();
    }

    #[nervix_primitives::test]
    async fn an_acknowledgement_is_not_swept_before_its_delivery_is_admitted() {
        let forwarded = ForwardedAck::registered();
        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);

        let registration = RemoteAckRegistration {
            ack_id: forwarded.ack_id,
            registrar: ClusterNodeIdentity::new(
                ClusterNodeName::parse("node-1").expect("the fixture node name is valid"),
                ClusterNodeIncarnation::new(1),
            ),
        };
        forwarded.registry.admit_acks(&[None, Some(registration)]);

        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);
        forwarded.sweep_failing_it();
    }

    #[nervix_primitives::test]
    async fn a_terminal_outcome_leaves_the_sweep_nothing_to_fail() {
        let forwarded = ForwardedAck::registered();
        forwarded.registry.admit_ack(forwarded.ack_id);
        forwarded.sweep_without_failing(REMOTE_ACK_SILENT_SWEEPS);

        assert!(
            forwarded
                .registry
                .resolve_ack(forwarded.ack_id, AckOutcome::Ack)
        );
        forwarded.sweep_without_failing(1);

        assert_eq!(
            timeout(ASYNC_EVENT_FAILSAFE, forwarded.completion.wait())
                .await
                .expect("the receiver's outcome resolves the root"),
            AckOutcome::Ack
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_running_sweep_fails_an_acknowledgement_at_the_silence_bound() {
        let registry = Arc::new(RemoteDispatchRegistry::new());
        let (acks, completion) = AckSet::root();
        let ack_id = registry.next_ack_id();
        registry.register_ack(ack_id, receiving_node(), acks);
        registry.admit_ack(ack_id);
        let admitted_at = Instant::now();
        let sweeping = registry.clone();
        let sweeps =
            nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });

        let outcome = completion.wait().await;
        let silent_for = admitted_at.elapsed();

        assert!(matches!(outcome, AckOutcome::NoAck(_)));
        assert!(
            silent_for >= REMOTE_ACK_SILENCE_TIMEOUT,
            "the acknowledgement failed after {silent_for:?} of silence"
        );
        assert!(
            silent_for <= REMOTE_ACK_SILENCE_TIMEOUT + REMOTE_ACK_SILENCE_SWEEP_INTERVAL,
            "the acknowledgement failed after {silent_for:?} of silence"
        );
        sweeps.abort();
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_running_sweep_keeps_an_acknowledgement_its_receiver_keeps_reporting() {
        let registry = Arc::new(RemoteDispatchRegistry::new());
        let (acks, completion) = AckSet::root();
        let ack_id = registry.next_ack_id();
        registry.register_ack(ack_id, receiving_node(), acks);
        registry.admit_ack(ack_id);
        let sweeping = registry.clone();
        let sweeps =
            nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });

        for _ in 0..10 {
            sleep(Duration::from_secs(7)).await;
            assert!(
                registry.report_ack(ack_id),
                "an acknowledgement reported within the bound stays pending"
            );
        }
        assert!(registry.resolve_ack(ack_id, AckOutcome::Ack));
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        sweeps.abort();
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_stalled_sweep_counts_once_for_the_time_it_missed() {
        let registry = Arc::new(RemoteDispatchRegistry::new());
        let (acks, _completion) = AckSet::root();
        let ack_id = registry.next_ack_id();
        registry.register_ack(ack_id, receiving_node(), acks);
        registry.admit_ack(ack_id);
        let sweeping = registry.clone();
        let sweeps =
            nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });
        nervix_primitives::task::yield_now().await;

        // The node's execution stalls for four silence bounds, as a paused container's does, so no
        // report could have reached it.
        nervix_primitives::time::advance(REMOTE_ACK_SILENCE_TIMEOUT * 4).await;
        nervix_primitives::task::yield_now().await;

        assert!(
            registry.holds_ack(ack_id),
            "a sweep that ran late counts once rather than for every interval it missed"
        );
        sweeps.abort();
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "remote_dispatch_shuttle_tests.rs"]
mod shuttle_tests;
