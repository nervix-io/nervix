//! Remote relay dispatch, admission, and acknowledgement coordination.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay wire encoding and decoding, epoch-scoped admission reconciliation,
//!   cancellation, progress handling, and remote acknowledgement correlation.
//! - **Depends on.** Branch-local relay boundaries, execution admission, cluster membership, and
//!   the authenticated interconnect.
//! - **Must not know.** Plexus text, transactions, scheduling policy, or connector internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "remote relay delivery, admission and acknowledgements run per payload attempt \
                  or frame"
    )
)]

use error_stack::ResultExt as _;
use nervix_interconnect::{RelayGrantRefusal, TransportError};

use super::{
    relay_send_attempts::{
        GrantCapacityRefusals, RelaySendAttempts, RelaySendFailure, RelaySendStep,
    },
    remote_ack_owner::RecordStorage,
    remote_ack_registrar::{RegistrarLook, RegistrarPresence},
    remote_ack_watch_charge::{RemoteAckWatchCharge, RemoteAckWatchHold},
    *,
};
use crate::runtime_schema::batch_payload_bytes;

pub(super) const REMOTE_RELAY_INSTANTIATION_WAIT: Duration = Duration::from_secs(5);

pub(super) const REMOTE_RELAY_INSTANTIATION_POLL: Duration = Duration::from_millis(25);

pub(super) const REMOTE_ACK_ALIVE_INTERVAL: Duration = nervix_models::HOST_RELAY_ALIVE_INTERVAL;

/// Silence bound for a forwarded record ACK after admission. The next report starts
/// `REMOTE_ACK_ALIVE_INTERVAL` after delivery or giving up at `DISPATCH_TIMEOUT`; two exhausted
/// reports fit this bound. Longer silence means a lost outcome, ended run or missing registrar.
/// A live-view gap ends held reports only after this bound plus one sweep.
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

pub(super) const REMOTE_RELAY_TOTAL_TIMEOUT: Duration = nervix_models::HOST_RELAY_ADMISSION_BOUND;

enum FailedRelayAdmissionResolution {
    Admitted,
    Failed(Report<RemoteDispatchError>),
    Indeterminate(Report<RemoteDispatchError>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum RemoteDispatchError {
    #[error("node '{target}' requires a smaller relay batch")]
    ReceiveBounds {
        target: ClusterNodeName,
        bounds: nervix_models::PeerReceiveBounds,
    },
    #[error("one relay row exceeds node '{target}' receive bounds")]
    RowAboveReceiveBounds { target: ClusterNodeName },
    #[error("remote correlation capacity ({capacity}) is exhausted")]
    CorrelationCapacity { capacity: usize },
    #[error("remote correlation identity space is exhausted")]
    CorrelationIdentityExhausted,
    #[error("remote acknowledgement owner could not reserve relay memory")]
    CorrelationMemory,
    #[error("relay branch delivery {sequence} to node '{target}' was evicted before admission")]
    BranchEvicted {
        target: ClusterNodeName,
        sequence: u64,
    },
    #[error("node '{target}' rejected relay admission")]
    AdmissionRejected { target: ClusterNodeName },
    #[error("node '{target}' refused the relay grant: {refusal}")]
    GrantRefused {
        target: ClusterNodeName,
        refusal: RelayGrantRefusal,
    },
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
    #[error("the payload is addressed to unit {addressed:?}, which does not own its branch key")]
    Unit { addressed: Unit },
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

/// What a receiver holds for the acknowledgement watches of a batch delivered to it.
enum RemoteAckWatchAdmission {
    /// No row asks this node to report its acknowledgement, or this node has not joined a cluster
    /// to report one to.
    Unwatched,
    /// The relay budget took the watches' charge, and `dispatcher` reports their outcomes.
    Charged {
        dispatcher: StdArc<RemoteDispatcher>,
        charge: RemoteAckWatchCharge,
    },
    /// The delivery stopped being admissible while it was held for that charge.
    Cancelled,
}

struct RemoteRelayAdmissionContext<'a> {
    registration: &'a RemoteAckRegistration,
    transport: &'a RelayAdmission,
    admitted: &'a mut bool,
}

/// One row's receiver-side acknowledgement, owned by the batch that admitted it. The batch
/// multiplexes progress and keepalive polls so a wide relay frame does not create a task per row.
struct RemoteAckWatch {
    completion: AckCompletion,
    registration: RemoteAckRegistration,
    registrar: RegistrarPresence,
    /// The terminal outcome the root reached while its registrar's run was missing from the live
    /// view, kept for the run's return: the completion delivers an outcome only once.
    unreturned: Option<AckOutcome>,
}

/// What one look at a receiver's acknowledgement watch sends to its registrar.
enum RemoteAckWatchReport {
    /// Send this report. A terminal outcome is the watch's last one.
    Send(RemoteAckOutcome),
    /// Send nothing now and look again later.
    Hold,
    /// End the watch without sending anything more.
    Release,
}

impl RemoteAckWatch {
    /// A watch that starts now, before the registrar's run has appeared in the live view.
    fn new(completion: AckCompletion, registration: RemoteAckRegistration) -> Self {
        Self {
            completion,
            registration,
            registrar: RegistrarPresence::undiscovered(Instant::now()),
            unreturned: None,
        }
    }

    /// What the watch sends after its root reported `progress` since the last look, when the
    /// live view shows `live` for the registrar's node at `now`.
    fn next_report(
        &mut self,
        progress: Option<AckProgress>,
        live: Option<ClusterNodeIncarnation>,
        now: Instant,
    ) -> RemoteAckWatchReport {
        let terminal = match progress {
            Some(AckProgress::Complete(outcome)) => Some(outcome),
            Some(AckProgress::Alive) | None => self.unreturned.take(),
        };
        let look = self
            .registrar
            .look(self.registration.registrar.incarnation(), live, now);
        match look {
            RegistrarLook::Report => {}
            RegistrarLook::Hold => {
                self.unreturned = terminal;
                return RemoteAckWatchReport::Hold;
            }
            RegistrarLook::Release => return RemoteAckWatchReport::Release,
        }
        let outcome = match terminal {
            Some(AckOutcome::Ack) => RemoteAckOutcome::Ack,
            Some(AckOutcome::NoAck(error)) => RemoteAckOutcome::NoAck(error),
            None => remote_ack_progress(&self.completion),
        };
        RemoteAckWatchReport::Send(outcome)
    }
}

async fn wait_remote_ack_progress(
    mut watch: RemoteAckWatch,
) -> (RemoteAckWatch, Option<AckProgress>) {
    if watch.unreturned.is_some() {
        // The root already delivered its outcome, so only the registrar's return is awaited.
        sleep(REMOTE_ACK_ALIVE_INTERVAL).await;
        return (watch, None);
    }
    let progress = nervix_primitives::select! {
        _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => None,
        progress = watch.completion.wait_for_progress() => Some(progress),
    };
    (watch, progress)
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
    /// The same fault seams the attaching runtime holds.
    pub(super) fault_injection: ConfiguredFaultInjection,
}

/// An outbound delivery retains every registration until admission, or resolves its held shares
/// negatively when the delivery future is cancelled. Accepted rows pass to their delivery owner.
pub(super) struct PendingRemoteAckDelivery {
    registry: Arc<RemoteDispatchRegistry>,
    registrations: Vec<Option<RemoteAckRegistration>>,
    admitted_rows: usize,
}

/// How storing the acknowledgement records of an outbound delivery ended.
pub(super) enum RemoteAckStorage {
    /// The records are stored and travel with the delivery under these registrations.
    Stored(PendingRemoteAckDelivery),
    /// The delivery's channel ended while it waited for room to store them.
    Cancelled,
}

impl PendingRemoteAckDelivery {
    pub(super) fn registrations(&self) -> &[Option<RemoteAckRegistration>] {
        &self.registrations
    }

    pub(super) fn admit(&mut self) {
        self.admit_prefix(self.registrations.len());
    }

    pub(super) fn admitted_rows(&self) -> usize {
        self.admitted_rows
    }

    pub(super) fn admit_prefix(&mut self, rows: usize) {
        assert!(rows >= self.admitted_rows && rows <= self.registrations.len());
        self.registry
            .admit_acks(&self.registrations[self.admitted_rows..rows]);
        self.admitted_rows = rows;
    }
}

impl Drop for PendingRemoteAckDelivery {
    fn drop(&mut self) {
        for registration in self.registrations[self.admitted_rows..].iter().flatten() {
            self.registry.resolve_ack(
                registration.ack_id,
                AckOutcome::NoAck("remote delivery ended before admission".to_string()),
            );
        }
    }
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

    pub(super) fn is_live_node_identity(&self, identity: &ClusterNodeIdentity) -> bool {
        if identity.node_id() == self.local_node_id() {
            return identity.incarnation() == self.local_node_incarnation();
        }
        self.cluster.live_node_incarnation(identity.node_id()) == Some(identity.incarnation())
    }

    /// A registration numbered in this process that names this run of the node, so its resolution
    /// can only ever resolve the entry registered under it here.
    fn registration(&self, ack_id: u64) -> RemoteAckRegistration {
        RemoteAckRegistration {
            ack_id,
            registrar: ClusterNodeIdentity::new(
                self.local_node_id().clone(),
                self.local_node_incarnation(),
            ),
        }
    }

    /// Whether this run of the node handed out `registration`. Each registration includes the
    /// process incarnation in addition to its delivery generation and record position.
    pub(super) fn registered(&self, registration: &RemoteAckRegistration) -> bool {
        registration.registrar.node_id() == self.local_node_id()
            && registration.registrar.incarnation() == self.local_node_incarnation()
    }

    /// Registers `acks` to be resolved by `receiver`, the node the returned registration is sent
    /// to.
    #[cfg(test)]
    pub(super) fn register_pending_ack(
        &self,
        receiver: &ClusterNodeName,
        acks: AckSet,
    ) -> error_stack::Result<RemoteAckRegistration, RemoteDispatchError> {
        let id = self.registry.register_ack(receiver.clone(), acks)?;
        Ok(self.registration(id))
    }

    /// The instant a delivery starting now must be admitted by: one bound covers storing its
    /// records, holding its grant and waiting for its admission.
    pub(super) fn delivery_deadline() -> Instant {
        Instant::now()
            .checked_add(REMOTE_RELAY_TOTAL_TIMEOUT)
            .assured("the fixed relay admission bound fits the monotonic clock")
    }

    /// Stores the records of `acks` to be resolved by `receiver`, the node the returned
    /// registrations are sent to, holding the delivery back while this node's relay budget has no
    /// room for them, until `deadline` or until `cancelled` resolves.
    pub(super) async fn store_pending_acks(
        &self,
        receiver: &ClusterNodeName,
        acks: Vec<AckSet>,
        deadline: Instant,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> error_stack::Result<RemoteAckStorage, RemoteDispatchError> {
        if acks.iter().any(|acks| !acks.is_empty()) {
            self.fault_injection
                .fill_relay_memory_before_record_storage_if_armed(
                    self.local_node_id(),
                    &self.executor,
                );
        }
        let storage = self
            .registry
            .store_acks(receiver.clone(), acks, deadline, cancelled)
            .await?;
        let RecordStorage::Stored(ids) = storage else {
            return Ok(RemoteAckStorage::Cancelled);
        };
        let registrations = ids
            .into_iter()
            .map(|id| id.map(|id| self.registration(id)))
            .collect();
        Ok(RemoteAckStorage::Stored(PendingRemoteAckDelivery {
            registry: self.registry.clone(),
            registrations,
            admitted_rows: 0,
        }))
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
    ) -> error_stack::Result<
        (RemoteAckRegistration, watch::Receiver<RelayAdmissionUpdate>),
        RemoteDispatchError,
    > {
        let (id, receiver) = self.registry.register_admission()?;
        Ok((self.registration(id), receiver))
    }

    pub(super) fn clear_pending_relay_admission(&self, registration: &RemoteAckRegistration) {
        self.registry.clear_ack(registration.ack_id);
    }

    pub(super) fn request_deadline<M: nervix_interconnect::InterconnectRequest>(&self) -> Duration {
        self.interconnect.request_deadline::<M>()
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
        payload: &mut RelayPayload,
        branch_channel: &RelayOutboundSlot,
        deadline: Instant,
        capacity_refusals: GrantCapacityRefusals,
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
        let (registration, admission) = self.register_pending_relay_admission()?;
        payload.admission = Some(registration.clone());
        let dispatch = self.dispatch_relay_payload(node_id, payload, deadline, capacity_refusals);
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
            if matches!(
                error.current_context(),
                RemoteDispatchError::ReceiveBounds { .. }
            ) {
                cancellation_guard.disarm();
                return Err(error);
            }
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
        let admission =
            Self::await_relay_admission(node_id, admission, Self::DISPATCH_TIMEOUT, deadline);
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
                    original_error
                        .change_context(TransportError::RelayRejected(reason))
                        .change_context(RemoteDispatchError::AdmissionRejected {
                            target: node_id.clone(),
                        }),
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
        total_deadline: Instant,
    ) -> error_stack::Result<(), RemoteDispatchError> {
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
                            return Err(Report::new(TransportError::RelayRejected(reason))
                                .change_context(RemoteDispatchError::AdmissionRejected {
                                    target: node_id.clone(),
                                }));
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
        unit: Unit,
        batch: &RelayRecordBatch,
        excluded_nodes: &BTreeSet<ClusterNodeName>,
    ) {
        let local_node_id = self.local_node_id();
        let interest_index = self.cluster.subscription_interest_index();
        let channels = services.subscription_channels(
            &batch.key,
            unit,
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
        let mut encoded_body: Option<(ChargedBytes, u64)> = None;
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
            let (batch_ipc, decoded_bytes) = match encoded_body.clone() {
                Some(bytes) => bytes,
                None => match batch.batch.encode_arrow_ipc(self.executor()).await {
                    Ok(bytes) => {
                        let bytes = (bytes, batch_payload_bytes(batch.batch.batch()));
                        encoded_body = Some(bytes.clone());
                        bytes
                    }
                    Err(error) => {
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %format_args!("{error:#}"),
                            "failed to serialize remote subscription batch"
                        );
                        return;
                    }
                },
            };
            let delivery = outbound_slot.next_delivery();
            // A session subscription never holds the relay owner's fan-out longer than the
            // dispatch bound, so a grant refused for capacity is retried only within it.
            if let Err(error) = self
                .dispatch_sliced_relay_payload(
                    node_id,
                    RelayPayload {
                        decoded_bytes,
                        delivery,
                        kind: RelayPayloadKind::SubscriptionFanout,
                        domain: domain.clone(),
                        relay: relay.clone(),
                        unit,
                        key: BranchKey::to_remote_key(&batch.key),
                        batch_ipc: batch_ipc.clone(),
                        metadata: batch.metadata.to_remote(),
                        acks: vec![None; batch.acks.len()],
                        admission: None,
                    },
                    &batch.batch,
                    &outbound_slot,
                    Self::delivery_deadline(),
                    GrantCapacityRefusals::GiveUp,
                    None,
                )
                .await
            {
                warn!(
                    target_node = %node_id,
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %format_args!("{error:#}"),
                    "failed to dispatch remote subscription payload"
                );
            }
        }
    }

    /// Retry policy belongs to [`RelaySendAttempts`]. Under [`GrantCapacityRefusals::Hold`], an
    /// answered capacity refusal preserves the stall budget until grant or the total deadline.
    /// A definitive sizing refusal returns to the slice owner; uncertain attempts still reconcile.
    async fn dispatch_relay_payload(
        &self,
        node_id: &ClusterNodeName,
        payload: &RelayPayload,
        deadline: Instant,
        capacity_refusals: GrantCapacityRefusals,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        let mut attempts = RelaySendAttempts::new(Instant::now(), deadline, capacity_refusals);
        let mut unresolved_attempt = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            let sent = nervix_primitives::time::timeout_at(
                attempts.attempt_deadline(),
                self.interconnect
                    .send(node_id, Envelope::RelayPayload(payload.clone())),
            )
            .await;
            let error = match sent {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => error,
                Err(_) => {
                    let end = attempts.after_attempt_deadline(Instant::now());
                    return Err(Report::new(end.dispatch_error(node_id)));
                }
            };
            let failure = match error.current_context() {
                TransportError::RelayBoundsRefused(bounds) => {
                    if unresolved_attempt {
                        return Err(error.change_context(RemoteDispatchError::Send {
                            target: node_id.clone(),
                        }));
                    }
                    let bounds = *bounds;
                    return Err(error.change_context(RemoteDispatchError::ReceiveBounds {
                        target: node_id.clone(),
                        bounds,
                    }));
                }
                TransportError::RelayGrantRefused(refusal) => RelaySendFailure::Refused(*refusal),
                _ => {
                    unresolved_attempt = true;
                    RelaySendFailure::Transport
                }
            };
            match attempts.after_failure(failure, Instant::now()) {
                RelaySendStep::RetryAt(retry_at) => sleep_until(retry_at).await,
                RelaySendStep::End(end) => {
                    return Err(error.change_context(end.dispatch_error(node_id)));
                }
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
    push_remote_scoped_runtime_consumer(consumers, node_id, relay, mode, None);
}

pub(super) fn push_remote_scoped_runtime_consumer(
    consumers: &mut Vec<RemoteRuntimeConsumer>,
    node_id: &ClusterNodeName,
    relay: &RelayName,
    mode: AckMode,
    unit: Option<Unit>,
) {
    if let Some(existing) = consumers
        .iter_mut()
        .find(|consumer| consumer.node_id == node_id.clone() && consumer.relay == *relay)
    {
        let selected = match unit {
            Some(unit) => existing.unit_modes.entry(unit).or_insert(mode),
            None => existing.mode.get_or_insert(mode),
        };
        if mode == AckMode::Attached {
            *selected = AckMode::Attached;
        }
        return;
    }

    consumers.push(RemoteRuntimeConsumer {
        node_id: node_id.clone(),
        relay: relay.clone(),
        mode: unit.is_none().then_some(mode),
        unit_modes: match unit {
            Some(unit) => BTreeMap::from([(unit, mode)]),
            None => BTreeMap::new(),
        },
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a node attaches its remote dispatcher once, as it joins its cluster"
        )
    )]
    pub(crate) fn attach_remote_dispatcher(
        &self,
        cluster: Arc<cluster::ClusterHandle>,
        interconnect: Transport,
    ) {
        self.inner
            .fault_injection
            .register_relay_memory_owner(interconnect.node_id(), &self.inner.executor);
        let dispatcher = RemoteDispatcher {
            cluster,
            interconnect,
            executor: self.inner.executor.clone(),
            registry: self.inner.remote_dispatch.clone(),
            fault_injection: self.inner.fault_injection.clone(),
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
        unit: Unit,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        services.inject_remote_message(unit, batch).await
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
                error = %format_args!("{error:#}"),
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
    ///
    /// The grant the body arrived under charged room to decode it. Once it is decoded, the body's
    /// charge keeps the body and the decoded columns, which the runtime holds until it has taken
    /// the batch in, and returns the decode room they did not need, so a delivery waiting for its
    /// relay's buffer holds no more relay memory than its data.
    async fn decode_remote_rows(
        &self,
        schema: &CompiledSchema,
        remote: &mut RelayPayload,
    ) -> error_stack::Result<DecodedRemoteRows, RemoteRelayDecodeError> {
        let batch = schema
            .decode_arrow_body(self.executor(), remote.batch_ipc.clone(), {
                let dispatcher = self.inner.remote_dispatcher.load();
                match dispatcher.as_ref() {
                    Some(dispatcher) => dispatcher
                        .interconnect
                        .local_receive_bounds()
                        .relay_decoded_bytes
                        .get(),
                    None => self.executor().limits().relay_decoded_bytes.as_u64(),
                }
            })
            .await
            .change_context(RemoteRelayDecodeError::Body)?;
        remote
            .batch_ipc
            .keep_charge_for(batch_payload_bytes(batch.batch()));
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

    /// Checks that the unit a payload is addressed to is the unit of the relay its key lands in, so
    /// a batch never enters the buffer, presence or state of a partition that does not own its key.
    fn check_remote_unit(
        services: &RelayBoundaryServices,
        key: &Option<BranchKey>,
        addressed: Unit,
    ) -> error_stack::Result<(), RemoteRelayDecodeError> {
        let owning = services
            .unit_of(key)
            .change_context(RemoteRelayDecodeError::Unit { addressed })?;
        if owning != addressed {
            return Err(Report::new(RemoteRelayDecodeError::Unit { addressed }));
        }
        Ok(())
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
        // An owner admits ingress only for a unit it owns; a payload addressed to a unit whose
        // owner moved meanwhile is refused as one for a relay this node does not instantiate.
        if owner_ingress && !self.owns_relay_unit(&services, remote.unit) {
            return Err(Report::new(RuntimeError::relay_not_instantiated(
                &remote.domain,
                &remote.relay,
            )));
        }
        // The watches are charged before the rows are decoded, so a delivery held for that charge
        // holds its received body and grant, not a decoded batch beside them.
        let watch_count = remote.acks.iter().flatten().count();
        let cancelled = async {
            match &admission {
                Some(admission) => admission.transport.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let watch_owner = match self.hold_remote_ack_watches(watch_count, cancelled).await {
            Ok(RemoteAckWatchAdmission::Cancelled) => return Ok(()),
            Ok(watch_owner) => watch_owner,
            Err(report) => {
                return Err(report.change_context(RuntimeError::RemoteAckAdmission {
                    domain: remote.domain.clone(),
                }));
            }
        };
        let decoded = match self.decode_remote_rows(&schema, &mut remote).await {
            Ok(decoded) => decoded,
            Err(report) => {
                return Err(report.change_context(RuntimeError::decode_remote_relay(
                    &remote.domain,
                    &remote.relay,
                )));
            }
        };
        if let Err(report) = Self::check_remote_unit(&services, &decoded.key, remote.unit) {
            return Err(report.change_context(RuntimeError::decode_remote_relay(
                &remote.domain,
                &remote.relay,
            )));
        }
        let mut watches = Vec::with_capacity(watch_count);
        let mut acks = Vec::with_capacity(remote.acks.len());
        for registration in remote.acks {
            let Some(registration) = registration else {
                acks.push(AckSet::empty());
                continue;
            };
            let (record_acks, completion) =
                AckSet::tracked_root(services.domain_ack_tracker.clone());
            watches.push(RemoteAckWatch::new(completion, registration));
            acks.push(record_acks);
        }
        if let RemoteAckWatchAdmission::Charged { dispatcher, charge } = watch_owner {
            self.spawn_remote_ack_watchers(remote.domain.clone(), dispatcher, watches, charge);
        }
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
                self.inject_remote_stream_boundary_message(&services, remote.unit, &batch)
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
    /// Only a registration this run of the node handed out is resolved. Receivers can keep
    /// resolving registrations after their registrar run ends. A new run can produce the same
    /// numeric route from its own positions and generations, so the full registrar identity fences
    /// those replies before routing them to the exact delivery generation and row this run holds.
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
        let found = match outcome {
            RemoteAckOutcome::Alive => self.inner.remote_dispatch.report_ack(ack_id),
            RemoteAckOutcome::Progress { sequence, parked } => self
                .inner
                .remote_dispatch
                .progress_ack(ack_id, sequence, parked),
            RemoteAckOutcome::Ack => self
                .inner
                .remote_dispatch
                .resolve_ack(ack_id, AckOutcome::Ack),
            RemoteAckOutcome::NoAck(error) => self
                .inner
                .remote_dispatch
                .resolve_ack(ack_id, AckOutcome::NoAck(error)),
        };
        if !found {
            debug!(
                ack_id,
                "received remote acknowledgement for an unknown registration"
            );
        }
    }

    /// Charge the relay budget for the acknowledgement watches of `watches` delivered rows,
    /// holding the delivery back while the budget has no room for them.
    ///
    /// The delivery is not admitted while it is held, so its sender keeps waiting for the
    /// admission and the transport keeps reporting the delivery alive to it. The hold ends when
    /// `cancelled` resolves.
    async fn hold_remote_ack_watches(
        &self,
        watches: usize,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> error_stack::Result<RemoteAckWatchAdmission, nervix_execution::AdmissionError> {
        let Some(watches) = NonZeroUsize::new(watches) else {
            return Ok(RemoteAckWatchAdmission::Unwatched);
        };
        let Some(dispatcher) = self.inner.remote_dispatcher.load_full() else {
            return Ok(RemoteAckWatchAdmission::Unwatched);
        };
        self.inner
            .fault_injection
            .fill_relay_memory_before_acknowledgement_watches_if_armed(
                dispatcher.local_node_id(),
                &dispatcher.executor,
            );
        match RemoteAckWatchCharge::hold(&dispatcher.executor, watches, cancelled).await? {
            RemoteAckWatchHold::Charged(charge) => {
                Ok(RemoteAckWatchAdmission::Charged { dispatcher, charge })
            }
            RemoteAckWatchHold::Cancelled => Ok(RemoteAckWatchAdmission::Cancelled),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn spawn_remote_ack_watcher(
        &self,
        domain: DomainName,
        completion: AckCompletion,
        ack: Option<RemoteAckRegistration>,
    ) -> error_stack::Result<(), nervix_execution::AdmissionError> {
        let Some(ack) = ack else {
            return Ok(());
        };
        let RemoteAckWatchAdmission::Charged { dispatcher, charge } = self
            .hold_remote_ack_watches(1, std::future::pending())
            .await?
        else {
            return Ok(());
        };
        self.spawn_remote_ack_watchers(
            domain,
            dispatcher,
            vec![RemoteAckWatch::new(completion, ack)],
            charge,
        );
        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the receiver batch owns the bounded set of row progress \
                                   futures that this stream polls")
    )]
    fn spawn_remote_ack_watchers(
        &self,
        domain: DomainName,
        dispatcher: StdArc<RemoteDispatcher>,
        watches: Vec<RemoteAckWatch>,
        charge: RemoteAckWatchCharge,
    ) {
        let fault_injection = self.inner.fault_injection.clone();
        let watcher = async move {
            // Each watch returns its share of the charge as it ends, so a held delivery finds room
            // as soon as rows are reported rather than when the whole batch is.
            let mut charge = charge;
            let mut pending = FuturesUnordered::new();
            for watch in watches {
                pending.push(wait_remote_ack_progress(watch));
            }
            while let Some((mut watch, progress)) = pending.next().await {
                let registrar = watch.registration.registrar.node_id();
                let live = fault_injection
                    .registrar_live_view_override(dispatcher.local_node_id(), registrar)
                    .unwrap_or_else(|| dispatcher.cluster.live_node_incarnation(registrar));
                let report = watch.next_report(progress, live, Instant::now());
                let outcome = match report {
                    RemoteAckWatchReport::Send(outcome) => outcome,
                    RemoteAckWatchReport::Hold => {
                        pending.push(wait_remote_ack_progress(watch));
                        continue;
                    }
                    RemoteAckWatchReport::Release => {
                        debug!(domain = domain.as_str(), ack_id = watch.registration.ack_id,
                            target_node = %watch.registration.registrar,
                            registrar = ?watch.registrar,
                            "stopped reporting a remote ack whose registrar run changed or \
                             stayed missing from the live view");
                        charge = charge.release_watch();
                        continue;
                    }
                };
                let ack = &watch.registration;
                let terminal = !outcome.is_progress();
                trace!(domain = domain.as_str(), ack_id = ack.ack_id, target_node = %ack.registrar,
                    terminal, "sending remote ack progress");
                if terminal
                    && fault_injection.loses_remote_acknowledgement(
                        dispatcher.local_node_id(),
                        ack.registrar.node_id(),
                    )
                {
                    charge = charge.release_watch();
                    continue;
                }
                if let Err(error) = dispatcher
                    .dispatch(
                        ack.registrar.node_id(),
                        Envelope::Ack(ack.resolution(outcome)),
                    )
                    .await
                {
                    warn!(domain = domain.as_str(), ack_id = ack.ack_id,
                        target_node = %ack.registrar, error = %format_args!("{error:#}"),
                        "failed to return remote ack progress");
                }
                if terminal {
                    charge = charge.release_watch();
                } else {
                    pending.push(wait_remote_ack_progress(watch));
                }
            }
        };
        self.spawn_remote_ack_watcher_task(watcher);
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
        // Common consumers read every unit; a partitioned processor reads its matching unit.
        let owned_relays = revision
            .nodes
            .values()
            .filter(|node| node.kind() == ModelKind::Relay)
            .filter(|node| !node.units_executing_on(local_node_id).is_empty())
            .map(|node| RelayName::from(&node.identifier))
            .collect::<HashSet<_>>();
        for spec in &revision.processors.processors {
            let Some(node) = revision
                .nodes
                .get(&NodeRef::new(spec.spec.kind, spec.spec.processor.clone()))
            else {
                continue;
            };
            for unit in node.units.keys().copied() {
                let Some(target_node) = node.unit_execution_node(unit) else {
                    continue;
                };
                for relay in &spec.spec.input_relays {
                    if !owned_relays.contains(relay) || target_node == local_node_id {
                        continue;
                    }
                    push_remote_scoped_runtime_consumer(
                        consumers.entry(relay.clone()).or_default(),
                        target_node,
                        relay,
                        spec.spec.mode,
                        (unit != Unit::SINGLE).then_some(unit),
                    );
                }
            }
        }
        Self::push_emitter_remote_consumers(revision, local_node_id, &owned_relays, &mut consumers);
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
#[path = "remote_dispatch_properties.rs"]
mod properties;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "remote_dispatch_shuttle_tests.rs"]
mod shuttle_tests;
