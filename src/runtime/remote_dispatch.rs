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
use futures_util::future::Either;
use nervix_interconnect::{RelayGrantRefusal, RelaySendProgress, TransportError};

use super::{
    relay_boundary::{RelayDeliveryChannel, UnusedRelayPosition},
    relay_send_attempts::{RelaySendAttempts, RelaySendEnd, RelaySendFailure, RelaySendStep},
    remote_ack_owner::RecordStorage,
    remote_ack_registrar::{RegistrarLook, RegistrarPresence},
    remote_ack_watch_charge::{RemoteAckWatchCharge, RemoteAckWatchHold},
    *,
};
use crate::runtime_schema::batch_payload_bytes;

/// The remote acknowledgement timing this node has installed, published whole.
///
/// The node installs each committed timing group here. A delivery loads the group it captures as it
/// starts. An owner of held work keeps this handle from its creation and loads the alive interval
/// only once that work waits, so a committed change reaches a running owner at its next keepalive
/// without rebuilding it.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct SharedRemoteAckTiming(Arc<ArcSwap<nervix_models::RemoteAckTiming>>);

impl Default for SharedRemoteAckTiming {
    /// The default timing, which a node holds until it installs its committed parameters.
    fn default() -> Self {
        Self(Arc::new(ArcSwap::from_pointee(
            nervix_models::RemoteAckTiming::default(),
        )))
    }
}

impl SharedRemoteAckTiming {
    pub(super) fn load(&self) -> nervix_models::RemoteAckTiming {
        **self.0.load()
    }

    pub(super) fn install(&self, timing: nervix_models::RemoteAckTiming) {
        self.0.store(StdArc::new(timing));
    }

    /// The committed `REMOTE_ACK_ALIVE_INTERVAL`: how often held work reports the acknowledgements
    /// it holds alive, as a receiver reports the records of a delivery it holds.
    pub(in crate::runtime) fn ack_alive_interval(&self) -> Duration {
        self.0.load().remote_ack_alive_interval
    }
}

/// How often a node applies membership and the admission bound to the remote deliveries and
/// acknowledgements it waits for, at the default timing the tests exercise.
#[cfg(test)]
pub(super) const REMOTE_ACK_SWEEP_INTERVAL: Duration =
    nervix_models::RemoteAckSweepInterval::DEFAULT;

#[cfg(test)]
pub(super) const REMOTE_RELAY_TOTAL_TIMEOUT: Duration =
    nervix_models::RemoteRelayTotalTimeout::DEFAULT;

/// The run of a delivery's receiver that admitted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RelayAdmitted {
    /// The process epoch of the receiver run that admitted the delivery, and that holds the
    /// acknowledgement watches of its rows.
    pub(super) receiver_run: u64,
}

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
    /// Membership no longer lists the receiver as live.
    #[error("node '{target}' left the cluster before admitting a relay batch")]
    TargetLeft { target: ClusterNodeName },
    /// The receiver run that granted the delivery ended, replaced by a newer run of its node or
    /// removed by membership, before it admitted the delivery.
    #[error("the run of node '{target}' that granted a relay batch ended before admitting it")]
    ReceiverRunEnded { target: ClusterNodeName },
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
    #[error("the outcome of relay delivery {sequence} on node '{target}' is indeterminate")]
    AdmissionIndeterminate {
        target: ClusterNodeName,
        sequence: u64,
    },
    /// No send of the delivery started an exchange with its receiver, so the receiver holds no
    /// attempt of it and cannot have admitted any of its rows.
    #[error("relay delivery {sequence} never reached node '{target}'")]
    Unreached {
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
    /// This node's runtime stopped before the envelope reached its node.
    #[error("this node's runtime stopped before a remote payload reached node '{target}'")]
    RuntimeStopped { target: ClusterNodeName },
}

/// Why a relay payload another node sent does not decode into a batch of its relay. A variant
/// that wraps a decoder keeps that decoder's own failure beneath it.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum RemoteRelayDecodeError {
    #[error("invalid remote acknowledgement timing policy")]
    AckTiming,
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
}

/// What one look at a receiver's acknowledgement watch sends to its registrar.
enum RemoteAckWatchReport {
    /// Send this report. A terminal outcome is the watch's last one.
    Send(RemoteAckOutcome),
    /// End the watch without sending anything more.
    Release,
}

impl RemoteAckWatch {
    /// A watch that starts now, before the registrar's run has appeared in the live view, for an
    /// acknowledgement a delivery captured `timing` for.
    fn new(
        completion: AckCompletion,
        registration: RemoteAckRegistration,
        timing: nervix_models::RemoteAckTiming,
    ) -> Self {
        Self {
            completion,
            registration,
            registrar: RegistrarPresence::undiscovered(
                Instant::now(),
                timing.remote_relay_total_timeout,
            ),
        }
    }

    /// What the watch sends after its root reported `progress` since the last look, when the
    /// live view shows `live` for the registrar's node and membership `keeps_node` or no longer
    /// lists it, at `now`.
    fn next_report(
        &mut self,
        progress: Option<AckProgress>,
        live: Option<ClusterNodeIncarnation>,
        keeps_node: bool,
        now: Instant,
    ) -> RemoteAckWatchReport {
        let look = self.registrar.look(
            self.registration.registrar.incarnation(),
            live,
            keeps_node,
            now,
        );
        if look == RegistrarLook::Release {
            return RemoteAckWatchReport::Release;
        }
        let outcome = match progress {
            Some(AckProgress::Complete(AckOutcome::Ack)) => RemoteAckOutcome::Ack,
            Some(AckProgress::Complete(AckOutcome::NoAck(error))) => RemoteAckOutcome::NoAck(error),
            Some(AckProgress::Alive) | None => remote_ack_progress(&self.completion),
        };
        RemoteAckWatchReport::Send(outcome)
    }
}

async fn wait_remote_ack_progress(
    mut watch: RemoteAckWatch,
    timing: &nervix_models::RemoteAckTiming,
) -> (RemoteAckWatch, Option<AckProgress>) {
    let progress = nervix_primitives::select! {
        _ = sleep(timing.remote_ack_alive_interval) => None,
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
    timing: nervix_models::RemoteAckTiming,
}

/// How storing the acknowledgement records of an outbound delivery ended.
pub(super) enum RemoteAckStorage {
    /// The records are stored and travel with the delivery under these registrations.
    Stored(PendingRemoteAckDelivery),
    /// The delivery's producer or channel ended while it waited for room to store them. Nothing
    /// was stored, and the shares it handed over come back unresolved: whatever ends the delivery
    /// resolves them, so that a delivery its batch carries on elsewhere leaves no share pending.
    Cancelled { unstored: Vec<AckSet> },
}

/// The producer can be aborted while admission is unresolved, so its shared channel must start
/// a fresh sequence before another producer can use the gate again.
struct InterruptedRelayDelivery<'a>(Option<&'a RelayOutboundSlot>);

impl Drop for InterruptedRelayDelivery<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.0
            && !slot.cancellation().is_cancelled()
        {
            slot.reopen_delivery_channel();
        }
    }
}

impl PendingRemoteAckDelivery {
    pub(super) fn timing(&self) -> nervix_models::RemoteAckTiming {
        self.timing
    }
    pub(super) fn registrations(&self) -> &[Option<RemoteAckRegistration>] {
        &self.registrations
    }

    /// Records that the receiver run with process epoch `receiver_run` admitted every row.
    pub(super) fn admit(&mut self, receiver_run: u64) {
        self.admit_prefix(self.registrations.len(), receiver_run);
    }

    pub(super) fn admitted_rows(&self) -> usize {
        self.admitted_rows
    }

    /// Records that the receiver run with process epoch `receiver_run` admitted the rows up to
    /// `rows`: their acknowledgements now wait for that run, while it lasts.
    pub(super) fn admit_prefix(&mut self, rows: usize, receiver_run: u64) {
        assert!(rows >= self.admitted_rows && rows <= self.registrations.len());
        self.registry
            .admit_acks(&self.registrations[self.admitted_rows..rows], receiver_run);
        self.admitted_rows = rows;
    }

    /// Hands the records no receiver admitted back to the delivery that carries them next, when
    /// this delivery never reached its receiver: their forwarded shares end, and the records stay
    /// undecided rather than failing as a delivery that ended before admission fails them.
    pub(super) fn release_unadmitted(mut self) {
        for registration in self.registrations[self.admitted_rows..].iter().flatten() {
            self.registry.release_ack(registration.ack_id);
        }
        self.admitted_rows = self.registrations.len();
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
    pub(super) fn delivery_deadline(&self, timing: nervix_models::RemoteAckTiming) -> Instant {
        Instant::now()
            .checked_add(timing.remote_relay_total_timeout)
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
        timing: nervix_models::RemoteAckTiming,
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
            .store_acks(receiver.clone(), acks, deadline, timing, cancelled)
            .await?;
        let (ids, timing) = match storage {
            RecordStorage::Stored {
                registrations,
                timing,
            } => (registrations, timing),
            RecordStorage::Cancelled { acks } => {
                return Ok(RemoteAckStorage::Cancelled { unstored: acks });
            }
        };
        let registrations = ids
            .into_iter()
            .map(|id| id.map(|id| self.registration(id)))
            .collect();
        Ok(RemoteAckStorage::Stored(PendingRemoteAckDelivery {
            registry: self.registry.clone(),
            registrations,
            admitted_rows: 0,
            timing,
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

    /// Send one typed request and wait for its answer for as long as the target and its
    /// connection live.
    pub(super) async fn request<M>(
        &self,
        node_id: &ClusterNodeName,
        message: M,
    ) -> error_stack::Result<M::Response, nervix_interconnect::RequestError>
    where
        M: InterconnectRequest,
    {
        self.interconnect.request(node_id, message).await
    }

    /// Send one typed request that also ends at `deadline`, the deadline its caller names and says
    /// the kind of.
    pub(super) async fn request_by<M>(
        &self,
        node_id: &ClusterNodeName,
        message: M,
        deadline: nervix_interconnect::ExchangeDeadline,
    ) -> error_stack::Result<M::Response, nervix_interconnect::RequestError>
    where
        M: InterconnectRequest,
    {
        self.interconnect
            .request_by(node_id, message, deadline)
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

    /// Send `payload` to `node_id` and wait for its runtime admission there, and return the
    /// receiver run that admitted it.
    ///
    /// Nothing but the delivery's own outcome ends it: its receiver admitting or refusing it, its
    /// receiver leaving the cluster or its run ending, its route changing, or `deadline`, the
    /// delivery's admission bound, passing. A receiver that is slow or applies backpressure holds
    /// the delivery back for as long as that takes.
    pub(super) async fn dispatch_admitted_relay_payload(
        &self,
        node_id: &ClusterNodeName,
        payload: &mut RelayPayload,
        channel: RelayDeliveryChannel<'_>,
        deadline: Instant,
    ) -> error_stack::Result<RelayAdmitted, RemoteDispatchError> {
        let mut interrupted =
            InterruptedRelayDelivery(channel.has_producer().then_some(channel.slot));
        let result = async {
            let result = self
                .dispatch_admitted_relay_attempt(node_id, payload, channel, deadline)
                .await;
            let error = match result {
                Ok(admitted) => return Ok(admitted),
                Err(error) => error,
            };
            if !matches!(
                error.current_context(),
                RemoteDispatchError::GrantRefused {
                    refusal: RelayGrantRefusal::ChannelUnopened,
                    ..
                }
            ) || payload.delivery.sequence == 0
            {
                return Err(error);
            }
            if Instant::now() >= deadline {
                return Err(
                    error.change_context(RemoteDispatchError::AdmissionTotalTimeout {
                        target: node_id.clone(),
                        timeout: payload.ack_timing.remote_relay_total_timeout,
                    }),
                );
            }
            // No body was granted. Retain this payload and its ACKs under the held channel gate.
            // A fresh channel starts at zero, so it can need no further reopening attempt.
            Err::<RelayAdmitted, _>(error).discarded(
                "the unopened channel admitted no body; its one fresh-channel attempt supplies \
                 the outcome",
            );
            channel.slot.reopen_delivery_channel();
            payload.delivery = channel.slot.next_delivery_with_policy(payload.ack_timing);
            self.dispatch_admitted_relay_attempt(node_id, payload, channel, deadline)
                .await
        }
        .await;
        interrupted.0 = None;
        result
    }

    async fn dispatch_admitted_relay_attempt(
        &self,
        node_id: &ClusterNodeName,
        payload: &mut RelayPayload,
        channel: RelayDeliveryChannel<'_>,
        deadline: Instant,
    ) -> error_stack::Result<RelayAdmitted, RemoteDispatchError> {
        if channel.slot.cancellation().is_cancelled() {
            // Nothing of this delivery was sent, so its receiver holds no attempt of it, and the
            // channel's next delivery takes its unused position.
            drop(UnusedRelayPosition::new(channel.slot, payload.delivery));
            return Err(Report::new(RemoteDispatchError::BranchEvicted {
                target: node_id.clone(),
                sequence: payload.delivery.sequence,
            })
            .change_context(RemoteDispatchError::Unreached {
                target: node_id.clone(),
                sequence: payload.delivery.sequence,
            }));
        }
        let delivery = payload.delivery;
        let mut cancellation_guard = self.interconnect.relay_cancellation_guard(
            node_id.clone(),
            delivery,
            payload.ack_timing,
        );
        let (registration, admission) = self.register_pending_relay_admission()?;
        payload.admission = Some(registration.clone());
        let progress = RelaySendProgress::default();
        let dispatch_result = {
            let dispatch = self.dispatch_relay_payload(node_id, payload, deadline, &progress);
            tokio::pin!(dispatch);
            nervix_primitives::select! {
                biased;
                result = &mut dispatch => result,
                () = channel.cancelled() => {
                    Err(Report::new(RemoteDispatchError::BranchEvicted {
                        target: node_id.clone(),
                        sequence: delivery.sequence,
                    }))
                },
            }
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
            if matches!(
                error.current_context(),
                RemoteDispatchError::GrantRefused {
                    refusal: RelayGrantRefusal::ChannelUnopened,
                    ..
                }
            ) {
                cancellation_guard.disarm();
                return Err(error);
            }
            let Some(receiver_run) = progress.receiver_run() else {
                // No send started an exchange, so the receiver holds no attempt of this delivery
                // and there is nothing to cancel or reconcile there. The delivery's position on
                // its channel was never used, so the channel's next delivery takes it.
                cancellation_guard.disarm();
                drop(UnusedRelayPosition::new(channel.slot, delivery));
                return Err(error.change_context(RemoteDispatchError::Unreached {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                }));
            };
            let resolution = self
                .resolve_failed_relay_admission(
                    node_id,
                    delivery,
                    error,
                    &mut cancellation_guard,
                    payload.ack_timing,
                )
                .await;
            return match resolution {
                FailedRelayAdmissionResolution::Admitted => Ok(RelayAdmitted { receiver_run }),
                FailedRelayAdmissionResolution::Failed(error) => Err(error),
                FailedRelayAdmissionResolution::Indeterminate(error) => {
                    channel.slot.reopen_delivery_channel();
                    Err(error)
                }
            };
        }
        let receiver_run = progress
            .receiver_run()
            .verified("a send that ended without failing started an exchange with its receiver");
        let admitted = Self::await_relay_admission(
            node_id,
            admission,
            self.receiver_gone(node_id, receiver_run),
            deadline,
            payload.ack_timing.remote_relay_total_timeout,
        );
        tokio::pin!(admitted);
        let result = nervix_primitives::select! {
            biased;
            result = &mut admitted => result,
            () = channel.cancelled() => {
                Err(Report::new(RemoteDispatchError::BranchEvicted {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                }))
            },
        };
        if let Err(error) = result {
            self.clear_pending_relay_admission(&registration);
            let resolution = self
                .resolve_failed_relay_admission(
                    node_id,
                    delivery,
                    error,
                    &mut cancellation_guard,
                    payload.ack_timing,
                )
                .await;
            return match resolution {
                FailedRelayAdmissionResolution::Admitted => Ok(RelayAdmitted { receiver_run }),
                FailedRelayAdmissionResolution::Failed(error) => Err(error),
                FailedRelayAdmissionResolution::Indeterminate(error) => {
                    channel.slot.reopen_delivery_channel();
                    Err(error)
                }
            };
        }
        cancellation_guard.disarm();
        Ok(RelayAdmitted { receiver_run })
    }

    /// Learn what became of a delivery whose send or admission wait failed, by cancelling it on
    /// its receiver, which answers with the attempt's status.
    ///
    /// The receiver's answer decides the outcome, even after the delivery's admission bound has
    /// passed: whether a live receiver admitted the rows decides whether anything is lost. A
    /// receiver that has not decided yet, and a cancellation that failed in transport while the
    /// receiver stays live, are asked again at the delivery's retry cadence, and each ask keeps the
    /// receiver's record of the attempt. The outcome stays indeterminate only when the receiver
    /// leaves the cluster, the run that held the attempt ended, or the transport refuses the
    /// cancellation for good; the delivery's cancellation guard then keeps cancelling it in the
    /// background.
    async fn resolve_failed_relay_admission(
        &self,
        node_id: &ClusterNodeName,
        delivery: RelayDelivery,
        original_error: Report<RemoteDispatchError>,
        cancellation_guard: &mut RelayCancellationGuard,
        timing: nervix_models::RemoteAckTiming,
    ) -> FailedRelayAdmissionResolution {
        let indeterminate = |original_error: Report<RemoteDispatchError>| {
            FailedRelayAdmissionResolution::Indeterminate(
                Report::new(RemoteDispatchError::AdmissionIndeterminate {
                    target: node_id.clone(),
                    sequence: delivery.sequence,
                })
                .attach_printable(original_error),
            )
        };
        let mut waits = RuntimeReconnectBackoff {
            initial: timing.retry_interval(),
            next: timing.retry_interval(),
            max: timing.retry_ceiling(),
            settings: nervix_connector::RetryBackoffSettings::default(),
        };
        let target_left = self.interconnect.target_left(node_id);
        tokio::pin!(target_left);
        let status = loop {
            nervix_primitives::task::consume_budget().await;
            let answer = nervix_primitives::select! {
                biased;
                () = &mut target_left => return indeterminate(original_error),
                answer = self.interconnect.cancel_relay(node_id, delivery, timing) => answer,
            };
            match answer {
                Ok(
                    status @ (RelayAdmissionStatus::Admitted
                    | RelayAdmissionStatus::Rejected(_)
                    | RelayAdmissionStatus::Cancelled
                    | RelayAdmissionStatus::Retired
                    | RelayAdmissionStatus::Indeterminate),
                ) => break status,
                Ok(
                    RelayAdmissionStatus::Reserved
                    | RelayAdmissionStatus::BodyReceived
                    | RelayAdmissionStatus::Unknown,
                ) => {}
                Err(error) => {
                    if !error.current_context().is_transient() {
                        let report = error
                            .change_context(RemoteDispatchError::AdmissionCancellation {
                                target: node_id.clone(),
                                sequence: delivery.sequence,
                            })
                            .attach_printable(original_error);
                        return FailedRelayAdmissionResolution::Indeterminate(report);
                    }
                    debug!(
                        target_node = %node_id,
                        sequence = delivery.sequence,
                        error = %format_args!("{error:#}"),
                        "a relay cancellation is asked again while its receiver stays live"
                    );
                }
            }
            let ask_again = Instant::now()
                .checked_add(waits.take_next_delay())
                .assured("a wait bounded by the retry ceiling fits the monotonic clock");
            nervix_primitives::select! {
                biased;
                () = &mut target_left => return indeterminate(original_error),
                () = sleep_until(ask_again) => {}
            }
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
                indeterminate(original_error)
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

    /// Resolves once the run of `node_id` with process epoch `receiver_run` is gone, as liveness
    /// and membership decide: membership removed the node, or a connection bound a newer run of it.
    async fn receiver_gone(
        &self,
        node_id: &ClusterNodeName,
        receiver_run: u64,
    ) -> Report<RemoteDispatchError> {
        nervix_primitives::select! {
            biased;
            () = self.interconnect.target_left(node_id) => {
                Report::new(RemoteDispatchError::TargetLeft {
                    target: node_id.clone(),
                })
            }
            () = self.interconnect.peer_run_ended(node_id, receiver_run) => {
                Report::new(RemoteDispatchError::ReceiverRunEnded {
                    target: node_id.clone(),
                })
            }
        }
    }

    /// Wait until `node_id` admits a delivery its receiver run granted.
    ///
    /// The receiver reports the delivery alive while it holds it unadmitted, and a receiver that is
    /// slow, or holds the delivery back for room, costs the delivery latency. The wait ends at the
    /// receiver's verdict, when `receiver_gone` resolves because that run is gone, or when
    /// `total_deadline`, the delivery's admission bound, passes.
    pub(super) async fn await_relay_admission(
        node_id: &ClusterNodeName,
        mut admission: watch::Receiver<RelayAdmissionUpdate>,
        receiver_gone: impl std::future::Future<Output = Report<RemoteDispatchError>>,
        total_deadline: Instant,
        total_timeout: Duration,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        tokio::pin!(receiver_gone);
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                changed = admission.changed() => {
                    if changed.is_err() {
                        return Err(Report::new(RemoteDispatchError::AdmissionResponseClosed {
                            target: node_id.clone(),
                        }));
                    }
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
                gone = &mut receiver_gone => return Err(gone),
                () = sleep_until(total_deadline) => {
                    return Err(Report::new(RemoteDispatchError::AdmissionTotalTimeout {
                        target: node_id.clone(),
                        timeout: total_timeout,
                    }));
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
                // A subscription delivery answers no waiting caller, so a full queue holds the
                // batch back rather than dropping it for its subscribers.
                None => match batch
                    .batch
                    .encode_arrow_ipc(
                        self.executor(),
                        nervix_execution::QueueAdmission::WaitForPlace,
                    )
                    .await
                {
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
            let timing = self.registry.timing();
            let deadline = self.delivery_deadline(timing);
            let delivery = outbound_slot.next_delivery_with_policy(timing);
            // A subscriber node that applies backpressure holds the relay owner's fan-out back,
            // as a BLOCKING session's full queue does: the delivery ends only when the node leaves
            // the cluster or the delivery's admission bound passes.
            if let Err(error) = self
                .dispatch_sliced_relay_payload(
                    node_id,
                    RelayPayload {
                        ack_timing: timing,
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
                    RelayDeliveryChannel::new(&outbound_slot, None),
                    deadline,
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

    /// Send `payload` to `node_id` until its receiver takes it in, under the retry policy of
    /// [`RelaySendAttempts`]: a capacity refusal or a failed send holds the delivery back and asks
    /// again, until a permanent refusal, the receiver leaving the cluster, or `deadline`, the
    /// delivery's admission bound. A definitive sizing refusal returns to the slice owner;
    /// uncertain attempts still reconcile.
    async fn dispatch_relay_payload(
        &self,
        node_id: &ClusterNodeName,
        payload: &RelayPayload,
        deadline: Instant,
        progress: &RelaySendProgress,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        let mut attempts = RelaySendAttempts::new(deadline, payload.ack_timing);
        let mut unresolved_attempt = false;
        let target_left = self.interconnect.target_left(node_id);
        tokio::pin!(target_left);
        loop {
            nervix_primitives::task::consume_budget().await;
            let sent = nervix_primitives::select! {
                biased;
                () = &mut target_left => {
                    return Err(Report::new(RemoteDispatchError::TargetLeft {
                        target: node_id.clone(),
                    }));
                }
                sent = nervix_primitives::time::timeout_at(
                    attempts.admission_deadline(),
                    self.interconnect.send_relay(node_id, payload.clone(), progress),
                ) => sent,
            };
            let error = match sent {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => error,
                Err(_) => {
                    return Err(Report::new(
                        RelaySendEnd::AdmissionBound.dispatch_error(node_id, payload.ack_timing),
                    ));
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
                TransportError::RelayGrantRefused(RelayGrantRefusal::ChannelUnopened)
                    if unresolved_attempt =>
                {
                    // A later refusal cannot prove what an earlier interrupted attempt did.
                    return Err(error.change_context(RemoteDispatchError::Send {
                        target: node_id.clone(),
                    }));
                }
                TransportError::RelayGrantRefused(refusal) => RelaySendFailure::Refused(*refusal),
                transport => {
                    if !transport.is_transient() {
                        return Err(error.change_context(RemoteDispatchError::Send {
                            target: node_id.clone(),
                        }));
                    }
                    unresolved_attempt = true;
                    RelaySendFailure::Transport
                }
            };
            match attempts.after_failure(failure, Instant::now()) {
                RelaySendStep::RetryAt(retry_at) => {
                    nervix_primitives::select! {
                        biased;
                        () = &mut target_left => {
                            return Err(error.change_context(RemoteDispatchError::TargetLeft {
                                target: node_id.clone(),
                            }));
                        }
                        () = sleep_until(retry_at) => {}
                    }
                }
                RelaySendStep::End(end) => {
                    return Err(
                        error.change_context(end.dispatch_error(node_id, payload.ack_timing))
                    );
                }
            }
        }
    }

    pub(super) async fn dispatch(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        self.dispatch_with_timing(node_id, envelope, self.registry.timing())
            .await
    }

    /// Send `envelope` to `node_id` until it is delivered: an admission outcome, a record
    /// acknowledgement or a report another node waits for.
    ///
    /// A send waits for its connection for as long as the connection lives. A send that fails in
    /// transport is sent again at the retry cadence of `timing` while membership keeps the node,
    /// so the envelope ends unsent only once the node leaves the cluster, this node's runtime
    /// stops, or no later send can succeed: this node's transport shutting down or an envelope it
    /// cannot encode.
    async fn dispatch_with_timing(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
        timing: nervix_models::RemoteAckTiming,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        self.dispatch_until_delivered(node_id, envelope, timing, false)
            .await
    }

    /// [`Self::dispatch_with_timing`], whose first send fails in transport when `fail_first` is
    /// set, as a scenario's fault injection makes a connection break under it.
    async fn dispatch_until_delivered(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
        timing: nervix_models::RemoteAckTiming,
        fail_first: bool,
    ) -> error_stack::Result<(), RemoteDispatchError> {
        let mut injected_failure = fail_first;
        let mut waits = RuntimeReconnectBackoff {
            initial: timing.retry_interval(),
            next: timing.retry_interval(),
            max: timing.retry_ceiling(),
            settings: nervix_connector::RetryBackoffSettings::default(),
        };
        let target_left = self.interconnect.target_left(node_id);
        tokio::pin!(target_left);
        loop {
            nervix_primitives::task::consume_budget().await;
            let sent = if injected_failure {
                injected_failure = false;
                Err(Report::new(TransportError::Io(std::io::Error::from(
                    std::io::ErrorKind::ConnectionReset,
                ))))
            } else {
                nervix_primitives::select! {
                    biased;
                    () = &mut target_left => {
                        return Err(Report::new(RemoteDispatchError::TargetLeft {
                            target: node_id.clone(),
                        }));
                    }
                    () = self.registry.stopped() => {
                        return Err(Report::new(RemoteDispatchError::RuntimeStopped {
                            target: node_id.clone(),
                        }));
                    }
                    sent = self.interconnect.send(node_id, envelope.clone()) => sent,
                }
            };
            let error = match sent {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if !error.current_context().is_transient() {
                return Err(error.change_context(RemoteDispatchError::Send {
                    target: node_id.clone(),
                }));
            }
            debug!(
                target_node = %node_id,
                error = %format_args!("{error:#}"),
                "a remote envelope is sent again while its node stays live"
            );
            let send_again = Instant::now()
                .checked_add(waits.take_next_delay())
                .assured("a wait bounded by the retry ceiling fits the monotonic clock");
            nervix_primitives::select! {
                biased;
                () = &mut target_left => {
                    return Err(error.change_context(RemoteDispatchError::TargetLeft {
                        target: node_id.clone(),
                    }));
                }
                () = self.registry.stopped() => {
                    return Err(error.change_context(RemoteDispatchError::RuntimeStopped {
                        target: node_id.clone(),
                    }));
                }
                () = sleep_until(send_again) => {}
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
    /// The remote acknowledgement timing this node installed. An owner of held work keeps a clone
    /// from its creation, and reads its committed alive interval only once that work waits.
    pub(in crate::runtime) fn remote_ack_timing(&self) -> &SharedRemoteAckTiming {
        self.inner.remote_dispatch.ack_timing()
    }

    /// Waits until this node publishes `domain`'s routing, which the installation that started a
    /// task publishes once the task's whole domain is in place. The wait ends with whoever owns
    /// the waiting work: `None` once `stopped` resolves, as it does when that owner stops the work
    /// or drops it.
    pub(crate) async fn wait_for_domain_routing(
        &self,
        domain: &DomainName,
        stopped: impl std::future::Future<Output = ()>,
    ) -> Option<SharedDomainRouting> {
        nervix_primitives::select! {
            biased;
            routing = self.inner.domain_routings.published(domain) => Some(routing),
            () = stopped => None,
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
        // The transport is how this node learns that a receiver left or its run ended, which
        // gives up the acknowledgements that receiver admitted.
        let membership = interconnect.clone();
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
            registry.sweep_pending_acks(&membership).await;
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

    /// The relay a delivery's rows enter on this node, once this node instantiates it.
    ///
    /// A delivery can reach this node before the installation that instantiates its relay
    /// publishes it: the delivery is held, with the room its grant reserved, until the relay is
    /// instantiated, and asks again at the delivery's retry cadence. The hold ends with the
    /// delivery: `None` once `cancelled` resolves, when its sender cancelled it or its sender's run
    /// ended, and the relay's absence once the delivery's own admission bound passes.
    pub(in crate::runtime) async fn wait_for_remote_stream_target(
        &self,
        routing: &mut DomainRoutingCache,
        domain: &DomainName,
        relay: &RelayName,
        timing: nervix_models::RemoteAckTiming,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> error_stack::Result<Option<RemoteRelayTarget>, RuntimeError> {
        let deadline = Instant::now()
            .checked_add(timing.remote_relay_total_timeout)
            .assured("a validated total admission budget fits the monotonic clock");
        let mut waits = RuntimeReconnectBackoff {
            initial: timing.retry_interval(),
            next: timing.retry_interval(),
            max: timing.retry_ceiling(),
            settings: nervix_connector::RetryBackoffSettings::default(),
        };
        tokio::pin!(cancelled);
        loop {
            nervix_primitives::task::consume_budget().await;
            let error = match self.remote_stream_target(routing.load(), domain, relay) {
                Ok(target) => return Ok(Some(target)),
                Err(error) => error,
            };
            let now = Instant::now();
            if now >= deadline {
                return Err(error);
            }
            let ask_again = now
                .checked_add(waits.take_next_delay())
                .assured("a wait bounded by the retry ceiling fits the monotonic clock")
                .min(deadline);
            nervix_primitives::select! {
                biased;
                () = &mut cancelled => return Ok(None),
                () = sleep_until(ask_again) => {}
            }
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
        remote
            .ack_timing
            .validate()
            .change_context(RemoteRelayDecodeError::AckTiming)?;
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
        let key = BranchKey::from_remote_key(std::mem::replace(
            &mut remote.key,
            nervix_models::RemoteBranchKey::Unbranched,
        ))
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
        let admission_cancelled = async {
            match &admission {
                Some(admission) => admission.transport.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let Some(RemoteRelayTarget { services, schema }) = self
            .wait_for_remote_stream_target(
                routing,
                &remote.domain,
                &remote.relay,
                remote.ack_timing,
                admission_cancelled,
            )
            .await?
        else {
            return Ok(());
        };
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
        let watch_owner = match self
            .hold_remote_ack_watches(
                watch_count,
                cancelled,
                remote.ack_timing.remote_relay_total_timeout,
            )
            .await
        {
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
            watches.push(RemoteAckWatch::new(
                completion,
                registration,
                remote.ack_timing,
            ));
            acks.push(record_acks);
        }
        if let RemoteAckWatchAdmission::Charged { dispatcher, charge } = watch_owner {
            self.spawn_remote_ack_watchers(
                remote.domain.clone(),
                dispatcher,
                watches,
                charge,
                remote.ack_timing,
            );
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
        // A routed batch has passed its owner's dispatch gate. Count it on this node before
        // acknowledging admission releases that owner's dispatch permit, so a cluster drain
        // cannot see the batch missing from both nodes. Owner ingress still waits for its gate
        // before entering the owner buffer.
        let dispatch = if owner_ingress {
            Either::Left(self.ingest_stream_boundary_message(
                &remote.domain,
                &remote.relay,
                &services,
                &batch,
            ))
        } else {
            Either::Right(services.inject_remote_message(remote.unit, &batch))
        };
        let dispatch = async {
            let dispatch = dispatch.await;
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
        total_timeout: Duration,
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
        match RemoteAckWatchCharge::hold(&dispatcher.executor, watches, cancelled, total_timeout)
            .await?
        {
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
        let timing = self.inner.remote_dispatch.timing();
        let RemoteAckWatchAdmission::Charged { dispatcher, charge } = self
            .hold_remote_ack_watches(1, std::future::pending(), timing.remote_relay_total_timeout)
            .await?
        else {
            return Ok(());
        };
        self.spawn_remote_ack_watchers(
            domain,
            dispatcher,
            vec![RemoteAckWatch::new(completion, ack, timing)],
            charge,
            timing,
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
        timing: nervix_models::RemoteAckTiming,
    ) {
        let fault_injection = self.inner.fault_injection.clone();
        let watcher = async move {
            // Each watch returns its share of the charge as it ends, so a held delivery finds room
            // as soon as rows are reported rather than when the whole batch is.
            let mut charge = charge;
            let mut pending = FuturesUnordered::new();
            for watch in watches {
                pending.push(wait_remote_ack_progress(watch, &timing));
            }
            while let Some((mut watch, progress)) = pending.next().await {
                let registrar = watch.registration.registrar.node_id();
                let live = fault_injection
                    .registrar_live_view_override(dispatcher.local_node_id(), registrar)
                    .unwrap_or_else(|| dispatcher.cluster.live_node_incarnation(registrar));
                let keeps_node = dispatcher.interconnect.target_is_live(registrar);
                let report = watch.next_report(progress, live, keeps_node, Instant::now());
                let outcome = match report {
                    RemoteAckWatchReport::Send(outcome) => outcome,
                    RemoteAckWatchReport::Release => {
                        debug!(domain = domain.as_str(), ack_id = watch.registration.ack_id,
                            target_node = %watch.registration.registrar,
                            registrar = ?watch.registrar,
                            "stopped reporting a remote ack whose registrar run was replaced or \
                             whose node left the cluster");
                        charge = charge.release_watch();
                        continue;
                    }
                };
                let ack = &watch.registration;
                let terminal = !outcome.is_progress();
                trace!(domain = domain.as_str(), ack_id = ack.ack_id, target_node = %ack.registrar,
                    terminal, "sending remote ack progress");
                let fail_first = terminal
                    && fault_injection.fails_remote_acknowledgement_send(
                        dispatcher.local_node_id(),
                        ack.registrar.node_id(),
                    );
                if let Err(error) = dispatcher
                    .dispatch_until_delivered(
                        ack.registrar.node_id(),
                        Envelope::Ack(ack.resolution(outcome)),
                        timing,
                        fail_first,
                    )
                    .await
                {
                    if let RemoteDispatchError::TargetLeft { .. } = error.current_context() {
                        debug!(domain = domain.as_str(), ack_id = ack.ack_id,
                            target_node = %ack.registrar,
                            "stopped returning remote ack progress to a node that left the cluster");
                    } else {
                        warn!(domain = domain.as_str(), ack_id = ack.ack_id,
                            target_node = %ack.registrar, error = %format_args!("{error:#}"),
                            "failed to return remote ack progress");
                    }
                }
                if terminal {
                    charge = charge.release_watch();
                } else {
                    pending.push(wait_remote_ack_progress(watch, &timing));
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
