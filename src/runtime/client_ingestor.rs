//! Client ingestors on the node that executes them.
//!
//! Layer: data plane.
//!
//! - **Owns.** The producers attached to each client ingestor this node executes, the batches they
//!   queued, their fair admission into the ingestor's one acknowledgement window, the ACK root of
//!   every admitted batch and its terminal outcome, the admission state producers are told, the
//!   end of their attachments, and the node's byte budget for producer payloads.
//! - **Depends on.** Typed client source plans, the ingestor's bound routes and quiesce control,
//!   the ingest group executor, ACK roots, and Arrow body decoding.
//! - **Must not know.** Sessions, the session or interconnect wire, NSPL, or how a producer
//!   reached this node.
//!
//! Each client ingestor this node executes has one endpoint task. It owns every attachment, the
//! batches each queued, the round-robin order among them, and how many admitted batches still
//! await their acknowledgement, which it awaits itself. Nothing else mutates that state, so nothing
//! locks it: producers, the admission worker, the quiesce watch and lifecycle changes all reach it
//! through its command channel, in the order they sent their commands.
//!
//! The endpoint task outlives one execution of its ingestor. An alteration that restarts the
//! ingestor without changing its endpoint contract finds the same producers attached once the new
//! execution is installed; one that changes the contract, a new domain generation, and the end of
//! the ingestor on this node all end the attachments with the reason that applies.
//!
//! One execution admits one batch at a time through its admission worker, which validates the
//! batch, gives it one tracked ACK root, and dispatches it through the ingestor's filter and
//! routes. The window bounds how many admitted batches may await their acknowledgement across
//! every producer, so attaching another producer never widens it.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "client source attachment creation and ending manage its exact producer lifetime"
    )
)]

use std::num::NonZeroU32;

use bytes::Bytes;
use futures_util::{future::BoxFuture, stream::FuturesUnordered};
use indexmap::IndexMap;
use nervix_connector::physical_time::actual_utc_now;
use nervix_models::{
    CLIENT_PRODUCER_NODE_BYTES, ClientAttachmentId, ClientEndpointContract,
    ClientOutcomeUncertainty, ClientProcessingFailure, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerEndReason, ClientProducerGrant, ClientProducerLimits,
    ClientProducerPolicy, ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal,
    MAX_CLIENT_BATCH_ROWS, SchemaField,
};
use nervix_primitives::sync::atomic::AtomicU64;

use super::*;
use crate::runtime_schema::ClientBatchLimits;

/// The most bytes of a failure's description a producer is told, so an outcome stays one small
/// frame whatever failed downstream.
const MAX_OUTCOME_DETAIL_BYTES: usize = 1024;

/// The identity the serving side of a producer gave one submitted batch, echoed in its outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ClientSubmissionId(NonZeroU64);

impl ClientSubmissionId {
    pub(crate) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> NonZeroU64 {
        self.0
    }
}

/// The node's budget for the Arrow IPC bytes producers may have outstanding: those submitted
/// through the sessions this node serves, and those forwarded to it for a client ingestor it
/// executes. A producer reserves its whole granted window when it opens and returns it when its
/// attachment ends, so a full budget refuses the open rather than stalling an opened producer.
///
/// This is a handle: every clone reserves from the same node budget.
#[derive(Debug, Clone)]
pub(crate) struct ClientProducerBudget {
    reserved: Arc<AtomicU64>,
}

impl Default for ClientProducerBudget {
    fn default() -> Self {
        Self {
            reserved: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ClientProducerBudget {
    /// Reserves `bytes` of the node's budget, or `None` when that would exceed it.
    pub(crate) fn try_reserve(&self, bytes: NonZeroU64) -> Option<ClientProducerReservation> {
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes.get())?;
            if next > CLIENT_PRODUCER_NODE_BYTES {
                return None;
            }
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ClientProducerReservation {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// The bytes every live reservation holds together.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }
}

/// Bytes of the node's producer budget, returned when this is dropped.
#[derive(Debug)]
pub(crate) struct ClientProducerReservation {
    budget: ClientProducerBudget,
    bytes: NonZeroU64,
}

impl Drop for ClientProducerReservation {
    fn drop(&mut self) {
        let previous = self
            .budget
            .reserved
            .fetch_sub(self.bytes.get(), Ordering::AcqRel);
        previous.checked_sub(self.bytes.get()).verified(
            "a reservation returns exactly the bytes it added, and it returns them once, when it \
             is dropped",
        );
    }
}

/// What a producer asks the node executing a client ingestor for.
pub(crate) struct ClientProducerOpenRequest {
    pub(crate) domain: DomainName,
    pub(crate) ingestor: IngestorName,
    pub(crate) expected_fields: Vec<SchemaField>,
    pub(crate) limits: ClientProducerLimits,
    /// The largest payload one submission can carry to this node, which the serving session's
    /// frame decides.
    pub(crate) max_batch_bytes: NonZeroU64,
    /// Whether this node retains the payloads for another node's session, which this node's
    /// budget then counts again.
    pub(crate) retention: ClientProducerRetention,
}

/// Where the payloads of a producer are retained while they are outstanding.
#[derive(Debug)]
pub(crate) enum ClientProducerRetention {
    /// The producer's session is on this node, whose budget its session already reserved.
    Local,
    /// The producer's session is on another node, which forwards its payloads here. The endpoint
    /// admits none of them before that node cleared it: it names each batch whose turn has come
    /// on `clearance_requests`, and the clearance returns through the producer's handle.
    Forwarded {
        clearance_requests: mpsc::UnboundedSender<ClientSubmissionId>,
    },
}

/// An opened producer: what it was told, the handle it submits through, and the events that
/// answer it.
pub(crate) struct OpenedClientProducer {
    pub(crate) description: ClientProducerDescription,
    pub(crate) handle: ClientProducerHandle,
    pub(crate) events: ClientProducerEvents,
}

/// What reaches a producer from the endpoint it is attached to.
///
/// `outcomes` carries one outcome for every batch the producer submitted and, once the endpoint
/// ends the attachment, its reason as the last event. It is bounded by the producer's granted
/// batches plus that one end. It closes once the endpoint has released the attachment, so a
/// producer that closed learns its release when every outcome before it has been read.
/// `admission` holds the newest admission state; changes in between are coalesced.
pub(crate) struct ClientProducerEvents {
    pub(crate) outcomes: mpsc::UnboundedReceiver<ClientProducerEvent>,
    pub(crate) admission: watch::Receiver<ClientProducerAdmission>,
}

/// One event about an attached producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientProducerEvent {
    /// The terminal outcome of one submitted batch, with a bounded, non-sensitive description of a
    /// refusal or failure.
    Outcome {
        submission: ClientSubmissionId,
        outcome: ClientSubmissionOutcome,
        detail: Option<String>,
    },
    /// The endpoint ended the attachment. Nothing about it follows.
    Ended(ClientProducerEndReason),
}

/// The handle a producer's serving side submits through. Dropping it detaches the producer
/// without answering what it still has outstanding; admitted work continues in the graph.
pub(crate) struct ClientProducerHandle {
    commands: mpsc::UnboundedSender<EndpointCommand>,
    attachment: ClientAttachmentId,
    detached: bool,
}

impl ClientProducerHandle {
    /// Hands one batch to the endpoint, which answers it with exactly one outcome.
    pub(crate) fn submit(&self, submission: ClientSubmissionId, body: Bytes) {
        let command = EndpointCommand::Submit {
            attachment: self.attachment,
            submission,
            body,
        };
        // An endpoint that ended already told the producer so, and that end answers the batch.
        self.commands
            .send(command)
            .means_shutdown("client ingestor endpoint");
    }

    /// Clears one batch of a forwarded producer for admission: the node that serves the producer
    /// has recorded that the batch may now be admitted. A batch the endpoint already refused, or a
    /// producer that already ended, ignores it.
    pub(crate) fn clear(&self, submission: ClientSubmissionId) {
        let command = EndpointCommand::Clear {
            attachment: self.attachment,
            submission,
        };
        self.commands
            .send(command)
            .means_shutdown("client ingestor endpoint");
    }

    /// Stops admission for this producer. The endpoint refuses the batches it has not admitted
    /// yet, answers each admitted one once its acknowledgement resolves, and then releases the
    /// producer, which closes its outcomes after the last of those answers.
    pub(crate) fn close(mut self) {
        let command = EndpointCommand::Close {
            attachment: self.attachment,
        };
        self.detached = true;
        // An endpoint that ended already ended this producer too, and closed its outcomes.
        self.commands
            .send(command)
            .means_shutdown("client ingestor endpoint");
    }
}

impl Drop for ClientProducerHandle {
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        let command = EndpointCommand::Detach {
            attachment: self.attachment,
        };
        self.commands
            .send(command)
            .means_shutdown("client ingestor endpoint");
    }
}

/// The endpoint task of one client ingestor on this node, reached through its commands, and the
/// counts it publishes for observation.
pub(in crate::runtime) struct ClientIngestorEndpoint {
    commands: mpsc::UnboundedSender<EndpointCommand>,
    gauges: Arc<PublishedClientGauges>,
}

/// The counts an endpoint task rewrites after every change, so `DESCRIBE` reads them without
/// reaching the task. Each count is exact when written; a reader may see one count of a change
/// before the others.
#[derive(Debug, Default)]
pub(in crate::runtime) struct PublishedClientGauges {
    producers: AtomicU64,
    forwarded_producers: AtomicU64,
    outstanding_batches: AtomicU64,
    outstanding_bytes: AtomicU64,
    admitted_batches: AtomicU64,
}

impl PublishedClientGauges {
    fn publish(&self, gauges: ClientIngestorGauges) {
        self.producers.store(gauges.producers, Ordering::Release);
        self.forwarded_producers
            .store(gauges.forwarded_producers, Ordering::Release);
        self.outstanding_batches
            .store(gauges.outstanding_batches, Ordering::Release);
        self.outstanding_bytes
            .store(gauges.outstanding_bytes, Ordering::Release);
        self.admitted_batches
            .store(gauges.admitted_batches, Ordering::Release);
    }

    fn snapshot(&self) -> ClientIngestorGauges {
        ClientIngestorGauges {
            producers: self.producers.load(Ordering::Acquire),
            forwarded_producers: self.forwarded_producers.load(Ordering::Acquire),
            outstanding_batches: self.outstanding_batches.load(Ordering::Acquire),
            outstanding_bytes: self.outstanding_bytes.load(Ordering::Acquire),
            admitted_batches: self.admitted_batches.load(Ordering::Acquire),
        }
    }
}

/// One running execution of a client ingestor, as its endpoint admits batches into it.
pub(in crate::runtime) struct ClientExecution {
    contract: ClientEndpointContract,
    generation: u64,
    fields: Vec<SchemaField>,
    window: NonZeroUsize,
    policy: ClientProducerPolicy,
    /// The admission worker of this execution. It takes one batch at a time.
    jobs: mpsc::Sender<AdmissionJob>,
}

/// Everything the admission worker of one execution dispatches through.
pub(in crate::runtime) struct ClientIntake {
    pub(in crate::runtime) handles: IngestTaskHandles,
    pub(in crate::runtime) runtime: Runtime,
    pub(in crate::runtime) domain: DomainName,
    pub(in crate::runtime) ingestor: IngestorName,
    pub(in crate::runtime) schema: Arc<CompiledSchema>,
    pub(in crate::runtime) timestamp_source: Option<IngestTimestampSource>,
    pub(in crate::runtime) output_routes: Arc<BoundIngestorRoutes>,
    pub(in crate::runtime) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(in crate::runtime) branched_senders:
        HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
    pub(in crate::runtime) metrics: MessageMetricsHandle,
    pub(in crate::runtime) quiesce: Arc<IngestorQuiesceControl>,
    pub(in crate::runtime) trackers: IngestorAckRootTrackers,
    pub(in crate::runtime) ack_timeout: Duration,
}

/// Whether an execution may admit a batch right now, as its quiesce publication decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained client intake checks each admitted payload against its lifetime"
    )
)]
pub(in crate::runtime) enum ClientIntakeState {
    /// Batches are admitted.
    Open,
    /// A quiesce or memory pressure holds admission; it opens again once released.
    Suspended,
    /// An ownership handoff or shutdown stopped intake for good on this execution.
    Draining,
}

impl ClientIntakeState {
    fn admission(self) -> ClientProducerAdmission {
        match self {
            Self::Open => ClientProducerAdmission::Open,
            Self::Suspended | Self::Draining => ClientProducerAdmission::Suspended,
        }
    }

    /// The refusal a batch arriving in this state receives, or `None` while batches are admitted.
    fn refusal(self) -> Option<ClientSubmissionRefusal> {
        match self {
            Self::Open => None,
            Self::Suspended => Some(ClientSubmissionRefusal::Suspended),
            Self::Draining => Some(ClientSubmissionRefusal::Draining),
        }
    }
}

enum EndpointCommand {
    Attach(AttachCommand),
    Submit {
        attachment: ClientAttachmentId,
        submission: ClientSubmissionId,
        body: Bytes,
    },
    Clear {
        attachment: ClientAttachmentId,
        submission: ClientSubmissionId,
    },
    Close {
        attachment: ClientAttachmentId,
    },
    Detach {
        attachment: ClientAttachmentId,
    },
    Install(Arc<ClientExecution>),
    Uninstall,
    Intake(ClientIntakeState),
    Admission(AdmissionReport),
    End {
        reason: ClientProducerEndReason,
        done: oneshot::Sender<()>,
    },
}

struct AttachCommand {
    expected_fields: Vec<SchemaField>,
    limits: ClientProducerLimits,
    max_batch_bytes: NonZeroU64,
    serving: ProducerServing,
    reply: oneshot::Sender<Result<AttachedProducer, ClientProducerRefusal>>,
}

/// How a producer's batches reach the endpoint, which decides how one is admitted.
enum ProducerServing {
    /// The producer's session is on this node. Its batch goes to the worker when its turn comes.
    Local,
    /// Another node serves the producer's session and forwards its batches. When a batch's turn
    /// comes, the endpoint asks that node to clear it and admits it only once it did, so that node
    /// knows which batches may have been admitted if this node is lost.
    Forwarded {
        /// The node budget reserved for the payloads this node retains for the other node.
        _reservation: ClientProducerReservation,
        clearance_requests: mpsc::UnboundedSender<ClientSubmissionId>,
    },
}

struct AttachedProducer {
    description: ClientProducerDescription,
    events: ClientProducerEvents,
}

/// One batch the endpoint hands its admission worker.
struct AdmissionJob {
    attachment: ClientAttachmentId,
    submission: ClientSubmissionId,
    body: Bytes,
    max_batch_bytes: NonZeroU64,
}

/// What the admission worker made of one batch.
struct AdmissionReport {
    attachment: ClientAttachmentId,
    submission: ClientSubmissionId,
    result: AdmissionResult,
}

enum AdmissionResult {
    /// The batch entered the graph under this ACK root, whose completion decides its outcome
    /// under the ACK timeout of the execution that admitted it.
    Admitted {
        completion: AckCompletion,
        ack_timeout: Duration,
    },
    /// No row of the batch entered the graph.
    Refused {
        refusal: ClientSubmissionRefusal,
        detail: Option<String>,
    },
}

/// A batch an attachment queued and the endpoint has not handed its worker yet.
struct QueuedSubmission {
    submission: ClientSubmissionId,
    body: Bytes,
}

/// One attached producer, as its endpoint task holds it.
struct Attachment {
    generation: u64,
    contract: ClientEndpointContract,
    grant: ClientProducerGrant,
    queue: VecDeque<QueuedSubmission>,
    /// Batches of a forwarded producer whose turn came, each holding a slot of the window, while
    /// their serving node clears them, in the order the clearances were requested.
    clearing: VecDeque<QueuedSubmission>,
    /// Batches that hold a slot of the window and wait for the worker: a local producer's batch
    /// whose turn came, or a forwarded producer's batch its serving node cleared.
    ready: VecDeque<QueuedSubmission>,
    /// Batches admitted or handed to the worker, not yet answered, with their payload bytes.
    outstanding: HashMap<ClientSubmissionId, u64>,
    /// Payload bytes of every batch the attachment holds.
    held_bytes: u64,
    events: mpsc::UnboundedSender<ClientProducerEvent>,
    admission: watch::Sender<ClientProducerAdmission>,
    /// Set once the producer asked to close; the attachment is released once nothing is
    /// outstanding.
    closing: bool,
    serving: ProducerServing,
}

impl Attachment {
    /// Batches this attachment holds that have not been answered.
    fn held_batches(&self) -> usize {
        let unadmitted = self
            .slotted_batches()
            .checked_add(self.queue.len())
            .assured("every count is bounded by the attachment's granted batches");
        unadmitted
            .checked_add(self.outstanding.len())
            .assured("every count is bounded by the attachment's granted batches")
    }

    /// Batches that hold a slot of the window without having reached the worker: those being
    /// cleared and those ready for the worker.
    fn slotted_batches(&self) -> usize {
        self.clearing
            .len()
            .checked_add(self.ready.len())
            .assured("both counts are bounded by the attachment's granted batches")
    }

    fn is_forwarded(&self) -> bool {
        match self.serving {
            ProducerServing::Local => false,
            ProducerServing::Forwarded { .. } => true,
        }
    }

    /// Makes `batch` outstanding and returns the job that hands it to the worker.
    fn outstanding_job(
        &mut self,
        attachment: ClientAttachmentId,
        batch: QueuedSubmission,
    ) -> AdmissionJob {
        let bytes: u64 = batch.body.len().arch_into();
        self.outstanding.insert(batch.submission, bytes);
        AdmissionJob {
            attachment,
            submission: batch.submission,
            body: batch.body,
            max_batch_bytes: self.grant.max_batch_bytes,
        }
    }

    /// Answers one batch, counting its outcome once.
    fn answer(
        &mut self,
        series: &ClientIngestorSeries,
        submission: ClientSubmissionId,
        outcome: ClientSubmissionOutcome,
        detail: Option<String>,
    ) {
        series.count(&outcome);
        let event = ClientProducerEvent::Outcome {
            submission,
            outcome,
            detail,
        };
        // A producer whose serving side is gone learns nothing more about this attachment.
        self.events.send(event).means_peer_left("client producer");
    }

    /// Refuses every batch that has not reached the worker, in the order the producer sent them.
    /// Those ready or being cleared held slots of the window, which the caller releases.
    fn refuse_unadmitted(
        &mut self,
        series: &ClientIngestorSeries,
        refusal: ClientSubmissionRefusal,
    ) {
        // Oldest first: a ready batch took its turn, or was cleared, before any batch still being
        // cleared, and both left the queue before the batches still in it.
        let mut unadmitted = std::mem::take(&mut self.ready);
        unadmitted.append(&mut self.clearing);
        unadmitted.append(&mut self.queue);
        for batch in unadmitted {
            let bytes: u64 = batch.body.len().arch_into();
            self.held_bytes = self
                .held_bytes
                .checked_sub(bytes)
                .verified("a held batch's bytes were added when it was queued");
            self.answer(
                series,
                batch.submission,
                ClientSubmissionOutcome::NotAdmitted(refusal),
                None,
            );
        }
    }

    /// Ends the attachment: every batch that has not reached the worker is refused, every
    /// outstanding one's outcome is unknown, and the reason is the last event.
    fn end(mut self, series: &ClientIngestorSeries, reason: ClientProducerEndReason) {
        self.refuse_unadmitted(series, ClientSubmissionRefusal::ProducerEnded);
        let outstanding = std::mem::take(&mut self.outstanding);
        for submission in outstanding.into_keys() {
            self.answer(
                series,
                submission,
                ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted),
                None,
            );
        }
        self.events
            .send(ClientProducerEvent::Ended(reason))
            .means_peer_left("client producer");
    }
}

/// An attachment an installed execution no longer serves, and why.
struct EndedAttachment {
    attachment: ClientAttachmentId,
    reason: ClientProducerEndReason,
}

/// The batch an endpoint handed its admission worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerBatch {
    attachment: ClientAttachmentId,
    submission: ClientSubmissionId,
}

/// The state one endpoint task owns.
struct Endpoint {
    domain: DomainName,
    ingestor: IngestorName,
    commands: mpsc::UnboundedReceiver<EndpointCommand>,
    /// The acknowledgement of every admitted batch, awaited by this task among its commands, so
    /// an ending endpoint leaves no task behind.
    acknowledgements: FuturesUnordered<BoxFuture<'static, ResolvedAcknowledgement>>,
    execution: Option<Arc<ClientExecution>>,
    intake: ClientIntakeState,
    attachments: IndexMap<ClientAttachmentId, Attachment, RandomState>,
    /// Where round-robin selection resumes.
    cursor: usize,
    /// Admitted batches still awaiting their acknowledgement, and the batch the worker holds.
    window_used: usize,
    /// The batch handed to the worker and not reported yet.
    in_worker: Option<WorkerBatch>,
    series: ClientIngestorSeries,
    gauges: Arc<PublishedClientGauges>,
    /// The counts last published, so a command that changes none of them publishes nothing.
    published: ClientIngestorGauges,
}

impl Runtime {
    /// The node's producer byte budget, which sessions serving producers reserve from.
    pub(crate) fn client_producer_budget(&self) -> ClientProducerBudget {
        self.inner.client_producer_budget.clone()
    }

    /// Attaches one producer to a client ingestor this node executes.
    ///
    /// The refusal names why nothing was attached: the domain, the ingestor or its kind, an
    /// execution this node does not run, the expected schema, the limits, or the budget.
    pub(crate) async fn open_client_producer(
        &self,
        request: ClientProducerOpenRequest,
    ) -> Result<OpenedClientProducer, ClientProducerRefusal> {
        let ClientProducerOpenRequest {
            domain,
            ingestor,
            expected_fields,
            limits,
            max_batch_bytes,
            retention,
        } = request;
        if !limits.is_within_bounds() {
            return Err(ClientProducerRefusal::InvalidLimits);
        }
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let commands = match self.inner.client_ingestors.get(&key) {
            Some(endpoint) => endpoint.commands.clone(),
            None => return Err(self.client_producer_refusal(&domain, &ingestor)),
        };
        let serving = match retention {
            ClientProducerRetention::Local => ProducerServing::Local,
            ClientProducerRetention::Forwarded { clearance_requests } => {
                let Some(reservation) = self.inner.client_producer_budget.try_reserve(limits.bytes)
                else {
                    return Err(ClientProducerRefusal::NodeCapacityExhausted);
                };
                ProducerServing::Forwarded {
                    _reservation: reservation,
                    clearance_requests,
                }
            }
        };
        let (reply, attached) = oneshot::channel();
        let command = EndpointCommand::Attach(AttachCommand {
            expected_fields,
            limits,
            max_batch_bytes,
            serving,
            reply,
        });
        if commands.send(command).is_err() {
            return Err(ClientProducerRefusal::EndpointUnavailable);
        }
        let Ok(attached) = attached.await else {
            return Err(ClientProducerRefusal::EndpointUnavailable);
        };
        let AttachedProducer {
            description,
            events,
        } = attached?;
        let handle = ClientProducerHandle {
            commands,
            attachment: description.attachment,
            detached: false,
        };
        Ok(OpenedClientProducer {
            description,
            handle,
            events,
        })
    }

    /// Why this node cannot attach a producer to an ingestor it runs no endpoint for.
    fn client_producer_refusal(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> ClientProducerRefusal {
        let status = self
            .inner
            .domains
            .get(domain)
            .map(|state| state.status.clone());
        match status {
            None => return ClientProducerRefusal::DomainNotFound,
            Some(nervix_models::DomainStatus::Stopped) => {
                return ClientProducerRefusal::DomainStopped;
            }
            Some(_) => {}
        }
        let Some(execution) = self.inner.executions.get(domain) else {
            return ClientProducerRefusal::EndpointUnavailable;
        };
        match execution
            .revision
            .entrypoints
            .ingestor(ingestor)
            .map(|plan| &plan.input)
        {
            None => ClientProducerRefusal::IngestorNotFound,
            Some(IngestorInputPlan::Transport(_)) => ClientProducerRefusal::NotClientIngestor,
            Some(IngestorInputPlan::Client(_)) => ClientProducerRefusal::EndpointUnavailable,
        }
    }

    /// Starts a client ingestor's execution on this node: its admission worker and the watch that
    /// tells its endpoint when admission opens or stops. The endpoint task is created the first
    /// time its ingestor starts here and kept across restarts.
    pub(in crate::runtime) fn host_client_source(
        &self,
        ingestor: &IngestorSpec,
        plan: &ClientIngestorStartPlan,
        generation: u64,
        quiesce: Arc<IngestorQuiesceControl>,
        dependencies: IngestorDependencies,
    ) {
        let IngestorDependencies {
            handles,
            output_routes,
            filter_where,
            branched_templates,
            metrics,
        } = dependencies;
        let domain = &ingestor.domain;
        let branched_runtime = self.start_branched_entrypoint_runtimes(
            domain,
            &ModelName::from(&ingestor.name),
            branched_templates,
        );
        self.prepare_ingestor_readiness(domain, &ingestor.name, NonZeroU64::MIN);
        let (shutdown_tx, _) = watch::channel(false);
        let (jobs, job_receiver) = mpsc::channel(1);
        let execution = Arc::new(ClientExecution {
            contract: plan.contract,
            generation,
            fields: plan.fields.clone(),
            window: plan.window_size(),
            policy: plan.policy,
            jobs,
        });
        let commands = self.client_ingestor_endpoint(domain, &ingestor.name);
        let intake = ClientIntake {
            handles,
            runtime: self.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            schema: plan.schema.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            output_routes,
            filter_where,
            branched_senders: branched_runtime.senders.clone(),
            metrics,
            quiesce: quiesce.clone(),
            trackers: self.ingestor_ack_root_trackers(domain, &ingestor.name),
            ack_timeout: plan.policy.ack_timeout,
        };
        if commands.send(EndpointCommand::Install(execution)).is_err() {
            debug!(
                domain = domain.as_str(),
                ingestor = ingestor.name.as_str(),
                "a client ingestor's endpoint ended while its execution was installed"
            );
        }
        // Both start after the installation was sent, so the endpoint applies it before the
        // intake watch's first report and before any admission report of the new worker.
        let worker = AdmissionWorker {
            intake,
            mailbox: WorkerMailbox {
                jobs: job_receiver,
                reports: commands.clone(),
                shutdown: shutdown_tx.subscribe(),
            },
        };
        let worker = nervix_primitives::task::spawn(worker.run());
        let watcher = nervix_primitives::task::spawn(watch_client_intake(
            quiesce,
            commands.clone(),
            shutdown_tx.subscribe(),
        ));
        self.mark_ingestor_instance_ready(domain, &ingestor.name, 0);
        info!(
            domain = domain.as_str(),
            ingestor = ingestor.name.as_str(),
            "started client ingestor"
        );
        self.inner.ingestors.insert(
            ingestor.runtime_key(),
            IngestorRuntime {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks: vec![worker, watcher],
            },
        );
    }

    /// The commands of the endpoint task of a client ingestor, starting the task if this node has
    /// none for it yet.
    fn client_ingestor_endpoint(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> mpsc::UnboundedSender<EndpointCommand> {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        if let Some(endpoint) = self.inner.client_ingestors.get(&key)
            && !endpoint.commands.is_closed()
        {
            return endpoint.commands.clone();
        }
        let (commands, receiver) = mpsc::unbounded_channel();
        let gauges = Arc::new(PublishedClientGauges::default());
        let endpoint = Endpoint {
            domain: domain.clone(),
            ingestor: ingestor.clone(),
            commands: receiver,
            acknowledgements: FuturesUnordered::new(),
            execution: None,
            intake: ClientIntakeState::Suspended,
            attachments: IndexMap::with_hasher(RandomState::default()),
            cursor: 0,
            window_used: 0,
            in_worker: None,
            series: self.inner.metrics.client_ingestor_series(domain, ingestor),
            gauges: gauges.clone(),
            published: ClientIngestorGauges::default(),
        };
        nervix_primitives::task::spawn(endpoint.run());
        self.inner.client_ingestors.insert(
            key,
            ClientIngestorEndpoint {
                commands: commands.clone(),
                gauges,
            },
        );
        commands
    }

    /// The producers attached to a client ingestor's endpoint on this node, or `None` when this
    /// node has no endpoint for it.
    pub(in crate::runtime) fn client_ingestor_gauges(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> Option<ClientIngestorGauges> {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let endpoint = self.inner.client_ingestors.get(&key)?;
        Some(endpoint.gauges.snapshot())
    }

    /// Tells a client ingestor's endpoint that its execution stopped. Its producers stay attached
    /// until the endpoint learns whether a restart keeps their contract.
    pub(in crate::runtime) fn uninstall_client_execution(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let Some(endpoint) = self.inner.client_ingestors.get(&key) else {
            return;
        };
        endpoint
            .commands
            .send(EndpointCommand::Uninstall)
            .means_shutdown("client ingestor endpoint");
    }

    /// Ends the endpoints of the client ingestors this node no longer executes, each with the
    /// reason that applies, once every ingestor this node should run has been started.
    pub(crate) async fn reconcile_client_ingestor_endpoints(
        &self,
        local_node_id: &ClusterNodeName,
    ) {
        let keys = self
            .inner
            .client_ingestors
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in keys {
            nervix_primitives::task::consume_budget().await;
            if self.inner.ingestors.contains_key(&key) {
                continue;
            }
            let Some(reason) = self.client_endpoint_end_reason(&key, local_node_id) else {
                continue;
            };
            self.end_client_ingestor_endpoint(&key, reason).await;
        }
    }

    /// Ends every client ingestor endpoint on this node, as the node shuts down.
    pub(in crate::runtime) async fn end_client_ingestor_endpoints(
        &self,
        reason: ClientProducerEndReason,
    ) {
        let keys = self
            .inner
            .client_ingestors
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in keys {
            nervix_primitives::task::consume_budget().await;
            self.end_client_ingestor_endpoint(&key, reason).await;
        }
    }

    async fn end_client_ingestor_endpoint(
        &self,
        key: &DomainNodeRef,
        reason: ClientProducerEndReason,
    ) {
        let Some((_, endpoint)) = self.inner.client_ingestors.remove(key) else {
            return;
        };
        let (done, ended) = oneshot::channel();
        if endpoint
            .commands
            .send(EndpointCommand::End { reason, done })
            .is_err()
        {
            return;
        }
        ended.await.means_shutdown("client ingestor endpoint");
    }

    /// Why an endpoint whose ingestor this node does not run should end, or `None` while the
    /// ingestor is still scheduled here and may start again.
    fn client_endpoint_end_reason(
        &self,
        key: &DomainNodeRef,
        local_node_id: &ClusterNodeName,
    ) -> Option<ClientProducerEndReason> {
        let status = self
            .inner
            .domains
            .get(&key.domain)
            .map(|state| state.status.clone());
        match status {
            None => return Some(ClientProducerEndReason::EndpointRemoved),
            Some(nervix_models::DomainStatus::Stopped) => {
                return Some(ClientProducerEndReason::DomainStopped);
            }
            Some(_) => {}
        }
        let Some(execution) = self.inner.executions.get(&key.domain) else {
            return Some(ClientProducerEndReason::DomainStopped);
        };
        let ingestor = IngestorName::from(key.identifier());
        let Some(plan) = execution.revision.entrypoints.ingestor(&ingestor) else {
            return Some(ClientProducerEndReason::EndpointRemoved);
        };
        if let IngestorInputPlan::Transport(_) = plan.input {
            return Some(ClientProducerEndReason::EndpointChanged);
        }
        let identity = NodeRef::new(ModelKind::Ingestor, ModelName::from(&ingestor));
        let scheduled = execution.revision.nodes.get(&identity)?;
        if Self::scheduled_node_executes_locally(scheduled, Some(local_node_id)) {
            return None;
        }
        Some(ClientProducerEndReason::Relocated)
    }
}

impl Endpoint {
    async fn run(mut self) {
        /// What the task takes next.
        enum Next {
            Resolved(ResolvedAcknowledgement),
            Command(Option<EndpointCommand>),
        }

        loop {
            nervix_primitives::task::consume_budget().await;
            // A resolution frees a slot of the window, so it is taken before further commands.
            let next = nervix_primitives::select! {
                biased;
                Some(resolved) = self.acknowledgements.next() => Next::Resolved(resolved),
                command = self.commands.recv() => Next::Command(command),
            };
            let command = match next {
                Next::Resolved(resolved) => {
                    self.resolved(resolved);
                    self.pump();
                    self.publish_gauges();
                    continue;
                }
                Next::Command(Some(command)) => command,
                Next::Command(None) => {
                    // Every command sender is gone: the node is shutting down and ended no
                    // producer itself.
                    self.end(ClientProducerEndReason::ShuttingDown);
                    return;
                }
            };
            match command {
                EndpointCommand::Attach(attach) => self.attach(attach),
                EndpointCommand::Submit {
                    attachment,
                    submission,
                    body,
                } => self.submit(attachment, submission, body),
                EndpointCommand::Clear {
                    attachment,
                    submission,
                } => self.clear(attachment, submission),
                EndpointCommand::Close { attachment } => self.close(attachment),
                EndpointCommand::Detach { attachment } => self.detach(attachment),
                EndpointCommand::Install(execution) => self.install(execution),
                EndpointCommand::Uninstall => self.uninstall(),
                EndpointCommand::Intake(intake) => self.set_intake(intake),
                EndpointCommand::Admission(report) => self.admission(report),
                EndpointCommand::End { reason, done } => {
                    self.end(reason);
                    if done.send(()).is_err() {
                        debug!("the caller that ended a client ingestor endpoint stopped waiting");
                    }
                    return;
                }
            }
            self.pump();
            self.publish_gauges();
        }
    }

    fn attach(&mut self, attach: AttachCommand) {
        let AttachCommand {
            expected_fields,
            limits,
            max_batch_bytes,
            serving,
            reply,
        } = attach;
        let attached = self.attached_producer(expected_fields, limits, max_batch_bytes, serving);
        if reply.send(attached).is_err() {
            // The open was cancelled before its answer arrived; the new attachment has no
            // producer and is released.
            debug!(
                domain = self.domain.as_str(),
                ingestor = self.ingestor.as_str(),
                "a producer open was cancelled before it was answered"
            );
        }
    }

    fn attached_producer(
        &mut self,
        expected_fields: Vec<SchemaField>,
        limits: ClientProducerLimits,
        max_batch_bytes: NonZeroU64,
        serving: ProducerServing,
    ) -> Result<AttachedProducer, ClientProducerRefusal> {
        let Some(execution) = self.execution.clone() else {
            return Err(ClientProducerRefusal::EndpointUnavailable);
        };
        if expected_fields != execution.fields {
            return Err(ClientProducerRefusal::SchemaMismatch);
        }
        let max_batch_bytes = max_batch_bytes.min(limits.bytes);
        let grant = ClientProducerGrant {
            batches: limits.batches,
            bytes: limits.bytes,
            max_batch_bytes,
            max_batch_rows: NonZeroU32::new(MAX_CLIENT_BATCH_ROWS)
                .assured("the row limit is a non-zero constant"),
        };
        let admission_state = self.intake.admission();
        let (events, outcomes) = mpsc::unbounded_channel();
        let (admission, admission_receiver) = watch::channel(admission_state);
        // A fresh identity, ordered by the time the attachment was made.
        let id = ClientAttachmentId::from_u128(uuid::Uuid::now_v7().as_u128());
        let description = ClientProducerDescription {
            attachment: id,
            fields: execution.fields.clone(),
            generation: execution.generation,
            contract: execution.contract,
            policy: execution.policy,
            grant,
            admission: admission_state,
        };
        self.attachments.insert(
            id,
            Attachment {
                generation: execution.generation,
                contract: execution.contract,
                grant,
                queue: VecDeque::new(),
                clearing: VecDeque::new(),
                ready: VecDeque::new(),
                outstanding: HashMap::new(),
                held_bytes: 0,
                events,
                admission,
                closing: false,
                serving,
            },
        );
        debug!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            attachment = %id,
            "a producer attached to a client ingestor"
        );
        Ok(AttachedProducer {
            description,
            events: ClientProducerEvents {
                outcomes,
                admission: admission_receiver,
            },
        })
    }

    fn submit(
        &mut self,
        attachment: ClientAttachmentId,
        submission: ClientSubmissionId,
        body: Bytes,
    ) {
        let Some(entry) = self.attachments.get_mut(&attachment) else {
            // A detached or ended producer's late batch has nobody to answer.
            return;
        };
        let bytes: u64 = body.len().arch_into();
        if entry.closing {
            entry.answer(
                &self.series,
                submission,
                ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::ProducerEnded),
                None,
            );
            return;
        }
        let batches_fit = entry.held_batches() < entry.grant.batches.get().arch_into();
        let bytes_fit = match entry.held_bytes.checked_add(bytes) {
            Some(held) => held <= entry.grant.bytes.get(),
            None => false,
        };
        if !batches_fit || !bytes_fit {
            entry.answer(
                &self.series,
                submission,
                ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::CreditExceeded),
                Some("the batch exceeds the producer's granted credit".to_string()),
            );
            self.end_attachment(attachment, ClientProducerEndReason::ProtocolViolated);
            return;
        }
        if let Some(refusal) = self.intake.refusal() {
            entry.answer(
                &self.series,
                submission,
                ClientSubmissionOutcome::NotAdmitted(refusal),
                None,
            );
            return;
        }
        entry.held_bytes = entry
            .held_bytes
            .checked_add(bytes)
            .verified("the credit check above established that the sum fits");
        entry.queue.push_back(QueuedSubmission { submission, body });
    }

    /// Moves a batch its serving node cleared to the batches ready for the worker. Clearances
    /// arrive in the order the endpoint asked for them, so a clearance names the oldest batch still
    /// being cleared, or a batch the endpoint already refused, which it ignores.
    fn clear(&mut self, attachment: ClientAttachmentId, submission: ClientSubmissionId) {
        let Some(entry) = self.attachments.get_mut(&attachment) else {
            // The producer ended or detached, and every batch it held with it.
            return;
        };
        let Some(oldest) = entry.clearing.front() else {
            return;
        };
        if oldest.submission != submission {
            return;
        }
        let cleared = entry
            .clearing
            .pop_front()
            .verified("the oldest batch being cleared was found above");
        entry.ready.push_back(cleared);
    }

    fn close(&mut self, attachment: ClientAttachmentId) {
        let Some(entry) = self.attachments.get_mut(&attachment) else {
            // The attachment already ended, which closed its outcomes.
            return;
        };
        let released = entry.slotted_batches();
        entry.refuse_unadmitted(&self.series, ClientSubmissionRefusal::ProducerEnded);
        entry.closing = true;
        self.release_window_slots(released);
        self.release_if_closed(attachment);
    }

    /// Returns the slots of the window that batches held without reaching the worker.
    fn release_window_slots(&mut self, slots: usize) {
        self.window_used = self.window_used.checked_sub(slots).verified(
            "every batch being cleared or ready for the worker holds one slot of the window",
        );
    }

    /// Releases a closing attachment once every batch it admitted has its outcome. Dropping it
    /// closes its outcomes behind the last one.
    fn release_if_closed(&mut self, attachment: ClientAttachmentId) {
        let finished = match self.attachments.get(&attachment) {
            Some(entry) => entry.closing && entry.outstanding.is_empty(),
            None => false,
        };
        if !finished {
            return;
        }
        self.attachments
            .shift_remove(&attachment)
            .verified("the attachment was found above, and this task alone removes attachments");
        debug!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            attachment = %attachment,
            "a closed producer was released"
        );
    }

    /// Lets go of a producer without answering it. Its admitted batches continue in the graph,
    /// and the batches it held that never reached the worker are dropped with their slots.
    fn detach(&mut self, attachment: ClientAttachmentId) {
        let Some(entry) = self.attachments.shift_remove(&attachment) else {
            return;
        };
        self.release_window_slots(entry.slotted_batches());
        debug!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            attachment = %attachment,
            "a producer detached from a client ingestor"
        );
    }

    fn install(&mut self, execution: Arc<ClientExecution>) {
        let mut ended = Vec::new();
        for (id, entry) in &self.attachments {
            if entry.generation != execution.generation {
                ended.push(EndedAttachment {
                    attachment: *id,
                    reason: ClientProducerEndReason::DomainStopped,
                });
            } else if entry.contract != execution.contract {
                ended.push(EndedAttachment {
                    attachment: *id,
                    reason: ClientProducerEndReason::EndpointChanged,
                });
            }
        }
        for EndedAttachment { attachment, reason } in ended {
            self.end_attachment(attachment, reason);
        }
        self.abandon_interrupted_job();
        self.execution = Some(execution);
        // The new execution's intake watch starts after this installation was sent, so its first
        // report, which opens admission, is applied after this.
        self.set_intake(ClientIntakeState::Suspended);
    }

    fn uninstall(&mut self) {
        self.abandon_interrupted_job();
        self.execution = None;
        self.set_intake(ClientIntakeState::Suspended);
    }

    /// Answers the batch handed to a worker that never reported it.
    ///
    /// A stopping worker reports every batch it took and refuses the one it was handed and never
    /// took, and its task is joined before the execution is uninstalled. A batch still held here
    /// was therefore taken by a worker that was aborted when its stop outlasted the grace period,
    /// possibly while dispatching it, so whether any of it entered the graph is unknown.
    fn abandon_interrupted_job(&mut self) {
        let Some(WorkerBatch {
            attachment,
            submission,
        }) = self.in_worker.take()
        else {
            return;
        };
        self.window_used = self
            .window_used
            .checked_sub(1)
            .verified("the batch handed to the worker holds one slot of the window");
        self.answer_outstanding(
            attachment,
            submission,
            ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted),
            None,
        );
    }

    fn set_intake(&mut self, intake: ClientIntakeState) {
        self.intake = intake;
        let admission = intake.admission();
        let refusal = intake.refusal();
        let mut released: usize = 0;
        for entry in self.attachments.values_mut() {
            if let Some(refusal) = refusal {
                released = released
                    .checked_add(entry.slotted_batches())
                    .assured("the released slots are bounded by the window");
                entry.refuse_unadmitted(&self.series, refusal);
            }
            entry.admission.send_if_modified(|current| {
                let changed = *current != admission;
                *current = admission;
                changed
            });
        }
        self.release_window_slots(released);
    }

    fn admission(&mut self, report: AdmissionReport) {
        let AdmissionReport {
            attachment,
            submission,
            result,
        } = report;
        if self.in_worker
            == Some(WorkerBatch {
                attachment,
                submission,
            })
        {
            self.in_worker = None;
        }
        match result {
            AdmissionResult::Admitted {
                completion,
                ack_timeout,
            } => {
                // An execution that stopped since keeps no hold on the batch: its ACK root still
                // decides the outcome.
                self.watch_acknowledgement(attachment, submission, completion, ack_timeout);
            }
            AdmissionResult::Refused { refusal, detail } => {
                self.window_used = self
                    .window_used
                    .checked_sub(1)
                    .verified("a batch the worker refused held one slot of the window");
                self.answer_outstanding(
                    attachment,
                    submission,
                    ClientSubmissionOutcome::NotAdmitted(refusal),
                    detail,
                );
            }
        }
    }

    fn resolved(&mut self, resolved: ResolvedAcknowledgement) {
        let ResolvedAcknowledgement {
            attachment,
            submission,
            acknowledgement,
        } = resolved;
        self.window_used = self
            .window_used
            .checked_sub(1)
            .verified("an admitted batch holds one slot of the window until it is resolved");
        self.answer_outstanding(
            attachment,
            submission,
            acknowledgement.outcome,
            acknowledgement.detail,
        );
    }

    /// Answers a batch an attachment holds outstanding, if the attachment still holds it.
    fn answer_outstanding(
        &mut self,
        attachment: ClientAttachmentId,
        submission: ClientSubmissionId,
        outcome: ClientSubmissionOutcome,
        detail: Option<String>,
    ) {
        let Some(entry) = self.attachments.get_mut(&attachment) else {
            return;
        };
        let Some(bytes) = entry.outstanding.remove(&submission) else {
            return;
        };
        entry.held_bytes = entry
            .held_bytes
            .checked_sub(bytes)
            .verified("an outstanding batch's bytes were added when it was queued");
        entry.answer(&self.series, submission, outcome, detail);
        self.release_if_closed(attachment);
    }

    /// Awaits an admitted batch's acknowledgement among this task's commands.
    fn watch_acknowledgement(
        &mut self,
        attachment: ClientAttachmentId,
        submission: ClientSubmissionId,
        completion: AckCompletion,
        ack_timeout: Duration,
    ) {
        let resolution = async move {
            let acknowledgement = await_client_acknowledgement(completion, ack_timeout).await;
            ResolvedAcknowledgement {
                attachment,
                submission,
                acknowledgement,
            }
        };
        self.acknowledgements.push(Box::pin(resolution));
    }

    /// Hands out every batch the execution can take now, while it admits. While the window has
    /// room, the attachments take their turns, and each turn gives the attachment's next queued
    /// batch a slot of the window whether or not the worker is free, so a busy worker never passes
    /// a producer over: a local producer's batch is then ready for the worker, and a forwarded
    /// producer's batch waits for its serving node to clear it first. A free worker takes a ready
    /// batch.
    fn pump(&mut self) {
        let Some(execution) = self.execution.clone() else {
            return;
        };
        if self.intake != ClientIntakeState::Open {
            return;
        }
        // Each batch handed out takes the worker or a slot of the window, so this ends after at
        // most one batch per slot and one more for the worker.
        while self.hand_out_one(&execution) {}
    }

    /// Hands out one batch, or returns `false` when nothing can be handed out now.
    fn hand_out_one(&mut self, execution: &ClientExecution) -> bool {
        if self.in_worker.is_none() && self.hand_ready_batch(execution) {
            return true;
        }
        if self.window_used >= execution.window.get() {
            return false;
        }
        self.take_next_turn()
    }

    /// The attachment `step` places after the one whose turn is next, among `count` of them.
    fn turn_index(&self, step: usize, count: usize) -> usize {
        let position = self
            .cursor
            .checked_add(step)
            .assured("the cursor and the step are both bounded by the attachment count");
        position % count
    }

    /// Hands the free worker the oldest ready batch of the first attachment in turn that holds
    /// one.
    fn hand_ready_batch(&mut self, execution: &ClientExecution) -> bool {
        let count = self.attachments.len();
        for step in 0..count {
            let index = self.turn_index(step, count);
            let Some((id, entry)) = self.attachments.get_index_mut(index) else {
                continue;
            };
            let Some(ready) = entry.ready.pop_front() else {
                continue;
            };
            let job = entry.outstanding_job(*id, ready);
            self.dispatch(execution, job);
            return true;
        }
        false
    }

    /// Gives the next attachment in turn that queued a batch a slot of the window for it. A local
    /// producer's batch is then ready for the worker; a forwarded producer's batch is sent to be
    /// cleared first.
    fn take_next_turn(&mut self) -> bool {
        let count = self.attachments.len();
        for step in 0..count {
            let index = self.turn_index(step, count);
            let Some((_, entry)) = self.attachments.get_index_mut(index) else {
                continue;
            };
            let Some(queued) = entry.queue.pop_front() else {
                continue;
            };
            match &entry.serving {
                ProducerServing::Local => entry.ready.push_back(queued),
                ProducerServing::Forwarded {
                    clearance_requests, ..
                } => {
                    // A serving node that is gone detaches the producer, which returns the slot.
                    clearance_requests
                        .send(queued.submission)
                        .means_peer_left("forwarded client producer");
                    entry.clearing.push_back(queued);
                }
            }
            self.take_window_slot(index);
            return true;
        }
        false
    }

    /// Takes one slot of the window for the batch of the attachment at `index`, whose turn it was,
    /// and passes the turn to the attachment after it.
    fn take_window_slot(&mut self, index: usize) {
        self.window_used = self
            .window_used
            .checked_add(1)
            .assured("the window is below its configured size, checked before a turn is taken");
        self.cursor = index
            .checked_add(1)
            .assured("an index below the attachment count has a successor");
    }

    /// Hands the free worker one outstanding batch that holds a slot of the window. A worker that
    /// stopped before its execution was uninstalled takes nothing: the batch is refused like any
    /// batch arriving while admission is held, and its slot is returned.
    fn dispatch(&mut self, execution: &ClientExecution, job: AdmissionJob) {
        let handed = WorkerBatch {
            attachment: job.attachment,
            submission: job.submission,
        };
        let refused = match execution.jobs.try_send(job) {
            Ok(()) => {
                self.in_worker = Some(handed);
                return;
            }
            Err(mpsc::error::TrySendError::Full(job) | mpsc::error::TrySendError::Closed(job)) => {
                job
            }
        };
        self.window_used = self
            .window_used
            .checked_sub(1)
            .verified("the batch the worker did not take held one slot of the window");
        self.answer_outstanding(
            refused.attachment,
            refused.submission,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::Suspended),
            None,
        );
    }

    fn end_attachment(&mut self, attachment: ClientAttachmentId, reason: ClientProducerEndReason) {
        let Some(entry) = self.attachments.shift_remove(&attachment) else {
            return;
        };
        debug!(
            domain = self.domain.as_str(),
            ingestor = self.ingestor.as_str(),
            attachment = %attachment,
            reason = reason.as_ref(),
            "a client ingestor ended a producer"
        );
        if let Some(held) = self.in_worker
            && held.attachment == attachment
        {
            // The worker still reports the batch; its report finds the attachment gone.
            debug!("an ended producer had a batch with the admission worker");
        }
        self.release_window_slots(entry.slotted_batches());
        entry.end(&self.series, reason);
    }

    fn end(&mut self, reason: ClientProducerEndReason) {
        // Every admitted batch is answered below as of unknown outcome, so its acknowledgement
        // is no longer awaited.
        self.acknowledgements.clear();
        let attachments = std::mem::take(&mut self.attachments);
        if !attachments.is_empty() {
            info!(
                domain = self.domain.as_str(),
                ingestor = self.ingestor.as_str(),
                producers = attachments.len(),
                reason = reason.as_ref(),
                "ended the producers of a client ingestor"
            );
        }
        for (_, entry) in attachments {
            entry.end(&self.series, reason);
        }
        self.window_used = 0;
        self.in_worker = None;
        self.publish_gauges();
    }

    /// Publishes the endpoint's counts for `DESCRIBE` and metrics when a command changed them.
    fn publish_gauges(&mut self) {
        let mut gauges = ClientIngestorGauges {
            admitted_batches: self.window_used.arch_into(),
            ..ClientIngestorGauges::default()
        };
        for entry in self.attachments.values() {
            let batches: u64 = entry.held_batches().arch_into();
            gauges.producers = gauges
                .producers
                .checked_add(1)
                .assured("every counted attachment is held in memory");
            if entry.is_forwarded() {
                gauges.forwarded_producers = gauges
                    .forwarded_producers
                    .checked_add(1)
                    .assured("every counted attachment is held in memory");
            }
            gauges.outstanding_batches = gauges
                .outstanding_batches
                .checked_add(batches)
                .assured("every counted batch is held in memory");
            gauges.outstanding_bytes = gauges
                .outstanding_bytes
                .checked_add(entry.held_bytes)
                .assured("every counted byte is held in memory");
        }
        if gauges == self.published {
            return;
        }
        self.published = gauges;
        self.gauges.publish(gauges);
        self.series.set(gauges);
    }
}

/// The producers attached to one client ingestor's endpoint, as its metrics and `DESCRIBE`
/// report them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ClientIngestorGauges {
    pub(crate) producers: u64,
    /// Producers whose sessions another node serves and forwards their batches from.
    pub(crate) forwarded_producers: u64,
    /// Batches the producers submitted that have no outcome yet.
    pub(crate) outstanding_batches: u64,
    /// The Arrow IPC bytes of those batches.
    pub(crate) outstanding_bytes: u64,
    /// Batches holding a slot of the acknowledgement window: being admitted, or admitted and
    /// awaiting their acknowledgement. Every producer shares the window, so this never exceeds
    /// its size however many producers are attached.
    pub(crate) admitted_batches: u64,
}

/// How one admitted batch's acknowledgement resolved, and what its producer is told about it.
struct ClientAcknowledgement {
    outcome: ClientSubmissionOutcome,
    detail: Option<String>,
}

/// The resolved acknowledgement of one admitted batch, and the batch it answers.
struct ResolvedAcknowledgement {
    attachment: ClientAttachmentId,
    submission: ClientSubmissionId,
    acknowledgement: ClientAcknowledgement,
}

/// Waits for an admitted batch's ACK root. The timeout counts the time without progress, so a
/// batch whose downstream work keeps reporting that it is alive does not time out.
async fn await_client_acknowledgement(
    mut completion: AckCompletion,
    ack_timeout: Duration,
) -> ClientAcknowledgement {
    loop {
        nervix_primitives::task::consume_budget().await;
        let progress =
            nervix_primitives::time::timeout(ack_timeout, completion.wait_for_progress()).await;
        match progress {
            Ok(AckProgress::Alive) => {}
            Ok(AckProgress::Complete(AckOutcome::Ack)) => {
                return ClientAcknowledgement {
                    outcome: ClientSubmissionOutcome::Completed,
                    detail: None,
                };
            }
            Ok(AckProgress::Complete(AckOutcome::NoAck(reason))) => {
                return ClientAcknowledgement {
                    outcome: ClientSubmissionOutcome::ProcessingFailed(
                        ClientProcessingFailure::Rejected,
                    ),
                    detail: Some(bounded_detail(reason)),
                };
            }
            Err(_) => {
                return ClientAcknowledgement {
                    outcome: ClientSubmissionOutcome::ProcessingFailed(
                        ClientProcessingFailure::AckTimedOut,
                    ),
                    detail: Some(format!(
                        "no acknowledgement progress within {}",
                        humantime::format_duration(ack_timeout)
                    )),
                };
            }
        }
    }
}

/// A failure's description cut to what one outcome carries, at a character boundary.
fn bounded_detail(mut detail: String) -> String {
    if detail.len() <= MAX_OUTCOME_DETAIL_BYTES {
        return detail;
    }
    let mut end = MAX_OUTCOME_DETAIL_BYTES;
    while !detail.is_char_boundary(end) {
        end = end
            .checked_sub(1)
            .assured("index zero is a character boundary, so the search stops there");
    }
    detail.truncate(end);
    detail
}

/// Tells an endpoint whenever its execution's quiesce publication changes whether batches are
/// admitted, starting with the state at installation.
async fn watch_client_intake(
    quiesce: Arc<IngestorQuiesceControl>,
    reports: mpsc::UnboundedSender<EndpointCommand>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut observation = quiesce.observation();
    loop {
        nervix_primitives::task::consume_budget().await;
        let state = quiesce.client_intake_state();
        if reports.send(EndpointCommand::Intake(state)).is_err() {
            // The endpoint ended, and nothing it served is left to tell.
            return;
        }
        nervix_primitives::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            () = quiesce.wait_for_change_since(&mut observation) => {}
        }
    }
}

/// The task that admits the batches one execution's endpoint hands over, one at a time.
struct AdmissionWorker {
    intake: ClientIntake,
    mailbox: WorkerMailbox,
}

impl AdmissionWorker {
    /// Admits batches until the execution stops, reporting every batch it takes, admitted or
    /// refused.
    async fn run(mut self) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(job) = self.mailbox.next_job().await else {
                return;
            };
            let attachment = job.attachment;
            let submission = job.submission;
            let result = self.intake.admit(job).await;
            let reported = self.mailbox.report(AdmissionReport {
                attachment,
                submission,
                result,
            });
            if !reported {
                return;
            }
        }
    }
}

/// The admission worker's side of its endpoint: the batch handed over, one at a time, the reports
/// sent back, and the signal that stops the execution.
struct WorkerMailbox {
    jobs: mpsc::Receiver<AdmissionJob>,
    reports: mpsc::UnboundedSender<EndpointCommand>,
    shutdown: watch::Receiver<bool>,
}

impl WorkerMailbox {
    /// The next batch handed over, or `None` once the execution stops. A stopping worker first
    /// refuses the batch it was handed and never took.
    async fn next_job(&mut self) -> Option<AdmissionJob> {
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() {
                        self.refuse_untaken_jobs();
                        return None;
                    }
                }
                job = self.jobs.recv() => return job,
            }
        }
    }

    /// Refuses the batch the endpoint handed over and the stopping worker never took: nothing of
    /// it was dispatched. Closing the channel first makes the endpoint refuse any later batch
    /// itself.
    fn refuse_untaken_jobs(&mut self) {
        self.jobs.close();
        while let Ok(job) = self.jobs.try_recv() {
            let reported = self.report(AdmissionReport {
                attachment: job.attachment,
                submission: job.submission,
                result: AdmissionResult::Refused {
                    refusal: ClientSubmissionRefusal::Suspended,
                    detail: None,
                },
            });
            if !reported {
                return;
            }
        }
    }

    /// Reports one batch to the endpoint. `false` means the endpoint ended, and it answered every
    /// batch it held when it did.
    fn report(&self, report: AdmissionReport) -> bool {
        self.reports
            .send(EndpointCommand::Admission(report))
            .is_ok()
    }
}

impl ClientIntake {
    /// Validates one batch and dispatches it into the graph under a new ACK root.
    async fn admit(&self, job: AdmissionJob) -> AdmissionResult {
        let limits = ClientBatchLimits {
            max_bytes: job.max_batch_bytes,
            max_rows: NonZeroUsize::new(MAX_CLIENT_BATCH_ROWS.arch_into())
                .assured("the row limit is a non-zero constant"),
        };
        let decoded = self
            .schema
            .decode_client_batch(self.runtime.executor(), job.body, limits)
            .await;
        let batch = match decoded {
            Ok(batch) => batch,
            Err(error) => {
                let failure = error.current_context();
                let Some(defect) = failure.defect() else {
                    return AdmissionResult::Refused {
                        refusal: ClientSubmissionRefusal::Busy,
                        detail: None,
                    };
                };
                return AdmissionResult::Refused {
                    refusal: ClientSubmissionRefusal::InvalidBatch(defect),
                    detail: Some(bounded_detail(failure.to_string())),
                };
            }
        };
        let (root, completion) = match self.quiesce.track_client_batch(&self.trackers) {
            Ok(tracked) => tracked,
            Err(refusal) => {
                return AdmissionResult::Refused {
                    refusal,
                    detail: None,
                };
            }
        };
        let dispatched = self
            .runtime
            .dispatch_client_batch(ClientBatchDispatch {
                handles: &self.handles,
                domain: &self.domain,
                ingestor: &self.ingestor,
                timestamp_source: self.timestamp_source.as_ref(),
                output_routes: &self.output_routes,
                filter_where: self.filter_where.as_ref(),
                branched_senders: &self.branched_senders,
                metrics: &self.metrics,
                batch,
                acks: root.attached(),
                ingested_at: actual_utc_now(),
            })
            .await;
        match dispatched {
            Ok(()) => root.ack_success(),
            Err(error) => {
                debug!(
                    domain = self.domain.as_str(),
                    ingestor = self.ingestor.as_str(),
                    error = %error,
                    "a client batch failed after it was admitted"
                );
                root.no_ack(error.current_context().to_string());
            }
        }
        AdmissionResult::Admitted {
            completion,
            ack_timeout: self.ack_timeout,
        }
    }
}

impl IngestorQuiesceControl {
    /// Tracks the ACK root of a validated client batch and then decides whether it may be
    /// dispatched: the fence against a quiesce that engaged while the batch was validated.
    ///
    /// The root is tracked before the decision is read, so either the drain that follows a
    /// quiesce counts this root and waits for it, or this read observes the quiesce and the batch
    /// is refused with its root resolved before anything was dispatched under it.
    pub(in crate::runtime) fn track_client_batch(
        &self,
        trackers: &IngestorAckRootTrackers,
    ) -> Result<(AckSet, AckCompletion), ClientSubmissionRefusal> {
        let (root, completion) = trackers.tracked_root();
        if let Some(refusal) = self.client_intake_state().refusal() {
            root.ack_success();
            return Err(refusal);
        }
        Ok((root, completion))
    }

    /// Whether a client source admits a batch under the current publication. An ownership
    /// handoff or a shutdown stops intake for good on this execution; every other hold is a
    /// suspension that the release ends.
    pub(in crate::runtime) fn client_intake_state(&self) -> ClientIntakeState {
        match self.cause() {
            None => ClientIntakeState::Open,
            Some(IngestorQuiesceCause::OwnershipHandoff | IngestorQuiesceCause::Shutdown) => {
                ClientIntakeState::Draining
            }
            Some(
                IngestorQuiesceCause::EntityHold
                | IngestorQuiesceCause::DomainPause
                | IngestorQuiesceCause::MemoryPressure,
            ) => ClientIntakeState::Suspended,
        }
    }
}

#[cfg(test)]
#[path = "client_ingestor_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "client_ingestor_shuttle_tests.rs"]
mod shuttle_tests;
