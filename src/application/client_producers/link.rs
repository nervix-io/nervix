//! The ordered link that carries forwarded producers between a serving node and the node that
//! executes their client ingestor.
//!
//! Layer: edges.
//!
//! - **Owns.** The one duplex stream a serving node keeps to each owning node while producers use
//!   it, the frames it carries in each direction, the link-local key of every forwarded producer,
//!   the heartbeat each end sends while idle, and ending every producer of a link that fails or
//!   falls silent.
//! - **Depends on.** The interconnect's ordered duplex streams and the runtime's client ingestor
//!   endpoints on the owning side.
//! - **Must not know.** Sessions, the session wire, or how a batch is decoded or admitted.
//!
//! Every producer a serving node forwards to one owning node shares that node's link, so forwarding
//! holds one relay stream per peer however many producers use it. Frames of one producer keep
//! their order in both directions: its open precedes its batches, and its outcomes, admission
//! changes and end follow the owning node's answer to its open. A link neither end hears from
//! within the silence limit is treated as lost, which the serving node reports to every producer
//! of the link as `OwnerLost`; the owning node detaches them, and admitted work continues in its
//! graph.

use std::{num::NonZeroU64, time::Duration};

use ahash::{HashMap, RandomState};
use bytes::Bytes;
use error_stack::Report;
use futures_util::StreamExt as _;
use nervix_interconnect::{
    ChargedItem, DuplexItems, DuplexResponses, DuplexSender, HandlerRegistrationError,
    InterconnectDuplexRequest, PoolClass, RequestSubquota, StreamHandlerError, Transport,
};
use nervix_models::{
    ClientProducerAdmission, ClientProducerDescription, ClientProducerEndReason,
    ClientProducerLimits, ClientProducerRefusal, ClientSubmissionOutcome, ClusterNodeName,
    DomainName, IngestorName, SchemaField,
};
use nervix_primitives::{
    collections::DashMap,
    stream::wrappers::ReceiverStream,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, oneshot, watch,
    },
};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::debug;
use triomphe::Arc;

use super::{OpenedRoute, ProducerOpen, ProducerRoute};
use crate::runtime::{
    ClientProducerEvent, ClientProducerEvents, ClientProducerHandle, ClientProducerOpenRequest,
    ClientProducerRetention, ClientSubmissionId, OpenedClientProducer, Runtime,
};

/// How often an idle end of a link tells the other it is still there.
const LINK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How long an end of a link waits without hearing anything from the other before it treats the
/// link as lost.
const LINK_SILENCE_LIMIT: Duration = Duration::from_secs(10);

const _: () = assert!(
    LINK_SILENCE_LIMIT.as_secs() >= 4 * LINK_HEARTBEAT_INTERVAL.as_secs(),
    "a link survives several heartbeats delayed behind the frames of a busy link",
);

/// How long a serving node waits for the owning node to answer a forwarded open, including
/// opening the link itself.
const FORWARDED_OPEN_DEADLINE: Duration = Duration::from_secs(20);

/// Answers queued for a serving node before a producer's forwarding waits for the link to take
/// them. Heartbeats are skipped rather than queued behind a full queue.
const OWNER_LINK_ANSWER_CAPACITY: usize = 64;

/// Opens the link a serving node keeps to one owning node.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) struct OpenClientProducerLink {
    /// The node that opens the link, which the owning node checks against the peer it
    /// authenticated.
    serving_node: ClusterNodeName,
}

impl InterconnectDuplexRequest for OpenClientProducerLink {
    type Item = ClientProducerLinkItem;
    type Response = ClientProducerLinkEvent;

    const NAME: &'static str = "client_producer_link";
    const CLASS: PoolClass = PoolClass::Relay;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
}

/// What a serving node sends over its link, each naming the producer it concerns by its key.
#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) enum ClientProducerLinkItem {
    Open {
        key: u64,
        domain: DomainName,
        ingestor: IngestorName,
        expected_fields: Vec<SchemaField>,
        limits: ClientProducerLimits,
        max_batch_bytes: NonZeroU64,
    },
    Submit {
        key: u64,
        submission: NonZeroU64,
        batch: Vec<u8>,
    },
    Close {
        key: u64,
    },
    Detach {
        key: u64,
    },
    Heartbeat,
}

/// What an owning node answers over a link.
#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
pub(super) enum ClientProducerLinkEvent {
    Opened {
        key: u64,
        description: ClientProducerDescription,
    },
    Refused {
        key: u64,
        refusal: ClientProducerRefusal,
    },
    Outcome {
        key: u64,
        submission: NonZeroU64,
        outcome: ClientSubmissionOutcome,
        detail: Option<String>,
    },
    Admission {
        key: u64,
        admission: ClientProducerAdmission,
    },
    /// The owning node ended the producer; nothing about it follows.
    Ended {
        key: u64,
        reason: ClientProducerEndReason,
    },
    /// The producer was released after it closed or detached; nothing about it follows.
    Closed {
        key: u64,
    },
    Heartbeat,
}

/// The links this node keeps to the owning nodes of the producers it forwards. Cloning it shares
/// them.
#[derive(Clone)]
pub(super) struct ProducerLinks {
    inner: Arc<LinksInner>,
}

struct LinksInner {
    interconnect: Transport,
    local_node: ClusterNodeName,
    links: DashMap<ClusterNodeName, LinkHandle, RandomState>,
    /// The key the next forwarded producer takes. Keys are never reused within the process, so a
    /// late frame about a producer can never reach a later one.
    next_key: AtomicU64,
}

/// The commands of one serving link's task.
#[derive(Clone)]
struct LinkHandle {
    commands: mpsc::UnboundedSender<LinkCommand>,
}

impl LinkHandle {
    /// A handle and the commands its link's task serves.
    fn new() -> (Self, mpsc::UnboundedReceiver<LinkCommand>) {
        let (commands, receiver) = mpsc::unbounded_channel();
        (Self { commands }, receiver)
    }
}

enum LinkCommand {
    Open {
        key: u64,
        open: ProducerOpen,
        reply: oneshot::Sender<Result<OpenedRoute, ClientProducerRefusal>>,
    },
    Submit {
        key: u64,
        submission: NonZeroU64,
        batch: Bytes,
    },
    Close {
        key: u64,
    },
    Detach {
        key: u64,
    },
}

/// A producer attached through a link. Dropping it detaches the producer.
pub(in crate::application) struct ForwardedProducer {
    key: u64,
    commands: mpsc::UnboundedSender<LinkCommand>,
    detached: bool,
}

impl ForwardedProducer {
    pub(super) fn submit(&self, submission: ClientSubmissionId, batch: Bytes) {
        let command = LinkCommand::Submit {
            key: self.key,
            submission: submission.get(),
            batch,
        };
        // A link that is gone already ended this producer, and that end answers the batch.
        self.commands
            .send(command)
            .means_shutdown("client producer link");
    }

    pub(super) fn close(mut self) {
        self.detached = true;
        self.commands
            .send(LinkCommand::Close { key: self.key })
            .means_shutdown("client producer link");
    }
}

impl Drop for ForwardedProducer {
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        self.commands
            .send(LinkCommand::Detach { key: self.key })
            .means_shutdown("client producer link");
    }
}

impl ProducerLinks {
    pub(super) fn new(interconnect: Transport, local_node: ClusterNodeName) -> Self {
        Self {
            inner: Arc::new(LinksInner {
                interconnect,
                local_node,
                links: DashMap::with_hasher(RandomState::default()),
                next_key: AtomicU64::new(1),
            }),
        }
    }

    /// Attaches a producer to the client ingestor `owner` executes, over this node's link to it.
    pub(super) async fn open(
        &self,
        owner: &ClusterNodeName,
        open: ProducerOpen,
    ) -> Result<OpenedRoute, ClientProducerRefusal> {
        let key = self.inner.next_key.fetch_add(1, Ordering::Relaxed);
        let (reply, answer) = oneshot::channel();
        let mut command = LinkCommand::Open { key, open, reply };
        // A link found closed has ended since it was looked up; the command comes back, and a new
        // link takes it.
        for _ in 0..2 {
            let link = self.link_to(owner);
            match link.commands.send(command) {
                Ok(()) => {
                    return match tokio::time::timeout(FORWARDED_OPEN_DEADLINE, answer).await {
                        Ok(Ok(opened)) => opened,
                        // The link ended before the owning node answered.
                        Ok(Err(_)) => Err(ClientProducerRefusal::EndpointUnavailable),
                        Err(_) => {
                            debug!(%owner, "a forwarded producer open outlived its deadline");
                            Err(ClientProducerRefusal::EndpointUnavailable)
                        }
                    };
                }
                Err(returned) => {
                    self.forget(owner, &link);
                    command = returned.0;
                }
            }
        }
        Err(ClientProducerRefusal::EndpointUnavailable)
    }

    /// The link to `owner`, opening one when there is none or the one there has ended. Two opens
    /// racing for one owner start one link, because the check and the replacement happen under
    /// the map's entry for that owner.
    fn link_to(&self, owner: &ClusterNodeName) -> LinkHandle {
        let mut started = None;
        let link = self
            .inner
            .links
            .entry(owner.clone())
            .and_modify(|current| {
                if current.commands.is_closed() {
                    let (link, commands) = LinkHandle::new();
                    *current = link;
                    started = Some(commands);
                }
            })
            .or_insert_with(|| {
                let (link, commands) = LinkHandle::new();
                started = Some(commands);
                link
            })
            .clone();
        if let Some(commands) = started {
            let serving = ServingLink {
                links: self.clone(),
                owner: owner.clone(),
                own: link.commands.clone(),
                pending: HashMap::default(),
                routes: HashMap::default(),
            };
            nervix_primitives::task::spawn(serving.run(commands));
        }
        link
    }

    /// Removes the link to `owner` if it is still `link`.
    fn forget(&self, owner: &ClusterNodeName, link: &LinkHandle) {
        self.inner
            .links
            .remove_if(owner, |_, current| {
                current.commands.same_channel(&link.commands)
            })
            .discarded("a link another open already replaced stays in place");
    }
}

/// A forwarded producer as the serving side of its link routes it.
struct ForwardedRoute {
    events: mpsc::UnboundedSender<ClientProducerEvent>,
    admission: watch::Sender<ClientProducerAdmission>,
}

/// The task that owns one serving link.
struct ServingLink {
    links: ProducerLinks,
    owner: ClusterNodeName,
    /// This link's own command sender, which names it in the registry.
    own: mpsc::UnboundedSender<LinkCommand>,
    /// Opens the owning node has not answered yet.
    pending: HashMap<u64, oneshot::Sender<Result<OpenedRoute, ClientProducerRefusal>>>,
    routes: HashMap<u64, ForwardedRoute>,
}

impl ServingLink {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<LinkCommand>) {
        let opened = self
            .links
            .inner
            .interconnect
            .open_duplex_stream(
                &self.owner,
                OpenClientProducerLink {
                    serving_node: self.links.inner.local_node.clone(),
                },
            )
            .await;
        let (sender, mut receiver) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                debug!(owner = %self.owner, error = %error, "a client producer link did not open");
                self.end(&mut commands);
                return;
            }
        };
        let (items, queued) = mpsc::unbounded_channel();
        let mut writer = nervix_primitives::task::spawn(write_link(sender, queued));
        let mut last_heard = Instant::now();
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                event = receiver.next() => match event {
                    Ok(Some(event)) => {
                        last_heard = Instant::now();
                        self.deliver(event);
                    }
                    Ok(None) => {
                        debug!(owner = %self.owner, "the owning node closed a client producer link");
                        break;
                    }
                    Err(error) => {
                        debug!(owner = %self.owner, error = %error, "a client producer link failed");
                        break;
                    }
                },
                command = commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    self.command(command, &items);
                }
                () = tokio::time::sleep_until(last_heard + LINK_SILENCE_LIMIT) => {
                    debug!(owner = %self.owner, "the owning node fell silent on a client producer link");
                    break;
                }
                _ = &mut writer => {
                    debug!(owner = %self.owner, "a client producer link stopped taking frames");
                    break;
                }
            }
        }
        writer.abort();
        self.end(&mut commands);
    }

    /// Ends the link: every open it has not answered is refused, and every producer it routes
    /// ends as lost with its owning node.
    fn end(mut self, commands: &mut mpsc::UnboundedReceiver<LinkCommand>) {
        self.links.forget(
            &self.owner,
            &LinkHandle {
                commands: self.own.clone(),
            },
        );
        commands.close();
        // Opens queued after the registry dropped this link are refused with the rest.
        while let Ok(command) = commands.try_recv() {
            if let LinkCommand::Open { reply, .. } = command {
                reply
                    .send(Err(ClientProducerRefusal::EndpointUnavailable))
                    .means_peer_left("forwarded producer open");
            }
        }
        for (_, reply) in self.pending.drain() {
            reply
                .send(Err(ClientProducerRefusal::EndpointUnavailable))
                .means_peer_left("forwarded producer open");
        }
        for (_, route) in self.routes.drain() {
            route
                .events
                .send(ClientProducerEvent::Ended(
                    ClientProducerEndReason::OwnerLost,
                ))
                .means_peer_left("forwarded producer");
        }
    }

    fn command(
        &mut self,
        command: LinkCommand,
        items: &mpsc::UnboundedSender<ClientProducerLinkItem>,
    ) {
        let item = match command {
            LinkCommand::Open { key, open, reply } => {
                let ProducerOpen {
                    domain,
                    ingestor,
                    expected_fields,
                    limits,
                    max_batch_bytes,
                } = open;
                self.pending.insert(key, reply);
                ClientProducerLinkItem::Open {
                    key,
                    domain,
                    ingestor,
                    expected_fields,
                    limits,
                    max_batch_bytes,
                }
            }
            LinkCommand::Submit {
                key,
                submission,
                batch,
            } => {
                if !self.routes.contains_key(&key) {
                    // The producer ended; that end answers every batch it still had.
                    return;
                }
                ClientProducerLinkItem::Submit {
                    key,
                    submission,
                    batch: batch.to_vec(),
                }
            }
            LinkCommand::Close { key } => {
                if !self.routes.contains_key(&key) {
                    return;
                }
                ClientProducerLinkItem::Close { key }
            }
            LinkCommand::Detach { key } => {
                self.routes.remove(&key);
                ClientProducerLinkItem::Detach { key }
            }
        };
        // A writer that stopped ends the link at the next turn of its loop.
        items
            .send(item)
            .means_shutdown("client producer link writer");
    }

    /// Routes one answer of the owning node to the producer it concerns. A frame about a producer
    /// this side already let go of is dropped: the owning node learns of that from the detach the
    /// release sent.
    fn deliver(&mut self, event: ClientProducerLinkEvent) {
        match event {
            ClientProducerLinkEvent::Opened { key, description } => {
                let Some(reply) = self.pending.remove(&key) else {
                    return;
                };
                let (events, outcomes) = mpsc::unbounded_channel();
                let (admission, admission_receiver) = watch::channel(description.admission);
                let forwarded = ForwardedProducer {
                    key,
                    commands: self.own.clone(),
                    detached: false,
                };
                let opened = OpenedRoute {
                    description,
                    route: ProducerRoute::Forwarded(forwarded),
                    events: ClientProducerEvents {
                        outcomes,
                        admission: admission_receiver,
                    },
                };
                // An opener that left before the answer drops the route with the answer, which
                // detaches the producer.
                if reply.send(Ok(opened)).is_err() {
                    return;
                }
                self.routes
                    .insert(key, ForwardedRoute { events, admission });
            }
            ClientProducerLinkEvent::Refused { key, refusal } => {
                let Some(reply) = self.pending.remove(&key) else {
                    return;
                };
                reply
                    .send(Err(refusal))
                    .means_peer_left("forwarded producer open");
            }
            ClientProducerLinkEvent::Outcome {
                key,
                submission,
                outcome,
                detail,
            } => {
                let Some(route) = self.routes.get(&key) else {
                    return;
                };
                let event = ClientProducerEvent::Outcome {
                    submission: ClientSubmissionId::new(submission),
                    outcome,
                    detail,
                };
                route
                    .events
                    .send(event)
                    .means_peer_left("forwarded producer");
            }
            ClientProducerLinkEvent::Admission { key, admission } => {
                if let Some(route) = self.routes.get(&key) {
                    route.admission.send_replace(admission);
                }
            }
            ClientProducerLinkEvent::Ended { key, reason } => {
                let Some(route) = self.routes.remove(&key) else {
                    return;
                };
                route
                    .events
                    .send(ClientProducerEvent::Ended(reason))
                    .means_peer_left("forwarded producer");
            }
            ClientProducerLinkEvent::Closed { key } => {
                // Dropping the route closes the producer's events behind its last outcome.
                self.routes.remove(&key);
            }
            ClientProducerLinkEvent::Heartbeat => {}
        }
    }
}

/// Sends a serving link's frames in order, with a heartbeat while it has nothing else to send.
async fn write_link(
    mut sender: DuplexSender<OpenClientProducerLink>,
    mut items: mpsc::UnboundedReceiver<ClientProducerLinkItem>,
) {
    let mut heartbeat = tokio::time::interval(LINK_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        nervix_primitives::task::consume_budget().await;
        let item = nervix_primitives::select! {
            biased;
            item = items.recv() => match item {
                Some(item) => item,
                None => break,
            },
            _ = heartbeat.tick() => ClientProducerLinkItem::Heartbeat,
        };
        if let Err(error) = sender.send(item).await {
            debug!(error = %error, "a client producer link refused a frame");
            return;
        }
    }
    sender
        .finish()
        .discarded("a link whose stream already failed has nothing left to half-close");
}

/// Answers the links serving nodes open to attach producers to the client ingestors this node
/// executes.
pub(super) fn serve_owner_links(
    interconnect: &Transport,
    runtime: Runtime,
) -> error_stack::Result<(), HandlerRegistrationError> {
    interconnect.register_duplex_handler::<OpenClientProducerLink, _, _>(
        move |context, request, items| {
            let runtime = runtime.clone();
            async move {
                if context.peer_node_id() != &request.serving_node {
                    return Err(Report::new(StreamHandlerError::new(format!(
                        "node '{}' opened a client producer link for node '{}'",
                        context.peer_node_id(),
                        request.serving_node
                    ))));
                }
                let (answers, answered) = mpsc::channel(OWNER_LINK_ANSWER_CAPACITY);
                let link = OwnerLink {
                    runtime,
                    serving_node: request.serving_node,
                    answers,
                    producers: HashMap::default(),
                };
                nervix_primitives::task::spawn(link.run(items));
                Ok(DuplexResponses::new(ReceiverStream::new(answered).map(Ok)))
            }
        },
    )
}

/// The owning side of one link: the producers it attached for the serving node.
struct OwnerLink {
    runtime: Runtime,
    serving_node: ClusterNodeName,
    answers: mpsc::Sender<ClientProducerLinkEvent>,
    producers: HashMap<u64, ClientProducerHandle>,
}

impl OwnerLink {
    async fn run(mut self, mut items: DuplexItems<ClientProducerLinkItem>) {
        let mut heartbeat = tokio::time::interval(LINK_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_heard = Instant::now();
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                item = items.next() => match item {
                    Ok(Some(ChargedItem { item, charge })) => {
                        last_heard = Instant::now();
                        // The producer's own grant, reserved from this node's budget when it
                        // opened, bounds a batch it holds from here on.
                        drop(charge);
                        if !self.item(item).await {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        debug!(
                            serving_node = %self.serving_node,
                            error = %error,
                            "a client producer link failed"
                        );
                        break;
                    }
                },
                _ = heartbeat.tick() => {
                    // A full queue is traffic the serving node will hear; a heartbeat adds nothing.
                    if let Err(mpsc::error::TrySendError::Closed(_)) =
                        self.answers.try_send(ClientProducerLinkEvent::Heartbeat)
                    {
                        break;
                    }
                }
                () = tokio::time::sleep_until(last_heard + LINK_SILENCE_LIMIT) => {
                    debug!(
                        serving_node = %self.serving_node,
                        "a serving node fell silent on a client producer link"
                    );
                    break;
                }
            }
        }
        // Dropping the handles detaches every producer of the link; admitted work continues.
        let detached = self.producers.len();
        if detached > 0 {
            debug!(
                serving_node = %self.serving_node,
                producers = detached,
                "a client producer link ended and detached its producers"
            );
        }
    }

    /// Serves one frame. `false` means the link can carry no more answers.
    async fn item(&mut self, item: ClientProducerLinkItem) -> bool {
        match item {
            ClientProducerLinkItem::Open {
                key,
                domain,
                ingestor,
                expected_fields,
                limits,
                max_batch_bytes,
            } => {
                let request = ClientProducerOpenRequest {
                    domain,
                    ingestor,
                    expected_fields,
                    limits,
                    max_batch_bytes,
                    retention: ClientProducerRetention::Forwarded,
                };
                let opened = self.runtime.open_client_producer(request).await;
                let OpenedClientProducer {
                    description,
                    handle,
                    events,
                } = match opened {
                    Ok(opened) => opened,
                    Err(refusal) => {
                        return self
                            .answers
                            .send(ClientProducerLinkEvent::Refused { key, refusal })
                            .await
                            .is_ok();
                    }
                };
                let answered = self
                    .answers
                    .send(ClientProducerLinkEvent::Opened { key, description })
                    .await;
                if answered.is_err() {
                    return false;
                }
                nervix_primitives::task::spawn(forward_events(key, events, self.answers.clone()));
                self.producers.insert(key, handle);
            }
            ClientProducerLinkItem::Submit {
                key,
                submission,
                batch,
            } => {
                if let Some(handle) = self.producers.get(&key) {
                    handle.submit(ClientSubmissionId::new(submission), Bytes::from(batch));
                }
            }
            ClientProducerLinkItem::Close { key } => {
                if let Some(handle) = self.producers.remove(&key) {
                    handle.close();
                }
            }
            ClientProducerLinkItem::Detach { key } => {
                self.producers.remove(&key);
            }
            ClientProducerLinkItem::Heartbeat => {}
        }
        true
    }
}

/// Carries one attached producer's events over its link, in the order its endpoint produced them,
/// until the producer ends or is released.
async fn forward_events(
    key: u64,
    events: ClientProducerEvents,
    answers: mpsc::Sender<ClientProducerLinkEvent>,
) {
    let ClientProducerEvents {
        mut outcomes,
        mut admission,
    } = events;
    let mut admission_open = true;
    loop {
        nervix_primitives::task::consume_budget().await;
        let answer = nervix_primitives::select! {
            biased;
            event = outcomes.recv() => match event {
                Some(ClientProducerEvent::Outcome {
                    submission,
                    outcome,
                    detail,
                }) => ClientProducerLinkEvent::Outcome {
                    key,
                    submission: submission.get(),
                    outcome,
                    detail,
                },
                Some(ClientProducerEvent::Ended(reason)) => {
                    answers
                        .send(ClientProducerLinkEvent::Ended { key, reason })
                        .await
                        .means_peer_left("client producer link");
                    return;
                }
                None => {
                    answers
                        .send(ClientProducerLinkEvent::Closed { key })
                        .await
                        .means_peer_left("client producer link");
                    return;
                }
            },
            changed = admission.changed(), if admission_open => {
                if changed.is_err() {
                    admission_open = false;
                    continue;
                }
                let state = *admission.borrow_and_update();
                ClientProducerLinkEvent::Admission { key, admission: state }
            }
        };
        if answers.send(answer).await.is_err() {
            return;
        }
    }
}
