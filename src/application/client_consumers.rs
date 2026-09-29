//! Routes a client emitter consumer to the node executing its emitter.
//!
//! Layer: edges.
//! - **Owns.** The local attachment or authenticated interconnect stream for one consumer,
//!   forwarding its Arrow attempts and settlement, and detaching it when either peer disappears.
//! - **Depends on.** The node's volatile emitter endpoint and the interconnect duplex contract.
//! - **Must not know.** Session request frames, NSPL text, or graph processing.

use std::{num::NonZeroU64, time::Duration};

use bytes::Bytes;
use futures_util::StreamExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::EmitterOpenRefusal;
use nervix_interconnect::{
    ChargedItem, DuplexItems, DuplexResponses, HandlerRegistrationError, InterconnectDuplexRequest,
    PoolClass, RequestSubquota, StreamHandlerError, Transport,
};
use nervix_models::{
    ClientConsumerLimits, ClusterNodeName, DomainName, EmitterName, RelayName, SchemaField,
    Timestamp,
};
use nervix_primitives::sync::atomic::{AtomicBool, Ordering};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;
use triomphe::Arc;
use uuid::Uuid;

use crate::runtime::{
    ClientEmitterAnswer, ClientEmitterDelivery, ClientEmitterDescription, ClientEmitterRefusal,
    ClientEmitterResponder, Runtime,
};

const HEARTBEAT_EACH: Duration = Duration::from_secs(2);
const PEER_SILENCE: Duration = Duration::from_secs(10);
const OPEN_DEADLINE: Duration = Duration::from_secs(20);
const CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct OpenConsumerStream {
    serving_node: ClusterNodeName,
    domain: DomainName,
    emitter: EmitterName,
    expected_fields: Vec<SchemaField>,
    limits: ClientConsumerLimits,
}

impl InterconnectDuplexRequest for OpenConsumerStream {
    type Item = ConsumerItem;
    type Response = ConsumerEvent;
    const NAME: &'static str = "client_consumer_stream";
    const CLASS: PoolClass = PoolClass::Relay;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
enum ConsumerItem {
    Settle {
        key: u64,
        reference: [u8; 16],
        answer: ClientEmitterAnswer,
    },
    Heartbeat,
    Close,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
enum ConsumerEvent {
    Opened(ClientEmitterDescription),
    Refused(ClientEmitterRefusal),
    DeliveryStart {
        identity: [u8; 16],
        reference: [u8; 16],
        source: RelayName,
        branch_fingerprint: Option<[u8; 32]>,
        members: u32,
        execution_now: Timestamp,
        bytes: u32,
    },
    DeliveryChunk {
        bytes: Vec<u8>,
        last: bool,
    },
    Settled {
        key: u64,
        outcome: Result<(), ClientEmitterRefusal>,
    },
    Ended,
    Heartbeat,
}

pub(in crate::application) struct ConsumerDelivery {
    pub(in crate::application) identity: Uuid,
    pub(in crate::application) reference: Uuid,
    pub(in crate::application) source: RelayName,
    pub(in crate::application) branch_fingerprint: Option<[u8; 32]>,
    pub(in crate::application) body: Bytes,
    pub(in crate::application) members: u32,
    pub(in crate::application) execution_now: Timestamp,
}

pub(in crate::application) struct OpenedConsumerRoute {
    pub(in crate::application) description: ClientEmitterDescription,
    pub(in crate::application) responder: ConsumerResponder,
    pub(in crate::application) deliveries: mpsc::UnboundedReceiver<ConsumerDelivery>,
}

pub(in crate::application) enum ConsumerResponder {
    Local(ClientEmitterResponder),
    Forwarded(ForwardedResponder),
}

impl ConsumerResponder {
    pub(in crate::application) async fn answer(
        &self,
        reference: Uuid,
        answer: ClientEmitterAnswer,
    ) -> Result<(), ClientEmitterRefusal> {
        match self {
            Self::Local(responder) => responder.answer(reference, answer).await,
            Self::Forwarded(responder) => responder.answer(reference, answer).await,
        }
    }

    pub(in crate::application) fn close(&self) {
        match self {
            Self::Local(responder) => responder.close(),
            Self::Forwarded(responder) => responder.close(),
        }
    }
}

#[derive(Clone)]
pub(in crate::application) struct ClientConsumerRouter {
    runtime: Runtime,
    interconnect: Transport,
    local_node: ClusterNodeName,
}

impl ClientConsumerRouter {
    pub(in crate::application) fn new(
        runtime: Runtime,
        interconnect: Transport,
        local_node: ClusterNodeName,
    ) -> Self {
        Self {
            runtime,
            interconnect,
            local_node,
        }
    }

    pub(in crate::application) fn serve_links(
        &self,
        interconnect: &Transport,
    ) -> error_stack::Result<(), HandlerRegistrationError> {
        let runtime = self.runtime.clone();
        interconnect.register_duplex_handler::<OpenConsumerStream, _, _>(
            move |context, request, items| {
                let runtime = runtime.clone();
                async move {
                    if context.peer_node_id() != &request.serving_node {
                        return Err(error_stack::Report::new(StreamHandlerError::new(
                            "consumer stream serving node does not match authenticated peer"
                                .to_string(),
                        )));
                    }
                    let (answers, received) = mpsc::channel(64);
                    tokio::spawn(serve_owner(runtime, request, items, answers));
                    Ok(DuplexResponses::new(ReceiverStream::new(received).map(Ok)))
                }
            },
        )
    }

    pub(in crate::application) async fn open(
        &self,
        owner: &ClusterNodeName,
        domain: DomainName,
        emitter: EmitterName,
        expected_fields: Vec<SchemaField>,
        limits: ClientConsumerLimits,
    ) -> Result<OpenedConsumerRoute, EmitterOpenRefusal> {
        if *owner == self.local_node {
            let (description, consumer) = self
                .runtime
                .open_client_consumer(
                    &domain,
                    &emitter,
                    &expected_fields,
                    limits.batches.get(),
                    limits.bytes,
                    false,
                )
                .await
                .map_err(refusal_for)?;
            let (responder, mut local) = consumer.split();
            let (deliveries, received) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                while let Some(delivery) = local.recv().await {
                    let payload = delivery.payload;
                    let forwarded = ConsumerDelivery {
                        identity: payload.identity,
                        reference: delivery.reference,
                        source: payload.source,
                        branch_fingerprint: payload
                            .branch
                            .map(|branch| *branch.fingerprint().fingerprint()),
                        body: payload.body,
                        members: u32::try_from(payload.members)
                            .assured("client batch maximum is at most 65536"),
                        execution_now: payload.execution_now,
                    };
                    if deliveries.send(forwarded).is_err() {
                        break;
                    }
                }
            });
            return Ok(OpenedConsumerRoute {
                description,
                responder: ConsumerResponder::Local(responder),
                deliveries: received,
            });
        }
        let opening = OpenConsumerStream {
            serving_node: self.local_node.clone(),
            domain,
            emitter,
            expected_fields,
            limits,
        };
        let stream = tokio::time::timeout(
            OPEN_DEADLINE,
            self.interconnect.open_duplex_stream(owner, opening),
        )
        .await
        .map_err(|_| EmitterOpenRefusal::EndpointUnavailable)?
        .map_err(|_| EmitterOpenRefusal::EndpointUnavailable)?;
        let (sender, mut receiver) = stream;
        let first = tokio::time::timeout(OPEN_DEADLINE, receiver.next())
            .await
            .map_err(|_| EmitterOpenRefusal::EndpointUnavailable)?
            .map_err(|_| EmitterOpenRefusal::EndpointUnavailable)?
            .ok_or(EmitterOpenRefusal::EndpointUnavailable)?;
        let description = match first {
            ConsumerEvent::Opened(description) => description,
            ConsumerEvent::Refused(refusal) => return Err(refusal_for(refusal)),
            _ => return Err(EmitterOpenRefusal::EndpointUnavailable),
        };
        let (commands, incoming) = mpsc::unbounded_channel();
        let (deliveries, received) = mpsc::unbounded_channel();
        tokio::spawn(serve_forwarded(
            sender,
            receiver,
            incoming,
            deliveries,
            limits.bytes,
        ));
        let responder = ConsumerResponder::Forwarded(ForwardedResponder {
            inner: Arc::new(ForwardedInner {
                commands,
                closed: AtomicBool::new(false),
            }),
        });
        Ok(OpenedConsumerRoute {
            description,
            responder,
            deliveries: received,
        })
    }
}

fn refusal_for(refusal: ClientEmitterRefusal) -> EmitterOpenRefusal {
    match refusal {
        ClientEmitterRefusal::SchemaMismatch => EmitterOpenRefusal::SchemaMismatch,
        ClientEmitterRefusal::InvalidWindow => EmitterOpenRefusal::InvalidLimits,
        ClientEmitterRefusal::NodeBudgetFull => EmitterOpenRefusal::NodeCapacityExhausted,
        _ => EmitterOpenRefusal::EndpointUnavailable,
    }
}

enum ForwardedCommand {
    Settle {
        reference: Uuid,
        answer: ClientEmitterAnswer,
        reply: oneshot::Sender<Result<(), ClientEmitterRefusal>>,
    },
    Close,
}

pub(in crate::application) struct ForwardedResponder {
    inner: Arc<ForwardedInner>,
}

struct ForwardedInner {
    commands: mpsc::UnboundedSender<ForwardedCommand>,
    closed: AtomicBool,
}

impl Drop for ForwardedInner {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.commands
                .send(ForwardedCommand::Close)
                .means_shutdown("forwarded consumer stream");
        }
    }
}

impl ForwardedResponder {
    async fn answer(
        &self,
        reference: Uuid,
        answer: ClientEmitterAnswer,
    ) -> Result<(), ClientEmitterRefusal> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ClientEmitterRefusal::Closed);
        }
        let (reply, received) = oneshot::channel();
        self.inner
            .commands
            .send(ForwardedCommand::Settle {
                reference,
                answer,
                reply,
            })
            .map_err(|_| ClientEmitterRefusal::Closed)?;
        received.await.unwrap_or(Err(ClientEmitterRefusal::Closed))
    }

    fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            self.inner
                .commands
                .send(ForwardedCommand::Close)
                .means_shutdown("forwarded consumer stream");
        }
    }
}

async fn serve_owner(
    runtime: Runtime,
    request: OpenConsumerStream,
    mut items: DuplexItems<ConsumerItem>,
    answers: mpsc::Sender<ConsumerEvent>,
) {
    let opened = async {
        let grant = runtime
            .client_emitter_budget()
            .try_grant(request.limits.bytes)
            .ok_or(ClientEmitterRefusal::NodeBudgetFull)?;
        let (description, consumer) = runtime
            .open_client_consumer(
                &request.domain,
                &request.emitter,
                &request.expected_fields,
                request.limits.batches.get(),
                request.limits.bytes,
                true,
            )
            .await?;
        Ok::<_, ClientEmitterRefusal>((description, consumer, grant))
    }
    .await;
    let (description, consumer, _grant) = match opened {
        Ok(opened) => opened,
        Err(refusal) => {
            answers
                .send(ConsumerEvent::Refused(refusal))
                .await
                .means_peer_left("consumer stream opener");
            return;
        }
    };
    if answers
        .send(ConsumerEvent::Opened(description))
        .await
        .is_err()
    {
        return;
    }
    let (responder, deliveries) = consumer.split();
    let mut forward = tokio::spawn(forward_deliveries(deliveries, answers.clone()));
    let mut heartbeat = tokio::time::interval(HEARTBEAT_EACH);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_heard = Instant::now();
    loop {
        tokio::select! {
            item = items.next() => match item {
                Ok(Some(ChargedItem { item: ConsumerItem::Settle { key, reference, answer }, .. })) => {
                    last_heard = Instant::now();
                    let outcome = responder.answer(Uuid::from_bytes(reference), answer).await;
                    if answers.send(ConsumerEvent::Settled { key, outcome }).await.is_err() { break; }
                }
                Ok(Some(ChargedItem { item: ConsumerItem::Heartbeat, .. })) => last_heard = Instant::now(),
                Ok(Some(ChargedItem { item: ConsumerItem::Close, .. })) => break,
                _ => break,
            },
            _ = heartbeat.tick() => {
                if Instant::now().duration_since(last_heard) >= PEER_SILENCE
                    || answers.send(ConsumerEvent::Heartbeat).await.is_err() { break; }
            }
            _ = &mut forward => break,
        }
    }
    forward.abort();
    responder.close();
}

async fn forward_deliveries(
    mut deliveries: mpsc::UnboundedReceiver<ClientEmitterDelivery>,
    answers: mpsc::Sender<ConsumerEvent>,
) {
    while let Some(delivery) = deliveries.recv().await {
        let payload = delivery.payload;
        let Ok(bytes) = u32::try_from(payload.body.len()) else {
            break;
        };
        let event = ConsumerEvent::DeliveryStart {
            identity: *payload.identity.as_bytes(),
            reference: *delivery.reference.as_bytes(),
            source: payload.source,
            branch_fingerprint: payload
                .branch
                .map(|branch| *branch.fingerprint().fingerprint()),
            members: u32::try_from(payload.members)
                .assured("client batch maximum is at most 65536"),
            execution_now: payload.execution_now,
            bytes,
        };
        if answers.send(event).await.is_err() {
            return;
        }
        let chunks = payload.body.chunks(CHUNK_BYTES).collect::<Vec<_>>();
        for (index, chunk) in chunks.iter().enumerate() {
            if answers
                .send(ConsumerEvent::DeliveryChunk {
                    bytes: chunk.to_vec(),
                    last: index + 1 == chunks.len(),
                })
                .await
                .is_err()
            {
                return;
            }
        }
    }
    answers
        .send(ConsumerEvent::Ended)
        .await
        .means_peer_left("consumer stream receiver");
}

struct Assembly {
    identity: Uuid,
    reference: Uuid,
    source: RelayName,
    branch_fingerprint: Option<[u8; 32]>,
    members: u32,
    execution_now: Timestamp,
    expected: usize,
    bytes: Vec<u8>,
}

async fn serve_forwarded(
    sender: nervix_interconnect::DuplexSender<OpenConsumerStream>,
    mut receiver: nervix_interconnect::DuplexReceiver<OpenConsumerStream>,
    mut commands: mpsc::UnboundedReceiver<ForwardedCommand>,
    deliveries: mpsc::UnboundedSender<ConsumerDelivery>,
    grant: NonZeroU64,
) {
    let (outbound, mut queued) = mpsc::channel::<ConsumerItem>(64);
    let mut writer = tokio::spawn(async move {
        let mut sender = sender;
        while let Some(item) = queued.recv().await {
            if sender.send(item).await.is_err() {
                break;
            }
        }
    });
    let mut pending =
        ahash::HashMap::<u64, oneshot::Sender<Result<(), ClientEmitterRefusal>>>::default();
    let mut next_key = 1_u64;
    let mut assembly: Option<Assembly> = None;
    let mut heartbeat = tokio::time::interval(HEARTBEAT_EACH);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_heard = Instant::now();
    loop {
        tokio::select! {
            event = receiver.next() => {
                let Ok(Some(event)) = event else { break; };
                last_heard = Instant::now();
                match event {
                    ConsumerEvent::DeliveryStart { identity, reference, source, branch_fingerprint,
                        members, execution_now, bytes } => {
                        if assembly.is_some() || bytes == 0 || u64::from(bytes) > grant.get() { break; }
                        assembly = Some(Assembly {
                            identity: Uuid::from_bytes(identity), reference: Uuid::from_bytes(reference),
                            source, branch_fingerprint, members, execution_now,
                            expected: usize::try_from(bytes)
                                .assured("a u32 IPC payload length fits in server usize"),
                            bytes: Vec::with_capacity(usize::try_from(bytes)
                                .assured("a u32 IPC payload length fits in server usize")),
                        });
                    }
                    ConsumerEvent::DeliveryChunk { bytes, last } => {
                        let Some(current) = assembly.as_mut() else { break; };
                        if bytes.is_empty() || bytes.len() > CHUNK_BYTES
                            || current.bytes.len().checked_add(bytes.len())
                                .is_none_or(|total| total > current.expected)
                        { break; }
                        current.bytes.extend_from_slice(&bytes);
                        if last {
                            let Some(current) = assembly.take() else { break; };
                            if current.bytes.len() != current.expected { break; }
                            if deliveries.send(ConsumerDelivery {
                                identity: current.identity, reference: current.reference,
                                source: current.source, branch_fingerprint: current.branch_fingerprint,
                                body: Bytes::from(current.bytes), members: current.members,
                                execution_now: current.execution_now,
                            }).is_err() { break; }
                        }
                    }
                    ConsumerEvent::Settled { key, outcome } => {
                        if let Some(reply) = pending.remove(&key) {
                            reply.send(outcome).means_peer_left("forwarded settlement requester");
                        }
                    }
                    ConsumerEvent::Ended => break,
                    ConsumerEvent::Heartbeat => {},
                    ConsumerEvent::Opened(_) | ConsumerEvent::Refused(_) => break,
                }
            }
            command = commands.recv() => match command {
                Some(ForwardedCommand::Settle { reference, answer, reply }) => {
                    let key = next_key;
                    next_key = next_key.checked_add(1)
                        .verified("one consumer cannot settle u64::MAX batches in one stream");
                    if outbound.try_send(ConsumerItem::Settle {
                        key, reference: *reference.as_bytes(), answer,
                    }).is_err() {
                        reply.send(Err(ClientEmitterRefusal::Closed))
                            .means_peer_left("forwarded settlement requester");
                        break;
                    }
                    pending.insert(key, reply);
                }
                Some(ForwardedCommand::Close) | None => {
                    outbound.try_send(ConsumerItem::Close)
                        .discarded("the stream is closing even if its close frame cannot be queued");
                    break;
                }
            },
            _ = heartbeat.tick() => {
                if Instant::now().duration_since(last_heard) >= PEER_SILENCE
                    || outbound.is_closed() { break; }
                outbound.try_send(ConsumerItem::Heartbeat)
                    .discarded("a missed best-effort heartbeat is detected by peer silence");
            }
            _ = &mut writer => break,
        }
    }
    writer.abort();
    // Dropping the delivery sender ends the session's reads. The owner detaches the consumer and
    // reassigns every unsettled attempt when this duplex stream ends.
}
