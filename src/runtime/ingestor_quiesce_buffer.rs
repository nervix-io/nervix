//! The volatile queue that retains one ingestor's payloads across a pause.
//!
//! Layer: data plane.
//! - **Owns.** Retention capacity, queue order, delivery ownership and cancellation of held input.
//! - **Depends on.** The ingestor's quiesce control and endpoint response guards.
//! - **Must not know.** Statement parsing, graph planning or persistence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns one ingestor's bounded pause buffer"),
    nervix::context(
        recurring,
        reason = "the ingestor host retains and drains admitted input"
    )
)]

use std::collections::BTreeMap;

use super::*;

/// One instance's retained payloads, oldest first.
///
/// The oldest payload may be out for delivery. It is then no longer in `payloads`, but it is still
/// retained: it stays counted in `bytes`, overflow cannot evict it, and nothing behind it is handed
/// out until its delivery ends, so payloads drain in the order they arrived.
#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the ingestor task checks its retained quiesce state before each admitted \
                  payload or poll"
    )
)]
pub(super) struct IngestorQuiesceBuffer {
    pending: RetainedIngestQueue,
    /// Every retained payload's bytes, including those of a payload out for delivery.
    pub(super) bytes: usize,
    /// The bytes of the oldest payload while it is out for delivery.
    delivering: Option<usize>,
}

#[derive(Debug)]
enum RetainedIngestQueue {
    Transport(VecDeque<BufferedIngestPayload>),
    Endpoint(Box<EndpointRetainedQueue>),
}

#[derive(Debug, Default)]
pub(super) struct EndpointRetainedQueue {
    pub(super) capacity: Arc<EndpointRequestCapacity>,
    /// Immediate responses use a plain FIFO until the first request that needs a reply.
    immediate: VecDeque<BufferedIngestPayload>,
    /// Any arrivals after that request join the index. The two queues drain in that order.
    indexed: Option<BTreeMap<EndpointPauseId, EndpointRetainedEntry>>,
    last_id: u64,
    pub(super) max_size: Option<usize>,
    pub(super) ended: bool,
}

impl EndpointRetainedQueue {
    fn reserve_id(&mut self) -> EndpointPauseId {
        self.last_id = self
            .last_id
            .checked_add(1)
            .assured("a single intake cannot receive u64::MAX buffered requests");
        EndpointPauseId(self.last_id)
    }

    fn push_immediate(&mut self, payload: BufferedIngestPayload) {
        if self.indexed.is_none() {
            self.immediate.push_back(payload);
            return;
        }
        let id = self.reserve_id();
        self.push_awaited(id, payload, None);
    }

    /// Opens the indexed tail without walking the immediate FIFO.
    pub(super) fn begin_waiting(&mut self) -> EndpointPauseId {
        self.indexed.get_or_insert_with(BTreeMap::new);
        self.reserve_id()
    }

    pub(super) fn push_awaited(
        &mut self,
        id: EndpointPauseId,
        payload: BufferedIngestPayload,
        reply: Option<EndpointPauseReply>,
    ) {
        let queue = self.indexed.as_mut().verified("this request has an index");
        queue.insert(id, EndpointRetainedEntry { payload, reply });
    }

    pub(super) fn remove_waiting(&mut self, id: EndpointPauseId) -> Option<BufferedIngestPayload> {
        self.indexed
            .as_mut()?
            .remove(&id)
            .map(|entry| entry.payload)
    }

    fn pop(&mut self) -> Option<RetainedIngestEntry> {
        if let Some(payload) = self.immediate.pop_front() {
            return Some(RetainedIngestEntry::ImmediateEndpoint(payload));
        }
        let (id, entry) = self.indexed.as_mut()?.pop_first()?;
        Some(RetainedIngestEntry::Endpoint { id, entry })
    }

    fn close_empty_index(&mut self) {
        if self.indexed.as_ref().is_some_and(BTreeMap::is_empty) {
            self.indexed = None;
        }
    }

    fn len(&self) -> usize {
        let indexed_count = match &self.indexed {
            Some(indexed) => indexed.len(),
            None => 0,
        };
        self.immediate
            .len()
            .checked_add(indexed_count)
            .assured("both queues contain payloads this node holds in memory")
    }
}

#[derive(Debug)]
pub(super) struct EndpointRetainedEntry {
    payload: BufferedIngestPayload,
    reply: Option<EndpointPauseReply>,
}

#[derive(Debug)]
pub(super) enum RetainedIngestEntry {
    Transport(BufferedIngestPayload),
    ImmediateEndpoint(BufferedIngestPayload),
    Endpoint {
        id: EndpointPauseId,
        entry: EndpointRetainedEntry,
    },
}

impl RetainedIngestEntry {
    pub(super) fn payload(&self) -> &BufferedIngestPayload {
        match self {
            Self::Transport(payload) | Self::ImmediateEndpoint(payload) => payload,
            Self::Endpoint { entry, .. } => &entry.payload,
        }
    }

    pub(super) fn reply(&self) -> Option<&EndpointPauseReply> {
        match self {
            Self::Transport(_) | Self::ImmediateEndpoint(_) => None,
            Self::Endpoint { entry, .. } => entry.reply.as_ref(),
        }
    }
}

impl IngestorQuiesceBuffer {
    pub(super) fn new(endpoint: bool) -> Self {
        let pending = if endpoint {
            RetainedIngestQueue::Endpoint(Box::default())
        } else {
            RetainedIngestQueue::Transport(VecDeque::new())
        };
        Self {
            pending,
            bytes: 0,
            delivering: None,
        }
    }

    pub(super) fn charge(&self, bytes: usize) -> usize {
        match &self.pending {
            RetainedIngestQueue::Transport(_) => bytes,
            RetainedIngestQueue::Endpoint(_) => bytes.max(1),
        }
    }

    pub(super) fn endpoint(&mut self) -> &mut EndpointRetainedQueue {
        let RetainedIngestQueue::Endpoint(queue) = &mut self.pending else {
            unreachable!("only an endpoint pause policy retains an endpoint response");
        };
        queue
    }

    /// The bytes this buffer may still admit before it reaches `max_size`.
    ///
    /// Admission is expressed as a remaining budget rather than a projected total so that a
    /// payload is never sized against a sum that could leave `usize`. Saturation is the meaning
    /// here: altering an ingestor may lower `max_size` under an already filled buffer, and such a
    /// buffer has no room left until it drains.
    pub(super) fn remaining_capacity(&self, max_size: usize) -> usize {
        nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "lowering the limit below retained bytes leaves zero admission capacity",
            max_size.saturating_sub(self.bytes)
        )
    }

    /// Whether `payload_bytes` would fit within `max_size` once overflow evicted every payload it
    /// may evict, which is every one but a payload out for delivery.
    pub(super) fn fits_after_eviction(&self, payload_bytes: usize, max_size: usize) -> bool {
        let needed = match self.delivering {
            Some(delivering) => self
                .charge(payload_bytes)
                .checked_add(self.charge(delivering))
                .assured("both operands count bytes of payloads this node holds in memory"),
            None => self.charge(payload_bytes),
        };
        needed <= max_size
    }

    pub(super) fn account(&mut self, payload_bytes: usize) {
        self.bytes = self
            .bytes
            .checked_add(self.charge(payload_bytes))
            .assured("both operands count bytes of payloads this node already holds in memory");
    }

    pub(super) fn admit(&mut self, payload: BufferedIngestPayload, payload_bytes: usize) {
        self.account(payload_bytes);
        match &mut self.pending {
            RetainedIngestQueue::Transport(pending) => pending.push_back(payload),
            RetainedIngestQueue::Endpoint(queue) => queue.push_immediate(payload),
        }
    }

    pub(super) fn pop_first(&mut self) -> Option<RetainedIngestEntry> {
        match &mut self.pending {
            RetainedIngestQueue::Transport(pending) => {
                pending.pop_front().map(RetainedIngestEntry::Transport)
            }
            RetainedIngestQueue::Endpoint(queue) => queue.pop(),
        }
    }

    pub(super) fn release(&mut self, payload_bytes: usize) {
        self.bytes = self
            .bytes
            .checked_sub(self.charge(payload_bytes))
            .verified("the released payload's bytes were added when it was admitted");
        if self.delivering.is_none()
            && let RetainedIngestQueue::Endpoint(queue) = &mut self.pending
        {
            queue.close_empty_index();
        }
    }

    /// Takes the oldest payload out for delivery, unless one already is.
    pub(super) fn take_oldest(&mut self) -> Option<RetainedIngestEntry> {
        if self.delivering.is_some() {
            return None;
        }
        let entry = self.pop_first()?;
        self.delivering = Some(entry.payload().byte_len());
        Some(entry)
    }

    /// Ends the delivery of the payload out for delivery with the payload leaving, and answers its
    /// bytes, or `None` when this buffer counts no delivery because the ingestor's termination
    /// replaced the buffer that did.
    pub(super) fn finish_delivery(&mut self) -> Option<usize> {
        let bytes = self.delivering.take()?;
        self.release(bytes);
        Some(bytes)
    }

    /// Ends the delivery of the payload out for delivery by putting `payload` back at the front,
    /// unless this buffer counts no delivery, in which case the payload is discarded with the
    /// buffer that counted it.
    pub(super) fn return_delivery(&mut self, payload: RetainedIngestEntry) {
        if self.delivering.take().is_none() {
            return;
        }
        match payload {
            RetainedIngestEntry::Transport(payload) => {
                let RetainedIngestQueue::Transport(pending) = &mut self.pending else {
                    unreachable!("a transport delivery belongs in its transport queue");
                };
                pending.push_front(payload);
            }
            RetainedIngestEntry::Endpoint { id, entry } => {
                self.endpoint().push_awaited(id, entry.payload, entry.reply);
            }
            RetainedIngestEntry::ImmediateEndpoint(payload) => {
                self.endpoint().immediate.push_front(payload)
            }
        }
    }

    /// Every payload this buffer retains, a payload out for delivery included.
    pub(super) fn retained_records(&self) -> usize {
        let pending = match &self.pending {
            RetainedIngestQueue::Transport(queue) => queue.len(),
            RetainedIngestQueue::Endpoint(queue) => queue.len(),
        };
        pending
            .checked_add(usize::from(self.delivering.is_some()))
            .assured("the retained payloads are held in this node's memory")
    }

    pub(super) fn end(&mut self) {
        self.bytes = 0;
        self.delivering = None;
        match &mut self.pending {
            RetainedIngestQueue::Transport(queue) => queue.clear(),
            RetainedIngestQueue::Endpoint(queue) => {
                queue.ended = true;
                queue.immediate.clear();
                queue.indexed = None;
            }
        }
    }
}

/// How the delivery of a retained payload ended.
#[derive(Debug)]
pub(super) enum DeliveryEnd {
    /// Its messages were accepted, or its codec rejected it: it leaves the buffer.
    Left,
    /// It could not be delivered now: it goes back to the front of the buffer.
    Returned(RetainedIngestEntry),
}

/// What a buffer does with a payload that does not fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetentionOverflow {
    /// Refuse the payload, as an endpoint buffer does: its sender was not answered yet.
    Reject,
    /// Apply the declared `ON OVERFLOW` side.
    Declared(IngestQuiesceOverflow),
}

/// The oldest payload one instance retained, out for delivery.
///
/// The buffer keeps counting the payload, with its bytes, while it is out, and hands out nothing
/// behind it. [`Self::finish`] ends the delivery with the payload leaving the buffer; a delivery
/// dropped before then returns it to the front, so a delivery that was interrupted, refused, or
/// ended with its task leaves the buffer exactly as it was.
#[derive(Debug)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the ingestor task delivers one retained payload on each loop turn while its \
                  buffer drains"
    )
)]
pub(in crate::runtime) struct RetainedDelivery<'control> {
    pub(super) control: &'control IngestorQuiesceControl,
    /// Present until [`Self::finish`] takes it or the drop returns it.
    pub(super) payload: Option<RetainedIngestEntry>,
}

impl RetainedDelivery<'_> {
    pub(in crate::runtime) fn payload(&self) -> &BufferedIngestPayload {
        self.payload
            .as_ref()
            .verified("only finishing or dropping the delivery takes its payload")
            .payload()
    }

    pub(in crate::runtime) fn reply(&self) -> Option<&EndpointPauseReply> {
        self.payload
            .as_ref()
            .verified("the delivery owns its payload")
            .reply()
    }

    pub(in crate::runtime) fn is_endpoint(&self) -> bool {
        matches!(
            self.payload,
            Some(RetainedIngestEntry::ImmediateEndpoint(_) | RetainedIngestEntry::Endpoint { .. })
        )
    }

    /// Runs `delivery`, the work that brings the payload to its ingest group, unless `shutdown`
    /// stops the ingestor or the ingestor is quiesced before it completes. Either ends `delivery`
    /// wherever it waits and answers `None`; the payload stays out until the caller ends its
    /// delivery.
    pub(in crate::runtime) async fn run_until_interrupted<T>(
        &self,
        shutdown: &watch::Receiver<bool>,
        delivery: impl Future<Output = T>,
    ) -> Option<T> {
        // A clone watches the stop, so the receiver the caller keeps, and the acknowledgements it
        // hands on, still see the change.
        let mut stopping = shutdown.clone();
        let abandoned = async {
            match self.reply() {
                Some(reply) => reply.wait_for_abandonment().await,
                None => std::future::pending::<()>().await,
            }
        };
        nervix_primitives::select! {
            biased;
            output = delivery => Some(output),
            _ = stopping.wait_for(|stop| *stop) => None,
            () = self.control.wait_until_quiesced() => None,
            () = abandoned => None,
        }
    }

    /// Ends the delivery with the payload leaving the buffer, and hands it over.
    pub(in crate::runtime) fn finish(mut self) -> BufferedIngestPayload {
        let entry = self.take();
        self.control.end_delivery(DeliveryEnd::Left);
        entry.0
    }

    fn take(&mut self) -> (BufferedIngestPayload, Option<EndpointPauseReply>) {
        let payload = self
            .payload
            .take()
            .verified("only finishing or dropping the delivery takes its payload");
        match payload {
            RetainedIngestEntry::Transport(payload)
            | RetainedIngestEntry::ImmediateEndpoint(payload) => (payload, None),
            RetainedIngestEntry::Endpoint { entry, .. } => (entry.payload, entry.reply),
        }
    }

    pub(in crate::runtime) fn refuse_endpoint(mut self) -> Option<EndpointPauseReply> {
        let (_, reply) = self.take();
        if self.control.end_delivery(DeliveryEnd::Left) {
            self.control.record_dropped(1);
        }
        reply
    }

    pub(in crate::runtime) fn into_endpoint(
        mut self,
        control: &Arc<IngestorQuiesceControl>,
    ) -> OrderedEndpointDelivery {
        let (payload, reply) = self.take();
        OrderedEndpointDelivery {
            payload,
            reply,
            _order: EndpointDeliveryOrder(control.clone()),
        }
    }
}

pub(in crate::runtime) struct OrderedEndpointDelivery {
    pub(in crate::runtime) payload: BufferedIngestPayload,
    pub(in crate::runtime) reply: Option<EndpointPauseReply>,
    _order: EndpointDeliveryOrder,
}

struct EndpointDeliveryOrder(Arc<IngestorQuiesceControl>);

impl Drop for EndpointDeliveryOrder {
    fn drop(&mut self) {
        self.0.end_delivery(DeliveryEnd::Left);
    }
}

impl Drop for RetainedDelivery<'_> {
    fn drop(&mut self) {
        if let Some(payload) = self.payload.take() {
            self.control.end_delivery(DeliveryEnd::Returned(payload));
        }
    }
}
