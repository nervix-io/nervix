//! A sequential reference for the channel's complete observable content.
//!
//! Layer: test harness.
//! - **Owns.** The oracle for modeled loads, FIFO admission and standstill restoration.
//! - **Depends on.** The public channel and Bolero.
//! - **Must not know.** Any production implementation state.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    task::Poll,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::runtime::Builder;

use super::{attached, poll_now, weight};
use crate::{Capture, Channel, Leave, Read, Reader, Restore, SavedEntry, Send, Snapshot};

const INPUT_BYTES: usize = 192;
const CASES: usize = 128;

#[derive(Clone, Copy)]
struct Request {
    item: u16,
    weight: NonZeroU64,
}

struct Stored {
    request: Request,
    unread: BTreeSet<u8>,
}

enum Ticket {
    Waiting,
    Granted,
    Failed(Send<u16>),
}

struct SenderRequest {
    request: Request,
    ticket: Ticket,
}

struct Reference {
    limit: NonZeroU64,
    closed: bool,
    readers: BTreeSet<u8>,
    pending: BTreeMap<u16, SenderRequest>,
    queue: VecDeque<u16>,
    entries: Vec<Stored>,
}

impl Reference {
    fn new() -> Self {
        Self {
            limit: weight(8),
            closed: false,
            readers: BTreeSet::new(),
            pending: BTreeMap::new(),
            queue: VecDeque::new(),
            entries: Vec::new(),
        }
    }

    fn used(&self) -> u64 {
        let buffered = self
            .entries
            .iter()
            .map(|entry| entry.request.weight.get())
            .sum::<u64>();
        let reserved = self
            .pending
            .values()
            .filter(|entry| matches!(entry.ticket, Ticket::Granted))
            .map(|entry| entry.request.weight.get())
            .sum::<u64>();
        buffered
            .checked_add(reserved)
            .assured("the input bounds the test charges")
    }

    fn enqueue(&mut self, request: Request) {
        self.queue.push_back(request.item);
        self.pending.insert(
            request.item,
            SenderRequest {
                request,
                ticket: Ticket::Waiting,
            },
        );
        self.advance();
    }

    fn advance(&mut self) {
        let mut waiting = std::mem::take(&mut self.queue);
        while let Some(id) = waiting.pop_front() {
            let request = self.pending[&id].request;
            if let Some(error) = self.refuse(request) {
                self.pending
                    .get_mut(&id)
                    .assured("the queue owns the request")
                    .ticket = Ticket::Failed(error);
            } else {
                self.queue.push_back(id);
            }
        }
        while let Some(id) = self.queue.front().copied() {
            let request = self.pending[&id].request;
            let total = self
                .used()
                .checked_add(request.weight.get())
                .assured("the property bounds the total weight");
            if total > self.limit.get() {
                break;
            }
            self.queue.pop_front();
            self.pending
                .get_mut(&id)
                .assured("the granted request is recorded")
                .ticket = Ticket::Granted;
        }
    }

    fn refuse(&self, request: Request) -> Option<Send<u16>> {
        if self.closed {
            return Some(Send::Closed(request.item));
        }
        if self.readers.is_empty() {
            return Some(Send::NoReaders(request.item));
        }
        if request.weight > self.limit {
            return Some(Send::Oversized {
                item: request.item,
                weight: request.weight,
                limit: self.limit,
            });
        }
        None
    }

    fn poll(&mut self, id: u16) -> Poll<Send<u16>> {
        if matches!(self.pending[&id].ticket, Ticket::Waiting) {
            return Poll::Pending;
        }
        let entry = self
            .pending
            .remove(&id)
            .assured("the ready request is present");
        match entry.ticket {
            Ticket::Failed(error) => Poll::Ready(error),
            Ticket::Granted if self.closed => {
                self.advance();
                Poll::Ready(Send::Closed(id))
            }
            Ticket::Granted if self.readers.is_empty() => {
                self.advance();
                Poll::Ready(Send::NoReaders(id))
            }
            Ticket::Granted => {
                self.entries.push(Stored {
                    request: entry.request,
                    unread: self.readers.clone(),
                });
                Poll::Ready(Send::Published)
            }
            Ticket::Waiting => unreachable!("a ready request was already selected"),
        }
    }

    fn cancel(&mut self, id: u16) {
        self.pending.remove(&id);
        self.queue.retain(|waiting| *waiting != id);
        self.advance();
    }

    fn read(&mut self, reader: u8) -> Read<u16> {
        for entry in &mut self.entries {
            if entry.unread.remove(&reader) {
                let item = entry.request.item;
                self.retire();
                return Read::Item(item);
            }
        }
        if self.closed {
            Read::Closed
        } else {
            Read::Empty
        }
    }

    fn leave(&mut self, reader: u8) {
        self.readers.remove(&reader);
        for entry in &mut self.entries {
            entry.unread.remove(&reader);
        }
        self.retire();
    }

    fn retire(&mut self) {
        self.entries.retain(|entry| !entry.unread.is_empty());
        self.advance();
    }

    fn snapshot(&self) -> Snapshot<u16, u8> {
        let entries = self
            .entries
            .iter()
            .map(|entry| SavedEntry {
                item: entry.request.item,
                weight: entry.request.weight,
                remaining: NonZeroUsize::new(entry.unread.len())
                    .assured("a retained entry has a reader"),
            })
            .collect();
        let positions = self
            .readers
            .iter()
            .map(|reader| {
                let position = self
                    .entries
                    .iter()
                    .position(|entry| entry.unread.contains(reader));
                (*reader, position.unwrap_or(self.entries.len()))
            })
            .collect();
        Snapshot {
            capacity: self.limit,
            closed: self.closed,
            entries,
            positions,
        }
    }
}

type Sending = Pin<Box<dyn Future<Output = Send<u16>>>>;

struct Check {
    model: Reference,
    channel: Channel<u16, u8>,
    readers: BTreeMap<u8, Reader<u16>>,
    pending: BTreeMap<u16, Sending>,
}

impl Check {
    fn new() -> Self {
        let model = Reference::new();
        let channel = Channel::new(model.limit);
        Self {
            model,
            channel,
            readers: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    async fn send(&mut self, request: Request) {
        self.model.enqueue(request);
        let mut sender = self.channel.sender();
        self.pending.insert(
            request.item,
            Box::pin(async move { sender.send(request.item, request.weight).await }),
        );
        self.poll(request.item).await;
    }

    async fn poll(&mut self, id: u16) {
        let future = self
            .pending
            .get_mut(&id)
            .assured("the request has a real future");
        let observed = poll_now(future.as_mut()).await;
        let expected = self.model.poll(id);
        assert_eq!(observed, expected);
        if observed.is_ready() {
            self.pending.remove(&id);
        }
    }

    fn restart(&mut self) {
        for id in std::mem::take(&mut self.pending).into_keys() {
            self.model.cancel(id);
        }
        self.readers.clear();
        let Capture::Saved(captured) = self.channel.capture() else {
            panic!("the property suspended its handles");
        };
        assert_eq!(captured, self.model.snapshot());
        let Restore::Restored(channel) = Channel::restore(captured) else {
            panic!("every captured state can be restored");
        };
        for key in &self.model.readers {
            self.readers.insert(*key, attached(channel.attach(key)));
        }
        self.channel = channel;
    }

    async fn apply(&mut self, step: usize, kind: u8, arg: u8, operand: u8) {
        let key = arg % 4;
        let pending = self
            .pending
            .keys()
            .nth(usize::from(arg) % self.pending.len().max(1))
            .copied();
        match kind % 16 {
            0..=4 => {
                let item =
                    u16::try_from(step).assured("the property limits the number of operations");
                let weight = weight(u64::from(operand % 13) + 1);
                self.send(Request { item, weight }).await;
            }
            5..=7 => {
                if let Some(reader) = self.readers.get_mut(&key) {
                    assert_eq!(reader.try_read(), self.model.read(key));
                }
            }
            8 => {
                if !self.model.closed && self.model.readers.insert(key) {
                    self.readers
                        .insert(key, attached(self.channel.subscribe(key)));
                }
            }
            9 => {
                if let Some(reader) = self.readers.remove(&key) {
                    drop(reader);
                    self.model.leave(key);
                    assert_eq!(self.channel.unsubscribe(&key).await, Leave::Left);
                }
            }
            10 => {
                let limit = weight(u64::from(operand % 13) + 1);
                self.channel.resize(limit);
                self.model.limit = limit;
                self.model.advance();
            }
            11 => {
                if let Some(id) = pending {
                    self.pending.remove(&id);
                    self.model.cancel(id);
                }
            }
            12 | 13 => {
                if let Some(id) = pending {
                    self.poll(id).await;
                }
            }
            14 => self.restart(),
            15 => {
                self.channel.close();
                self.model.closed = true;
                self.model.advance();
            }
            _ => unreachable!("the input is reduced to a valid operation"),
        }
        assert_eq!(self.channel.capacity(), self.model.limit);
        assert_eq!(self.channel.charged(), self.model.used());
    }
}

#[test]
fn bolero_channel_matches_reference_and_restores_the_rest_of_the_sequence() {
    let runtime = Builder::new_current_thread()
        .build()
        .assured("this sequential property creates one runtime");
    bolero::check!()
        .with_iterations(CASES)
        .with_max_len(INPUT_BYTES)
        .for_each(|bytes: &[u8]| {
            runtime.block_on(async {
                let mut check = Check::new();
                let (operations, _) = bytes.as_chunks::<3>();
                for (step, &[kind, arg, operand]) in operations.iter().enumerate() {
                    check.apply(step, kind, arg, operand).await;
                }
                check.restart();
                assert_eq!(check.channel.charged(), check.model.used());
            });
        });
}
