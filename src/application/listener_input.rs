//! Bounded, executor-accounted request input without copying a single data frame.
//!
//! Layer: edges.
//! - **Owns.** Input admission, allocation charges, body assembly and refused transport teardown.
//! - **Depends on.** The executor, selected synchronization, HTTP bodies and byte buffers.
//! - **Must not know.** Routes, schemas, codecs or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns bounded public request input")
)]

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use http_body_util::BodyExt as _;
use hyper::body::{Body as _, Incoming};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{
    AdmissionError, Cancellation, CpuClass, Executor, MemoryClass, Reservation,
};
use nervix_primitives::{
    sync::{Arc, CancellationToken, blocking::Mutex},
    time::{Instant, timeout},
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{listener_capacity::InputRefusal, listener_settings::ListenerSettings};

/// A read window belongs to one backing allocation. No pointer is dereferenced here.
#[derive(Clone, Copy)]
struct ReadAllocation {
    start: usize,
    end: usize,
}
impl ReadAllocation {
    fn len(self) -> usize {
        self.end
            .checked_sub(self.start)
            .assured("a read window is built from its start and capacity")
    }
    fn contains(self, other: Self) -> bool {
        self.start <= other.start && other.end <= self.end
    }
    fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }
    /// The window spanning both, which has no gap when they overlap.
    fn union(self, other: Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}
struct Ledger {
    current: Option<ReadAllocation>,
    active: Option<ActiveInput>,
    disposition: InputDisposition,
}
/// A read window the ledger has not seen whole, and how many of its bytes no earlier charge covers.
struct WindowGrowth {
    window: ReadAllocation,
    uncharged: usize,
}
impl Ledger {
    /// What observing `range` adds, or nothing when the current window already contains it.
    ///
    /// Once no frame holds Hyper's read allocation, Hyper reuses it, rewinding to its start or
    /// extending it in place. A window that overlaps the current one is therefore that same
    /// allocation, or memory released under a charge already made, so only its bytes outside the
    /// current window are new.
    fn grow_window(&self, range: ReadAllocation) -> Option<WindowGrowth> {
        let Some(current) = self.current else {
            return Some(WindowGrowth {
                window: range,
                uncharged: range.len(),
            });
        };
        if current.contains(range) {
            return None;
        }
        if current.overlaps(range) {
            let window = current.union(range);
            let uncharged = window
                .len()
                .checked_sub(current.len())
                .assured("a union spans each window it joins");
            return Some(WindowGrowth { window, uncharged });
        }
        Some(WindowGrowth {
            window: range,
            uncharged: range.len(),
        })
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum InputDisposition {
    Serving,
    Refused,
}
struct ActiveInput {
    reservation: Reservation,
    /// What the request's read allocations take of `reservation`.
    required: u64,
    /// The part of `reservation` held for the copy of a body whose length the request declared,
    /// until the copy takes it.
    body_share: u64,
    refusal: Option<InputRefusal>,
    executor: Executor,
    /// The room a read waits for before it reads into an allocation the charge does not cover
    /// yet. It ends with the request, which gives up its place in the class's queue.
    growth: Option<GrowthWait>,
}

/// A wait for the bytes a newly observed read allocation adds to the request's charge.
type GrowthWait = Pin<Box<dyn Future<Output = Result<Reservation, Report<AdmissionError>>> + Send>>;

/// What one observation of a read allocation decided under the ledger's lock.
enum Observed {
    /// The charge covers the allocation; the read goes ahead.
    Covered,
    /// The read waits for room for the allocation.
    Waiting,
}

/// The foreign I/O contract carries the complete admission report, including its typed cause.
#[derive(Debug, Error)]
#[error("{0:#}")]
struct InputReadReport(Report<InputRefusal>);

/// The connection reader and one HTTP/1 request exchange only allocation descriptions and charge.
/// The mutex is never held across a wait, I/O poll or body-processing operation.
#[derive(Clone)]
pub(super) struct InputAccounting {
    ledger: Arc<Mutex<Ledger>>,
    settings: ListenerSettings,
}
impl InputAccounting {
    pub(super) fn new(settings: ListenerSettings) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(Ledger {
                current: None,
                active: None,
                disposition: InputDisposition::Serving,
            })),
            settings,
        }
    }
    pub(super) fn settings(&self) -> ListenerSettings {
        self.settings
    }
    pub(super) fn mark_refused(&self) {
        self.ledger.lock().disposition = InputDisposition::Refused;
    }
    pub(super) fn was_refused(&self) -> bool {
        self.ledger.lock().disposition == InputDisposition::Refused
    }
    /// Hyper has finished the response and half-closed writes. Discard opaque transport bytes
    /// until the peer closes, shutdown starts or the declared grace expires (RFC 9112 §9.6).
    /// No HTTP message is parsed here, and no discarded bytes enter a payload or decoder.
    pub(super) async fn finish_refusal<IO>(&self, io: IO, shutdown: &CancellationToken)
    where
        IO: AsyncRead + Unpin,
    {
        let transport = super::listener_io::RefusedTransport::new(io, self.settings);
        nervix_primitives::select! {
            () = shutdown.cancelled() => {},
            () = transport.finish() => {},
        }
    }
    /// Admit one request's input: its read scratch, and the copy of a body whose length the
    /// request declared, `body_share` bytes. A full class holds the request back until room frees
    /// or the body's deadline passes; only input larger than the whole class is refused at once.
    async fn begin(
        &self,
        executor: &Executor,
        deadline: &InputDeadline,
        body_share: u64,
    ) -> Result<InputGuard, Report<InputRefusal>> {
        // The first frame may have arrived with the headers. BytesMut can retain its original
        // allocation after splitting; four configured read buffers conservatively cover it.
        let initial = self
            .settings
            .input_scratch_bytes()
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        let charge = initial
            .checked_add(body_share)
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        let reservation = timeout(
            deadline.remaining(),
            executor.reserve(MemoryClass::Intake, charge),
        )
        .await
        .map_err(Report::new)
        .change_context(InputRefusal::MemoryCapacity)?
        .change_context(InputRefusal::MemoryCapacity)?;
        Ok(self.admit(executor, reservation, initial, body_share))
    }

    /// Make `reservation`, which holds `initial` bytes of read scratch and `body_share` bytes for
    /// the body's copy, the request's input charge.
    fn admit(
        &self,
        executor: &Executor,
        reservation: Reservation,
        initial: u64,
        body_share: u64,
    ) -> InputGuard {
        let mut ledger = self.ledger.lock();
        ledger.active = Some(ActiveInput {
            reservation,
            required: initial,
            body_share,
            refusal: None,
            executor: executor.clone(),
            growth: None,
        });
        InputGuard {
            accounting: self.clone(),
        }
    }

    /// Charge the read allocation `range` before a read fills it. An allocation the charge does
    /// not cover yet waits for room in the class, holding the read back, and wakes `context`
    /// once it is granted; only one larger than the whole class is refused.
    fn poll_observe(
        &self,
        range: ReadAllocation,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Report<InputRefusal>>> {
        let mut ledger = self.ledger.lock();
        loop {
            match Self::observe_locked(&mut ledger, range) {
                Ok(Observed::Covered) => return Poll::Ready(Ok(())),
                Ok(Observed::Waiting) => {}
                Err(refusal) => return Poll::Ready(Err(refusal)),
            }
            let Some(mut active) = ledger.active.take() else {
                return Poll::Ready(Ok(()));
            };
            let wait = active
                .growth
                .as_mut()
                .assured("an observation that waits has parked its growth");
            match wait.as_mut().poll(context) {
                Poll::Pending => {
                    ledger.active = Some(active);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(extra)) => {
                    active.growth = None;
                    let reservation = active
                        .reservation
                        .merge(extra)
                        .assured("two charges of one class granted by one executor merge");
                    ledger.active = Some(ActiveInput {
                        reservation,
                        ..active
                    });
                }
                Poll::Ready(Err(refusal)) => {
                    active.growth = None;
                    ledger.active = Some(active);
                    return Poll::Ready(Err(refusal.change_context(InputRefusal::MemoryCapacity)));
                }
            }
        }
    }

    /// Charge `range` from what the request holds or can take now, or park a wait for the bytes
    /// it adds when the class has no room for them now.
    fn observe_locked(
        ledger: &mut Ledger,
        range: ReadAllocation,
    ) -> Result<Observed, Report<InputRefusal>> {
        let Some(growth) = ledger.grow_window(range) else {
            return Ok(Observed::Covered);
        };
        let Some(active) = ledger.active.as_mut() else {
            ledger.current = Some(growth.window);
            return Ok(Observed::Covered);
        };
        if active.growth.is_some() {
            return Ok(Observed::Waiting);
        }
        // Hyper asks for body I/O only when its read buffer is empty. Chunk framing is
        // consumed incrementally, and trailers have their own bounded decoder buffer.
        // The initial scratch covers the backing shared with the request head; each new
        // body backing needs its exposed capacity, without another request-head allowance.
        let uncharged = u64::try_from(growth.uncharged)
            .map_err(|_| Report::new(InputRefusal::MemoryCapacity))?;
        let target = active
            .required
            .checked_add(uncharged)
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        // The share held for the body's copy is not room for read allocations.
        let charged = target
            .checked_add(active.body_share)
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        if charged > active.reservation.bytes() {
            match active.reservation.grow_to(charged) {
                Ok(()) => {}
                Err(refusal)
                    if matches!(
                        refusal.current_context(),
                        AdmissionError::BudgetExhausted { .. }
                    ) =>
                {
                    let extra = charged
                        .checked_sub(active.reservation.bytes())
                        .assured("a charge that could not grow holds less than its target");
                    let executor = active.executor.clone();
                    active.growth = Some(Box::pin(async move {
                        executor.reserve(MemoryClass::Intake, extra).await
                    }));
                    return Ok(Observed::Waiting);
                }
                Err(refusal) => return Err(refusal.change_context(InputRefusal::MemoryCapacity)),
            }
        }
        active.required = target;
        ledger.current = Some(growth.window);
        Ok(Observed::Covered)
    }
    fn refuse_read(&self, error: Report<InputRefusal>) -> io::Error {
        if let Some(active) = self.ledger.lock().active.as_mut() {
            // Only the HTTP status classification is shared out of band. Hyper owns the report.
            active.refusal = Some(*error.current_context());
        }
        io::Error::other(InputReadReport(error))
    }
    fn read_error(&self, error: hyper::Error) -> Report<InputRefusal> {
        let refusal = {
            let mut ledger = self.ledger.lock();
            let pending = match ledger.active.as_mut() {
                Some(active) => active.refusal.take(),
                None => None,
            };
            match pending {
                Some(refusal) => refusal,
                None => InputRefusal::InvalidBody,
            }
        };
        Report::new(error).change_context(refusal)
    }
}
struct InputGuard {
    accounting: InputAccounting,
}
impl InputGuard {
    /// The share admitted for the copy of the body the request declared, once.
    fn take_body_share(&self) -> Option<Reservation> {
        let mut ledger = self.accounting.ledger.lock();
        let mut active = ledger
            .active
            .take()
            .assured("the body share is taken while the request's input is admitted");
        let share = active.body_share;
        if share == 0 {
            ledger.active = Some(active);
            return None;
        }
        let kept = active
            .reservation
            .bytes()
            .checked_sub(share)
            .assured("the input charge holds the body share it was admitted with");
        let (reservation, body) = active
            .reservation
            .split(kept)
            .assured("a charge splits at a size it holds");
        active.body_share = 0;
        ledger.active = Some(ActiveInput {
            reservation,
            ..active
        });
        Some(body)
    }

    fn take(&self) -> Reservation {
        self.accounting
            .ledger
            .lock()
            .active
            .take()
            .assured("collect takes the input charge once, after its frame loop")
            .reservation
    }
}
impl Drop for InputGuard {
    fn drop(&mut self) {
        self.accounting.ledger.lock().active = None;
    }
}

pub(super) struct AccountedBody {
    bytes: Bytes,
    _reservation: Reservation,
}
impl AsRef<[u8]> for AccountedBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AccountedBody {
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// The time one body has to arrive. Waiting for its frames and growing its buffer share it.
struct InputDeadline {
    started: Instant,
    budget: Duration,
}
impl InputDeadline {
    fn start(budget: Duration) -> Self {
        Self {
            started: Instant::now(),
            budget,
        }
    }
    /// What is left of the budget, or nothing once it has passed.
    fn remaining(&self) -> Duration {
        self.budget
            .checked_sub(self.started.elapsed())
            .unwrap_or_default()
    }
}

/// How one request's body buffer is sized and grown, resolved from its listener settings.
#[derive(Clone, Copy)]
struct BodyGrowth {
    /// The body limit, which the buffer never outgrows.
    limit: usize,
    /// The most bytes the connection copies at once: one read buffer, as much as it reads from
    /// the transport at once. Growing a body that already holds more runs on a data worker.
    inline_copy: usize,
    /// The bytes a data worker copies between cancellation checks.
    copy_unit: usize,
}
impl BodyGrowth {
    fn new(settings: ListenerSettings) -> Self {
        Self {
            limit: usize::try_from(settings.http_body_bytes().as_u64())
                .assured("the body-limit setting checks native byte representation"),
            inline_copy: usize::try_from(settings.buffer_bytes().as_u64())
                .assured("the read-buffer setting checks native byte representation"),
            copy_unit: usize::try_from(settings.http_body_copy_bytes().as_u64())
                .assured("the copy-unit setting checks native byte representation"),
        }
    }
    /// The capacity a buffer of `capacity` bytes grows to so it holds `needed`. Doubling keeps the
    /// number of moves logarithmic in the body's size; the limit caps it, and the caller has
    /// already refused a body above the limit, so the result always holds `needed`.
    fn grown(self, capacity: usize, needed: usize) -> usize {
        let doubled = capacity.checked_mul(2).unwrap_or(self.limit);
        doubled.max(needed).min(self.limit)
    }
}

/// A body copied out of the frames it arrived in, charged for its whole allocation.
struct BodyBuffer {
    bytes: Vec<u8>,
    reservation: Reservation,
}
impl BodyBuffer {
    /// Copy the first two data frames into a buffer sized to the declared length, or to twice
    /// what they hold when the request declared none. Each frame is at most one read buffer.
    ///
    /// A declared body's copy uses the `share` admitted with the request. Any other copy waits
    /// for its charge, holding the request's read scratch, until the body's deadline: the bytes
    /// received so far cannot be given back and read again.
    async fn start(
        first: &Bytes,
        second: &Bytes,
        declared: Option<u64>,
        growth: BodyGrowth,
        executor: &Executor,
        deadline: &InputDeadline,
        share: Option<Reservation>,
    ) -> Result<Self, Report<InputRefusal>> {
        let needed = first
            .len()
            .checked_add(second.len())
            .ok_or_else(|| Report::new(InputRefusal::BodyCapacity))?;
        let capacity = match declared {
            Some(declared) => usize::try_from(declared)
                .map_err(|_| Report::new(InputRefusal::BodyCapacity))?
                .max(needed),
            None => growth.grown(needed, needed),
        };
        let charge = u64::try_from(capacity).assured("a native capacity fits a byte count");
        let reservation = match share {
            Some(share) if share.bytes() >= charge => share,
            share => {
                drop(share);
                timeout(
                    deadline.remaining(),
                    executor.reserve(MemoryClass::Intake, charge),
                )
                .await
                .map_err(Report::new)
                .change_context(InputRefusal::MemoryCapacity)?
                .change_context(InputRefusal::MemoryCapacity)?
            }
        };
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(first);
        bytes.extend_from_slice(second);
        Ok(Self { bytes, reservation })
    }

    /// Copy one more data frame into the buffer, after growing it when the frame does not fit.
    async fn append(
        self,
        frame: &Bytes,
        growth: BodyGrowth,
        executor: &Executor,
        deadline: &InputDeadline,
    ) -> Result<Self, Report<InputRefusal>> {
        let needed = self
            .bytes
            .len()
            .checked_add(frame.len())
            .ok_or_else(|| Report::new(InputRefusal::BodyCapacity))?;
        let mut buffer = if needed > self.bytes.capacity() {
            self.grow(needed, growth, executor, deadline).await?
        } else {
            self
        };
        buffer.bytes.extend_from_slice(frame);
        Ok(buffer)
    }

    /// Move the body into an allocation that holds `needed`. Both allocations exist while the
    /// bytes move, so both stay charged until the old one is released. A body of at most one
    /// read buffer moves on the connection; a larger one moves on a data worker, whose charge
    /// follows it until the worker exits.
    ///
    /// A class with no room for the larger allocation holds the body back until room frees or
    /// the body's deadline passes, while the body keeps what it holds: its bytes cannot be read
    /// again. Only a body larger than the whole class is refused at once.
    async fn grow(
        self,
        needed: usize,
        growth: BodyGrowth,
        executor: &Executor,
        deadline: &InputDeadline,
    ) -> Result<Self, Report<InputRefusal>> {
        let Self {
            bytes,
            mut reservation,
        } = self;
        let capacity = growth.grown(bytes.capacity(), needed);
        let held = u64::try_from(bytes.capacity()).assured("a native capacity fits a byte count");
        let grown = u64::try_from(capacity).assured("a native capacity fits a byte count");
        let overlap = held
            .checked_add(grown)
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        if let Err(refusal) = reservation.grow_to(overlap) {
            if !matches!(
                refusal.current_context(),
                AdmissionError::BudgetExhausted { .. }
            ) {
                return Err(refusal.change_context(InputRefusal::MemoryCapacity));
            }
            let extra = overlap
                .checked_sub(reservation.bytes())
                .assured("a charge that could not grow holds less than its target");
            let granted = timeout(
                deadline.remaining(),
                executor.reserve(MemoryClass::Intake, extra),
            )
            .await
            .map_err(Report::new)
            .change_context(InputRefusal::MemoryCapacity)?
            .change_context(InputRefusal::MemoryCapacity)?;
            reservation = reservation
                .merge(granted)
                .change_context(InputRefusal::MemoryCapacity)?;
        }
        if bytes.len() <= growth.inline_copy {
            let mut moved = Vec::with_capacity(capacity);
            moved.extend_from_slice(&bytes);
            drop(bytes);
            return Self::release_moved(moved, reservation);
        }
        let copy_unit = growth.copy_unit;
        let worker = executor.run_cpu(
            CpuClass::Data,
            reservation,
            move |reservation, cancellation| {
                Self::move_on_worker(bytes, capacity, copy_unit, reservation, cancellation)
            },
        );
        timeout(deadline.remaining(), worker)
            .await
            .map_err(Report::new)
            .change_context(InputRefusal::WorkerCapacity)?
            .change_context(InputRefusal::WorkerCapacity)?
    }

    /// Copy the body into a new allocation in `copy_unit` steps, checking for cancellation before
    /// each, then release the old allocation and its charge.
    fn move_on_worker(
        bytes: Vec<u8>,
        capacity: usize,
        copy_unit: usize,
        reservation: Reservation,
        cancellation: &Cancellation,
    ) -> Result<Self, Report<InputRefusal>> {
        let mut moved = Vec::with_capacity(capacity);
        for chunk in bytes.chunks(copy_unit) {
            cancellation
                .check()
                .map_err(Report::new)
                .change_context(InputRefusal::WorkerCapacity)?;
            moved.extend_from_slice(chunk);
        }
        drop(bytes);
        Self::release_moved(moved, reservation)
    }

    /// Keep the charge for the moved body's allocation alone, now that the old one is gone.
    fn release_moved(
        bytes: Vec<u8>,
        reservation: Reservation,
    ) -> Result<Self, Report<InputRefusal>> {
        let capacity =
            u64::try_from(bytes.capacity()).assured("a native capacity fits a byte count");
        let (reservation, released) = reservation
            .split(capacity)
            .change_context(InputRefusal::MemoryCapacity)?;
        drop(released);
        Ok(Self { bytes, reservation })
    }
}

/// The body as far as it has arrived. A single data frame stays the shared buffer it arrived in.
/// From the second one on, every frame is copied into one owned buffer and released at once, so
/// the connection keeps reusing its read buffer rather than pinning one per read, and the charge
/// follows the body's size however the network split it.
enum ArrivedBody {
    Empty,
    Shared(Bytes),
    Copied(BodyBuffer),
}
impl ArrivedBody {
    async fn receive(
        self,
        frame: Bytes,
        declared: Option<u64>,
        growth: BodyGrowth,
        executor: &Executor,
        deadline: &InputDeadline,
        guard: &InputGuard,
    ) -> Result<Self, Report<InputRefusal>> {
        match self {
            Self::Empty => Ok(Self::Shared(frame)),
            Self::Shared(first) => {
                let share = guard.take_body_share();
                let buffer =
                    BodyBuffer::start(&first, &frame, declared, growth, executor, deadline, share)
                        .await?;
                Ok(Self::Copied(buffer))
            }
            Self::Copied(buffer) => {
                let buffer = buffer.append(&frame, growth, executor, deadline).await?;
                Ok(Self::Copied(buffer))
            }
        }
    }

    /// The collected body and the charge it keeps until dispatch ends. A shared frame keeps the
    /// whole input charge, which covers the read buffer it still pins. A copied body keeps the
    /// charge for its own allocation, and the read buffers' charge is returned.
    fn into_body(self, guard: InputGuard) -> AccountedBody {
        match self {
            // A body that needed no copy gives back the share admitted for one.
            Self::Empty => {
                drop(guard.take_body_share());
                AccountedBody {
                    bytes: Bytes::new(),
                    _reservation: guard.take(),
                }
            }
            Self::Shared(bytes) => {
                drop(guard.take_body_share());
                AccountedBody {
                    bytes,
                    _reservation: guard.take(),
                }
            }
            Self::Copied(buffer) => {
                drop(guard);
                AccountedBody {
                    bytes: Bytes::from(buffer.bytes),
                    _reservation: buffer.reservation,
                }
            }
        }
    }
}

pub(super) async fn collect(
    body: &mut Incoming,
    executor: &Executor,
    accounting: &InputAccounting,
) -> Result<AccountedBody, Report<InputRefusal>> {
    let settings = accounting.settings;
    let cap = settings.http_body_bytes().as_u64();
    let hint = body.size_hint();
    if hint.lower() > cap {
        return Err(Report::new(InputRefusal::BodyCapacity));
    }
    // Read before the first frame: the hint shrinks as the body is consumed.
    let declared = hint.exact();
    let deadline = InputDeadline::start(settings.http_body_timeout());
    let growth = BodyGrowth::new(settings);
    // A body whose length the request declared is admitted whole with it, so its copy never
    // waits for room while the request holds its read scratch.
    let guard = accounting
        .begin(executor, &deadline, declared.unwrap_or(0))
        .await?;
    let mut arrived = ArrivedBody::Empty;
    let mut total = 0_u64;
    loop {
        nervix_primitives::task::consume_budget().await;
        let next = timeout(deadline.remaining(), body.frame())
            .await
            .map_err(Report::new)
            .change_context(InputRefusal::BodyTimeout)?;
        let Some(frame) = next else {
            break;
        };
        let frame = frame.map_err(|error| accounting.read_error(error))?;
        let Ok(bytes) = frame.into_data() else {
            continue;
        };
        if bytes.is_empty() {
            continue;
        }
        let length =
            u64::try_from(bytes.len()).assured("a native slice length is at most u64::MAX");
        total = total
            .checked_add(length)
            .ok_or_else(|| Report::new(InputRefusal::BodyCapacity))?;
        if total > cap {
            return Err(Report::new(InputRefusal::BodyCapacity));
        }
        arrived = arrived
            .receive(bytes, declared, growth, executor, &deadline, &guard)
            .await?;
    }
    Ok(arrived.into_body(guard))
}

/// Hyper passes a buffer's spare window to this owner. Admission occurs before reading into a
/// newly observed allocation, and a class without room for it holds the read back until room
/// frees; the constant per-connection scratch bound covers the allocation Hyper made before asking
/// for I/O. Captured ranges contain no data and are never dereferenced.
pub(super) struct AccountedIo<IO> {
    io: IO,
    accounting: InputAccounting,
}
impl<IO> AccountedIo<IO> {
    pub(super) fn new(io: IO, accounting: InputAccounting) -> Self {
        Self { io, accounting }
    }
    pub(super) fn into_inner(self) -> IO {
        self.io
    }
}
impl<IO: AsyncRead + Unpin> AsyncRead for AccountedIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let start = buf.filled().as_ptr().addr();
        let Some(end) = start.checked_add(buf.capacity()) else {
            return Poll::Ready(Err(this
                .accounting
                .refuse_read(Report::new(InputRefusal::MemoryCapacity))));
        };
        match this
            .accounting
            .poll_observe(ReadAllocation { start, end }, cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                return Poll::Ready(Err(this.accounting.refuse_read(error)));
            }
            Poll::Ready(Ok(())) => {}
        }
        Pin::new(&mut this.io).poll_read(cx, buf)
    }
}
impl<IO: AsyncWrite + Unpin> AsyncWrite for AccountedIo<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "loom", feature = "deloxide"))
))]
mod tests {
    use nervix_execution::ExecutionConfig;
    use nervix_recovery::{Discarded as _, NoReceiver as _};

    use super::*;

    /// Admit a request's read scratch from a class with room for it now.
    fn admitted(accounting: &InputAccounting, executor: &Executor) -> InputGuard {
        let initial = accounting
            .settings
            .input_scratch_bytes()
            .assured("the default scratch fits");
        let reservation = executor
            .try_reserve(MemoryClass::Intake, initial)
            .assured("the class has room for the scratch");
        accounting.admit(executor, reservation, initial, 0)
    }

    /// Observe `range` once, as a read would before filling it.
    fn observed(
        accounting: &InputAccounting,
        range: ReadAllocation,
    ) -> Poll<Result<(), Report<InputRefusal>>> {
        let mut context = Context::from_waker(std::task::Waker::noop());
        accounting.poll_observe(range, &mut context)
    }

    /// Observe `range`, which the class has room for now.
    fn observed_now(accounting: &InputAccounting, range: ReadAllocation) {
        let Poll::Ready(observation) = observed(accounting, range) else {
            panic!("an allocation the class has room for is charged at once");
        };
        observation.assured("the allocation is charged");
    }

    #[nervix_primitives::test]
    async fn refusal_teardown_keeps_its_write_eof_readable_while_draining_and_cancels() {
        use std::{pin::pin, task::Waker};

        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let accounting = InputAccounting::new(ListenerSettings::default());
        assert!(!accounting.was_refused());
        accounting.mark_refused();
        assert!(accounting.was_refused());
        let (mut server, mut peer) = tokio::io::duplex(64);
        server
            .write_all(b"refused")
            .await
            .assured("the fixture holds the reply");
        server
            .shutdown()
            .await
            .assured("the reply's write half closes");
        let stop = CancellationToken::new();
        let mut drain = pin!(accounting.finish_refusal(server, &stop));
        let mut context = Context::from_waker(Waker::noop());
        assert!(drain.as_mut().poll(&mut context).is_pending());
        let mut reply = Vec::new();
        timeout(
            std::time::Duration::from_secs(1),
            peer.read_to_end(&mut reply),
        )
        .await
        .assured("the half-closed reply is immediately complete")
        .assured("the reply is readable while the peer's write half stays open");
        assert_eq!(reply, b"refused");
        peer.write_all(b"residual")
            .await
            .assured("the server still accepts transport bytes");
        assert!(drain.as_mut().poll(&mut context).is_pending());
        stop.cancel();
        timeout(std::time::Duration::from_secs(1), drain)
            .await
            .assured("shutdown releases the drain without waiting for its grace deadline");
    }

    #[nervix_primitives::test]
    async fn refusal_teardown_ends_at_its_declared_deadline_without_peer_closure() {
        let settings =
            ListenerSettings::resolve(super::super::listener_settings::ListenerOverrides {
                http_refusal_drain_timeout: Some(std::time::Duration::from_millis(1)),
                ..Default::default()
            })
            .assured("the fixture's positive drain deadline is valid");
        let accounting = InputAccounting::new(settings);
        let (server, _held_peer) = tokio::io::duplex(64);
        timeout(
            std::time::Duration::from_secs(1),
            accounting.finish_refusal(server, &CancellationToken::new()),
        )
        .await
        .assured("the declared drain deadline releases an unclosed peer");
    }

    #[test]
    fn read_allocations_are_charged_once_and_released_on_input_cancellation() {
        let executor = Executor::new(ExecutionConfig::default()).assured("valid executor");
        let accounting = InputAccounting::new(ListenerSettings::default());
        let guard = admitted(&accounting, &executor);
        let scratch = executor.memory(MemoryClass::Intake).reserved_bytes;
        assert!(scratch > 0);
        let charged_beyond_scratch = || {
            executor
                .memory(MemoryClass::Intake)
                .reserved_bytes
                .checked_sub(scratch)
                .assured("observations only add to the scratch")
        };
        let window = |start: usize, end: usize| ReadAllocation { start, end };
        // A fresh allocation's spare window.
        observed_now(&accounting, window(4096, 12288));
        assert_eq!(charged_beyond_scratch(), 8192);
        // The rest of the same window after a read.
        observed_now(&accounting, window(8192, 12288));
        assert_eq!(charged_beyond_scratch(), 8192);
        // The same allocation rewound to its start.
        observed_now(&accounting, window(0, 12288));
        assert_eq!(
            charged_beyond_scratch(),
            12288,
            "a rewound allocation is charged only for its bytes outside the charged window"
        );
        // The same allocation extended in place.
        observed_now(&accounting, window(0, 16384));
        assert_eq!(charged_beyond_scratch(), 16384);
        // Another allocation.
        observed_now(&accounting, window(65536, 73728));
        assert_eq!(charged_beyond_scratch(), 24576);
        drop(guard);
        assert_eq!(executor.memory(MemoryClass::Intake).reserved_bytes, 0);
        assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, 0);
    }

    #[test]
    fn a_body_buffer_doubles_up_to_its_limit() {
        let growth = BodyGrowth {
            limit: 100,
            inline_copy: 8,
            copy_unit: 4,
        };
        assert_eq!(growth.grown(10, 11), 20);
        assert_eq!(
            growth.grown(10, 25),
            25,
            "a frame larger than the doubling is held"
        );
        assert_eq!(growth.grown(60, 61), 100, "growth stops at the body limit");
        assert_eq!(growth.grown(usize::MAX, 100), 100);
    }

    /// Keeps the executor's only data worker busy until released, so a test can tell work that
    /// needs a data worker from work that does not.
    struct OccupiedWorker {
        release: nervix_primitives::sync::blocking::mpsc::Sender<()>,
        holder: nervix_primitives::task::AbortOnDropHandle<()>,
    }
    impl OccupiedWorker {
        async fn start(executor: &Executor) -> Self {
            use nervix_primitives::{
                sync::{blocking::mpsc, oneshot},
                task::{self, AbortOnDropHandle},
            };
            let (release, held) = mpsc::channel::<()>();
            let (ready, started) = oneshot::channel();
            let worker = executor.clone();
            let holder = AbortOnDropHandle::new(task::spawn(async move {
                let reservation = worker
                    .try_reserve(MemoryClass::Intake, 0)
                    .assured("empty worker charge");
                worker
                    .run_cpu(
                        CpuClass::Data,
                        reservation,
                        move |_reservation, _cancellation| {
                            ready.send(()).means_peer_left(
                                "the listener-owner test withdrew its readiness waiter",
                            );
                            // The test keeps its sole data worker occupied until the sender
                            // drops, including during unwinding; no product callback parks this
                            // way.
                            held.recv().discarded(
                                "dropping the test's sender releases its occupied worker, \
                                 including on unwinding",
                            );
                        },
                    )
                    .await
                    .assured("the occupied worker finishes when released");
            }));
            started.await.assured("the data worker is occupied");
            Self { release, holder }
        }
        async fn release(self) {
            drop(self.release);
            self.holder.await.assured("the holder task joins");
        }
    }

    async fn intake_returns_to_zero(executor: &Executor) {
        timeout(std::time::Duration::from_secs(30), async {
            while executor.memory(MemoryClass::Intake).reserved_bytes != 0 {
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .assured("every body charge returns once its owner, or the worker holding it, ends");
    }

    #[nervix_primitives::test]
    async fn a_body_within_one_read_buffer_grows_without_a_data_worker() {
        let executor = crate::runtime::single_worker_executor();
        let occupied = OccupiedWorker::start(&executor).await;
        let growth = BodyGrowth {
            limit: 64,
            inline_copy: 16,
            copy_unit: 1,
        };
        let started = InputDeadline::start(std::time::Duration::from_secs(30));
        let buffer = BodyBuffer::start(
            &Bytes::from_static(b"left"),
            &Bytes::from_static(b"right"),
            None,
            growth,
            &executor,
            &started,
            None,
        )
        .await
        .assured("the first two frames are charged");
        assert_eq!(buffer.bytes.capacity(), 18);
        // A worker would miss this deadline; the move of nine bytes needs none.
        let deadline = InputDeadline::start(std::time::Duration::from_millis(1));
        let buffer = buffer
            .append(
                &Bytes::from_static(b"0123456789"),
                growth,
                &executor,
                &deadline,
            )
            .await
            .assured("a body of at most one read buffer moves on the connection");
        assert_eq!(buffer.bytes, b"leftright0123456789");
        assert_eq!(buffer.bytes.capacity(), 36);
        assert_eq!(
            executor.memory(MemoryClass::Intake).reserved_bytes,
            36,
            "only the moved allocation stays charged"
        );
        drop(buffer);
        occupied.release().await;
        assert_eq!(executor.memory(MemoryClass::Intake).reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn a_growth_deadline_on_a_data_worker_reports_server_capacity_and_releases_the_body() {
        let executor = crate::runtime::single_worker_executor();
        let occupied = OccupiedWorker::start(&executor).await;
        let growth = BodyGrowth {
            limit: 64,
            inline_copy: 4,
            copy_unit: 1,
        };
        let started = InputDeadline::start(std::time::Duration::from_secs(30));
        let buffer = BodyBuffer::start(
            &Bytes::from_static(b"left"),
            &Bytes::from_static(b"right"),
            None,
            growth,
            &executor,
            &started,
            None,
        )
        .await
        .assured("the first two frames are charged");
        let deadline = InputDeadline::start(std::time::Duration::from_millis(1));
        let result = buffer
            .append(
                &Bytes::from_static(b"0123456789"),
                growth,
                &executor,
                &deadline,
            )
            .await;
        let Err(error) = result else {
            panic!("an occupied worker cannot move a body larger than one read buffer");
        };
        assert_eq!(*error.current_context(), InputRefusal::WorkerCapacity);
        assert!(
            error.contains::<nervix_primitives::time::error::Elapsed>(),
            "the queued growth must expire, rather than be refused at queue admission"
        );
        assert_eq!(
            error.current_context().status(),
            hyper::StatusCode::SERVICE_UNAVAILABLE
        );
        occupied.release().await;
        intake_returns_to_zero(&executor).await;
    }

    /// A read allocation the whole class could never hold is refused at once, before any I/O,
    /// and the refusal keeps its typed admission cause.
    #[nervix_primitives::test]
    async fn input_memory_beyond_the_class_refuses_immediately_and_keeps_its_typed_cause() {
        let mut config = ExecutionConfig::default();
        config.budgets.intake = ubyte::ByteUnit::Mebibyte(2);
        let executor = Executor::new(config).assured("positive independent input pool");
        let accounting = InputAccounting::new(ListenerSettings::default());
        let guard = accounting
            .begin(
                &executor,
                &InputDeadline::start(std::time::Duration::from_secs(30)),
                0,
            )
            .await
            .assured("initial read scratch fits");
        let mut stream = AccountedIo::new(tokio::io::empty(), accounting.clone());
        let mut bytes = vec![0; 1048576];
        let mut read = ReadBuf::new(&mut bytes);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let Poll::Ready(Err(error)) = Pin::new(&mut stream).poll_read(&mut context, &mut read)
        else {
            panic!("a new backing the whole class cannot hold is refused before I/O");
        };
        let cause = error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<InputReadReport>())
            .assured("the foreign I/O error owns the complete report");
        assert!(cause.0.contains::<nervix_execution::AdmissionError>());
        assert_eq!(*cause.0.current_context(), InputRefusal::MemoryCapacity);
        assert!(read.filled().is_empty(), "refused input performs no read");
        let ledger = accounting.ledger.lock();
        assert_eq!(
            ledger.active.as_ref().assured("active input").refusal,
            Some(InputRefusal::MemoryCapacity)
        );
        drop(ledger);
        drop(guard);
        assert_eq!(executor.memory(MemoryClass::Intake).reserved_bytes, 0);
    }

    /// A read into an allocation the class has no room for now waits for room rather than
    /// failing the request, and reads once other input releases enough.
    #[nervix_primitives::test]
    async fn a_read_into_a_new_allocation_waits_for_intake_room_and_reads_once_it_frees() {
        let mut config = ExecutionConfig::default();
        config.budgets.intake = ubyte::ByteUnit::Mebibyte(4);
        let executor = Executor::new(config).assured("positive independent input pool");
        let accounting = InputAccounting::new(ListenerSettings::default());
        let guard = admitted(&accounting, &executor);
        let free = executor.memory(MemoryClass::Intake);
        let room = free
            .capacity_bytes
            .checked_sub(free.reserved_bytes)
            .assured("the scratch fits the class");
        // Other input holds all but 64 KiB of the room, which a 1 MiB allocation needs more of.
        let held = executor
            .try_reserve(MemoryClass::Intake, room - 64 * 1024)
            .assured("the rest of the class is free");
        let mut stream = AccountedIo::new(tokio::io::empty(), accounting.clone());
        let mut bytes = vec![0; 1048576];
        let mut read = ReadBuf::new(&mut bytes);
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert!(
            Pin::new(&mut stream)
                .poll_read(&mut context, &mut read)
                .is_pending(),
            "a full class holds the read back instead of refusing it"
        );
        drop(held);
        let read_once_room_frees =
            std::future::poll_fn(|context| Pin::new(&mut stream).poll_read(context, &mut read));
        timeout(std::time::Duration::from_secs(30), read_once_room_frees)
            .await
            .assured("the released room reaches the waiting read")
            .assured("the read goes ahead once its allocation is charged");
        let charged = executor.memory(MemoryClass::Intake).reserved_bytes;
        assert!(
            charged >= 1048576,
            "the read's allocation is charged before it is filled, found {charged}"
        );
        drop(guard);
        assert_eq!(executor.memory(MemoryClass::Intake).reserved_bytes, 0);
    }

    /// A request that ends while its read waits for room gives up its place in the class's
    /// queue and every charge it held.
    #[nervix_primitives::test]
    async fn a_request_ending_while_its_read_waits_gives_up_its_place_and_charge() {
        let mut config = ExecutionConfig::default();
        config.budgets.intake = ubyte::ByteUnit::Mebibyte(4);
        let executor = Executor::new(config).assured("positive independent input pool");
        let accounting = InputAccounting::new(ListenerSettings::default());
        let guard = admitted(&accounting, &executor);
        let free = executor.memory(MemoryClass::Intake);
        let room = free
            .capacity_bytes
            .checked_sub(free.reserved_bytes)
            .assured("the scratch fits the class");
        let held = executor
            .try_reserve(MemoryClass::Intake, room - 64 * 1024)
            .assured("the rest of the class is free");
        let window = ReadAllocation {
            start: 0,
            end: 1048576,
        };
        assert!(
            observed(&accounting, window).is_pending(),
            "the read waits for room"
        );
        drop(guard);
        drop(held);
        let whole = executor.memory(MemoryClass::Intake).capacity_bytes;
        let after = executor
            .try_reserve(MemoryClass::Intake, whole)
            .assured("the ended request left no waiter ahead and held nothing");
        drop(after);
    }
}

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "loom", feature = "deloxide"))
))]
#[path = "listener_input_replay_tests.rs"]
mod replay_tests;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_checks {
    use super::*;

    #[test]
    fn shuttle_read_allocation_racing_input_cancellation_returns_its_entire_charge() {
        nervix_model_harness::shuttle::check_dfs(
            || {
                let executor = Executor::new(nervix_execution::ExecutionConfig::default())
                    .assured("valid executor");
                let accounting = InputAccounting::new(ListenerSettings::default());
                let initial = accounting
                    .settings
                    .input_scratch_bytes()
                    .assured("the default scratch fits");
                let reservation = executor
                    .try_reserve(MemoryClass::Intake, initial)
                    .assured("input admission");
                let guard = accounting.admit(&executor, reservation, initial, 0);
                let reader = accounting.clone();
                let observation = nervix_primitives::thread::spawn(move || {
                    let mut context = Context::from_waker(std::task::Waker::noop());
                    let observed = reader.poll_observe(
                        ReadAllocation {
                            start: 4096,
                            end: 12288,
                        },
                        &mut context,
                    );
                    let Poll::Ready(observed) = observed else {
                        panic!("a bounded observation within the class is charged at once");
                    };
                    observed.assured("bounded observation");
                });
                drop(guard);
                observation.join().assured("the observation completes");
                assert_eq!(executor.memory(MemoryClass::Intake).reserved_bytes, 0);
                assert!(accounting.ledger.lock().active.is_none());
            },
            None,
        );
    }
}
