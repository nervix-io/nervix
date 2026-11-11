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
    collections::VecDeque,
    io,
    mem::size_of,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use http_body_util::BodyExt as _;
use hyper::body::{Body as _, Incoming};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{CpuClass, Executor, MemoryClass, Reservation};
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
    fn contains(self, other: Self) -> bool {
        self.start <= other.start && other.end <= self.end
    }
}
struct Ledger {
    current: Option<ReadAllocation>,
    active: Option<ActiveInput>,
    disposition: InputDisposition,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum InputDisposition {
    Serving,
    Refused,
}
struct ActiveInput {
    reservation: Reservation,
    required: u64,
    refusal: Option<InputRefusal>,
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
    fn begin(&self, executor: &Executor) -> Result<InputGuard, Report<InputRefusal>> {
        // The first frame may have arrived with the headers. BytesMut can retain its original
        // allocation after splitting; four configured read buffers conservatively cover it.
        let initial = self
            .settings
            .input_scratch_bytes()
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        let reservation = executor
            .try_reserve(MemoryClass::Intake, initial)
            .change_context(InputRefusal::MemoryCapacity)?;
        let mut ledger = self.ledger.lock();
        ledger.active = Some(ActiveInput {
            reservation,
            required: initial,
            refusal: None,
        });
        Ok(InputGuard {
            accounting: self.clone(),
        })
    }
    fn observe(&self, range: ReadAllocation, capacity: usize) -> Result<(), Report<InputRefusal>> {
        let mut ledger = self.ledger.lock();
        if ledger
            .current
            .is_some_and(|current| current.contains(range))
        {
            return Ok(());
        }
        if let Some(active) = ledger.active.as_mut() {
            // Hyper asks for body I/O only when its read buffer is empty. Chunk framing is
            // consumed incrementally, and trailers have their own bounded decoder buffer.
            // The initial scratch covers the backing shared with the request head; each new
            // body backing needs its exposed capacity, without another request-head allowance.
            let capacity =
                u64::try_from(capacity).map_err(|_| Report::new(InputRefusal::MemoryCapacity))?;
            let target = active
                .required
                .checked_add(capacity)
                .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
            active
                .reservation
                .grow_to(target)
                .change_context(InputRefusal::MemoryCapacity)?;
            active.required = target;
        }
        ledger.current = Some(range);
        Ok(())
    }
    fn charge_frame(&self) -> Result<(), Report<InputRefusal>> {
        let mut ledger = self.ledger.lock();
        let active = ledger
            .active
            .as_mut()
            .assured("receive_input retains its InputGuard throughout the frame loop");
        let metadata =
            u64::try_from(2 * size_of::<Bytes>()).assured("byte-buffer metadata fits a byte count");
        let total = active
            .required
            .checked_add(metadata)
            .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
        active
            .reservation
            .grow_to(total)
            .change_context(InputRefusal::MemoryCapacity)?;
        active.required = total;
        Ok(())
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
    fn take(&self) -> Reservation {
        self.accounting
            .ledger
            .lock()
            .active
            .take()
            .assured("receive_input takes its charge once after its frame loop")
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

struct ReceivedBody {
    frames: VecDeque<Bytes>,
    total: u64,
    reservation: Reservation,
}

pub(super) async fn collect(
    body: &mut Incoming,
    executor: &Executor,
    accounting: &InputAccounting,
) -> Result<AccountedBody, Report<InputRefusal>> {
    let started = Instant::now();
    let budget = accounting.settings.http_body_timeout();
    let received = timeout(budget, receive_input(body, executor, accounting))
        .await
        .map_err(Report::new)
        .change_context(InputRefusal::BodyTimeout)??;
    let remaining = budget.checked_sub(started.elapsed()).unwrap_or_default();
    let copy_unit = usize::try_from(accounting.settings.http_body_copy_bytes().as_u64())
        .assured("the copy-unit setting checks native byte representation");
    finish_input(received, executor, copy_unit, remaining).await
}
async fn receive_input(
    body: &mut Incoming,
    executor: &Executor,
    accounting: &InputAccounting,
) -> Result<ReceivedBody, Report<InputRefusal>> {
    let cap = accounting.settings.http_body_bytes().as_u64();
    if body.size_hint().lower() > cap {
        return Err(Report::new(InputRefusal::BodyCapacity));
    }
    let guard = accounting.begin(executor)?;
    let mut frames = VecDeque::new();
    let mut total = 0_u64;
    while let Some(frame) = body.frame().await {
        nervix_primitives::task::consume_budget().await;
        let frame = frame.map_err(|error| accounting.read_error(error))?;
        let Ok(bytes) = frame.into_data() else {
            continue;
        };
        if bytes.is_empty() {
            continue;
        }
        let length =
            u64::try_from(bytes.len()).assured("a native slice length is at most u64::MAX");
        let next_total = total
            .checked_add(length)
            .ok_or_else(|| Report::new(InputRefusal::BodyCapacity))?;
        if next_total > cap {
            return Err(Report::new(InputRefusal::BodyCapacity));
        }
        total = next_total;
        accounting.charge_frame()?;
        frames.push_back(bytes);
    }
    Ok(ReceivedBody {
        frames,
        total,
        reservation: guard.take(),
    })
}
async fn finish_input(
    received: ReceivedBody,
    executor: &Executor,
    copy_unit: usize,
    remaining: std::time::Duration,
) -> Result<AccountedBody, Report<InputRefusal>> {
    timeout(remaining, assemble_input(received, executor, copy_unit))
        .await
        .map_err(Report::new)
        .change_context(InputRefusal::WorkerCapacity)?
}
async fn assemble_input(
    received: ReceivedBody,
    executor: &Executor,
    copy_unit: usize,
) -> Result<AccountedBody, Report<InputRefusal>> {
    let ReceivedBody {
        mut frames,
        total,
        mut reservation,
    } = received;
    if frames.len() <= 1 {
        return Ok(AccountedBody {
            bytes: frames.pop_front().unwrap_or_default(),
            _reservation: reservation,
        });
    }
    let charge = reservation
        .bytes()
        .checked_add(total)
        .ok_or_else(|| Report::new(InputRefusal::MemoryCapacity))?;
    reservation
        .grow_to(charge)
        .change_context(InputRefusal::MemoryCapacity)?;
    executor
        .run_cpu(
            CpuClass::Data,
            reservation,
            move |reservation, cancellation| {
                let mut bytes = Vec::with_capacity(
                    usize::try_from(total)
                        .assured("receive_input caps total at the validated native body limit"),
                );
                for frame in frames {
                    for chunk in frame.chunks(copy_unit) {
                        cancellation
                            .check()
                            .map_err(Report::new)
                            .change_context(InputRefusal::WorkerCapacity)?;
                        bytes.extend_from_slice(chunk);
                    }
                }
                let (retained, scratch) = reservation
                    .split(total)
                    .change_context(InputRefusal::MemoryCapacity)?;
                drop(scratch);
                Ok(AccountedBody {
                    bytes: Bytes::from(bytes),
                    _reservation: retained,
                })
            },
        )
        .await
        .change_context(InputRefusal::WorkerCapacity)?
}

/// Hyper passes a buffer's spare window to this owner. Admission occurs before reading into a
/// newly observed allocation; the constant per-connection scratch bound covers the allocation
/// Hyper made before asking for I/O. Captured ranges contain no data and are never dereferenced.
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
        if let Err(error) = this
            .accounting
            .observe(ReadAllocation { start, end }, buf.capacity())
        {
            return Poll::Ready(Err(this.accounting.refuse_read(error)));
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
    fn retained_read_allocations_and_metadata_are_released_on_input_cancellation() {
        let executor = Executor::new(ExecutionConfig::default()).assured("valid executor");
        let accounting = InputAccounting::new(ListenerSettings::default());
        let guard = accounting.begin(&executor).assured("input admission");
        let first = executor.memory(MemoryClass::Intake).reserved_bytes;
        assert!(first > 0);
        accounting
            .observe(
                ReadAllocation {
                    start: 4096,
                    end: 12288,
                },
                8192,
            )
            .assured("first backing");
        let allocated = executor.memory(MemoryClass::Intake).reserved_bytes;
        assert!(allocated > first);
        accounting
            .observe(
                ReadAllocation {
                    start: 8192,
                    end: 12288,
                },
                4096,
            )
            .assured("same backing");
        assert_eq!(
            executor.memory(MemoryClass::Intake).reserved_bytes,
            allocated
        );
        for _ in 0..128 {
            accounting.charge_frame().assured("retained frame metadata");
        }
        assert!(executor.memory(MemoryClass::Intake).reserved_bytes > allocated);
        drop(guard);
        assert_eq!(executor.memory(MemoryClass::Intake).reserved_bytes, 0);
        assert_eq!(executor.memory(MemoryClass::Relay).reserved_bytes, 0);
    }

    #[nervix_primitives::test]
    async fn an_assembly_deadline_reports_server_capacity_and_releases_the_queued_body() {
        use std::time::Duration;

        use nervix_primitives::{
            sync::{blocking::mpsc, oneshot},
            task::{self, AbortOnDropHandle},
        };
        let executor = crate::runtime::single_worker_executor();
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
                        // The test keeps its sole data worker occupied until the sender drops,
                        // including during unwinding; no product callback parks this way.
                        held.recv().discarded(
                            "dropping the test's sender releases its occupied worker, including \
                             on unwinding",
                        );
                    },
                )
                .await
                .assured("the occupied worker finishes when released");
        }));
        started.await.assured("the data worker is occupied");
        let received = ReceivedBody {
            frames: [Bytes::from_static(b"left"), Bytes::from_static(b"right")].into(),
            total: 9,
            reservation: executor
                .try_reserve(MemoryClass::Intake, 4096)
                .assured("body charge"),
        };
        let result = finish_input(received, &executor, 1, Duration::from_millis(1)).await;
        let Err(error) = result else {
            panic!("an occupied worker cannot assemble the queued body");
        };
        assert_eq!(*error.current_context(), InputRefusal::WorkerCapacity);
        assert!(
            error.contains::<nervix_primitives::time::error::Elapsed>(),
            "the queued assembly must expire, rather than be refused at queue admission"
        );
        assert_eq!(
            error.current_context().status(),
            hyper::StatusCode::SERVICE_UNAVAILABLE
        );
        drop(release);
        holder.await.assured("the holder task joins");
        timeout(Duration::from_secs(30), async {
            while executor.memory(MemoryClass::Intake).reserved_bytes != 0 {
                task::yield_now().await;
            }
        })
        .await
        .assured("cancellation releases the queued body after the worker returns");
    }

    #[test]
    fn input_memory_refuses_immediately_and_keeps_its_typed_admission_cause() {
        let mut config = ExecutionConfig::default();
        config.budgets.intake = ubyte::ByteUnit::Mebibyte(2);
        let executor = Executor::new(config).assured("positive independent input pool");
        let accounting = InputAccounting::new(ListenerSettings::default());
        let guard = accounting
            .begin(&executor)
            .assured("initial read scratch fits");
        let mut stream = AccountedIo::new(tokio::io::empty(), accounting.clone());
        let mut bytes = vec![0; 1048576];
        let mut read = ReadBuf::new(&mut bytes);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let Poll::Ready(Err(error)) = Pin::new(&mut stream).poll_read(&mut context, &mut read)
        else {
            panic!("a new backing exceeds remaining capacity before I/O");
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
}

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
                let guard = accounting.begin(&executor).assured("input admission");
                let reader = accounting.clone();
                let observation = nervix_primitives::thread::spawn(move || {
                    reader
                        .observe(
                            ReadAllocation {
                                start: 4096,
                                end: 12288,
                            },
                            8192,
                        )
                        .assured("bounded observation");
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
