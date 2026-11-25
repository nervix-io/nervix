//! A session a host holds, and the commands it prepares.
//!
//! - **Owns.** The Tokio runtime a session runs on, opening and ending the session, the byte
//!   bounds, stream counts and queue capacities a host passes for it, sharing it with the
//!   producers, consumers and deliveries opened on it, running a blocking call on the host's
//!   thread, preparing and executing commands, waiting for subscription and domain clock events
//!   and for server notices, and reading the clock of a domain the session follows.
//! - **Depends on.** The Rust session client, cancellation, and the outcomes, events, clock
//!   events, notices and domain clocks it returns.
//! - **Must not know.** Which host is calling, or how it schedules its threads.
//!
//! A host thread blocks in [`SessionRuntime::block_on`] while the runtime's own threads drive the
//! exchange, so no host code ever runs on a runtime thread and no runtime thread ever waits for a
//! host. The session ends when the host has released it and every producer, consumer and
//! delivery opened on it, in whichever order, because each of them keeps the runtime its own
//! calls block on.

use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
};

use meticulous::OptionExt as _;
use nervix_client_core::{
    AutocompleteOutcome, Client, ConnectOptions, DomainName, ExecutionHandle,
};
use nervix_primitives::{sync::Arc, unmodeled::runtime::Runtime};

use crate::{
    cancel::Cancel,
    clock_event::ClockEvent,
    domain_clock::DomainClock,
    event::Event,
    failure::{Failure, FailureKind},
    notice::Notice,
    outcome::Outcome,
};

/// The threads the runtime of one session drives its exchange with. The exchange is one stream
/// and its reader, so a second thread only keeps a busy decoder from delaying a timer.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "Client Host Bindings gives each connected session a native runtime of two \
                  worker threads, and a blocking call runs its future on the host's own thread"
    )
)]
const RUNTIME_THREADS: usize = 2;

/// An open session, as the host holds it.
#[derive(Clone)]
pub struct Session {
    shared: Arc<SessionRuntime>,
}

/// The runtime a session runs on and the client it drives, shared by the session and by every
/// producer, consumer and delivery opened on it.
pub(crate) struct SessionRuntime {
    /// Taken only when the last holder drops the session.
    runtime: Option<Runtime>,
    /// Taken only when the last holder drops the session.
    client: Option<Client>,
}

/// One command, with the durable execution identity it keeps across every attempt.
#[derive(Debug)]
pub struct Execution {
    handle: ExecutionHandle,
}

impl Execution {
    pub fn reference(&self) -> &str {
        self.handle.reference().as_str()
    }
}

impl Session {
    /// A byte bound a host passed, a receive window, header list or send buffer, as a session's
    /// options take it. A bound no `u32` holds, zero included, is refused here as `argument`; one
    /// outside the option's own bounds is refused when the session connects, as every other
    /// option is.
    pub fn byte_bound(
        argument: &'static str,
        bytes: impl TryInto<u32>,
    ) -> Result<NonZeroU32, Failure> {
        let bound = match bytes.try_into() {
            Ok(bytes) => NonZeroU32::new(bytes),
            Err(_) => None,
        };
        let Some(bound) = bound else {
            return Err(Failure::invalid_argument(
                argument,
                "must be a positive number of bytes that fits 32 bits",
            ));
        };
        Ok(bound)
    }

    /// A count of HTTP/2 streams a host passed, as a session's options take it. A count no `u32`
    /// holds, zero included, is refused here as `argument`.
    pub fn stream_count(
        argument: &'static str,
        streams: impl TryInto<u32>,
    ) -> Result<NonZeroU32, Failure> {
        let count = match streams.try_into() {
            Ok(streams) => NonZeroU32::new(streams),
            Err(_) => None,
        };
        let Some(count) = count else {
            return Err(Failure::invalid_argument(
                argument,
                "must be a positive number of streams that fits 32 bits",
            ));
        };
        Ok(count)
    }

    /// A queue capacity a host passed, as a session's options take it. A capacity no `usize`
    /// holds, zero included, is refused here as `argument`; one beyond the native channel limit is
    /// refused when the session connects.
    pub fn queue_capacity(
        argument: &'static str,
        capacity: impl TryInto<usize>,
    ) -> Result<NonZeroUsize, Failure> {
        let queue = match capacity.try_into() {
            Ok(capacity) => NonZeroUsize::new(capacity),
            Err(_) => None,
        };
        let Some(queue) = queue else {
            return Err(Failure::invalid_argument(
                argument,
                "must be a positive capacity this platform can address",
            ));
        };
        Ok(queue)
    }

    /// Opens a session on `server` with the Rust client's `options`, blocking the calling thread
    /// until it is open.
    pub fn connect(
        server: &str,
        domain: Option<&str>,
        options: ConnectOptions,
        cancel: Option<&Cancel>,
    ) -> Result<Self, Failure> {
        let domain = match domain {
            Some(domain) => match DomainName::try_from(domain) {
                Ok(domain) => Some(domain),
                Err(error) => {
                    return Err(Failure::invalid_argument("domain", &error.to_string()));
                }
            },
            None => None,
        };
        let runtime = nervix_primitives::unmodeled::runtime::Builder::new_multi_thread()
            .worker_threads(RUNTIME_THREADS)
            .thread_name("nervix-client")
            .enable_all()
            .build()
            .map_err(|error| {
                Failure::new(
                    FailureKind::Connect,
                    format!("failed to start the session runtime: {error}"),
                )
            })?;
        let connecting = async {
            match Client::connect_with_options(server, domain, options).await {
                Ok(client) => Ok(client),
                Err(error) => Err(Failure::from(error)),
            }
        };
        let client = SessionRuntime::run(&runtime, cancel, connecting)?;
        let shared = SessionRuntime {
            runtime: Some(runtime),
            client: Some(client),
        };
        Ok(Self {
            shared: Arc::new(shared),
        })
    }

    /// The runtime and client this session shares with what is opened on it.
    pub(crate) fn shared(&self) -> &Arc<SessionRuntime> {
        &self.shared
    }

    fn client(&self) -> &Client {
        self.shared.client()
    }

    fn block_on<T>(
        &self,
        cancel: Option<&Cancel>,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        self.shared.block_on(cancel, work)
    }

    /// Captures `query` and a fresh reference before anything is sent. Session-local requests do
    /// not use it as a durable execution identity.
    pub fn prepare(&self, query: &str, cancel: Option<&Cancel>) -> Result<Execution, Failure> {
        let preparing = async {
            let handle = self.client().prepare_execution(query).await;
            Ok(Execution { handle })
        };
        self.block_on(cancel, preparing)
    }

    /// Runs a prepared statement. A durable command whose outcome remains open names its
    /// execution reference, and running the same execution again recovers that outcome. Repeating
    /// a session-local request may be refused after it took effect.
    pub fn execute(
        &self,
        execution: &Execution,
        cancel: Option<&Cancel>,
    ) -> Result<Outcome, Failure> {
        let executing = async {
            match self.client().execute_prepared(&execution.handle).await {
                Ok(outcome) => Ok(Outcome::new(outcome)),
                Err(error) => Err(Failure::from(error)),
            }
        };
        match self.block_on(cancel, executing) {
            Ok(outcome) => Ok(outcome),
            Err(failure) => Err(failure.concerning(execution.handle.reference())),
        }
    }

    /// Waits for the next event of any subscription the session holds, continuing across a lost
    /// session as [`nervix_client_core::Client::next_subscription`] does.
    pub fn next_event(&self, cancel: Option<&Cancel>) -> Result<Event, Failure> {
        let waiting = async {
            match self.client().next_subscription().await {
                Ok(event) => Event::new(event),
                Err(error) => Err(Failure::from(error)),
            }
        };
        self.block_on(cancel, waiting)
    }

    /// Waits for the next event about the domain clocks the session follows.
    pub fn next_clock_event(&self, cancel: Option<&Cancel>) -> Result<ClockEvent, Failure> {
        let waiting = async {
            match self.client().next_domain_clock_event().await {
                Ok(event) => Ok(ClockEvent::new(event)),
                Err(report) => Err(Failure::from(report)),
            }
        };
        self.block_on(cancel, waiting)
    }

    /// Waits for the next server notice, continuing across a lost session as
    /// [`nervix_client_core::Client::next_server_event`] does: the notices of a session end with
    /// it, and an overflow reports the notices the client dropped before the ones after the gap.
    pub fn next_notice(&self, cancel: Option<&Cancel>) -> Result<Notice, Failure> {
        let waiting = async {
            match self.client().next_server_event().await {
                Ok(event) => Ok(Notice::new(event)),
                Err(report) => Err(Failure::from(report)),
            }
        };
        self.block_on(cancel, waiting)
    }

    /// The clock of `domain` as the session last received it, as
    /// [`nervix_client_core::Client::domain_clock`] reads it, or `None` when the session follows
    /// no clock of that domain. It reads state the client already holds, so it never blocks.
    pub fn domain_clock(&self, domain: &DomainName) -> Option<DomainClock> {
        let clock = self.client().domain_clock(domain)?;
        Some(DomainClock::new(clock))
    }

    /// Reads one bounded completion page from the current session context.
    pub fn suggest_page(
        &self,
        input: &str,
        cursor: usize,
        page_size: u16,
        continuation: Option<&str>,
        cancel: Option<&Cancel>,
    ) -> Result<AutocompleteOutcome, Failure> {
        let suggesting = async {
            self.client()
                .suggest(
                    input.to_string(),
                    cursor,
                    page_size,
                    continuation.map(str::to_string),
                )
                .await
                .map_err(Failure::from)
        };
        self.block_on(cancel, suggesting)
    }
}

impl SessionRuntime {
    /// Runs `work` on `runtime`, blocking the calling thread until it finishes or `cancel` ends
    /// it.
    fn run<T>(
        runtime: &Runtime,
        cancel: Option<&Cancel>,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        runtime.block_on(async {
            match cancel {
                Some(cancel) => cancel.bound(work).await,
                None => work.await,
            }
        })
    }

    fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .assured("the runtime is taken only when the last holder drops the session")
    }

    pub(crate) fn client(&self) -> &Client {
        self.client
            .as_ref()
            .assured("the client is taken only when the last holder drops the session")
    }

    /// Runs `work` on the session's runtime, blocking the calling thread until it finishes or
    /// `cancel` ends it.
    pub(crate) fn block_on<T>(
        &self,
        cancel: Option<&Cancel>,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        Self::run(self.runtime(), cancel, work)
    }

    /// Drops `value` inside the session's runtime, so a handle whose drop releases its server
    /// attachment can start that release without blocking the calling thread.
    pub(crate) fn release<T>(&self, value: T) {
        let entered = self.runtime().enter();
        drop(value);
        drop(entered);
    }
}

impl Drop for SessionRuntime {
    /// Ends the session without blocking the calling thread, which may be a host's finalizer.
    fn drop(&mut self) {
        let (Some(runtime), Some(client)) = (self.runtime.take(), self.client.take()) else {
            return;
        };
        let entered = runtime.enter();
        drop(client);
        drop(entered);
        runtime.shutdown_background();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FailureKind;

    /// Every host's byte bound reaches the options unchanged when a `u32` holds it, and is refused
    /// under the host's own argument name otherwise; the option's own bounds are left to connect.
    #[test]
    fn a_byte_bound_is_kept_or_refused_under_the_hosts_argument() {
        for bytes in [1_i64, 65_534, i64::from(u32::MAX)] {
            let bound = Session::byte_bound("stream_window_bytes", bytes)
                .unwrap_or_else(|failure| panic!("{bytes} fits a bound: {}", failure.message()));
            assert_eq!(i64::from(bound.get()), bytes);
        }
        for bytes in [0_i64, -1, i64::from(u32::MAX) + 1] {
            let Err(failure) = Session::byte_bound("header_list_bytes", bytes) else {
                panic!("{bytes} is refused as a byte bound");
            };
            assert_eq!(failure.kind(), FailureKind::InvalidArgument);
            assert!(
                failure.message().contains("header_list_bytes"),
                "the refusal names the host's argument: {}",
                failure.message()
            );
        }
        let Err(failure) = Session::byte_bound("bytes", u64::MAX) else {
            panic!("a C host's largest value is refused as a byte bound");
        };
        assert_eq!(failure.kind(), FailureKind::InvalidArgument);
    }

    /// A stream count and a queue capacity reach the options unchanged when their type holds them,
    /// and are refused under the host's own argument name otherwise.
    #[test]
    fn stream_counts_and_queue_capacities_are_kept_or_refused_under_the_hosts_argument() {
        for streams in [1_i64, 50, i64::from(u32::MAX)] {
            let count = Session::stream_count("remembered_reset_streams", streams)
                .unwrap_or_else(|failure| panic!("{streams} fits a count: {}", failure.message()));
            assert_eq!(i64::from(count.get()), streams);
        }
        for streams in [0_i64, -1, i64::from(u32::MAX) + 1] {
            let Err(failure) = Session::stream_count("max_error_reset_streams", streams) else {
                panic!("{streams} is refused as a stream count");
            };
            assert_eq!(failure.kind(), FailureKind::InvalidArgument);
            assert!(
                failure.message().contains("max_error_reset_streams"),
                "the refusal names the host's argument: {}",
                failure.message()
            );
        }
        let queue = Session::queue_capacity("call_queue_capacity", 1_024_i64)
            .unwrap_or_else(|failure| panic!("1,024 fits a queue: {}", failure.message()));
        assert_eq!(queue.get(), 1_024);
        for capacity in [0_i64, -1] {
            let Err(failure) = Session::queue_capacity("call_queue_capacity", capacity) else {
                panic!("{capacity} is refused as a queue capacity");
            };
            assert_eq!(failure.kind(), FailureKind::InvalidArgument);
            assert!(
                failure.message().contains("call_queue_capacity"),
                "the refusal names the host's argument: {}",
                failure.message()
            );
        }
    }
}
