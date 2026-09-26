//! A session a host holds: `nx_session` and the commands it prepares, `nx_execution`.
//!
//! - **Owns.** The Tokio runtime a session runs on, opening and ending the session, running a
//!   blocking call on the host's thread, and preparing, executing and waiting for events.
//! - **Depends on.** The Rust session client, cancellation, and the outcome and event handles.
//! - **Must not know.** Which host is calling, or how it schedules its threads.
//!
//! A host thread blocks in [`Session::block_on`] while the runtime's own threads drive the
//! exchange, so no host code ever runs on a runtime thread and no runtime thread ever waits for a
//! host.

use std::future::Future;

use nervix_client_core::{
    AutocompleteOutcome, Client, ConnectOptions, DomainName, ExecutionHandle,
};
use tokio::runtime::Runtime;

use crate::{
    abi,
    cancel::Cancel,
    event::Event,
    failure::{Failure, FailureKind},
    outcome::Outcome,
};

/// The threads the runtime of one session drives its exchange with. The exchange is one stream
/// and its reader, so a second thread only keeps a busy decoder from delaying a timer.
const RUNTIME_THREADS: usize = 2;

/// The credentials a session presents on every call.
#[derive(Debug, Clone, Copy)]
pub struct Credentials<'a> {
    pub username: &'a str,
    pub password: &'a str,
}

/// An open session and the runtime it runs on.
pub struct Session {
    runtime: Runtime,
    client: Client,
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
    /// Opens a session on `server`, blocking the calling thread until it is open.
    pub fn connect(
        server: &str,
        domain: Option<&str>,
        credentials: Option<Credentials<'_>>,
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
        let mut options = ConnectOptions::default();
        if let Some(credentials) = credentials {
            options = options.with_basic_auth(credentials.username, credentials.password);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
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
        let client = Self::run(&runtime, cancel, connecting)?;
        Ok(Self { runtime, client })
    }

    /// Runs `work` on the session's runtime, blocking the calling thread until it finishes or
    /// `cancel` ends it.
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

    fn block_on<T>(
        &self,
        cancel: Option<&Cancel>,
        work: impl Future<Output = Result<T, Failure>>,
    ) -> Result<T, Failure> {
        Self::run(&self.runtime, cancel, work)
    }

    /// Captures `query` and a fresh execution identity before anything is sent.
    pub fn prepare(&self, query: &str, cancel: Option<&Cancel>) -> Result<Execution, Failure> {
        let preparing = async {
            let handle = self.client.prepare_execution(query).await;
            Ok(Execution { handle })
        };
        self.block_on(cancel, preparing)
    }

    /// Runs a prepared command. A failure that leaves the command's outcome open names its
    /// execution reference, and running the same execution again recovers that outcome.
    pub fn execute(
        &self,
        execution: &Execution,
        cancel: Option<&Cancel>,
    ) -> Result<Outcome, Failure> {
        let executing = async {
            match self.client.execute_prepared(&execution.handle).await {
                Ok(outcome) => Ok(Outcome::new(outcome)),
                Err(error) => Err(Failure::from(error)),
            }
        };
        match self.block_on(cancel, executing) {
            Ok(outcome) => Ok(outcome),
            Err(failure) => Err(failure.concerning(execution.handle.reference())),
        }
    }

    /// Waits for the next event of any subscription the session holds.
    pub fn next_event(&self, cancel: Option<&Cancel>) -> Result<Event, Failure> {
        let waiting = async {
            match self.client.next_subscription().await {
                Ok(event) => Event::new(event),
                Err(error) => Err(Failure::from(error)),
            }
        };
        self.block_on(cancel, waiting)
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
            self.client
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

    /// Ends the session without blocking the calling thread, which may be a host's finalizer.
    fn close(self) {
        let Self { runtime, client } = self;
        let entered = runtime.enter();
        drop(client);
        drop(entered);
        runtime.shutdown_background();
    }
}

/// # Safety
///
/// Every non-null text argument addresses its length in readable bytes, a non-null `cancel` is a
/// live token, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_connect(
    server: *const u8,
    server_len: usize,
    domain: *const u8,
    domain_len: usize,
    username: *const u8,
    username_len: usize,
    password: *const u8,
    password_len: usize,
    cancel: *const Cancel,
    out: *mut *mut Session,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_connected(
            server,
            server_len,
            domain,
            domain_len,
            username,
            username_len,
            password,
            password_len,
            cancel,
            out,
        )
    })
}

/// # Safety
///
/// As [`nx_session_connect`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each string as a pointer and a length"
)]
unsafe fn write_connected(
    server: *const u8,
    server_len: usize,
    domain: *const u8,
    domain_len: usize,
    username: *const u8,
    username_len: usize,
    password: *const u8,
    password_len: usize,
    cancel: *const Cancel,
    out: *mut *mut Session,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees every non-null argument is valid for its length.
    let (server, domain, username, password) = unsafe {
        (
            abi::text(server, server_len, "server")?,
            abi::optional_text(domain, domain_len, "domain")?,
            abi::optional_text(username, username_len, "username")?,
            abi::optional_text(password, password_len, "password")?,
        )
    };
    let credentials = match (username, password) {
        (Some(username), Some(password)) => Some(Credentials { username, password }),
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            return Err(Failure::invalid_argument(
                "username",
                "and `password` must both be present or both be absent",
            ));
        }
    };
    // SAFETY: the caller guarantees a live token or null.
    let cancel = unsafe { cancel.as_ref() };
    let session = Session::connect(server, domain, credentials, cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(session)) };
    Ok(())
}

/// # Safety
///
/// A non-null `session` is a session this library returned that has not been freed, and no other
/// thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_free(session: *mut Session) {
    if session.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased session no other thread uses.
    let session = unsafe { Box::from_raw(session) };
    session.close();
}

/// # Safety
///
/// `session` is a live session, `query` addresses `query_len` readable bytes, a non-null
/// `cancel` is a live token, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_prepare(
    session: *const Session,
    query: *const u8,
    query_len: usize,
    cancel: *const Cancel,
    out: *mut *mut Execution,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_prepared(session, query, query_len, cancel, out) })
}

/// # Safety
///
/// As [`nx_session_prepare`].
unsafe fn write_prepared(
    session: *const Session,
    query: *const u8,
    query_len: usize,
    cancel: *const Cancel,
    out: *mut *mut Execution,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session, a readable query and a live token or null.
    let (session, query, cancel) = unsafe {
        (
            abi::handle(session, "session")?,
            abi::text(query, query_len, "query")?,
            cancel.as_ref(),
        )
    };
    let execution = session.prepare(query, cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(execution)) };
    Ok(())
}

/// # Safety
///
/// `execution` is a live execution this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_execution_reference(
    execution: *const Execution,
    reference: *mut *const u8,
    reference_len: *mut usize,
) {
    // SAFETY: the header requires a live execution and writable out-parameters.
    unsafe {
        let execution = abi::accessor(execution);
        abi::write_bytes(reference, reference_len, execution.reference().as_bytes());
    }
}

/// # Safety
///
/// A non-null `execution` is an execution this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_execution_free(execution: *mut Execution) {
    // SAFETY: the header requires an unreleased execution or null.
    unsafe { abi::release(execution) };
}

/// # Safety
///
/// `session` and `execution` are live, a non-null `cancel` is a live token, and a non-null `out`
/// is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_execute(
    session: *const Session,
    execution: *const Execution,
    cancel: *const Cancel,
    out: *mut *mut Outcome,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_executed(session, execution, cancel, out) })
}

/// # Safety
///
/// As [`nx_session_execute`].
unsafe fn write_executed(
    session: *const Session,
    execution: *const Execution,
    cancel: *const Cancel,
    out: *mut *mut Outcome,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session and execution, and a live token or null.
    let (session, execution, cancel) = unsafe {
        (
            abi::handle(session, "session")?,
            abi::handle(execution, "execution")?,
            cancel.as_ref(),
        )
    };
    let outcome = session.execute(execution, cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(outcome)) };
    Ok(())
}

/// # Safety
///
/// `session` is live, a non-null `cancel` is a live token, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_next_event(
    session: *const Session,
    cancel: *const Cancel,
    out: *mut *mut Event,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_next_event(session, cancel, out) })
}

/// # Safety
///
/// As [`nx_session_next_event`].
unsafe fn write_next_event(
    session: *const Session,
    cancel: *const Cancel,
    out: *mut *mut Event,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session and a live token or null.
    let (session, cancel) = unsafe { (abi::handle(session, "session")?, cancel.as_ref()) };
    let event = session.next_event(cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, event.into_shared()) };
    Ok(())
}
