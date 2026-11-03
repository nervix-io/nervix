//! `nx_session`: a session a host holds, and the commands it prepares, `nx_execution`.
//!
//! - **Owns.** Opening a session through the C ABI with the text a host passes, preparing and
//!   executing commands, waiting for subscription and domain clock events, reading the clock of a
//!   domain the session follows, and freeing the session and its executions.
//! - **Depends on.** The shared session, the cancellation, outcome, event, clock event and domain
//!   clock handles, and the C ABI boundary.
//! - **Must not know.** The runtime a session runs on; the shared session owns it.

use std::ptr;

use nervix_client_core::DomainName;
use nervix_client_host::{
    Cancel, ClockEvent, Credentials, DomainClock, Event, Execution, Failure, Outcome, Session,
};

use crate::abi;

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
    // SAFETY: the header requires an unreleased session no other thread uses. The session ends
    // once nothing opened on it holds it either.
    unsafe { abi::release(session) };
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
    unsafe { abi::write(out, abi::into_shared(event)) };
    Ok(())
}

/// # Safety
///
/// `session` is live, a non-null `cancel` is a live token, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_next_clock_event(
    session: *const Session,
    cancel: *const Cancel,
    out: *mut *mut ClockEvent,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_next_clock_event(session, cancel, out) })
}

/// # Safety
///
/// As [`nx_session_next_clock_event`].
unsafe fn write_next_clock_event(
    session: *const Session,
    cancel: *const Cancel,
    out: *mut *mut ClockEvent,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session and a live token or null.
    let (session, cancel) = unsafe { (abi::handle(session, "session")?, cancel.as_ref()) };
    let event = session.next_clock_event(cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_shared(event)) };
    Ok(())
}

/// # Safety
///
/// `session` is live, `domain` addresses `domain_len` readable bytes, and a non-null `out` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_domain_clock(
    session: *const Session,
    domain: *const u8,
    domain_len: usize,
    out: *mut *mut DomainClock,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_domain_clock(session, domain, domain_len, out) })
}

/// # Safety
///
/// As [`nx_session_domain_clock`].
unsafe fn write_domain_clock(
    session: *const Session,
    domain: *const u8,
    domain_len: usize,
    out: *mut *mut DomainClock,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session and a readable domain name.
    let (session, domain) = unsafe {
        (
            abi::handle(session, "session")?,
            abi::text(domain, domain_len, "domain")?,
        )
    };
    let domain = match DomainName::try_from(domain) {
        Ok(domain) => domain,
        Err(error) => return Err(Failure::invalid_argument("domain", &error.to_string())),
    };
    // A domain whose clock the session does not follow reads as no clock, which the header
    // writes as NULL.
    let clock = match session.domain_clock(&domain) {
        Some(clock) => abi::into_shared(clock),
        None => ptr::null_mut(),
    };
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, clock) };
    Ok(())
}
